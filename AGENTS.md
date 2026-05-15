# AI Agent Instructions (AGENTS.md / .cursorrules)

You are an expert Data Engineer and DevOps Architect. Whenever you generate, refactor, or review code in this repository, strictly adhere to the following architectural guidelines and naming conventions.

---

## 1. Domain-Driven Naming Conventions (STRICT)

Do not invent names. Follow this exact taxonomy:

| Concept | Name | Notes |
|---|---|---|
| Domain / Python package | `egisz` / `egisz_elt` | Package lives in `src/egisz_elt/` |
| Main DAG file | `egisz_elt_dag.py` | In `airflow/dags/` |
| Source database (Firebird 5) | `proxy_egisz` | Airflow conn ID: `proxy_egisz_fb` |
| Target DWH (PostgreSQL) | `dwh_egisz` | Airflow conn ID: `dwh_egisz_pg` |
| Raw EXCHANGELOG table | `exchangelog_raw` | PK: `logid bigint` |
| Raw EGISZ_MESSAGES table | `egisz_messages_raw` | PK: `egmid bigint` |
| Dimension: organizations | `dim_organizations` | Source: Firebird `JPERSONS` |
| Dimension: licenses | `dim_licenses` | Source: Firebird `EGISZ_LICENSES` |
| Dimension: SEMD types | `dim_semd_types` | Static reference table |
| Fact table | `fact_egisz_transactions` | Populated by `egisz_transform_raw_to_facts()` |
| Materialized views | `mv_egisz_*` | Auto-refreshed after each transform |
| Watermark table | `elt_state` | Tracks `last_log_id` and `last_egmid` per pipeline |

**NEVER** use the legacy term `proxy_reports` in any variable, class, file, or SQL object name.

---

## 2. Airflow-Native Architecture (TaskFlow API)

Airflow version: **2.11.2**. Use it as a first-class orchestrator, not a cron wrapper.

### Decorators

Always use `@dag` and `@task`. Never use legacy `PythonOperator`, `BashOperator`, or similar for Python logic.

### Task pipeline (in order)

```
sync_dimensions >> extract_from_proxy >> load_to_dwh >> transform_data >> refresh_materialized_views >> update_watermark
```

| Task | Responsibility |
|---|---|
| `sync_dimensions` | Full reload of `dim_organizations` (from `JPERSONS`) and `dim_licenses` (from `EGISZ_LICENSES`) via UPSERT |
| `extract_from_proxy` | Read current watermarks from `elt_state`; fetch `EXCHANGELOG` by `LOGID` and `EGISZ_MESSAGES` by `EGMID`; resolve cross-referenced messages from XML payload; return XCom dict |
| `load_to_dwh` | UPSERT fetched rows into `exchangelog_raw` and `egisz_messages_raw`; pass XCom dict downstream |
| `transform_data` | Call `public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)`. The function does not refresh materialized views; refresh is a separate task. |
| `refresh_materialized_views` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` for `v_egisz_transactions_enriched_ui` and `v_stg_channel_errors_by_document` |
| `update_watermark` | UPSERT `elt_state` using `GREATEST(current, new)` for both `last_log_id` and `last_egmid` |

### Batch size and schedule

```python
BATCH_SIZE = 5000   # rows per Firebird fetch
schedule  = "*/5 * * * *"
max_active_runs = 1
```

### XCom payload shape (between tasks)

```python
{
    "count": int,           # EXCHANGELOG rows fetched
    "message_count": int,   # EGISZ_MESSAGES rows fetched (including related)
    "last_log_id": int,     # watermark before this run
    "last_egmid": int,      # watermark before this run
    "max_id": int,          # max LOGID in this batch
    "max_egmid": int,       # max EGMID in this batch (cursor-based, not including related)
    "rows": list[dict],     # serialized EXCHANGELOG rows
    "message_rows": list[dict],  # serialized EGISZ_MESSAGES rows
}
```

All tasks must be idempotent. PostgreSQL writes use `INSERT ... ON CONFLICT DO UPDATE`.

---

## 3. Secrets & Connections Management

- **NEVER** read credentials from `.env` files or `os.getenv()` inside DAGs or ELT modules.
- Always use Airflow connection management:
  - Firebird source: `BaseHook.get_connection('proxy_egisz_fb')`
  - PostgreSQL DWH: `BaseHook.get_connection('dwh_egisz_pg')`
- Service accounts:
  - `egisz` — DWH owner; used by Airflow ELT for writes and transforms
  - `postgres` — read-only/BI access for Metabase

---

## 4. DWH Model and SQL

### Schema initialization

All DDL lives in **`db/dwh_init.sql`**. Run it as:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

The script is idempotent (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). When adding new tables or columns, extend `db/dwh_init.sql` in the same style.

### Transform function

Data transformation happens **during DAG execution**, inside the `transform_data` Airflow task. The task calls a PostgreSQL stored function that does all the real work — parsing SOAP/XML payloads, enriching rows, writing to `fact_egisz_transactions`, and refreshing `mv_egisz_*` materialized views:

```sql
SELECT public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)
```

The Python task only invokes this function; **all transformation logic lives in SQL, not Python**. Do not implement row-level transformation in Python (e.g., iterating rows and mutating them before writing). `normalize.py` is a compatibility stub — it contains no active logic.

Transformation is **not** deferred to dashboard query time. By the time Metabase reads the data, `fact_egisz_transactions` and materialized views are already populated.

### Firebird-specific serialization

- BLOB/text columns are read via `_serialize_firebird_text()` in `fb_client.py` → returns `str | None`.
- Date/datetime fields are converted with `.isoformat()` before XCom serialization.
- EGISZ UUIDs may be wrapped as `<urn:uuid:...>` — normalize with `normalize_message_id()` in `pg_client.py`.
- Keyset pagination on Firebird uses `WHERE col > ? ORDER BY col ROWS N` syntax (not `LIMIT/OFFSET`).

### `dim_semd_types`

Contains SEMD document type reference data seeded directly in `db/dwh_init.sql`. Do not move this data to a separate file or external source.

---

## 5. BI & Metabase Integration

- Metabase version: **v0.60.2.5**, deployed in Kubernetes (`k8s/metabase/`).
- Dashboard JSON files are in `metabase_dashboards/` (6 dashboards: operational, service, documents, quality, executive, SEMD archive).
- Dashboards are imported at container startup via `metabase/setup-dashboards.sh`.
- Field filter defaults are in `metabase_dashboards/field_filter_defaults.yaml`; they are applied by `scripts/apply_metabase_field_filters.py`.
- Metabase uses its own PostgreSQL database (`metabase_app`). Do not mix it with `dwh_egisz` or `airflow_db`.
- When altering the DWH schema, verify compatibility with:
  1. Field filters in `scripts/apply_metabase_field_filters.py`
  2. Dashboard JSON definitions in `metabase_dashboards/`

---

## 6. Kubernetes Deployment

Both Airflow and Metabase run in Kubernetes (Docker Desktop by default).

| Component | Manifests | Image |
|---|---|---|
| Airflow | `k8s/airflow/values.yaml` (Helm), `k8s/airflow/airflow-connections-secret.yaml` | `egisz-airflow-worker:latest` |
| Metabase | `k8s/metabase/metabase.yaml`, `k8s/metabase/metabase-connections-secret.yaml` | `egisz-metabase:latest` |

Deploy everything with:

```powershell
.\up.ps1               # build images, install/upgrade both components
.\up.ps1 -Component Airflow   # Airflow only
.\up.ps1 -Component Metabase  # Metabase only
```

- `k8s/metabase/metabase-connections-secret.yaml` is not committed; generate from the `.example.yaml` file.
- Airflow internal metadata DB: `airflow_db` (separate from `dwh_egisz`).
- Ports: Airflow → `localhost:8080`, Metabase → `localhost:3000`.

---

## 7. Code Style

- Python ≥ 3.11; dependencies: `firebird-driver>=1.10.0,<2.0.0`, `psycopg2-binary>=2.9.9`.
- Fully typed signatures (`from typing import Any`, etc.).
- Comments only when the **why** is non-obvious (hidden constraint, workaround, subtle invariant). No task/ticket references in code.
- No monolithic scripts — logic is decomposed into focused functions in `fb_client.py`, `pg_client.py`.

---

## 8. Anti-Patterns (What NOT to do)

- Do NOT generate monolithic Python scripts for a single Airflow task.
- Do NOT use `os.getenv('DB_PASSWORD')` or `.env` files inside DAGs or ELT modules.
- Do NOT use the term `proxy_reports` in any identifier.
- Do NOT write data to the `postgres` system database — all DWH writes target `dwh_egisz`.
- Do NOT add Python-side row transformation logic; transformation belongs in the PostgreSQL function `egisz_transform_raw_to_facts`.
- Do NOT use `LIMIT/OFFSET` for Firebird pagination — use `WHERE col > ? ROWS N` keyset pagination.
- Do NOT use legacy Airflow operators (`PythonOperator`, `BashOperator`) for Python logic.
