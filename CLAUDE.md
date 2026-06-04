# CLAUDE.md — project contract for Claude Code

This file is your operating contract in this repository.

Domain context (what ЕГИСЗ/СЭМД are, how parsing works, what each dashboard shows) lives in @README.md. Do not restate the README in code or comments; link to the relevant section instead.

---

## TL;DR (read this first)

**What this project is:** an ELT service for operational analytics of СЭМД exchange with ЕГИСЗ (Russia's federal healthcare records registry). Every 5 minutes it pulls the gateway's Firebird journal, loads the raw staging layer into PostgreSQL, runs a SQL transformation into persistent DWH fact tables, refreshes materialized views, and advances the watermark. On top of the DWH — 8 Metabase dashboards.

**Stack:** Apache Airflow 2.11.2 (TaskFlow API), Python ≥ 3.11, PostgreSQL 16 (DWH `dwh_egisz`), Firebird 5 (source `proxy_egisz`), Metabase v0.60.2.5. Deployment — Kubernetes (Helm chart for Airflow + plain manifest for Metabase); locally — Docker Desktop via `up.ps1`.

**6 hard rules** (full list — §1, §8):

1. **Names are fixed.** Domain — `egisz` / Python package — `egisz_elt` / DWH — `dwh_egisz` / source — `proxy_egisz` / central fact table — `fact_egisz_documents`.
2. **All transformation business logic lives in PL/pgSQL.** SOAP/XML parsing, status normalization, error classification, and building `fact_egisz_documents` live inside [db/parts/50_transform.sql](db/parts/50_transform.sql) plus helpers from `20_functions_parsing.sql` / `40_functions_errors.sql`. Python only does Airflow orchestration and raw/staging loading.
3. **DWH schema lives only in `db/dwh_init.sql` + `db/parts/*.sql`.** No migration files (`migrations/`, alembic, etc.). Every module is idempotent (`CREATE ... IF NOT EXISTS`, `CREATE OR REPLACE`, `INSERT ... ON CONFLICT`).
4. **Watermark only moves via `GREATEST(current, new)`.** Never roll it back — doing so breaks idempotency of the entire pipeline.
5. **Secrets via `BaseHook.get_connection(...)`.** No `os.getenv('DB_PASSWORD')` or `.env` inside DAG / ELT modules. Connection IDs are fixed: `proxy_egisz_fb`, `dwh_egisz_pg`.
6. **Reporting never reads staging.** `exchangelog_raw` is a disposable input layer. Views and dashboards must use `fact_egisz_documents`, `fact_egisz_channel_errors`, dimensions, and document-grain MVs; `fact_egisz_transactions` is internal callback lineage only.

**Code entry points:**
- DAG: [airflow/dags/egisz_elt_dag.py](airflow/dags/egisz_elt_dag.py)
- Firebird source: [src/egisz_elt/fb_client.py](src/egisz_elt/fb_client.py)
- Load + transform invoker: [src/egisz_elt/pg_client.py](src/egisz_elt/pg_client.py)
- DWH schema: [db/dwh_init.sql](db/dwh_init.sql) + [db/parts/](db/parts/)
- Dashboards: [metabase_dashboards/](metabase_dashboards/)
- Tests: [tests/](tests/)

---

## 1. Domain-Driven Naming Conventions (STRICT)

Do not invent names. Use exactly this taxonomy:

| Concept | Name | Notes |
|---|---|---|
| Domain / Python package | `egisz` / `egisz_elt` | Package lives in `src/egisz_elt/` |
| Main DAG file | `egisz_elt_dag.py` | In `airflow/dags/`. DAG ID — `egisz_elt_dag`, pipeline key in `elt_state` — `egisz` |
| Source database (Firebird 5) | `proxy_egisz` | Airflow conn ID: `proxy_egisz_fb` |
| Target DWH (PostgreSQL) | `dwh_egisz` | Airflow conn ID: `dwh_egisz_pg` |
| Raw EXCHANGELOG table | `exchangelog_raw` | PK: `logid bigint` |
| Dimension: organizations | `dim_organizations` | Source: Firebird `JPERSONS`, PK `jid` |
| Dimension: licenses | `dim_licenses` | Source: Firebird `EGISZ_LICENSES`, PK `id` |
| Dimension: СЭМД types | `dim_semd_types` | Static reference table, PK `code` |
| Retired compatibility names | `fact_egisz_messages`, `v_egisz_transactions_enriched_ui`, `stg_egisz_messages` | Deprecated; do not recreate |
| Central fact table: EMD documents | `fact_egisz_documents` | One row per СЭМД/document, keyed by `document_key`; contains `jid`, document identifiers, status, dates, error fields, patient/doctor hashes, and callback linkage |
| Internal lineage table | `fact_egisz_transactions` | Callback/event lineage populated by `egisz_transform_raw_to_facts()`, PK `exchangelog_log_id`; not a reporting base |
| Fact table: channel errors | `fact_egisz_channel_errors` | Persistent parsed transport/channel errors; source for `v_stg_channel_errors_by_document` |
| Materialized views | `v_*` (MV variants — `v_egisz_documents_enriched_ui`, `v_egisz_documents_daily_ui`, `v_stg_channel_errors_by_document`) | Refreshed by a dedicated Airflow task |
| Reporting views | `v_rpt_*_ui` | Plain views on top of MVs |
| Healthcheck views | `v_health_*_ui` | Plain views for dashboard `02_service.json` |
| Watermark table | `elt_state` | Tracks `last_logid` per `pipeline` |

---

## 2. Airflow-Native Architecture (TaskFlow API)

Airflow version: **2.11.2**. Use it as a first-class orchestrator, not a cron wrapper.

### Decorators

Only `@dag` and `@task`. No legacy operators (`PythonOperator`, `BashOperator`) for Python logic.

### Task pipeline (order matters)

```
sync_dimensions >> extract_and_load_batch >> analyze_staging >> transform_data >> refresh_materialized_views >> update_watermark >> reconcile_late_arrivals
```

| Task | Responsibility |
|---|---|
| `sync_dimensions` | Full reload of `dim_organizations` (from `JPERSONS`) and `dim_licenses` (from `EGISZ_LICENSES`) via `sync_directory()` (UPSERT by PK with pagination `DIRECTORY_SYNC_PAGE_SIZE=1000`) |
| `extract_and_load_batch` | Read watermark from `elt_state` (`last_logid`), pull the EXCHANGELOG batch by `LOGID > last_logid` (no date floor), and UPSERT it into `exchangelog_raw` **in-process** (extract + load fused). Returns XCom `{count, last_logid, cursor_logid}` |
| `analyze_staging` | `ANALYZE` for `exchangelog_raw`. Runs in autocommit. **Mandatory task**: without fresh stats after bulk load, PostgreSQL may miss the functional indexes used during parsing and matching. |
| `transform_data` | Calls `public.egisz_transform_raw_to_facts(from_logid, to_logid)`. The function parses raw/staging payloads into document-grain `fact_egisz_documents` first, then writes `fact_egisz_channel_errors` and internal callback lineage in `fact_egisz_transactions`. The function does **not** refresh MVs — that's a separate task |
| `refresh_materialized_views` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` for `v_egisz_documents_daily_ui` (the enriched mart `v_egisz_documents_enriched_ui` is a persistent table maintained incrementally by the transform, not refreshed here) |
| `update_watermark` | UPSERT into `elt_state` via `GREATEST(current, new)` for `last_logid` only — guards against accidental cursor rollback during manual re-runs of older batches |
| `reconcile_late_arrivals` | **Self-healing safety net.** The proxy journal materializes rows **out of LOGID order** — async СЭМД callbacks and gateway backfills land below an already-advanced watermark, so the forward `LOGID > last_logid` cursor loses them permanently. Each cycle this task set-diffs the proxy's **LOGID band just below the watermark** — `(last_logid - RECONCILE_WATERMARK_LOOKBACK_LOGIDS, last_logid]`, lookback set in the DAG config block — against `exchangelog_raw` (`fetch_exchangelog_logids_in_band` ↔ `get_raw_logids_in_band`, both banded so neither scans the whole journal/staging) and load+transforms whatever is missing — **without moving the watermark** (GREATEST stays the only writer of `last_logid`). The band is **LOGID-bounded, not date-bounded**: cheap and bounded regardless of journal size; the band must be wide enough to cover the out-of-order span (incident 2026-06-01 was ~68k below the watermark). Linkage of a recovered callback to its parent document still happens by `relatesToMessage` inside the transform. Recovered LOGIDs are transformed per **dense window** (`transform_missing_windows` / `coalesce_logid_windows`, `RECONCILE_WINDOW_MAX_GAP`), not one `min..max` span. No-op in steady state. **Do not remove** — see §8. |

### Batch size and schedule

```python
BATCH_SIZE = 5000   # rows per Firebird fetch
schedule  = "*/5 * * * *"
max_active_runs = 1   # parallel runs forbidden (race on watermark)
```

### XCom payload shape (between tasks)

```python
{
    "count": int,           # EXCHANGELOG rows read in this batch
    "last_logid": int,      # LOGID cursor at the start of this run
    "cursor_logid": int,    # next LOGID cursor, max LOGID actually read in this batch
}
# transform_data adds "transformed": int. reconcile_late_arrivals takes no XCom
# input and returns an int (rows reconciled) — it reads the watermark from elt_state
# and set-diffs the LOGID band just below it against exchangelog_raw.
```

This contract is fixed — adding or renaming keys requires a synchronized edit in `airflow/dags/egisz_elt_dag.py` and this file.

All tasks are **idempotent**. PostgreSQL writes use `INSERT ... ON CONFLICT DO UPDATE`. Re-running the same batch creates no duplicates; a late callback may overwrite an older record.

---

## 3. Secrets & Connections Management

- **NEVER** read credentials from `.env` files or `os.getenv()` inside DAG / ELT modules.
- Always use Airflow connection management:
  - Firebird source: `BaseHook.get_connection('proxy_egisz_fb')`
  - PostgreSQL DWH: `BaseHook.get_connection('dwh_egisz_pg')`
- Connection secrets live in k8s manifests:
  - `k8s/airflow/airflow-connections-secret.yaml` (not committed)
  - `k8s/metabase/metabase-connections-secret.yaml` (not committed; example — `metabase-connections-secret.example.yaml`)
- DWH roles:
  - `egisz` — owner; used by Airflow ELT for writes and transformations
  - `postgres` — used by Metabase as a read-only BI account (the name is a historical artifact; privileges are restricted to SELECT)

---

## 4. DWH Model and SQL

### Schema initialization

All DDL lives in **[db/dwh_init.sql](db/dwh_init.sql)** (a thin assembler via `\i`) plus **11 ordered modules** in **[db/parts/](db/parts/)**. Run:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Prerequisites on a clean install:

```sql
CREATE ROLE egisz LOGIN PASSWORD 'egisz';
CREATE DATABASE dwh_egisz OWNER postgres;
```

Module order (matters — views depend on functions, functions on tables):

```
db/parts/00_bootstrap.sql                  — role egisz + GRANTs on schema public
db/parts/10_tables.sql                     — tables, indexes (incl. functional), dim_semd_types seed
db/parts/20_functions_parsing.sql          — egisz_xml_text, egisz_normalize_message_id, egisz_clean_host, egisz_clean_text_value, egisz_extract_jid_from_endpoint, egisz_normalize_semd_code, safe_cast_timestamptz
db/parts/30_error_rules.sql                — egisz_error_interpretation_rules + rule seed
db/parts/40_functions_errors.sql           — error classify / interpretation / build_errors_json / semd_type_report_label
db/parts/50_transform.sql                  — egisz_transform_raw_to_facts (the main function)
db/parts/60_drop_dependents.sql            — DROP dependent views and legacy columns before rebuild
db/parts/70_views_core.sql                 — v_egisz_documents_enriched_ui (MV) + v_rpt_error_interpretations_ui
db/parts/75_views_stg.sql                  — v_stg_channel_errors_by_document (MV) + v_stg_channel_network_errors_by_document (alias view)
db/parts/80_views_rpt.sql                  — v_rpt_*_ui (network_errors_detail, documents_no_response, semd_archive, clinic_connectivity_daily, connectivity_global_daily, error_category_breakdown, client_documents)
db/parts/90_views_health_and_finalize.sql  — v_health_*_ui + GRANT verification + REFRESH of all materialized views
```

Every module is individually idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, `ALTER TABLE ... IF EXISTS`, `INSERT ... ON CONFLICT`). When adding a new table, column, function, or view — edit the corresponding `db/parts/*.sql` in the same idempotent style.

**Do not create migration files.** There is no `migrations/` folder; `db/dwh_init.sql` + `db/parts/` is the single source of truth for the DWH schema. A fresh dev DWH is built by one run of `psql -f db/dwh_init.sql`.

### Transform function

Data transformation happens **during the DAG run**, in the `transform_data` task. The task calls a PL/pgSQL function that does all real work — SOAP/XML parsing, enrichment, writing to `fact_egisz_documents`, channel errors, and callback lineage:

```sql
SELECT public.egisz_transform_raw_to_facts(from_logid, to_logid)
```

The Python task only invokes the function; **all transformation business logic lives in SQL, not in Python**. MV refresh is a separate task `refresh_materialized_views`; do not duplicate it inside `egisz_transform_raw_to_facts()`.

Do not implement row-level transformation in Python (e.g. iterating rows and mutating them before write). If you feel tempted to add such logic — it belongs in PL/pgSQL.

Transformation is **not** deferred to query time. By the time Metabase reads data, `fact_egisz_documents` and the MVs are already populated.

`_raw` tables are staging only. Do not build reporting views, healthcheck views, dashboard SQL, or Metabase field filters on `exchangelog_raw`; production cleanup may truncate it after data has been parsed into the fact tables. Do not recreate retired staging tables (`egisz_messages_raw`, `stg_egisz_messages`).

### Firebird-specific serialization

- BLOB/text columns are read via `_serialize_firebird_text()` in `fb_client.py` → returns `str | None`.
- Date/datetime fields are serialized via `.isoformat()` before being placed in XCom.
- ЕГИСЗ identifiers may arrive as `<urn:uuid:...>` / `urn:uuid:...` / bare UUID — normalize via `egisz_normalize_message_id()` in SQL (functional indexes: `idx_fact_egisz_message_id_norm`, `idx_fact_egisz_relates_to_norm`).
- Keyset pagination on Firebird: `WHERE col > ? ORDER BY col ROWS N`. **Do not use `LIMIT/OFFSET` on the Firebird dialect.**

### `dim_semd_types`

The СЭМД type reference is seeded directly in `db/parts/10_tables.sql` (a large `INSERT ... ON CONFLICT`). Do not move this seed to a separate file, Python, or an external source.

---

## 5. BI & Metabase Integration

- Metabase version: **v0.60.2.5**, deployed to Kubernetes (`k8s/metabase/`).
- **8 dashboards**, ~100 native cards total. JSON definitions — in `metabase_dashboards/`:
  - `01_operational.json` — operational monitoring of the outgoing flow
  - `02_service.json` — ETL and channel healthcheck
  - `03_documents_no_response.json` — escalation queue (callback not received)
  - `04_quality_and_errors.json` — detailed failure analysis (69-category classification)
  - `05_executive.json` — management summary. Операционные + финансовые KPI на реальных данных DWH. Финансовая модель — фикс-подписка **10 000 ₽/JID/мес** (MRR = `COUNT(DISTINCT jid) × 10 000`). Раньше дашборд опирался на заглушечные таблицы service_audit (CAC/LTV/SLA/MTTR/...) с захардкоженными константами; те таблицы и ~20 `v_rpt_service_audit_*` views удалены, метрики на придуманных данных сняты
  - `06_semd_archive.json` — locate a specific document by any identifier
  - `07_client_service.json` — per-client service dashboard with JID auth-stub filter, period, and document type
  - `08_client_bianalytic.json` — per-client BI-analytic dashboard (patient/doctor hash counts, no PII)
- Most cards run on top of `v_egisz_documents_enriched_ui` and `v_rpt_*_ui`.
- Dashboards are imported **automatically on container start** via `metabase/entrypoint.sh` → `metabase/provision.sh` (if `METABASE_AUTO_PROVISION=true`, default — true). Additionally, after `kubectl apply` `up.ps1` invokes `setup-dashboards.sh` explicitly (`kubectl exec deploy/metabase -- /bin/bash /app/setup-dashboards.sh`) — re-running is idempotent (it verifies a sha256 manifest).
- Field filters for dashboards are configured by `scripts/apply_metabase_field_filters.py` using declarative rules in `metabase_dashboards/field_filter_defaults.yaml` (format — **version: 2**). This is a resolver, not business logic.
- **Filter-propagation contract.** A dashboard parameter must reach every card whose source view physically has the column: add the `[[AND {{tag}}]]` clause **and** the `dimension` template-tag, then run `apply_metabase_field_filters.py` to fill `metabase-field-filters` (do not hand-write bindings the resolver can derive). A dashboard parameter maps to a card only when the card's template-tags contain a tag named `slug` minus the `_filter` suffix (see `metabase/setup-dashboards.sh`). Where the source view lacks the column (e.g. `v_rpt_error_category_breakdown_ui` has no `Статус`/`localUid`/`emdrId`/`LOGID`), do **not** wire the filter — the card keeps its own scope rather than silently ignoring it. "Now" snapshots (healthcheck, proxy-DB summary, 24h/72h windows) and trailing-30d KPIs (`MRR`/`ARR`) are intentionally period-independent and say so in the card name.
- **Status color palette (fixed).** The four canonical statuses are colored identically across every chart (`pie.rows` / `series_settings`): `Успешно зарегистрирован` → `#84BB4C`, `Ошибка асинхронного ответа РЭМД` → `#A989C5`, `Ошибка связи` → `#F2994A`, `В обработке` → `#509EE3`. The series/slice key must be the exact RU `Статус` label from `v_rpt_documents_ui`, or the color will not bind.
- **Trend period = filter.** Hourly trend cards bind their axis to the period filter, not a hardcoded `MAX(...) - INTERVAL` floor; give the bound date param a `past30days~` default so an empty filter does not scan full history (`parse_created_filter` on `02`/`04`).
- Metabase uses its **own** PostgreSQL database `metabase_app` (StatefulSet `metabase-postgres`). Do not mix it with `dwh_egisz` or `airflow_db`.
- When changing the DWH schema — verify compatibility with:
  1. Field-filter mapping in `metabase_dashboards/field_filter_defaults.yaml`
  2. Dashboard JSON in `metabase_dashboards/`
  3. Snapshot tests in `tests/test_dashboards.py`

---

## 6. Kubernetes Deployment

Airflow and Metabase both run on Kubernetes (Docker Desktop Kubernetes by default).

| Component | Manifests | Image |
|---|---|---|
| Airflow | `k8s/airflow/values.yaml` (Helm chart `apache-airflow/airflow`), `k8s/airflow/airflow-connections-secret.yaml`, `k8s/airflow/Dockerfile` | `egisz-airflow-worker:latest` |
| Metabase | `k8s/metabase/metabase.yaml`, `k8s/metabase/metabase-connections-secret.yaml`, `metabase/Dockerfile` | `egisz-metabase:latest` |

Stand management — `up.ps1` (PowerShell, `-Action` parameter):

```powershell
.\up.ps1                         # = -Action Start: full bring-up/update of Airflow + Metabase
.\up.ps1 -Action Airflow         # only Airflow
.\up.ps1 -Action Metabase        # only Metabase
.\up.ps1 -Action Stop            # full shutdown (scale to 0, PVCs preserved)
.\up.ps1 -Action Stop-Airflow    # stop only Airflow
.\up.ps1 -Action Stop-Metabase   # stop only Metabase
```

Stop actions do `scale --replicas=0`, not `helm uninstall` — PVCs and data are preserved.

- `k8s/metabase/metabase-connections-secret.yaml` is not committed; `up.ps1` generates it from `.example.yaml` on first run.
- Airflow's metadata DB: `airflow_db` (inside the Helm chart, separate from `dwh_egisz`).
- Ports: Airflow → `localhost:8080`, Metabase → `localhost:3000`.
- The DWH schema is deployed separately from `up.ps1`: `psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql`.

---

## 7. Code Style

- Python ≥ **3.11**; dependencies (`pyproject.toml`):
  - runtime: `firebird-driver>=1.10.0,<2.0.0`, `psycopg2-binary>=2.9.9`, `pyyaml>=6.0`
  - dev: `apache-airflow==2.11.2`, `pytest>=7.0`
- The package lives in `src/egisz_elt/` (setuptools src-layout); modules — `fb_client.py` (Firebird source), `pg_client.py` (DWH target + identifier normalization).
- Fully typed signatures (`from __future__ import annotations`, `from typing import Any`, etc.).
- Comments — only when the **why** is non-obvious (hidden invariant, bug workaround, incident reference). Do not write task/ticket references in code.
- No monolithic scripts — logic is decomposed into focused functions in `fb_client.py` / `pg_client.py`. The DAG is thin — orchestration only.
- Tests — `pytest`, located in `tests/` (`test_dashboards.py`, `test_fb_client.py`, `test_pg_client.py`). Run: `pytest` from the repo root.

---

## 8. ⛔ Anti-Patterns (DON'T)

These prohibitions are not stylistic preferences — they encode past incidents. Every entry has broken prod at some point.

| ⛔ DON'T | ✅ DO instead |
|---|---|
| Monolithic Python script for one Airflow task | Decompose into focused functions in `fb_client.py` / `pg_client.py`; keep the DAG thin (orchestration only) |
| `os.getenv('DB_PASSWORD')` / `.env` inside DAG / ELT modules | `BaseHook.get_connection('proxy_egisz_fb' \| 'dwh_egisz_pg')` |
| Use the word `legacy proxy report` in identifiers | Use only the fixed names from §1 (`egisz` / `egisz_elt` / `dwh_egisz`) |
| Write analytical data into the system DB `postgres` | All DWH writes go to `dwh_egisz` |
| Python-side row transformation (iterate and mutate before write) | All business logic in `egisz_transform_raw_to_facts` (PL/pgSQL); Python only invokes |
| `LIMIT/OFFSET` for Firebird pagination | Keyset pagination: `WHERE col > ? ORDER BY col ROWS N` |
| Legacy operators `PythonOperator` / `BashOperator` for Python logic | Decorators `@dag` / `@task` (TaskFlow API) |
| Create migration files (`migrations/`, alembic) | Change schema via idempotent edits in `db/parts/*.sql` |
| Refresh MVs inside `egisz_transform_raw_to_facts()` | That's the job of the separate `refresh_materialized_views` task |
| Parse SOAP/XML on the Python side in the DAG / `fb_client.py` | Parsing belongs in `db/parts/20_functions_parsing.sql` (`egisz_xml_text`, `egisz_normalize_message_id`, etc.) |
| Reintroduce compatibility aliases (`fact_egisz_messages`, `v_egisz_transactions_enriched_ui`, `stg_egisz_messages`) | Use canonical objects only: `fact_egisz_documents` and `v_egisz_documents_enriched_ui` |
| Roll the watermark back on manual reruns | `update_watermark` UPSERTs via `GREATEST(current, new)` — bypassing this breaks idempotency |
| Remove `analyze_staging` "as an optimization" | This task guards parsing/matching plans after bulk load (see §2) |
| Remove `reconcile_late_arrivals` or treat `LOGID > last_logid` as complete | The proxy journal materializes rows **out of LOGID order** (late СЭМД callbacks / gateway backfills land below the watermark). The forward cursor alone loses them permanently — `reconcile_late_arrivals` set-diffs the LOGID band just below the watermark against `exchangelog_raw` and recovers whatever is missing |
| Re-introduce a date floor (`source_min_created_at`), or detect late rows by scanning XML (`MSGTEXT CONTAINING relatesToMessage`) | The date floor is gone (forward extract is LOGID-cursor only). Reconcile detection is a **LOGID-band set-diff** (`(last_logid - RECONCILE_WATERMARK_LOOKBACK_LOGIDS, last_logid]`) — cheap and date-free. A per-document `relatesToMessage`/`CONTAINING` scan is O(open×journal) and was measured hanging >12 min on ~10k open chains — do not reintroduce it. Linkage of a recovered callback to its parent still happens by `relates_to_id` against `dim_egisz_exchangelog_refs` inside the transform |
| Recover stranded-below-watermark rows by rolling `last_logid` back | Let `reconcile_late_arrivals` load+transform the missing chain messages **without** moving the watermark; if recovering manually, load + `transform_missing_windows` (or per-window `egisz_transform_raw_to_facts(lo-1, hi)`), never lower `last_logid` |
| Build reporting or healthcheck views directly on `_raw` tables | Parse raw payloads into persistent DWH tables first; views read `fact_egisz_*`, dimensions, and MVs only |
| Add ticket/task references in code comments | Comments — only when the **why** is non-obvious (hidden invariant, bug workaround) |
| Записывать `'unknown'` в `error_type` для нераспознанных callback'ов | `error_type IS NULL` для `status` ∈ {`pending`, `unknown`}; видимость через сам `status` и человекочитаемый лейбл `"Статус (отчёт)"` в `v_egisz_documents_enriched_ui` |
| Создавать в DWH «теневые» таблицы без ELT-источника (commercial/financial mart с захардкоженными константами в view) | Считать управленческие метрики прямо в SQL-карточках 05_executive из `fact_egisz_documents` / `v_egisz_documents_enriched_ui` / `v_rpt_documents_no_response_ui`. Финансовая модель — фикс-тариф `10 000 ₽/JID/мес`, явно зашит в SQL карточек, не в view |

---

## 9. Developer Commands

All commands — from the repo root. PowerShell syntax (working environment — Windows + Docker Desktop).

### Tests

```powershell
pytest                                  # all pytest tests (tests/)
pytest tests/test_dashboards.py -v      # only dashboard snapshot tests
pytest tests/test_pg_client.py -k normalize  # filter by name
```

### DWH schema

Prerequisites on a clean install:

```sql
CREATE ROLE egisz LOGIN PASSWORD 'egisz';
CREATE DATABASE dwh_egisz OWNER postgres;
```

Deploy/update the DWH schema (idempotent):

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Refresh MVs manually (outside the DAG):

```powershell
psql -U postgres -d dwh_egisz -c "REFRESH MATERIALIZED VIEW CONCURRENTLY v_egisz_documents_enriched_ui;"
psql -U postgres -d dwh_egisz -c "REFRESH MATERIALIZED VIEW CONCURRENTLY v_egisz_documents_daily_ui;"
psql -U postgres -d dwh_egisz -c "REFRESH MATERIALIZED VIEW CONCURRENTLY v_stg_channel_errors_by_document;"
```

### Local stand

```powershell
.\up.ps1                         # full bring-up of Airflow + Metabase
.\up.ps1 -Action Airflow         # only Airflow
.\up.ps1 -Action Metabase        # only Metabase
.\up.ps1 -Action Stop            # shutdown (scale to 0, PVCs preserved)
.\up.ps1 -Action Stop-Airflow    # stop only Airflow
```

### Metabase dashboards

```powershell
# Apply field filters from field_filter_defaults.yaml (idempotent: a repeat run is a no-op)
python scripts/apply_metabase_field_filters.py

# Export the current state of a dashboard from a live Metabase (for diff against metabase_dashboards/*.json)
python scripts/export_dashboard.py
```

### Run the same set as CI (`.github/workflows/ci.yml`)

```powershell
pytest -q
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql   # run twice — verifies idempotency
python scripts/apply_metabase_field_filters.py                         # second run must report "Patched 0"
```

---

## 10. Claude Code: working notes

This section concerns Claude Code working practices in this repository.

- **Environment is Windows.** System shell is PowerShell 5.1 (Bash is also available via the Bash tool). Use PowerShell syntax: `$env:VAR` instead of `$VAR`, `;` or `if ($?) {...}` instead of `&&`, do NOT use `New-Item -Force` on an existing file (it truncates).
- **Long-running repo commands** (`pytest`, `psql -f db/dwh_init.sql`, `.\up.ps1`) — run them in the foreground so you see the exit code and logs. Do not send them to background if the result is needed for the next step.
- **When editing `db/parts/*.sql`** — after the edit, run `psql -f db/dwh_init.sql` twice against the local `dwh_egisz` to verify idempotency (exactly as CI does). The change is not done until this passes.
- **When editing dashboards** — after editing JSON, run `python scripts/apply_metabase_field_filters.py` and `pytest tests/test_dashboards.py`. Snapshot tests will fail if structure drifted.
- **When changing the XCom shape or task order in the DAG** — synchronously update §2 in this file. The contract is pinned in two places at once: code and CLAUDE.md.
- **Hidden incidents easy to forget:**
  - `analyze_staging` cannot be removed (see §2 and §8).
  - `update_watermark` uses `GREATEST` — do not simplify to a direct UPDATE.
  - SOAP/XML parsing belongs in PL/pgSQL only — do not pull `xml.etree` into Python.
- **README and this file are different genres.** README explains "what and why" at the domain level (in Russian, for humans); CLAUDE.md explains "how to write code" (in English, for LLMs). When documenting the domain, write to README. When documenting an agent rule, write here.
- **Language convention.** This file is maintained in English; the only Cyrillic kept is for domain proper nouns without standard English equivalents (ЕГИСЗ, РЭМД, СЭМД, etc.). When adding new content, write it in English.
