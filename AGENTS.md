# AI Agent Instructions (AGENTS.md / .cursorrules)

You are an expert Data Engineer and DevOps Architect. Whenever you generate, refactor, or review code in this repository, strictly adhere to the following architectural guidelines and naming conventions.

For deep domain context (что такое ЕГИСЗ/СЭМД, как устроен парсинг payload, что показывает каждый дашборд) — читай [README.md](README.md). Этот файл — инструкция «как писать код в этом репо», а не учебник по предметной области.

---

## 1. Domain-Driven Naming Conventions (STRICT)

Do not invent names. Follow this exact taxonomy:

| Concept | Name | Notes |
|---|---|---|
| Domain / Python package | `egisz` / `egisz_elt` | Package lives in `src/egisz_elt/` |
| Main DAG file | `egisz_elt_dag.py` | В `airflow/dags/`. DAG ID — `egisz_elt_dag`, pipeline key в `elt_state` — `egisz` |
| Source database (Firebird 5) | `proxy_egisz` | Airflow conn ID: `proxy_egisz_fb` |
| Target DWH (PostgreSQL) | `dwh_egisz` | Airflow conn ID: `dwh_egisz_pg` |
| Raw EXCHANGELOG table | `exchangelog_raw` | PK: `logid bigint` |
| Raw EGISZ_MESSAGES table | `egisz_messages_raw` | PK: `egmid bigint` |
| Dimension: organizations | `dim_organizations` | Source: Firebird `JPERSONS`, PK `jid` |
| Dimension: licenses | `dim_licenses` | Source: Firebird `EGISZ_LICENSES`, PK `id` |
| Dimension: SEMD types | `dim_semd_types` | Static reference table, PK `code` |
| Fact table | `fact_egisz_transactions` | Populated by `egisz_transform_raw_to_facts()`, PK `exchangelog_log_id` |
| Materialized views | `v_*` (MV-варианты — `v_egisz_transactions_enriched_ui`, `v_stg_channel_errors_by_document`) | Освежаются отдельной Airflow-задачей |
| Reporting views | `v_rpt_*_ui` | Обычные view поверх MV |
| Healthcheck views | `v_health_*_ui` | Обычные view для дашборда `02_service.json` |
| Watermark table | `elt_state` | Tracks `last_log_id` and `last_egmid` per `pipeline` |

**NEVER** use the legacy term `proxy_reports` в любом имени переменной, класса, файла, SQL-объекта или identifier'а.

---

## 2. Airflow-Native Architecture (TaskFlow API)

Airflow version: **2.11.2**. Use it as a first-class orchestrator, not a cron wrapper.

### Decorators

Always use `@dag` and `@task`. Never use legacy `PythonOperator`, `BashOperator`, или похожие для Python-логики.

### Task pipeline (in order)

```
sync_dimensions >> extract_from_proxy >> load_to_dwh >> analyze_raw_tables >> transform_data >> refresh_materialized_views >> update_watermark
```

| Task | Responsibility |
|---|---|
| `sync_dimensions` | Полная перезагрузка `dim_organizations` (из `JPERSONS`) и `dim_licenses` (из `EGISZ_LICENSES`) через `sync_directory()` (UPSERT по PK с пагинацией `DIRECTORY_SYNC_PAGE_SIZE=1000`) |
| `extract_from_proxy` | Прочитать watermarks из `elt_state` (`get_cursors(pipeline)`), забрать батч `EXCHANGELOG` по `LOGID > last_log_id` и `EGISZ_MESSAGES` по `EGMID > last_egmid`, дополнительно подтянуть связанные старые сообщения по `<messageId>` / `<relatesToMessage>` / `<relatesTo>` / `<localUid>` / `<DOCUMENTID>` из XML и по `MSGID` из журнала. Вернуть XCom dict |
| `load_to_dwh` | UPSERT в `exchangelog_raw` и `egisz_messages_raw` через `execute_values` + `INSERT ... ON CONFLICT DO UPDATE`. Пробрасывает XCom dict дальше |
| `analyze_raw_tables` | `ANALYZE public.exchangelog_raw` / `public.egisz_messages_raw` только для тех таблиц, в которые в этом батче что-то загрузилось. Запускается в autocommit. **Обязательная задача**: без неё планировщик после первичного bulk-COPY использует `pg_class.reltuples=0` и идёт seq-scan по `exchangelog_raw` (~1.2 ГБ) вместо функциональных индексов `msgid_norm` / `document_id_norm` — запросы Metabase виснут на 8–16 минут. Autovacuum на спокойном пайплайне не успевает. Стоит ~1с на батч |
| `transform_data` | Вызов `public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)`. Функция **не** освежает MV — это отдельная задача |
| `refresh_materialized_views` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` для `v_egisz_transactions_enriched_ui` и `v_stg_channel_errors_by_document`. CONCURRENTLY требует уникального индекса на MV — он есть на `transaction_id` |
| `update_watermark` | UPSERT в `elt_state` через `GREATEST(current, new)` для `last_log_id` и `last_egmid` — защита от случайного отката курсора при ручных перезапусках старых батчей |

### Batch size and schedule

```python
BATCH_SIZE = 5000   # строк за один fetch из Firebird
schedule  = "*/5 * * * *"
max_active_runs = 1   # параллельные прогоны запрещены (гонка за watermark)
```

### XCom payload shape (between tasks)

```python
{
    "count": int,           # EXCHANGELOG-строк прочитано
    "message_count": int,   # EGISZ_MESSAGES-строк (включая «подтянутые» связанные)
    "last_log_id": int,     # watermark на начало этого прогона
    "last_egmid": int,      # watermark на начало этого прогона
    "max_id": int,          # максимальный LOGID в батче
    "max_egmid": int,       # максимальный EGMID в основном (cursor-based) батче, без подтянутых
    "rows": list[dict],     # сериализованные EXCHANGELOG-строки
    "message_rows": list[dict],  # сериализованные EGISZ_MESSAGES-строки
}
```

Контракт зафиксирован — добавление/переименование ключей требует синхронной правки `airflow/dags/egisz_elt_dag.py` и этого файла.

Все задачи **идемпотентны**. PostgreSQL-записи — `INSERT ... ON CONFLICT DO UPDATE`. Повторный прогон того же батча не создаёт дублей, поздний callback может перетереть устаревшую запись.

---

## 3. Secrets & Connections Management

- **NEVER** read credentials from `.env` files or `os.getenv()` inside DAGs or ELT modules.
- Always use Airflow connection management:
  - Firebird source: `BaseHook.get_connection('proxy_egisz_fb')`
  - PostgreSQL DWH: `BaseHook.get_connection('dwh_egisz_pg')`
- Connection-секреты лежат в k8s-манифестах:
  - `k8s/airflow/airflow-connections-secret.yaml` (не коммитится)
  - `k8s/metabase/metabase-connections-secret.yaml` (не коммитится; пример — `metabase-connections-secret.example.yaml`)
- Роли в DWH:
  - `egisz` — owner; используется Airflow-ELT для записи и трансформаций
  - `postgres` — используется Metabase как read-only/BI-аккаунт (имя унаследовано исторически, права ограничены до SELECT)

---

## 4. DWH Model and SQL

### Schema initialization

Вся DDL живёт в **`db/dwh_init.sql`** (тонкий собиратель через `\i`) плюс **11 упорядоченных модулей** в **`db/parts/`**. Запуск:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Перед первым прогоном на чистой инсталляции:

```sql
CREATE ROLE egisz LOGIN PASSWORD 'egisz';
CREATE DATABASE dwh_egisz OWNER postgres;
```

Порядок модулей (имеет значение — view зависят от функций, функции от таблиц):

```
db/parts/00_bootstrap.sql                  — role egisz + GRANT'ы на schema public
db/parts/10_tables.sql                     — таблицы, индексы (включая функциональные), dim_semd_types seed
db/parts/20_functions_parsing.sql          — egisz_xml_text, egisz_normalize_message_id, egisz_clean_host, egisz_clean_text_value, egisz_extract_jid_from_endpoint, egisz_normalize_semd_code, safe_cast_timestamptz
db/parts/30_error_rules.sql                — egisz_error_interpretation_rules + seed правил
db/parts/40_functions_errors.sql           — error classify / interpretation / build_errors_json / semd_type_report_label
db/parts/50_transform.sql                  — egisz_transform_raw_to_facts (главная функция)
db/parts/60_drop_dependents.sql            — DROP зависимых view и legacy-колонок перед пересборкой
db/parts/70_views_core.sql                 — v_egisz_transactions_enriched_ui (MV) + v_rpt_error_interpretations_ui
db/parts/75_views_stg.sql                  — v_stg_channel_errors_by_document (MV) + v_stg_channel_network_errors_by_document (alias view)
db/parts/80_views_rpt.sql                  — v_rpt_*_ui (network_errors_detail, documents_no_response, semd_archive, clinic_connectivity_daily, connectivity_global_daily, error_category_breakdown)
db/parts/90_views_health_and_finalize.sql  — v_health_*_ui + проверка GRANT'ов + REFRESH обеих MV
```

Каждый модуль индивидуально идемпотентен (`CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, `ALTER TABLE ... IF EXISTS`, `INSERT ... ON CONFLICT`). При добавлении новой таблицы, колонки, функции или view — править соответствующий `db/parts/*.sql` в этом же идемпотентном стиле.

**Не создавать миграционные файлы.** Папки `migrations/` нет, `db/dwh_init.sql` + `db/parts/` — единственный источник правды по схеме DWH. Новый dev-DWH разворачивается одним прогоном `psql -f db/dwh_init.sql`.

### Transform function

Трансформация данных происходит **во время прогона DAG**, в задаче `transform_data`. Задача вызывает PL/pgSQL-функцию, которая делает всю реальную работу — парсинг SOAP/XML, обогащение, запись в `fact_egisz_transactions`:

```sql
SELECT public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)
```

Python-задача только инвоксит функцию; **вся бизнес-логика трансформации живёт в SQL, не в Python**. Освежение MV — отдельная задача `refresh_materialized_views`, дублировать его внутри `egisz_transform_raw_to_facts()` нельзя.

Не реализовывать row-level трансформацию в Python (например, итерировать строки и мутировать перед записью). Если возникает соблазн добавить такую логику — она должна быть в PL/pgSQL.

Трансформация **не** откладывается до query-time. К моменту, когда Metabase читает данные, `fact_egisz_transactions` и MV уже заполнены.

### Firebird-specific serialization

- BLOB/text-колонки читаются через `_serialize_firebird_text()` в `fb_client.py` → возвращает `str | None`.
- Date/datetime-поля сериализуются через `.isoformat()` перед укладкой в XCom.
- EGISZ-идентификаторы могут приходить как `<urn:uuid:...>` / `urn:uuid:...` / голый UUID — нормализуй через `normalize_message_id()` в `egisz_elt.pg_client` (на Python-стороне) или `egisz_normalize_message_id()` (на SQL-стороне; на эту нормализованную форму повешены функциональные индексы — `idx_egisz_messages_msgid_norm`, `idx_fact_egisz_message_id_norm`, `idx_fact_egisz_relates_to_norm`).
- Keyset-пагинация на Firebird: `WHERE col > ? ORDER BY col ROWS N`. **`LIMIT/OFFSET` на Firebird-диалекте не использовать.**

### `dim_semd_types`

Справочник типов СЭМД seedится прямо в `db/parts/10_tables.sql` (большой `INSERT ... ON CONFLICT`). Не выносить этот seed в отдельный файл, в Python или во внешний источник.

---

## 5. BI & Metabase Integration

- Metabase version: **v0.60.2.5**, развёрнут в Kubernetes (`k8s/metabase/`).
- Дашбордов **6**, ~75 native-карточек суммарно. JSON-определения — в `metabase_dashboards/`:
  - `01_operational.json` — оперативный мониторинг потока отправки
  - `02_service.json` — healthcheck ETL и канала
  - `03_documents_no_response.json` — очередь эскалации (callback не получен)
  - `04_quality_and_errors.json` — детальный анализ отказов (69-категорийная классификация)
  - `05_executive.json` — управленческая сводка
  - `06_semd_archive.json` — поиск конкретного документа по любому идентификатору
- Большинство карточек работает поверх `v_egisz_transactions_enriched_ui` и `v_rpt_*_ui`.
- Импорт дашбордов происходит **автоматически при старте контейнера** через `metabase/entrypoint.sh` → `metabase/provision.sh` (если `METABASE_AUTO_PROVISION=true`, дефолт — true). Дополнительно `up.ps1` после `kubectl apply` запускает `setup-dashboards.sh` явно (`kubectl exec deploy/metabase -- /bin/bash /app/setup-dashboards.sh`) — повторный прогон идемпотентен (сверяет sha256-манифест).
- Field-фильтры для дашбордов настраиваются `scripts/apply_metabase_field_filters.py` по декларативным правилам из `metabase_dashboards/field_filter_defaults.yaml` (формат — **version: 2**). Это резолвер, а не бизнес-логика.
- Metabase использует **свою** PostgreSQL-базу `metabase_app` (StatefulSet `metabase-postgres`). Не смешивать ни с `dwh_egisz`, ни с `airflow_db`.
- При изменении схемы DWH — проверить совместимость с:
  1. Field filter-маппингом в `metabase_dashboards/field_filter_defaults.yaml`
  2. Дашборд-JSON в `metabase_dashboards/`
  3. Snapshot-тестами в `tests/test_dashboards.py`

---

## 6. Kubernetes Deployment

Airflow и Metabase оба живут в Kubernetes (по умолчанию — Docker Desktop Kubernetes).

| Component | Manifests | Image |
|---|---|---|
| Airflow | `k8s/airflow/values.yaml` (Helm-чарт `apache-airflow/airflow`), `k8s/airflow/airflow-connections-secret.yaml`, `k8s/airflow/Dockerfile` | `egisz-airflow-worker:latest` |
| Metabase | `k8s/metabase/metabase.yaml`, `k8s/metabase/metabase-connections-secret.yaml`, `metabase/Dockerfile` | `egisz-metabase:latest` |

Управление стендом — `up.ps1` (PowerShell, параметр `-Action`):

```powershell
.\up.ps1                         # = -Action Start: полный запуск/обновление Airflow + Metabase
.\up.ps1 -Action Airflow         # только Airflow
.\up.ps1 -Action Metabase        # только Metabase
.\up.ps1 -Action Stop            # полная остановка (scale to 0, PVC сохраняются)
.\up.ps1 -Action Stop-Airflow    # остановить только Airflow
.\up.ps1 -Action Stop-Metabase   # остановить только Metabase
```

Stop-действия делают `scale --replicas=0`, не `helm uninstall` — PVC и данные сохраняются.

- `k8s/metabase/metabase-connections-secret.yaml` не коммитится; `up.ps1` генерирует его из `.example.yaml` при первом запуске.
- Airflow служебная metadata-БД: `airflow_db` (внутри Helm-чарта, отдельно от `dwh_egisz`).
- Porты: Airflow → `localhost:8080`, Metabase → `localhost:3000`.
- DWH-схема разворачивается отдельно от `up.ps1`: `psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql`.

---

## 7. Code Style

- Python ≥ **3.11**; зависимости (`pyproject.toml`):
  - runtime: `firebird-driver>=1.10.0,<2.0.0`, `psycopg2-binary>=2.9.9`, `pyyaml>=6.0`
  - dev: `apache-airflow==2.11.2`, `pytest>=7.0`
- Пакет лежит в `src/egisz_elt/` (setuptools src-layout); модули — `fb_client.py` (Firebird-источник), `pg_client.py` (DWH-таргет + нормализация идентификаторов).
- Полностью типизированные сигнатуры (`from __future__ import annotations`, `from typing import Any` и т.д.).
- Комментарии — только если **why** неочевиден (скрытая инварианта, обход бага, ссылка на инцидент). Не писать ссылки на задачи/тикеты в коде.
- Никаких монолитных скриптов — логика декомпозирована по фокусным функциям в `fb_client.py` / `pg_client.py`. DAG — тонкий, только оркестрация.
- Тесты — `pytest`, лежат в `tests/` (`test_dashboards.py`, `test_fb_client.py`, `test_pg_client.py`). Запуск: `pytest` из корня репо.

---

## 8. Anti-Patterns (What NOT to do)

- НЕ генерировать монолитные Python-скрипты для одной Airflow-задачи.
- НЕ использовать `os.getenv('DB_PASSWORD')` или `.env` внутри DAG/ELT-модулей.
- НЕ использовать слово `proxy_reports` ни в одном идентификаторе.
- НЕ писать данные в системную БД `postgres` — все DWH-записи идут в `dwh_egisz`.
- НЕ добавлять Python-side row-трансформацию; вся трансформация — в `egisz_transform_raw_to_facts` (PL/pgSQL).
- НЕ использовать `LIMIT/OFFSET` для Firebird-пагинации — только keyset `WHERE col > ? ROWS N`.
- НЕ использовать legacy Airflow-операторы (`PythonOperator`, `BashOperator`) для Python-логики.
- НЕ создавать миграционные файлы (`migrations/`, alembic, и т. п.) — менять схему через идемпотентные правки в `db/parts/*.sql`.
- НЕ освежать MV внутри `egisz_transform_raw_to_facts()` — это работа задачи `refresh_materialized_views`.
- НЕ парсить SOAP/XML на Python-стороне в DAG/`fb_client.py` — это работа функций из `db/parts/20_functions_parsing.sql`.
- НЕ ронять watermark назад: `update_watermark` UPSERT'ит через `GREATEST(current, new)`; обход этого механизма ломает идемпотентность.
- НЕ удалять задачу `analyze_raw_tables` из пайплайна — это не «оптимизация», а защита от 8–16-минутных зависаний Metabase (см. §2).
