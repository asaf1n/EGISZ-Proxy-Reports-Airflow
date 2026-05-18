# EGISZ Airflow ELT

Проект строит витрину аналитики "Интеграция с ЕГИСЗ": инкрементально читает журнал обмена из Firebird `proxy_egisz`, загружает raw-слой в PostgreSQL `dwh_egisz`, выполняет SQL-трансформацию в факты и публикует дашборды Metabase.

Основные принципы:

- Python только извлекает и загружает данные.
- Вся бизнес-трансформация живет в PostgreSQL-функции `public.egisz_transform_raw_to_facts(...)`.
- DWH-схема поддерживается из одного источника истины: `db/dwh_init.sql` и модулей `db/parts/`.
- Metabase использует отдельную служебную БД `metabase_app`, а не `dwh_egisz`.

## Архитектура

Компоненты:

- Источник: Firebird 5, база `proxy_egisz`.
- Оркестрация: Apache Airflow 2.11.2, DAG `airflow/dags/egisz_elt_dag.py`.
- DWH: PostgreSQL, база `dwh_egisz`.
- BI: Metabase v0.60.2.5.
- Локальный деплой: Docker Desktop Kubernetes.

Ключевые Airflow connections:

- `proxy_egisz_fb` -> Firebird source.
- `dwh_egisz_pg` -> PostgreSQL DWH.

Основной пайплайн DAG:

```text
sync_dimensions
  >> extract_from_proxy
  >> load_to_dwh
  >> transform_data
  >> refresh_materialized_views
  >> update_watermark
```

Параметры DAG:

- `BATCH_SIZE = 5000`
- `schedule = "*/5 * * * *"`
- `max_active_runs = 1`

## DWH bootstrap

База и DWH-объекты поднимаются одним идемпотентным запуском:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

`db/dwh_init.sql` теперь является тонким коллектором, который подключает упорядоченные модули из `db/parts/`:

```text
db/parts/00_bootstrap.sql
db/parts/10_tables.sql
db/parts/20_functions_parsing.sql
db/parts/30_error_rules.sql
db/parts/40_functions_errors.sql
db/parts/50_transform.sql
db/parts/60_drop_dependents.sql
db/parts/70_views_core.sql
db/parts/75_views_stg.sql
db/parts/80_views_rpt.sql
db/parts/90_views_health_and_finalize.sql
```

Это и есть единственный источник истины для схемы. Отдельных migration-файлов в проекте больше нет: любые изменения таблиц, функций, materialized views и report views вносятся прямо в соответствующий модуль `db/parts/`.

Ключевые DWH-объекты:

- `elt_state`
- `exchangelog_raw`
- `egisz_messages_raw`
- `dim_organizations`
- `dim_licenses`
- `dim_semd_types`
- `fact_egisz_transactions`
- `v_egisz_transactions_enriched_ui`
- `v_stg_channel_errors_by_document`

## ELT-логика

1. `sync_dimensions` синхронизирует `dim_organizations` из `JPERSONS` и `dim_licenses` из `EGISZ_LICENSES`.
2. `extract_from_proxy` читает курсоры из `elt_state`, затем получает батчи `EXCHANGELOG` по `LOGID` и `EGISZ_MESSAGES` по `EGMID`, включая связанные сообщения.
3. `load_to_dwh` делает `INSERT ... ON CONFLICT DO UPDATE` в `exchangelog_raw` и `egisz_messages_raw`.
4. `transform_data` вызывает `SELECT public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)`.
5. `refresh_materialized_views` отдельно обновляет materialized views.
6. `update_watermark` сохраняет `last_log_id` и `last_egmid` через `GREATEST(current, new)`.

Важно:

- Python не должен парсить SOAP/XML построчно и вычислять бизнес-поля.
- Firebird-пагинация строится через keyset `WHERE col > ? ORDER BY col ROWS N`.
- Нормализация идентификаторов EGISZ делается в SQL / `pg_client.py`, а не вручную в дашбордах.

## Metabase

Дашборды лежат в `metabase_dashboards/`:

- `01_operational.json`
- `02_service.json`
- `03_documents_no_response.json`
- `04_quality_and_errors.json`
- `05_executive.json`
- `06_semd_archive.json`

Provisioning делает `metabase/setup-dashboards.sh`:

- создает коллекцию "Интеграция с ЕГИСЗ";
- регистрирует DWH как источник данных;
- выполняет preflight-проверку нужных DWH-объектов;
- запускает `sync_schema`;
- подставляет реальные field ids в native query filters;
- импортирует JSON-дашборды.

Правила field filters хранятся декларативно в `metabase_dashboards/field_filter_defaults.yaml`, а `scripts/apply_metabase_field_filters.py` выступает как резолвер этих правил.

## Локальный запуск

Перед первым запуском подготовьте секреты:

```powershell
Copy-Item k8s/metabase/metabase-connections-secret.example.yaml k8s/metabase/metabase-connections-secret.yaml
```

Полный запуск:

```powershell
.\up.ps1
```

Только Airflow:

```powershell
.\up.ps1 -Component Airflow
```

Только Metabase:

```powershell
.\up.ps1 -Component Metabase
```

Эти сценарии эквивалентны текущему компонентному bootstrap-потоку репозитория: `up.ps1` сам собирает локальные образы, выполняет rollout и запускает provisioning.

Локальные UI:

- Airflow: `http://localhost:8080`
- Metabase: `http://localhost:3000`

## Чистый старт

Если нужен чистый DWH bootstrap:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Если нужен полный пересбор компонент в Kubernetes:

```powershell
.\up.ps1
```

Остановка:

```powershell
helm uninstall airflow
kubectl delete deployment/metabase service/metabase
```

Если требуется полностью сбросить Airflow metadata DB, дополнительно удалите соответствующий PVC после `helm uninstall airflow`.

## CI и проверки

В репозитории есть GitHub Actions workflow `.github/workflows/ci.yml`, который проверяет:

- `pytest`
- двукратный идемпотентный прогон `db/dwh_init.sql` на `postgres:16`
- идемпотентность `scripts/apply_metabase_field_filters.py`
- корректный JSON всех файлов в `metabase_dashboards/`

Локально полезно запускать:

```powershell
pytest -q
python scripts/apply_metabase_field_filters.py
```

## Структура репозитория

```text
airflow/dags/                  Airflow DAG
db/dwh_init.sql                thin bootstrap collector
db/parts/                      DWH modules
k8s/                           Helm values и Kubernetes manifests
metabase/                      Dockerfile и provisioning scripts
metabase_dashboards/           dashboard JSON и field-filter rules
scripts/                       вспомогательные скрипты
src/egisz_elt/                 Firebird/PostgreSQL клиенты
tests/                         pytest coverage для DWH/dashboards/clients
up.ps1                         основной локальный bootstrap
AGENTS.md                      контракт для AI-агентов в этом репозитории
```

## Git hygiene

В git не должны попадать:

- локальные `.claude/*.local.json`
- runtime/build логи `*.log`
- локальные tag-файлы `.airflow-image-tag`, `.metabase-image-tag`
- ad-hoc SQL/verification файлы `tmp_*.txt`, `tmp_*.sql`

Если после локальной проверки появились такие файлы, их нужно удалить или оставить только в ignored-состоянии, не коммитя в репозиторий.
