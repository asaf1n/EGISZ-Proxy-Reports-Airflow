# EGISZ Airflow ELT

Проект описывает сервис парсинга и аналитики по интеграции с ЕГИСЗ. Это рабочий прототип для будущей миграции Airflow DAG и Metabase в корпоративную сеть, где уже существуют отдельные Kubernetes-кластеры, базы данных и служебная инфраструктура. Поэтому README сфокусирован не на локальном окружении, а на логике отбора данных, трансформации, отчётах и скриптах, которые поддерживают DAG и Metabase.

## Назначение сервиса

Сервис собирает эксплуатационную аналитику обмена с ЕГИСЗ:

- какие сообщения были отправлены;
- какие получили callback или не получили его;
- какие ошибки вернул канал или РЭМД;
- какие СЭМД, клиники и типы документов участвуют в обмене;
- где есть проблемы полноты, связности и свежести данных.

Источник данных:

- Firebird 5, база `proxy_egisz`

Целевая аналитическая база:

- PostgreSQL `dwh_egisz`

BI-слой:

- Metabase v0.60.2.5

Оркестрация:

- Apache Airflow 2.11.2
- DAG: `airflow/dags/egisz_elt_dag.py`

## Что делает пайплайн

Пайплайн построен как ELT:

- Python извлекает данные из Firebird и загружает raw-слой в PostgreSQL.
- Вся предметная логика парсинга и аналитического обогащения выполняется в SQL.
- Metabase работает поверх уже подготовленных фактов и витрин, а не парсит JSON/XML на лету.

Основная последовательность задач DAG:

```text
sync_dimensions
  >> extract_from_proxy
  >> load_to_dwh
  >> transform_data
  >> refresh_materialized_views
  >> update_watermark
```

Параметры текущего инкрементального контура:

- `BATCH_SIZE = 5000`
- `schedule = "*/5 * * * *"`
- `max_active_runs = 1`

## Логика отбора данных

### 1. Синхронизация справочников

`sync_dimensions` обновляет справочники, которые нужны для аналитики и связности:

- `dim_organizations` из `JPERSONS`
- `dim_licenses` из `EGISZ_LICENSES`

Они используются для привязки клиник, лицензий, `mo_uid`, JID и последующего обогащения отчётов.

### 2. Инкрементальная выгрузка журнала и сообщений

`extract_from_proxy` читает курсоры из `elt_state` и забирает:

- `EXCHANGELOG` по `LOGID`
- `EGISZ_MESSAGES` по `EGMID`
- связанные сообщения, найденные по payload и идентификаторам корреляции

Firebird-пагинация работает по keyset-схеме:

```sql
WHERE col > :last_seen
ORDER BY col
ROWS :limit
```

Это важно для стабильной догрузки больших журналов без `LIMIT/OFFSET`.

### 3. Загрузка raw-слоя

`load_to_dwh` сохраняет данные в:

- `exchangelog_raw`
- `egisz_messages_raw`

Запись идемпотентна и идет через `INSERT ... ON CONFLICT DO UPDATE`.

### 4. SQL-парсинг и построение фактов

`transform_data` вызывает:

```sql
SELECT public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)
```

Именно эта функция выполняет основную предметную работу:

- парсит SOAP/XML payload;
- вытаскивает `message_id`, `relates_to_id`, `local_uid_semd`, `emdr_id`, `semd_code`, `semd_name`;
- нормализует статусы;
- выделяет транспортные и бизнес-ошибки;
- строит итоговые записи в `fact_egisz_transactions`.

Python не должен дублировать эту логику построчно.

### 5. Обновление витрин и курсоров

После трансформации:

- `refresh_materialized_views` обновляет materialized views;
- `update_watermark` продвигает `last_log_id` и `last_egmid` в `elt_state`.

## DWH и SQL-слой

Схема DWH поддерживается единым bootstrap-источником:

```bash
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

`db/dwh_init.sql` является тонким коллектором для модулей `db/parts/`:

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

Ключевые объекты модели:

- `elt_state`
- `exchangelog_raw`
- `egisz_messages_raw`
- `dim_organizations`
- `dim_licenses`
- `dim_semd_types`
- `fact_egisz_transactions`
- `v_egisz_transactions_enriched_ui`
- `v_stg_channel_errors_by_document`

### Что именно хранится в фактах

`fact_egisz_transactions` собирает нормализованный контур аналитики:

- идентификаторы обмена и документа;
- статус обработки;
- дату отправки и дату обработки;
- клинику и организационные атрибуты;
- тип СЭМД;
- классификацию ошибок;
- поля для поиска документов без ответа и проблемных сообщений.

### Как трактуются ошибки

Ошибка в аналитике не равна просто тексту из payload. SQL-слой:

- выделяет транспортные ошибки канала;
- извлекает `code` и `message` из XML-ответов;
- прогоняет их через правила интерпретации;
- пишет в факты канонизированные поля вроде `error_type`, `error_summary`, `error_json_text`.

Это позволяет строить понятные отчёты без чтения сырых XML.

## Скрипты и модули, которые поддерживают DAG

Основные Python-модули:

- `src/egisz_elt/fb_client.py` — чтение Firebird, сериализация текстов и дат
- `src/egisz_elt/pg_client.py` — загрузка raw-слоя, курсоры, вызов SQL-трансформации
- `airflow/dags/egisz_elt_dag.py` — orchestration через TaskFlow API

Ключевые SQL-модули:

- `db/parts/20_functions_parsing.sql` — безопасный парсинг payload
- `db/parts/30_error_rules.sql` — правила интерпретации ошибок
- `db/parts/40_functions_errors.sql` — функции классификации и подготовки error-полей
- `db/parts/50_transform.sql` — `egisz_transform_raw_to_facts`
- `db/parts/70_views_core.sql`, `75_views_stg.sql`, `80_views_rpt.sql` — пользовательские и отчётные витрины

## Metabase и отчёты

Metabase в этом проекте отражает уже подготовленную аналитику. Основные дашборды лежат в `metabase_dashboards/`:

- `01_operational.json` — оперативная динамика обмена
- `02_service.json` — сервисные показатели и health
- `03_documents_no_response.json` — документы без найденного ответа
- `04_quality_and_errors.json` — ошибки и качество данных
- `05_executive.json` — укрупнённая управленческая сводка
- `06_semd_archive.json` — архив СЭМД

Основные типы отчётов:

- динамика отправки и обработки сообщений;
- очередь документов без callback;
- детализация транспортных и сетевых ошибок;
- проблемные документы по СЭМД, клиникам и типам ошибок;
- health-витрины по свежести и полноте данных;
- архивные выборки по СЭМД и связанным идентификаторам.

### Скрипты Metabase

`metabase/setup-dashboards.sh` отвечает за provisioning:

- создает коллекцию "Интеграция с ЕГИСЗ";
- регистрирует DWH как источник данных;
- проверяет, что нужные витрины и поля существуют;
- синхронизирует схему в Metabase;
- импортирует карточки и дашборды.

`scripts/apply_metabase_field_filters.py` подставляет корректные field filters для native query cards.

Правила этих фильтров задаются декларативно в:

- `metabase_dashboards/field_filter_defaults.yaml`

Это важно для переносимости карточек между окружениями и для будущей миграции в корпоративный контур.

## Роль проекта как прототипа миграции

Этот репозиторий нужен не только для локальной эксплуатации, но и как прототип целевого сервиса, который затем будет перенесён в корпоративную инфраструктуру. Поэтому его ценность в следующем:

- фиксирует логику отбора и связывания сообщений;
- фиксирует SQL-правила аналитики и интерпретации ошибок;
- показывает, какие raw-таблицы, факты и витрины нужны для отчётности;
- хранит Metabase-отчёты как код;
- отделяет инфраструктурный слой от предметной логики DAG и отчётов.

При переносе в отдельные корпоративные Kubernetes-кластеры, базы и сети должны сохраниться прежде всего:

- контракт raw-слоя;
- SQL-трансформация;
- структура фактов и отчётных витрин;
- правила Metabase provisioning;
- бизнес-смысл самих отчётов.

## Структура репозитория

```text
airflow/dags/                  Airflow DAG
db/dwh_init.sql                bootstrap коллектор DWH
db/parts/                      модули таблиц, функций и витрин
k8s/                           Kubernetes manifests и values
metabase/                      provisioning и образ Metabase
metabase_dashboards/           дашборды и field-filter rules
scripts/                       вспомогательные скрипты для Metabase и валидации
src/egisz_elt/                 Firebird/PostgreSQL клиенты и загрузочная логика
tests/                         тесты Python-слоя и dashboard-структур
up.ps1                         локальный bootstrap-скрипт
AGENTS.md                      контракт для AI-агентов
```
