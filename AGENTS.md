# Контракт репозитория EGISZ-Proxy-Reports-Airflow

Файл дополняет общие правила агента фактами этого репозитория. Предметная область, модель данных и регламент конвейера описаны в [README.md](README.md) и [docs/error-catalog.md](docs/error-catalog.md).

## Размещение логики

- Разбор, нормализация, классификация ошибок и сборка документа выполняются в PostgreSQL (`db/*.sql`). DAG в `dags/` вызывают функции DWH, ведут позиции выгрузки и обновляют витрины.
- Подключения DAG — Airflow Connections `dwh_bi_pg` и `proxy_egisz_fb`.
- Правила ошибок задаются в `db/02_functions.sql`; накопленные данные приводит к новым правилам задача `reclassify_errors`.

## Схема DWH

- Точка сборки — `db/dwh_init.sql`, подключает `01_schema.sql` … `04_views.sql`.
- Представления и материализованные представления пересоздаются: удаление — в секции `drop_dependents` файла `04_views.sql`, в порядке зависимостей. Новое представление добавляется в эту секцию.
- Объекты адресуются схемой слоя: `raw_egisz`, `stg_egisz`, `mart_egisz`, `serving_egisz`, `mart_egisz_admin`, `etl_meta`. Сборка выполняется с `search_path = pg_catalog`, поэтому имя без схемы приводит к ошибке.
- `ANALYZE` выполняется и после `REFRESH MATERIALIZED VIEW`.
- Позиции выгрузки и разбора фиксируются только после успешного шага.

## Потребители

- Дашборды Metabase (`metabase_dashboards/`) и репозиторий `bi_superset` читают `serving_egisz`, `mart_egisz` и служебные представления `mart_egisz_admin`. Новые отчёты на `raw_egisz` и `stg_egisz` не строятся; исходный текст ошибок для отчётов — из слоя витрин (`mart_egisz.documents.error_text`, `mart_egisz.exchangelog_errors.error_text`) и выдаётся только через функцию скрытия персональных данных `mart_egisz.masking_personal_data`. В `stg_egisz` — только разобранные данные: функций объединения источников и скрытия персональных данных там нет. Исходные записи справочников НСИ (`raw_json`) на слое витрин не хранятся.
- Состояние документа к выдаче — `serving_egisz.documents_current`; документы без ответа за срок ожидания — только `serving_egisz.documents_no_response`. `serving_egisz.document_versions` читают объекты, которым нужны все версии или состояние на прошлый момент.
- Изменение столбцов `serving_egisz` синхронно отражается в дашбордах Metabase, в `bi_superset` (или в описании изменения, если тот репозиторий не входит в задачу), в README и в тестах.

## Проверка

- Схема: `psql -U egisz -d dwh_bi -v ON_ERROR_STOP=1 -f db/dwh_init.sql`, два прогона.
- Тесты: `python -m pytest tests -q`.
- `.\up.ps1` и сценарии `deploy/` запускаются только по запросу пользователя. Контуры с `prod` в имени — внешние, работа с ними требует отдельного подтверждения.
