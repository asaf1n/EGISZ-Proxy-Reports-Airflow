# Контракт репозитория EGISZ-Proxy-Reports-Airflow (состояние прода)

Ветка соответствует состоянию прода: БД `dwh_egisz`, объекты ЕГИСЗ в `public` и слоях `stg_egisz`, `mart_egisz`, `mart_egisz_selfservice`. Предметная область, регламент конвейера и правила — [README.md](README.md), [docs/error-catalog.md](docs/error-catalog.md), раскладка схем — [docs/dwh-schema-naming-migration.md](docs/dwh-schema-naming-migration.md). Развитие на новых схемах (`dwh_bi`, `serving_egisz`, `mart_egisz_admin`) ведётся в проекте `bi_platform`, здесь не делается.

## Размещение логики

- Разбор, нормализация, классификация ошибок и сборка документа выполняются в PostgreSQL (`db/*.sql`). DAG в `dags/` вызывают функции DWH, ведут позиции выгрузки и обновляют витрины.
- Подключения DAG — Airflow Connections `dwh_egisz_pg` и `proxy_egisz_fb`.
- Секреты, пароли и токены в git не размещаются: в `k8s/**/*secret*.yaml` значения задаются переменными окружения `${ИМЯ}`, `up.ps1` подставляет их при применении.

## Схема DWH

- Точка сборки — `db/dwh_init.sql`, подключает `01_schema.sql` … `04_views.sql`; идемпотентна.
- Дашборды Metabase (`metabase_dashboards/`, `metabase_models/`) читают те объекты, которые есть на проде; импорт останавливается, если объекта нет в БД.
- Дашборды и модели правятся в JSON и генераторах (`scripts/apply_dashboard_plan.py`, `scripts/layout_operational_tab.py`), не в живом Metabase.

## Проверка

- Тесты: `python -m pytest tests -q` (с `EGISZ_TEST_PG_DSN` — и живые).
- `.\up.ps1` и сценарии `deploy/` запускаются только по запросу пользователя. Работа с прод-базой требует подтверждения.
