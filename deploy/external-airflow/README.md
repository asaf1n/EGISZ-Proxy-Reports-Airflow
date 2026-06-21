# Перенос EGISZ ELT DAG в сторонний Airflow

Самодостаточный набор для создания трёх DAG в **уже настроенном** Airflow (не в этом
локальном тестовом контуре). Здесь нет Helm/up.ps1/Kubernetes — только сами DAG, Python-пакет
`egisz_elt`, список зависимостей и инструкция, что настроить и что загрузить.

```
deploy/external-airflow/
├── dags/                    # положить в DAGs-папку целевого Airflow
│   ├── egisz_extract_dag.py
│   ├── egisz_dimensions_dag.py
│   └── egisz_reconcile_dag.py
├── egisz_elt/               # Python-пакет, импортируемый из DAG (нужен на PYTHONPATH)
│   ├── __init__.py
│   ├── common.py            # подключения, watermark, raw-load, transform, mart-reconcile
│   ├── extract.py           # выборка EXCHANGELOG (keyset по LOGID)
│   ├── dimensions.py        # справочники JPERSONS / EGISZ_LICENSES → dim_*
│   └── reconcile.py         # полная сверка источник↔raw
└── requirements.txt
```

## 0. Предусловия на целевом контуре

- **Apache Airflow 2.x** (проверено на 2.11.2), **Python 3.11**.
- Сетевой доступ воркеров Airflow к: **Firebird** (`proxy_egisz`) и **PostgreSQL DWH** (`dwh_egisz`).
- На воркерах — **клиентская библиотека Firebird** (`libfbclient`), её требует `firebird-driver`.
  Debian/Ubuntu: `apt-get install -y firebird3.0-common libfbclient2`.
- **DWH-схема уже создана** в целевой БД `dwh_egisz`. DAG вызывают серверные функции
  (`public.egisz_transform_raw_to_facts`, `public.egisz_reconcile_enriched_ui` и т.д.) и пишут в
  `exchangelog_raw` / `dim_*` / `fact_egisz_documents` / `elt_state`. Перед первым запуском
  прогнать из этого репозитория:
  ```
  psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
  ```
  (идемпотентно; нужны роль `egisz` и БД `dwh_egisz` — см. README проекта §DWH-модель).

## 1. Что загрузить

1. **Пакет `egisz_elt`** — должен быть импортируем интерпретатором Airflow. Любой из вариантов:
   - положить папку `egisz_elt/` в каталог на `PYTHONPATH` (например `$AIRFLOW_HOME/plugins/`
     или каталог из `AIRFLOW__CORE__PLUGINS_FOLDER`); либо
   - собрать wheel из исходников проекта (`pip install .` в корне репозитория — там `pyproject.toml`
     с `package-dir=src`) и поставить в окружение Airflow.
2. **DAG-файлы** `dags/*.py` → в DAGs-папку целевого Airflow (`AIRFLOW__CORE__DAGS_FOLDER`).
3. **Зависимости** из `requirements.txt`: `pip install -r requirements.txt` в окружение Airflow.

> DAG'и используют **только Airflow Connections** (`BaseHook.get_connection`), без `os.getenv`.
> Никаких `.env` на целевом контуре не требуется — все секреты через Connections (п. 2).

## 2. Airflow Connections (обязательно)

Имена фиксированы в коде (`egisz_elt/common.py`): `proxy_egisz_fb` и `dwh_egisz_pg`.
**Важно:** поле **Schema** в Airflow Connection используется как **имя базы данных** для обоих.

| Connection Id | Тип | Host | Port | Schema | Login / Password | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| `proxy_egisz_fb` | Generic | хост Firebird | `3050` | путь/алиас БД Firebird | пользователь / пароль | `{"charset":"UTF8"}` |
| `dwh_egisz_pg` | Postgres | хост PostgreSQL | `5432` | `dwh_egisz` | пользователь / пароль | — |

`connect_fb` строит DSN как `host/port:schema` (`schema` = путь к файлу БД или алиас Firebird).
`connect_pg` берёт `schema` как имя БД, `login`/`password`/`host`/`port` — как обычно.

CLI-вариант (на целевом контуре):
```bash
airflow connections add dwh_egisz_pg \
  --conn-uri 'postgresql://USER:PASSWORD@PG_HOST:5432/dwh_egisz?sslmode=disable'

# Firebird: проще задать по полям, schema = путь/алиас БД
airflow connections add proxy_egisz_fb \
  --conn-type generic \
  --conn-host FB_HOST --conn-port 3050 \
  --conn-schema '/path/or/alias/proxy_egisz' \
  --conn-login USER --conn-password PASSWORD \
  --conn-extra '{"charset":"UTF8"}'
```

## 3. Airflow Variables (необязательно — есть дефолты через `default_var`)

Задавать только если нужно отличное от дефолта. Расписания читаются на parse-time DAG.

| Variable | Дефолт | Назначение |
| --- | --- | --- |
| `egisz_extract_schedule` | `*/5 * * * *` | Расписание extract-DAG |
| `egisz_dimensions_schedule` | `@hourly` | Расписание dimensions-DAG |
| `egisz_reconcile_schedule` | `@daily` | Расписание reconcile-DAG |
| `egisz_batch_size` | `5000` | Размер батча `load_exchangelog_batch` |
| `egisz_reconcile_window_max_gap` | `500` | Склейка LOGID-окон в сверке |
| `egisz_reconcile_max_logids` | `20000000` | Порог memory-guard сверки (выше — hard-skip) |

```bash
airflow variables set egisz_extract_schedule '*/5 * * * *'
# … остальные по необходимости
```

## 4. DAG, которые появятся

| dag_id | Расписание | Задачи |
| --- | --- | --- |
| `egisz_extract_dag` | `*/5` | `load_exchangelog_batch → build_document_facts → refresh_materialized_views → advance_logid_watermark` |
| `egisz_dimensions_dag` | `@hourly` | `sync_dimensions → maintain_enriched_ui` |
| `egisz_reconcile_dag` | `@daily` | `reconcile_proxy_raw` (полная сверка источник↔raw, watermark не двигает) |

Все три `max_active_runs=1`. `advance_logid_watermark` в extract-DAG — единственный writer
`elt_state.last_logid` (через `GREATEST`, без отката). Подробности — README проекта
§«ELT-конвейер Airflow» и §«Полная сверка константности источник↔raw».

## 5. Проверка после загрузки

```bash
airflow dags list-import-errors          # ожидаемо пусто
airflow dags list | grep egisz           # три DAG в списке
# при готовности снять с паузы:
airflow dags unpause egisz_extract_dag
airflow dags unpause egisz_dimensions_dag
airflow dags unpause egisz_reconcile_dag
```

Если `dags list-import-errors` показывает `ModuleNotFoundError: egisz_elt` — пакет не на
`PYTHONPATH` (см. п. 1). Если падает `firebird` — не установлена `libfbclient` на воркере (п. 0).
