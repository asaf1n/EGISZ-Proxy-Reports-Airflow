# Перенос EGISZ ELT DAG в сторонний Airflow

Самодостаточный бандл для создания трёх DAG в **уже настроенном** Airflow (не в локальном
тестовом контуре этого репозитория). Здесь нет Helm/up.ps1/Kubernetes — только DAG,
Python-пакет `egisz_elt`, зависимости и то, что нужно настроить на целевой стороне.

```
airflow/                     # корень бандла (dist/external/airflow)
├── dags/                    # положить в DAGs-папку целевого Airflow
│   ├── egisz_extract_dag.py
│   ├── egisz_dimensions_dag.py
│   └── egisz_reconcile_dag.py
├── egisz_elt/               # Python-пакет, импортируемый из DAG (нужен на PYTHONPATH)
│   ├── __init__.py
│   ├── common.py            # подключения, watermark, raw-load, transform, mart-maintenance
│   ├── extract.py           # выборка EXCHANGELOG (keyset по LOGID) + transform-циклы
│   ├── dimensions.py        # справочники JPERSONS / EGISZ_LICENSES → dim_*
│   └── reconcile.py         # полная сверка источник↔raw
├── pyproject.toml           # для установки пакета через pip install .
├── requirements.txt         # рантайм-зависимости (сгенерирован из pyproject.toml)
└── BUILD_INFO.txt           # git-коммит и дата сборки бандла
```

## 0. Предусловия на целевом контуре

- **Apache Airflow 2.x** (проверено на 2.11.2), **Python 3.11**.
- Сетевой доступ воркеров Airflow к **Firebird** (`proxy_egisz`, порт 3050) и
  **PostgreSQL DWH** (`dwh_egisz`, порт 5432).
- На воркерах — **клиентская библиотека Firebird** (`libfbclient`), её требует
  `firebird-driver`. Debian/Ubuntu: `apt-get install -y firebird3.0-common libfbclient2`.
- **DWH-схема уже создана** в целевой БД `dwh_egisz` (бандл `dwh`, см. deploy/external-dwh).
  DAG вызывают серверные функции (`transform_raw_to_facts` и др.) и пишут в
  `exchangelog_raw` / `dim_*` / `documents` / `elt_state`.

## 1. Что загрузить

1. **Пакет `egisz_elt`** — должен быть импортируем интерпретатором Airflow. Любой вариант:
   - положить папку `egisz_elt/` в каталог на `PYTHONPATH` (например `$AIRFLOW_HOME/plugins/`
     или каталог из `AIRFLOW__CORE__PLUGINS_FOLDER`); либо
   - `pip install .` в корне бандла (рядом лежит `pyproject.toml`) в окружение Airflow.
2. **DAG-файлы** `dags/*.py` → в DAGs-папку целевого Airflow (`AIRFLOW__CORE__DAGS_FOLDER`).
3. **Зависимости**: `pip install -r requirements.txt` в окружение Airflow (воркеры и scheduler).

> DAG используют **только Airflow Connections** (`BaseHook.get_connection`), без `os.getenv`.
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

> Подключение `dwh_egisz_pg` должно логиниться ролью **`egisz`** (или другой ролью с тем же
> `ALTER ROLE ... SET timezone TO 'Europe/Moscow'`): на роли закреплён часовой пояс, из-за
> которого наивные Firebird-даты интерпретируются как МСК. Логин иной ролью без пина
> сдвинет границы суток в отчётности.

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

## 3. Пул `dwh_postgres` (обязательно)

Задачи transform/dimensions/reconcile объявлены с `pool="dwh_postgres"` — эксклюзивный
писатель DWH, 1 слот. **Если пула нет, задачи молча не планируются**: ни ошибки в UI, ни
падения — только строка «Tasks using non-existent pool 'dwh_postgres' will not be scheduled»
в логе scheduler'а, а task instances вечно висят в `scheduled`.

```bash
airflow pools set dwh_postgres 1 "Exclusive DWH transform / reconcile / enriched mart maintenance"
```

## 4. Airflow Variables (необязательно — есть дефолты через `default_var`)

Задавать только если нужно отличное от дефолта. Расписания читаются на parse-time DAG
(смена подхватится при следующем парсинге DAG-файлов).

| Variable | Дефолт | Назначение |
| --- | --- | --- |
| `extract_schedule` | `*/5 * * * *` | Расписание extract-DAG |
| `extract_raw_rows` | `2000` | Размер батча выборки EXCHANGELOG из Firebird |
| `extract_raw_rounds` | `3` | Максимум extract-циклов за один запуск |
| `transform_rows` | `5000` | Размер батча transform_raw_to_facts |
| `transform_rounds` | `6` | Максимум transform-циклов за один запуск |
| `dimensions_schedule` | `@hourly` | Расписание dimensions-DAG |
| `reconcile_schedule` | `@daily` | Расписание reconcile-DAG |
| `reconcile_max_logids` | `20000000` | Порог memory-guard полной сверки (выше — hard-fail) |

```bash
airflow variables set extract_schedule '*/5 * * * *'
# … остальные по необходимости
```

## 5. DAG, которые появятся

| dag_id | Расписание | Задачи |
| --- | --- | --- |
| `egisz_extract_dag` | `*/5` | `extract_exchangelog → transform_exchangelog` |
| `egisz_dimensions_dag` | `@hourly` | `sync_dimensions` (+ обновление enriched-марта при изменениях) |
| `egisz_reconcile_dag` | `@daily` | `reconcile_proxy_raw` (полная сверка источник↔raw, watermark не двигает) |

Все три `max_active_runs=1`, `catchup=False`. Watermark `elt_state.last_logid` двигает только
transform-шаг extract-DAG (через `GREATEST`, без отката). Новые DAG создаются на паузе —
снять после настройки Connections и пула (п. 6).

## 6. Проверка после загрузки

```bash
airflow dags list-import-errors          # ожидаемо пусто
airflow dags list | grep egisz           # три DAG в списке
# при готовности снять с паузы:
airflow dags unpause egisz_extract_dag
airflow dags unpause egisz_dimensions_dag
airflow dags unpause egisz_reconcile_dag
```

Смоук: запустить `egisz_extract_dag` вручную и убедиться, что в DWH растут
`exchangelog_raw` / `documents`, а `elt_state.last_logid` продвинулся.

Если `dags list-import-errors` показывает `ModuleNotFoundError: egisz_elt` — пакет не на
`PYTHONPATH` (п. 1). Если падает `firebird` — не установлена `libfbclient` на воркере (п. 0).
Если задачи вечно в `scheduled` — не создан пул `dwh_postgres` (п. 3).
