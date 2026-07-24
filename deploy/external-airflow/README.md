# Перенос EGISZ ELT DAG в сторонний Airflow

Самодостаточный бандл для создания трёх DAG в **уже настроенном** Airflow (не в локальном
тестовом контуре этого репозитория). Здесь нет Helm/up.ps1/Kubernetes — только DAG-файлы,
зависимости и то, что нужно настроить на целевой стороне.

Каждый DAG-файл самодостаточен: подключения, watermark, transform-циклы, справочники и
сверка лежат в самом файле. Отдельно разворачивать пакет (`PYTHONPATH` / `pip install`)
не нужно. Файлы не редактировать на целевой стороне — правки вносятся в исходники
репозитория (`airflow/dags/*.py`) с последующей пересборкой бандла.

```
airflow/                     # корень бандла (dist/external/airflow)
├── dags/                    # положить в DAGs-папку целевого Airflow
│   ├── egisz_extract_dag.py     # выборка EXCHANGELOG (keyset по LOGID) + transform-циклы
│   ├── egisz_dimensions_dag.py  # справочники JPERSONS / EGISZ_LICENSES → dim_*
│   └── egisz_reconcile_dag.py   # полная сверка источник↔raw
├── requirements.txt         # рантайм-зависимости (сгенерирован из pyproject.toml)
└── BUILD_INFO.txt           # git-коммит и дата сборки бандла
```

## 0. Предусловия на целевом контуре

- **Apache Airflow 3.x** (проверено на 3.2.2), **Python 3.11+**. DAG написаны на
  Task SDK (`airflow.sdk`), снятые в 3.x пути `airflow.decorators` / `airflow.models`
  не используются; на Airflow 2.x файлы не загрузятся.
- Сетевой доступ воркеров Airflow к **Firebird** (`proxy_egisz`, порт 3050) и
  **PostgreSQL DWH** (`dwh_egisz`, порт 5432).
- На воркерах — **клиентская библиотека Firebird** (`libfbclient`), её требует
  `firebird-driver`. Debian/Ubuntu: `apt-get install -y firebird3.0-common libfbclient2`.
- **DWH-схема уже создана** в целевой БД `dwh_egisz` (бандл `dwh`, см. deploy/external-dwh).
  DAG вызывают серверные функции (`transform_raw_to_facts` и др.) и пишут в
  `exchangelog_raw` / `dim_*` / `documents` / `elt_state`.

## 1. Что загрузить

1. **DAG-файлы** `dags/*.py` → в DAGs-папку целевого Airflow (`AIRFLOW__CORE__DAGS_FOLDER`).
2. **Зависимости**: `pip install -r requirements.txt` в окружение Airflow (воркеры и scheduler).

> При обновлении с прежней раскладки бандла: удалить ранее развёрнутый пакет `egisz_elt`
> (из `PYTHONPATH`/plugins или `pip uninstall egisz-elt`) — самодостаточные DAG-файлы
> несут собственную копию кода, устаревший пакет рядом лишь маскирует её версию.

> Задача `refresh_report_marts` обновляет витрины динамики (`rpt_documents_weekly` /
> `rpt_error_breakdown_weekly` / `rpt_documents_monthly` / `rpt_error_breakdown_monthly`),
> на которых построены вкладки «Динамика по неделям» и «Динамика по месяцам»
> управленческого дашборда. Витрины создаются бандлом `dwh` — при отставшей схеме
> задача упадёт на `REFRESH MATERIALIZED VIEW`.

> Секреты подключений DAG берут **только из Airflow Connections** (`Connection.get`), не из
> `os.getenv`. Переменные `EGISZ_*` (п. 4) несут лишь настройки поведения, не секреты.

## 2. Airflow Connections (обязательно)

Имена фиксированы в коде каждого DAG-файла: `proxy_egisz_fb` и `dwh_egisz_pg`.
**Важно:** поле **Schema** в Airflow Connection используется как **имя базы данных** для обоих.

| Connection Id | Тип | Host | Port | Schema | Login / Password | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| `proxy_egisz_fb` | Generic | хост Firebird | `3050` | путь/алиас БД Firebird | пользователь / пароль | `{"charset":"WIN1251"}` |
| `dwh_egisz_pg` | Postgres | хост PostgreSQL | `5432` | `dwh_egisz` | пользователь / пароль | — |

`connect_fb` строит DSN как `host/port:schema` (`schema` = путь к файлу БД или алиас Firebird).
`charset` в Extra должен совпадать с кодировкой БД журнала прокси (в проверенных контурах —
`WIN1251`; без Extra код подключается с `UTF8`, и чтение падает на транслитерации кириллицы).
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
  --conn-extra '{"charset":"WIN1251"}'
```

## 3. Пул `dwh_postgres` (обязательно)

Задачи transform/dimensions/reconcile объявлены с `pool="dwh_postgres"` — эксклюзивный
писатель DWH, 1 слот. **Если пула нет, задачи молча не планируются**: ни ошибки в UI, ни
падения — только строка «Tasks using non-existent pool 'dwh_postgres' will not be scheduled»
в логе scheduler'а, а task instances вечно висят в `scheduled`.

```bash
airflow pools set dwh_postgres 1 "Exclusive DWH transform / reconcile / enriched mart maintenance"
```

## 4. Настройки DAG — переменные окружения

Все настройки (расписания и параметры выполнения) читаются из переменной окружения
`EGISZ_<KEY>` процессов Airflow, иначе из словаря `DEFAULTS` в файле DAG. **Airflow
Variables не используются**: их top-level чтение при парсинге DAG-файла в воркере на
Airflow 3 уходит в supervisor RPC и виснет на «Filling up the DagBag», подвешивая DAG —
поэтому все параметры резолвятся без обращения к метабазе.

| Env-переменная | Дефолт | Назначение |
| --- | --- | --- |
| `EGISZ_EXTRACT_SCHEDULE` | `*/5 * * * *` | Расписание extract-DAG |
| `EGISZ_DIMENSIONS_SCHEDULE` | `@hourly` | Расписание dimensions-DAG |
| `EGISZ_RECONCILE_SCHEDULE` | `@hourly` | Расписание reconcile-DAG |
| `EGISZ_EXTRACT_RAW_ROWS` | `1000` | Размер батча выборки EXCHANGELOG из Firebird |
| `EGISZ_EXTRACT_RAW_ROUNDS` | `3` | Максимум extract-циклов за один запуск |
| `EGISZ_TRANSFORM_ROWS` | `3000` | Размер батча transform_raw_to_facts |
| `EGISZ_TRANSFORM_ROUNDS` | `6` | Максимум transform-циклов за один запуск |
| `EGISZ_RECONCILE_LOOKBACK_DAYS` | `30` | Глубина сверки (дней по `COALESCE(LOGDATE, CREATEDATE)`) |
| `EGISZ_RECONCILE_MAX_LOGIDS` | `20000000` | Memory-guard: макс. LOGID **внутри окна** lookback (выше — hard-fail) |

Значения по умолчанию рабочие — переопределять не обязательно. Задать на целевом контуре
(пример для Helm-чарта Airflow — во все компоненты через `extraEnv`; смена расписания
подхватится при следующем парсинге, параметров выполнения — при следующем запуске задачи):

```yaml
extraEnv: |
  - name: EGISZ_EXTRACT_SCHEDULE
    value: "*/5 * * * *"
  - name: EGISZ_TRANSFORM_ROWS
    value: "3000"
```

## 5. DAG, которые появятся

| dag_id | Расписание | Задачи |
| --- | --- | --- |
| `egisz_extract_dag` | `*/5` | `extract_exchangelog → transform_exchangelog → refresh_report_marts` |
| `egisz_dimensions_dag` | `@hourly` | `sync_dimensions` (+ обновление enriched-марта при изменениях) |
| `egisz_reconcile_dag` | `@daily` | `reconcile_proxy_raw` (сверка источник↔raw за последние N дней, watermark не двигает) |

Все три `max_active_runs=1`, `catchup=False`. Watermark `elt_state.last_logid` двигает только
transform-шаг extract-DAG (через `GREATEST`, без отката). Новые DAG появятся на паузе
(стандартный `dags_are_paused_at_creation=True`; в коде DAG флаг не переопределён) —
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

Если `dags list-import-errors` показывает `ModuleNotFoundError: firebird` /
`psycopg2` — не установлены зависимости из `requirements.txt` (п. 1). Если падает
`firebird` при подключении — не установлена `libfbclient` на воркере (п. 0).
Если задачи вечно в `scheduled` — не создан пул `dwh_postgres` (п. 3).
