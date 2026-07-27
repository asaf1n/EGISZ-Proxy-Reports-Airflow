# Перенос настроек сервиса аналитики ЕГИСЗ на внешний контур

Пакет для развёртывания настроек проекта в **уже развёрнутой** инфраструктуре: PostgreSQL,
Apache Airflow, Metabase. Kubernetes, Helm и образы контейнеров не используются — переносятся
только схема хранилища, DAG-файлы и содержимое Metabase.

```
egisz-bi/
├─ db/                       # схема DWH: точка входа + модули
│  ├─ dwh_init.sql
│  ├─ 01_schema.sql          # таблицы, индексы, справочники
│  ├─ 02_functions.sql       # парсинг, классификация ошибок, сервисные функции
│  ├─ 03_transform.sql       # transform_raw_to_facts и сборка документа
│  └─ 04_views.sql           # отчётный слой rpt_*, права владельца, ANALYZE
├─ dags/                     # DAG-файлы Airflow (переносятся вручную)
│  ├─ egisz_etl_dag.py
│  └─ egisz_maintenance_dag.py
├─ metabase/
│  ├─ setup-dashboards.sh    # импортёр (запускать его)
│  ├─ sync-models.sh         # синхронизация моделей (подключается импортёром)
│  ├─ include/mb_list.sh     # общие функции (подключается импортёром)
│  ├─ dashboards/*.json      # дашборды
│  └─ models/*.json          # Metabase Models
└─ README.md
```

Порядок обязателен:

1. **DWH** — схема должна существовать до запуска DAG и импорта Metabase.
2. **Airflow** — DAG пишут в DWH и вызывают его серверные функции.
3. **Metabase** — импортёр проверяет контракт DWH (наличие таблиц и представлений) до
   загрузки и останавливается, если схемы нет.

При обновлении уже работающего контура DAG-и `egisz_*` ставятся на паузу до применения
схемы и снимаются с паузы после импорта: применение пересобирает отчётный слой целиком и
конкурирует за блокировки с пятиминутным приёмом фактов.

---

## 1. DWH

### 1.1. Предусловия

- PostgreSQL 14+ и клиент `psql`.
- Роль `egisz` существует и **владеет** целевой БД.

### 1.2. Разовый bootstrap (администратором с правом `CREATE ROLE/DATABASE`)

```sql
CREATE ROLE egisz LOGIN PASSWORD '<пароль>';
CREATE DATABASE dwh_egisz OWNER egisz;   -- egisz как владелец получает и public-схему
```

Роль `egisz` — рабочая учётка конвейера и BI; пароль передаётся администраторам Airflow и
Metabase (Connections, `APP_DB_*`), в файлы не записывается. Дальше суперпользователь не
нужен: весь `dwh_init.sql` идёт под `egisz`.

### 1.3. Применение схемы

**Строго из корня пакета** — точка входа подключает модули относительными путями
(`\i db/...`):

```bash
cd <корень-пакета>
psql -h PG_HOST -U egisz -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Скрипт идемпотентен (`CREATE ... IF NOT EXISTS`, `CREATE OR REPLACE`, `INSERT ... ON
CONFLICT`): повторный прогон обязан пройти чисто — так же применяются и обновления схемы.
`01_schema.sql` не создаёт роль и не требует `postgres`: он закрепляет часовой пояс роли и
фиксирует гранты владельца, `04_views.sql` переназначает владельца объектов на `egisz`
(no-op, раз объекты создаёт сам `egisz`). Успешный прогон заканчивается сообщением
`DWH init complete`.

### 1.4. Часовой пояс

`01_schema.sql` закрепляет `ALTER ROLE egisz SET timezone TO 'Europe/Moscow'` — наивные даты
журнала шлюза интерпретируются как МСК. Все сессии конвейера и Metabase должны логиниться
ролью `egisz` (или ролью с тем же закреплением), иначе сместятся границы суток.

### 1.5. Витрины

`04_views.sql` пересоздаёт отчётный слой целиком, включая пять материализованных
представлений (`rpt_error_breakdown`, `rpt_documents_weekly`, `rpt_error_breakdown_weekly`,
`rpt_documents_monthly`, `rpt_error_breakdown_monthly`) — они создаются сразу с данными.
Отдельное обновление после применения схемы не нужно; оно требуется только если данные
правились после неё. Порядок обязателен — разбивка ошибок раньше периодических витрин:

```bash
psql -h PG_HOST -U egisz -d dwh_egisz \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_error_breakdown" \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_documents_weekly" \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_error_breakdown_weekly" \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_documents_monthly" \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_error_breakdown_monthly"
```

В штатной эксплуатации все пять витрин обновляет `egisz_etl_dag` вслед за фактами.

### 1.6. Проверка

```bash
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dt public.*"      # etl_state, exchangelog_raw, documents, transactions, dim_*
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dv public.rpt_*"  # представления: rpt_documents, rpt_document_versions, rpt_health_*
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dm public.rpt_*"  # витрины: rpt_error_breakdown, rpt_*_weekly, rpt_*_monthly
psql -h PG_HOST -U egisz -d dwh_egisz -c "SHOW timezone"     # Europe/Moscow
```

---

## 2. Airflow

### 2.1. Предусловия

- **Apache Airflow 3.x** (проверено на 3.2.2), **Python 3.11+**. DAG написаны на Task SDK
  (`airflow.sdk`); снятые в 3.x пути `airflow.decorators` / `airflow.models` не
  используются, на Airflow 2.x файлы не загрузятся.
- Сетевой доступ воркеров к **Firebird** (журнал шлюза, порт 3050) и **PostgreSQL DWH**
  (`dwh_egisz`, порт 5432).
- На воркерах — клиентская библиотека Firebird (`libfbclient`), её требует
  `firebird-driver`. Debian/Ubuntu: `apt-get install -y firebird3.0-common libfbclient2`.
- Схема DWH применена (§1).

### 2.2. Что загрузить

DAG-файлы переносятся вручную: `dags/*.py` копируются в DAGs-папку целевого Airflow
(`AIRFLOW__CORE__DAGS_FOLDER`). Каждый файл самодостаточен — подключения, watermark,
transform-циклы, справочники и проверка полноты лежат в самом файле, разворачивать пакет
(`PYTHONPATH` / `pip install`) не нужно. Файлы не редактировать на целевой стороне: правки
вносятся в исходники проекта с последующей пересборкой пакета.

Рантайм-зависимости в окружение Airflow (воркеры и планировщик):

```bash
pip install 'firebird-driver>=1.10.0,<2.0.0' 'psycopg2-binary>=2.9.9'
```

### 2.3. Airflow Connections (обязательно)

Имена фиксированы в коде каждого DAG-файла: `proxy_egisz_fb` и `dwh_egisz_pg`.
**Важно:** поле **Schema** в Airflow Connection используется как **имя базы данных** для обоих.

| Connection Id | Тип | Host | Port | Schema | Login / Password | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| `proxy_egisz_fb` | Generic | хост Firebird | `3050` | путь или алиас БД Firebird | пользователь / пароль | `{"charset":"WIN1251"}` |
| `dwh_egisz_pg` | Postgres | хост PostgreSQL | `5432` | `dwh_egisz` | пользователь / пароль | — |

`connect_fb` строит DSN как `host/port:schema`. `charset` в Extra должен совпадать с
кодировкой БД журнала шлюза (в проверенных контурах — `WIN1251`; без Extra код подключается
с `UTF8`, и чтение падает на транслитерации кириллицы). `connect_pg` берёт `schema` как имя
БД, `login` / `password` / `host` / `port` — как обычно.

Подключение `dwh_egisz_pg` должно логиниться ролью `egisz` (или другой ролью с тем же
закреплением часового пояса, §1.4).

Секреты подключений DAG берут **только из Airflow Connections** (`Connection.get`), не из
переменных окружения процесса.

```bash
airflow connections add dwh_egisz_pg \
  --conn-uri 'postgresql://USER:PASSWORD@PG_HOST:5432/dwh_egisz?sslmode=disable'

# Firebird: проще задать по полям, schema = путь или алиас БД
airflow connections add proxy_egisz_fb \
  --conn-type generic \
  --conn-host FB_HOST --conn-port 3050 \
  --conn-schema '/path/or/alias/proxy_egisz' \
  --conn-login USER --conn-password PASSWORD \
  --conn-extra '{"charset":"WIN1251"}'
```

### 2.4. Пул `dwh_postgres` (обязательно)

Задачи трансформации, справочников и сверки объявлены с `pool="dwh_postgres"` —
эксклюзивный писатель DWH, 1 слот. **Если пула нет, задачи молча не планируются**: ни ошибки
в интерфейсе, ни падения — только строка «Tasks using non-existent pool 'dwh_postgres' will
not be scheduled» в логе планировщика, а экземпляры задач вечно висят в `scheduled`.

```bash
airflow pools set dwh_postgres 1 "Exclusive DWH transform / marts / maintenance"
```

### 2.5. Настройки DAG — переменные окружения

Все настройки читаются из переменной окружения `EGISZ_<KEY>` процессов Airflow, иначе из
словаря `DEFAULTS` в файле DAG. **Airflow Variables не используются**: их чтение при
парсинге DAG-файла в воркере на Airflow 3 уходит в supervisor RPC и подвешивает DAG.

| Env-переменная | Дефолт | Назначение |
| --- | --- | --- |
| `EGISZ_ETL_SCHEDULE` | `*/5 * * * *` | Расписание DAG фактов |
| `EGISZ_MAINTENANCE_SCHEDULE` | `@daily` | Расписание DAG обслуживания |
| `EGISZ_EXTRACT_RAW_ROWS` | `1000` | Размер батча выборки журнала обмена |
| `EGISZ_EXTRACT_RAW_ROUNDS` | `3` | Максимум циклов выборки за один запуск |
| `EGISZ_REGISTRY_ROWS` | `5000` | Размер батча выборки реестра подач |
| `EGISZ_REGISTRY_ROUNDS` | `3` | Максимум циклов выборки реестра за один запуск |
| `EGISZ_EXTRACT_DEPTH_DAYS` | `30` | Глубина выгрузки по `CREATEDATE` источника; `0` снимает ограничение |
| `EGISZ_TRANSFORM_ROWS` | `3000` | Размер батча `transform_raw_to_facts` |
| `EGISZ_TRANSFORM_ROUNDS` | `6` | Максимум циклов трансформации за один запуск |
| `EGISZ_CONSISTENCY_LOOKBACK_DAYS` | `7` | Окно суточной проверки полноты журнала |

Значения по умолчанию рабочие — переопределять не обязательно. Смена расписания подхватится
при следующем парсинге, параметров выполнения — при следующем запуске задачи.

Глубина выгрузки работает нижней границей отметки, а не условием отбора внутри страницы:
выборка идёт keyset-пагинацией по идентификатору, и фильтр по дате на старом хвосте вернул
бы пустую страницу — цикл принял бы её за конец данных. Поэтому отметка ниже окна
поднимается к его границе, а выше — остаётся на месте: отметки только растут. Чтобы забрать
историю глубже окна, задайте `EGISZ_EXTRACT_DEPTH_DAYS` больше нужного и опустите
`etl_state.extract_logid_cursor` вручную — сам по себе он вниз не пойдёт.

### 2.6. DAG, которые появятся

| dag_id | Расписание | Задачи |
| --- | --- | --- |
| `egisz_etl_dag` | `*/5` | `extract_exchangelog → extract_registry → sync_dictionaries → transform → recompute_documents → refresh_marts` |
| `egisz_maintenance_dag` | `@daily` | `consistency_check`, `maintain_partitions` |

Оба — `max_active_runs=1`, `catchup=False`. Курсоры `etl_state` двигает только DAG фактов
(через `GREATEST`, без отката): `extract_logid_cursor` и `extract_egmid_cursor` — задачи
выгрузки, `transform_logid_cursor` — разбор. `recompute_documents` и `refresh_marts`
завершаются статусом `skipped`, когда менять нечего, — это штатный исход. Новые DAG
появятся на паузе — снять после настройки Connections и пула.

### 2.7. Проверка

```bash
airflow dags list-import-errors          # ожидаемо пусто
airflow dags list | grep egisz           # два DAG в списке
airflow dags unpause egisz_etl_dag
airflow dags unpause egisz_maintenance_dag
```

Смоук: запустить `egisz_etl_dag` вручную и убедиться, что в DWH растут `exchangelog_raw`,
`documents`, `dim_message_document`, а курсоры `etl_state` продвинулись. Суточный
`consistency_check` при исправной выгрузке завершается статусом `skipped` — это ожидаемый
исход, а не сбой.

`ModuleNotFoundError: firebird` / `psycopg2` — не установлены зависимости (§2.2). Падение
`firebird` при подключении — не установлена `libfbclient` (§2.1). Задачи вечно в
`scheduled` — не создан пул `dwh_postgres` (§2.4).

---

## 3. Metabase

### 3.1. Предусловия

- Запущенный Metabase; проверено на **v0.61.1.5** и **v0.62.1** — импортёр содержит обходы
  особенностей API этих версий.
- Схема DWH применена (§1): импортёр проверяет наличие таблиц и представлений, на которые
  ссылаются карточки, и останавливается, если их нет.
- На хосте запуска: `bash`, `curl`, `jq`, `psql`, `sha256sum`; `flock` опционален (без него
  пропускается защита от параллельного запуска).
- Сетевой доступ к Metabase (HTTP) **и** к DWH PostgreSQL (проверка контракта идёт напрямую
  через `psql`).

### 3.2. Переменные окружения

Все значения — плейсхолдеры; секреты передавать только через окружение запуска.

| Переменная | Дефолт | Назначение |
| --- | --- | --- |
| `METABASE_URL` (или `MB_URL`) | `http://localhost:3000` | адрес целевого Metabase |
| `METABASE_API_KEY` | — | ключ API вместо логина и пароля (§3.4) |
| `ADMIN_EMAIL` / `METABASE_ADMIN_EMAIL` | `admin@egisz.local` | логин администратора |
| `ADMIN_PASSWORD` / `METABASE_ADMIN_PASSWORD` | `egisz` | пароль администратора |
| `METABASE_DASHBOARDS_DIR` | `/app/metabase_dashboards` | путь к JSON дашбордов (в пакете — задать явно) |
| `METABASE_MODELS_DIR` | `/app/metabase_models` | путь к JSON моделей (в пакете — задать явно) |
| `APP_DB_HOST` / `APP_DB_PORT` | `host.docker.internal` / `5432` | хост и порт DWH |
| `APP_DB_NAME` | `dwh_egisz` | БД DWH |
| `APP_DB_USER` / `APP_DB_PASSWORD` | `postgres` / `postgres` | учётка DWH для Metabase (нужна роль `egisz`) |
| `APP_DB_DISPLAY_NAME` | `DWH ЕГИСЗ` | имя подключения в Metabase |
| `METABASE_COLLECTION_NAME` | `Интеграция с ЕГИСЗ` | коллекция для карточек и дашбордов |
| `METABASE_MANAGE_INSTANCE_SETTINGS` | `false` | разрешить менять настройки всего инстанса (§3.5) |
| `METABASE_SITE_NAME` | `Интеграция с ЕГИСЗ` | имя инстанса (применяется только при §3.5) |
| `METABASE_FORCE_PROVISION` | `auto` | `always` — переимпорт при неизменных JSON |
| `METABASE_PUBLIC_CLIENT_DASHBOARD` | `true` | публичная ссылка клиентского дашборда; любое другое значение — ссылка не создаётся |
| `METABASE_AUTO_APPLY_FILTERS` | `true` | автоприменение фильтров на дашбордах |

### 3.3. Запуск

Из каталога `metabase/` пакета:

```bash
cd <корень-пакета>/metabase
METABASE_URL=https://metabase.example.org \
METABASE_API_KEY="${METABASE_API_KEY}" \
METABASE_DASHBOARDS_DIR="$PWD/dashboards" \
METABASE_MODELS_DIR="$PWD/models" \
APP_DB_HOST=PG_HOST APP_DB_NAME=dwh_egisz \
APP_DB_USER=egisz APP_DB_PASSWORD="${APP_DB_PASSWORD}" \
./setup-dashboards.sh
```

Привязка фильтров к полям записана в JSON дашбордов ключами `metabase-field-filters` —
импортёр читает только `*.json`, отдельного файла правил нет.

### 3.4. Ключ API

Если парольная учётка недоступна, вместо `ADMIN_EMAIL` / `ADMIN_PASSWORD` задаётся
`METABASE_API_KEY` (Admin → Authentication → API keys; группа ключа — администраторы).
Импортёр проверяет права ключа через `/api/user/current` и не выполняет сессионный логин.
Ветка первичной инициализации `/api/setup` с ключом недоступна — инстанс должен быть уже
инициализирован.

### 3.5. Настройки всего инстанса

Глобальный часовой пояс, локаль, формат валюты и времени, кеш результатов и
`enable-public-sharing` — настройки **всего** Metabase: на общем инстансе они меняют
поведение чужих сервисов. По умолчанию импортёр их не трогает
(`METABASE_MANAGE_INSTANCE_SETTINGS=false`) и работает только со своей коллекцией и своим
подключением к БД; часовой пояс `Europe/Moscow` на подключении ставится всегда — он
ограничен нашей БД. Публичная ссылка клиентского дашборда создаётся и без флага, если
`enable-public-sharing` уже включён владельцем инстанса; иначе шаг завершается записью в лог.

### 3.6. Что делает импортёр (идемпотентно, безопасно повторять)

1. Ждёт `/api/health`; на неинициализированном Metabase создаёт администратора через
   `/api/setup`, иначе логинится (с ключом API — проверка прав вместо логина).
2. Регистрирует или переиспользует подключение к DWH и ставит на нём `report-timezone
   Europe/Moscow`; настройки всего инстанса — только при явном разрешении (§3.5).
3. Проверяет контракт DWH, синхронизирует метаданные схемы.
4. Создаёт или обновляет Metabase Models из `models/*.json`.
5. Создаёт или обновляет карточки и дашборды (вкладки, фильтры, drill-through, публичная
   ссылка клиентского дашборда); архивирует карточки и дашборды коллекции, которых больше
   нет в JSON.

Пропуск импорта при неизменных JSON опирается на sha256-манифест предыдущего запуска
(`/tmp/metabase-dashboards.sha256`): если каталог `/tmp` между запусками не сохраняется,
импорт всегда полный. Принудительный полный проход — `METABASE_FORCE_PROVISION=always`.

---

## 4. Сборка пакета (на стороне проекта)

```powershell
.\scripts\build_external_bundle.ps1        # dist\egisz-bi\
.\scripts\build_external_bundle.ps1 -Zip   # + dist\egisz-bi.zip
```

Пакет собирается из канонических исходников репозитория (`db/`, `dags/`, `metabase/`,
`metabase_dashboards/`, `metabase_models/`) — копии в git не хранятся, чтобы исключить их
дрейф. Рядом лежат сценарии рабочего места оператора `deploy/apply-dwh-schema.ps1` и
`deploy/import-metabase.ps1`: они выполняют §1.3–§1.6 и §3.3 с рабочей станции Windows и в
пакет не входят.
