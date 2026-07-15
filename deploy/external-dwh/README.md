# Развёртывание DWH-схемы в стороннем PostgreSQL

Самодостаточный бандл со схемой DWH `dwh_egisz`: таблицы, функции парсинга/трансформации,
правила классификации ошибок, отчётные вьюхи. Один идемпотентный вход — `db/dwh_init.sql`.

```
dwh/                         # корень бандла (dist/external/dwh)
├── db/
│   ├── dwh_init.sql         # точка входа (подключает parts относительными \i)
│   └── parts/               # 00_bootstrap … 90_views_health_and_finalize
├── README.md
└── BUILD_INFO.txt
```

## 0. Предусловия

- PostgreSQL 14+ и клиент `psql`.
- Роль с правом создавать роли/объекты в целевой БД (обычно `postgres`).

## 1. Разовый bootstrap (однократно, из maintenance-БД `postgres`)

```sql
CREATE ROLE egisz LOGIN PASSWORD '<пароль>';
CREATE DATABASE dwh_egisz OWNER postgres;
```

Роль `egisz` — рабочая учётка конвейера и BI; пароль передать администраторам Airflow и
Metabase (Connections/`APP_DB_*`), в файлы не записывать.

> Если роль не создать заранее, `00_bootstrap.sql` создаст её сам — но с дефолтным
> паролем `egisz`. В этом случае сразу после наката: `ALTER ROLE egisz PASSWORD '<пароль>';`.

## 2. Применение схемы

**Строго из корня бандла** — `dwh_init.sql` подключает части относительными путями
(`\i db/parts/...`):

```bash
cd <корень-бандла>
psql -h PG_HOST -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Скрипт идемпотентен (`CREATE ... IF NOT EXISTS`, `CREATE OR REPLACE`, `INSERT ... ON
CONFLICT`): повторный прогон обязан пройти чисто — так же накатываются и обновления схемы.

## 3. Часовой пояс (важно)

`00_bootstrap.sql` закрепляет `ALTER ROLE egisz SET timezone TO 'Europe/Moscow'` — наивные
даты Firebird-журнала интерпретируются как МСК. Все сессии конвейера и Metabase должны
логиниться ролью `egisz` (или ролью с тем же пином), иначе сместятся границы суток.

## 4. Проверка

```bash
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dt public.*"      # elt_state, exchangelog_raw, documents, transactions, dim_*
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dv public.rpt_*"  # вьюхи: rpt_documents, rpt_document_versions, rpt_health_*
psql -h PG_HOST -U egisz -d dwh_egisz -c "\dm public.rpt_*"  # matview: rpt_error_breakdown
psql -h PG_HOST -U egisz -d dwh_egisz -c "SHOW timezone"     # Europe/Moscow
```

Успешный прогон заканчивается сообщением `DWH init complete` в выводе psql.
