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
- Роль `egisz` существует и **владеет** целевой БД (создаётся один раз администратором, п.1).

## 1. Разовый bootstrap (однократно, администратором с правом `CREATE ROLE/DATABASE`)

```sql
CREATE ROLE egisz LOGIN PASSWORD '<пароль>';
CREATE DATABASE dwh_egisz OWNER egisz;   -- egisz как владелец получает и public-схему
```

Роль `egisz` — рабочая учётка конвейера и BI; пароль передать администраторам Airflow и
Metabase (Connections/`APP_DB_*`), в файлы не записывать. Дальше суперпользователь не нужен:
весь `dwh_init.sql` (части 00–90) идёт под `egisz`.

## 2. Применение схемы (под ролью `egisz`)

**Строго из корня бандла** — `dwh_init.sql` подключает части относительными путями
(`\i db/parts/...`):

```bash
cd <корень-бандла>
psql -h PG_HOST -U egisz -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

`00_bootstrap.sql` больше не создаёт роль и не требует `postgres`: он пинит пояс роли
(`ALTER ROLE egisz SET ...` — роль вправе для себя) и фиксирует гранты владельца. Часть 90
переназначает владельца объектов на `egisz` (no-op, раз объекты создаёт сам `egisz`).

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
