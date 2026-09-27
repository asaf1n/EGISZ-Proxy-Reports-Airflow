\encoding UTF8
-- ============================================================================
-- dwh_init.sql — idempotent build of EGISZ DWH schema.
--
-- Mandatory one-time bootstrap (run against maintenance DB `postgres`):
--   CREATE ROLE egisz LOGIN PASSWORD 'egisz';
--   CREATE DATABASE dwh_egisz OWNER egisz;
--
-- Usage (run from the repository/bundle root — parts are included by relative \i):
--   psql -U egisz -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
-- ============================================================================

\set ON_ERROR_STOP on

SET lock_timeout = '30s';
SET statement_timeout = '60min';
-- Все объекты адресуются схемой слоя. search_path без пользовательских схем: имя без схемы
-- падает сразу, а не уходит в схему, которую подставила настройка базы (в общей базе она
-- указывает на чужие схемы).
SET search_path = pg_catalog;

DO $$
BEGIN
    IF current_database() <> 'dwh_egisz' THEN
        RAISE EXCEPTION 'dwh_init.sql must run against dwh_egisz, current DB: %', current_database();
    END IF;
END
$$;

-- Таблица роли в public означает, что данные ещё не перенесены в схемы слоёв: модули
-- создали бы рядом пустые таблицы, и конвейер писал бы в них.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relkind IN ('r', 'p')
          AND c.relowner = current_user::regrole
    ) THEN
        RAISE EXCEPTION 'public still holds tables of role %: move them into the layer schemas first', current_user;
    END IF;
END
$$;

\i db/01_schema.sql
\i db/02_functions.sql
\i db/03_transform.sql
\i db/04_views.sql

\echo 'DWH init complete: dwh_egisz schema is up to date'
