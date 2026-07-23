-- ============================================================================
-- 00_bootstrap.sql — Header, role, grants
-- Loaded by db/dwh_init.sql via \i db/parts/00_bootstrap.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

\encoding UTF8
-- DWH initialization script for EGISZ proxy reports.
-- Run once (and re-run safely on updates) as PostgreSQL superuser against dwh_egisz.
--
-- Prerequisites — execute as superuser against the 'postgres' database:
--   CREATE ROLE egisz LOGIN PASSWORD 'egisz';
--   CREATE DATABASE dwh_egisz;
--
-- Usage:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql


-- Idempotent role creation
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'egisz') THEN
        EXECUTE format('CREATE ROLE egisz LOGIN PASSWORD %L', 'egisz');
    END IF;
END;
$$;

-- Все компоненты работают в МСК. Extract пишет наивное Firebird-время (EXCHANGELOG.CREATEDATE,
-- лицензии) как timestamptz: без фиксированного пояса сессии Postgres пометил бы его дефолтом
-- сервера (не МСК) и сдвинул бы момент. Пин роли на Europe/Moscow гарантирует, что и ingest,
-- и чтение раскладывают сутки по московской границе.
ALTER ROLE egisz SET timezone TO 'Europe/Moscow';

GRANT CONNECT ON DATABASE dwh_egisz TO egisz;
GRANT USAGE, CREATE ON SCHEMA public TO egisz;

