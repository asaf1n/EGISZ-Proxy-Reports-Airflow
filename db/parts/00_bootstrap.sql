-- ============================================================================
-- 00_bootstrap.sql — Header, role, grants
-- Source: db/dwh_init.sql, lines [1..27).
-- Loaded by db/dwh_init.sql via \i db/parts/00_bootstrap.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
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

GRANT CONNECT ON DATABASE dwh_egisz TO egisz;
GRANT USAGE, CREATE ON SCHEMA public TO egisz;

