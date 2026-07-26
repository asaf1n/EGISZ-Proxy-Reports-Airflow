\encoding UTF8
-- ============================================================================
-- dwh_init.sql — idempotent build of EGISZ DWH schema.
--
-- Mandatory one-time bootstrap (run against maintenance DB `postgres`):
--   CREATE ROLE egisz LOGIN PASSWORD 'egisz';
--   CREATE DATABASE dwh_egisz OWNER postgres;
--
-- Usage:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
-- ============================================================================

\set ON_ERROR_STOP on

SET lock_timeout = '30s';
SET statement_timeout = '60min';

DO $$
BEGIN
    IF current_database() <> 'dwh_egisz' THEN
        RAISE EXCEPTION 'dwh_init.sql must run against dwh_egisz, current DB: %', current_database();
    END IF;
END
$$;

\i db/01_schema.sql
\i db/02_functions.sql
\i db/03_transform.sql
\i db/04_views.sql

\echo 'DWH init complete: dwh_egisz schema is up to date'
