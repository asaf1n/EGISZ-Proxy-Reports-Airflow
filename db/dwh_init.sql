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

\i db/parts/00_bootstrap.sql
\i db/parts/10_tables.sql
\i db/parts/20_functions_parsing.sql
\i db/parts/30_error_rules.sql
\i db/parts/40_functions_errors.sql
\i db/parts/50_transform.sql
\i db/parts/60_drop_dependents.sql
\i db/parts/70_views_core.sql
\i db/parts/80_views_rpt.sql
\i db/parts/85_views_weekly.sql
\i db/parts/86_views_monthly.sql
\i db/parts/90_views_health_and_finalize.sql

\echo 'DWH init complete: dwh_egisz schema is up to date'
