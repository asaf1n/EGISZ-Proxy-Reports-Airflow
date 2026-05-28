\encoding UTF8
-- ============================================================================
-- dwh_erase.sql — destructive cleanup of DWH schema in dwh_egisz.
--
-- What this script does:
--   1) terminates active sessions to dwh_egisz,
--   2) drops and recreates schema public in dwh_egisz,
--   3) restores baseline schema grants and keeps role egisz.
--
-- Firebird is NOT touched by this script.
--
-- Usage (PowerShell):
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -v CONFIRM_DWH_ERASE=1 -f db/dwh_erase.sql
--
-- After successful cleanup:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
-- ============================================================================

\set ON_ERROR_STOP on
\if :{?CONFIRM_DWH_ERASE}
\else
\set CONFIRM_DWH_ERASE 0
\endif

\if :CONFIRM_DWH_ERASE
\else
\echo 'Refusing to run destructive cleanup: pass -v CONFIRM_DWH_ERASE=1'
\quit 3
\endif

SELECT CASE WHEN current_database() = 'dwh_egisz' THEN 1 ELSE 0 END AS is_dwh \gset
\if :is_dwh
\else
\echo 'dwh_erase.sql must be executed against database dwh_egisz'
\quit 4
\endif

\echo 'Terminating active sessions to dwh_egisz...'
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid();

\echo 'Dropping and recreating public schema in dwh_egisz...'
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT USAGE, CREATE ON SCHEMA public TO PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;
COMMENT ON SCHEMA public IS 'standard public schema';

\echo 'Ensuring role egisz exists...'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'egisz') THEN
        CREATE ROLE egisz LOGIN PASSWORD 'egisz';
    END IF;
END
$$;

\echo 'DWH schema cleanup complete.'
\echo 'Next step: psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql'
