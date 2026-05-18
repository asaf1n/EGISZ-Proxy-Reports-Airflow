\encoding UTF8
-- ============================================================================
-- DWH initialization script for EGISZ proxy reports.
-- Run once (and re-run safely on updates) as PostgreSQL superuser against
-- dwh_egisz.
--
-- Prerequisites — execute as superuser against the 'postgres' database:
--   CREATE ROLE egisz LOGIN PASSWORD 'egisz';
--   CREATE DATABASE dwh_egisz;
--
-- Usage:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
--
-- Script is broken into ordered modules under db/parts/. Order matters:
-- tables → parsing helpers → error rules → error functions → transform →
-- drop dependent views → recreate views (core, stg, rpt, health). Modules
-- are individually idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE,
-- ALTER ... IF EXISTS) — the same as the original monolith.
-- ============================================================================

\set ON_ERROR_STOP on

SET lock_timeout = '30s';
SET statement_timeout = '60min';

\i db/parts/00_bootstrap.sql
\i db/parts/10_tables.sql
\i db/parts/20_functions_parsing.sql
\i db/parts/30_error_rules.sql
\i db/parts/40_functions_errors.sql
\i db/parts/50_transform.sql
\i db/parts/60_drop_dependents.sql
\i db/parts/70_views_core.sql
\i db/parts/75_views_stg.sql
\i db/parts/80_views_rpt.sql
\i db/parts/90_views_health_and_finalize.sql

\echo 'DWH init complete: see db/parts/ for individual modules'
