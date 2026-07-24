-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views/marts before re-creating them.
-- CREATE OR REPLACE VIEW не меняет состав колонок, поэтому rpt-слой дропается
-- целиком и собирается заново в 80–90.
-- ============================================================================

DROP VIEW IF EXISTS public.rpt_health_by_clinic CASCADE;
DROP VIEW IF EXISTS public.rpt_health_signals CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.rpt_health_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_network_errors CASCADE;
-- Недельный (85_views_weekly) и месячный (86_views_monthly) слои зависят от
-- rpt_documents/rpt_error_breakdown —
-- дропаем до них. Эти объекты никогда не существовали как plain VIEW, relkind-обход
-- (как у rpt_error_breakdown ниже) не нужен.
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_monthly CASCADE;
-- rpt_error_breakdown — MATERIALIZED VIEW. DROP VIEW IF EXISTS / DROP MATERIALIZED VIEW
-- IF EXISTS подавляют только отсутствие объекта, но НЕ несовпадение типа (VIEW vs MATVIEW),
-- поэтому дропаем по фактическому relkind.
DO $$
DECLARE
    kind "char";
BEGIN
    SELECT c.relkind INTO kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'rpt_error_breakdown';

    IF kind = 'm' THEN
        DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE;
    ELSIF kind IS NOT NULL THEN
        DROP VIEW public.rpt_error_breakdown CASCADE;
    END IF;
END $$;
DROP VIEW IF EXISTS public.rpt_documents CASCADE;
DROP VIEW IF EXISTS public.rpt_document_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;

-- Классификация даёт документу два поля: error_types (канонические типы,
-- error_classify) и error_text (исходные <message>, error_messages_row).
-- Прочие интерпретаторы и канонизация на чтении вне контракта; их дроп и снятие
-- error_summary идут ПОСЛЕ дропа витрин выше — rpt-слой на них ссылается.
DROP FUNCTION IF EXISTS public.error_interpretation_schematron_chunk(text);
DROP FUNCTION IF EXISTS public.error_interpretation_item(text, text);
DROP FUNCTION IF EXISTS public.error_interpretation_row(jsonb);
DROP FUNCTION IF EXISTS public.error_atom_normalize(text);
DROP FUNCTION IF EXISTS public.canonical_error_atom(text);
DROP FUNCTION IF EXISTS public.canonical_error_list(text);
DROP FUNCTION IF EXISTS public.error_category(text);
ALTER TABLE public.documents DROP COLUMN IF EXISTS error_summary;
ALTER TABLE public.transactions DROP COLUMN IF EXISTS error_summary;
-- Мёртвый предшественник dim_error_rules: жил только в старых развёртываниях,
-- ни одна функция его не читает.
DROP TABLE IF EXISTS public.error_interpretation_rules;
