-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views/marts before re-creating them.
-- ============================================================================

DROP VIEW IF EXISTS public.rpt_health_by_clinic CASCADE;
DROP VIEW IF EXISTS public.rpt_health_signals CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.rpt_health_versions CASCADE;
DROP VIEW IF EXISTS public.v_rpt_client_kpi_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_clinic_semd_slice_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_client_documents_ui CASCADE;
DROP VIEW IF EXISTS public.rpt_network_errors CASCADE;
-- rpt_error_breakdown стал MATERIALIZED VIEW. DROP VIEW IF EXISTS / DROP MATERIALIZED VIEW
-- IF EXISTS подавляют только отсутствие объекта, но НЕ несовпадение типа (VIEW vs MATVIEW),
-- поэтому дропаем по фактическому relkind — идемпотентно при переходе и пересборках.
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
DROP VIEW IF EXISTS public.v_rpt_error_interpretations_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui CASCADE;
DROP VIEW IF EXISTS public.rpt_documents CASCADE;
DROP VIEW IF EXISTS public.rpt_document_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document CASCADE;
DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document CASCADE;
DROP TABLE IF EXISTS public.dim_exchangelog_refs CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;
DROP TABLE IF EXISTS public.v_documents_enriched_ui CASCADE;
DROP VIEW IF EXISTS public.v_documents_enriched_ui CASCADE;
DROP VIEW IF EXISTS public.v_documents_enriched_src CASCADE;

-- documents.sent_at дублировал first_sent_at (оба = min(createdate)). Схлопываем в
-- first_sent_at ПОСЛЕ дропа зависимых витрин выше — иначе DROP COLUMN упрётся в них.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'documents'
                 AND column_name = 'sent_at') THEN
        UPDATE public.documents SET first_sent_at = sent_at
            WHERE first_sent_at IS NULL AND sent_at IS NOT NULL;
        ALTER TABLE public.documents DROP COLUMN sent_at;
    END IF;
END $$;

-- Легаси-дубль fact_documents остался после консолидации fact_*→documents: RENAME в
-- 10_tables пропускается, когда documents уже есть, и старая таблица зависает с устаревшими
-- витринами v_health_*_ui / v_rpt_network_errors_detail_ui (ссылаются на снятый sent_at и
-- джойнят dim_organizations по jid). Они блокируют расширение jid→bigint ниже. Сносим дубль
-- вместе с его витринами — только когда боевая documents на месте, чтобы не задеть данные.
DO $$
BEGIN
    IF to_regclass('public.documents') IS NOT NULL
       AND to_regclass('public.fact_documents') IS NOT NULL THEN
        DROP TABLE public.fact_documents CASCADE;
    END IF;
END $$;

-- JPERSONS.JID перерос int4 (psycopg2 NumericValueOutOfRange при UPSERT справочников).
-- Расширяем ключ клиники до bigint в справочниках и фактах. ALTER COLUMN TYPE упёрся бы
-- в зависимые витрины, поэтому идёт ПОСЛЕ их дропа выше. Идемпотентно: повторный прогон
-- на уже bigint-колонке не переписывает таблицу.
ALTER TABLE public.dim_organizations ALTER COLUMN jid TYPE bigint;
ALTER TABLE public.dim_licenses ALTER COLUMN jid TYPE bigint;
ALTER TABLE public.documents ALTER COLUMN jid TYPE bigint;
ALTER TABLE public.transactions ALTER COLUMN jid TYPE bigint;
ALTER TABLE public.transactions ALTER COLUMN xml_jid TYPE bigint;
