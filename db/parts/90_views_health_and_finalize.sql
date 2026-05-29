-- ============================================================================
-- 90_views_health_and_finalize.sql — v_health_* views, final GRANT verification,
-- refresh, and ANALYZE.
-- ============================================================================

-- Business backfill lives only in public.egisz_transform_raw_to_facts().

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
WITH anchor AS (
    SELECT COALESCE(MAX(COALESCE(last_callback_at, sent_at, document_created_at)), now()) AS ref_ts
    FROM public.fact_egisz_documents
),
fact_24h AS (
    SELECT
        d.jid::text AS clinic_jid,
        MAX(COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || d.jid::text)) AS clinic_name,
        COUNT(DISTINCT d.document_key)::bigint AS docs_cnt,
        COUNT(DISTINCT d.document_key) FILTER (WHERE d.status IN ('async_error', 'network_error'))::bigint AS err_cnt
    FROM public.fact_egisz_documents d
    CROSS JOIN anchor
    LEFT JOIN public.dim_organizations o ON o.jid = d.jid
    WHERE COALESCE(d.last_callback_at, d.sent_at, d.document_created_at) >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY d.jid
),
queue AS (
    SELECT jid::text AS clinic_jid, COUNT(DISTINCT document_key)::bigint AS queue_cnt
    FROM public.fact_egisz_documents
    WHERE status = 'waiting'
    GROUP BY jid
)
SELECT
    f.clinic_jid AS "JID клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f.clinic_jid) AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.queue_cnt, 0)::bigint AS "В очереди (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.queue_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.queue_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN queue q ON q.clinic_jid = f.clinic_jid;

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM public.fact_egisz_documents)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(DISTINCT document_key) FROM public.fact_egisz_documents WHERE status = 'waiting')::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT document_key) FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT document_key) FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at >= now() - INTERVAL '24 hours' AND sent_at < now() - INTERVAL '1 hour')::bigint AS "Очередь 1-24ч",
    (SELECT COUNT(DISTINCT document_key) FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT MAX(sent_at) FROM public.fact_egisz_documents) AS "DWH max Sent",
    (SELECT updated_at FROM elt_state WHERE pipeline = 'egisz') AS "Последний апдейт курсора",
    (SELECT last_logid FROM elt_state WHERE pipeline = 'egisz') AS "elt_state.last_logid",
    (SELECT MAX(callback_log_id) FROM public.fact_egisz_documents) AS "DWH max LOGID fact",
    (SELECT COUNT(DISTINCT document_key) FROM public.fact_egisz_documents)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
WITH anchor AS (
    SELECT MAX(COALESCE(last_callback_at, sent_at, document_created_at)) AS last_fact_ts
    FROM public.fact_egisz_documents
)
SELECT * FROM (
    VALUES
        ('parsed_documents', 'Разложенные документы proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM public.fact_egisz_documents), 'документов', 'fact_egisz_documents', 'Контроль поступления СЭМД в DWH'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT document_key)::numeric FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at < now() - INTERVAL '24 hours'), 'документов', 'fact_egisz_documents.status=waiting', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT document_key)::numeric FROM public.fact_egisz_documents WHERE status = 'network_error'), 'документов', 'fact_egisz_documents.status=network_error', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки асинхронного ответа РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM public.fact_egisz_documents WHERE status = 'async_error'), 'документов', 'fact_egisz_documents.status=async_error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('pending_backlog_24h',
         'Документы в обработке > 24ч (backlog)',
         CASE
             WHEN (SELECT COUNT(*) FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at < now() - INTERVAL '72 hours') >= 100 THEN 'red'
             WHEN (SELECT COUNT(*) FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at < now() - INTERVAL '72 hours') >= 20  THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM public.fact_egisz_documents WHERE status = 'waiting' AND sent_at < now() - INTERVAL '72 hours'),
         'документов > 72ч',
         'v_rpt_documents_no_response_ui (просрочено)',
         'Проверить транспорт клиник в дашборде 03; pending — норма, просрочено — эскалация'),
        ('unknown_high',
         'Доля «Нераспознан» (status=unknown)',
         'green',
         0::numeric,
         '% (за 24ч)',
         'fact_egisz_documents: no unknown document status',
         'Проверить regex egisz_classify_async_status, если появится новый шаблон ответа РЭМД'),
        ('data_freshness',
         'Свежесть данных (последний факт)',
         CASE
             WHEN (SELECT last_fact_ts FROM anchor) IS NULL THEN 'red'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '1 hour'  THEN 'green'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '24 hours' THEN 'yellow'
             ELSE 'red'
         END,
         ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT last_fact_ts FROM anchor), now()))) / 60.0, 1)::numeric,
         'минут с последнего факта',
         'fact_egisz_documents.last_callback_at/sent_at',
         'Проверить ELT-цикл, Airflow scheduler и доступ к Firebird')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    LOOP
        IF r.relkind IN ('r', 'p') THEN
            EXECUTE format('ALTER TABLE public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'v' THEN
            EXECUTE format('ALTER VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'S' THEN
            EXECUTE format('ALTER SEQUENCE public.%I OWNER TO egisz', r.relname);
        END IF;
    END LOOP;

    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO egisz', r.sig);
    END LOOP;
END;
$$;

DO $$
DECLARE
    can_create boolean;
    can_usage  boolean;
BEGIN
    SELECT
        has_schema_privilege('egisz', 'public', 'CREATE'),
        has_schema_privilege('egisz', 'public', 'USAGE')
    INTO can_create, can_usage;

    IF NOT (can_create AND can_usage) THEN
        RAISE EXCEPTION 'egisz is still missing public schema privileges';
    END IF;
END;
$$;

UPDATE public.fact_egisz_documents d
SET error_text = src.error_text,
    updated_at = now()
FROM (
    SELECT DISTINCT ON (f.document_key)
        f.document_key,
        COALESCE(NULLIF(btrim(f.error_json_text), ''), f.message) AS error_text
    FROM public.fact_egisz_transactions f
    WHERE COALESCE(NULLIF(btrim(f.error_json_text), ''), NULLIF(btrim(f.message), '')) IS NOT NULL
    ORDER BY f.document_key, f.log_date DESC NULLS LAST, f.exchangelog_log_id DESC
) src
WHERE d.document_key = src.document_key
  AND d.error_text IS DISTINCT FROM src.error_text;

-- Полная сборка витрины при init. Дальше её сопровождает egisz_transform_raw_to_facts
-- инкрементально (по затронутым document_key), без полного пересчёта каждые 5 минут.
TRUNCATE public.v_egisz_documents_enriched_ui;
INSERT INTO public.v_egisz_documents_enriched_ui
SELECT * FROM public.v_egisz_documents_enriched_src;
REFRESH MATERIALIZED VIEW public.v_egisz_documents_daily_ui;
ANALYZE public.exchangelog_raw;
ANALYZE public.fact_egisz_documents;
ANALYZE public.fact_egisz_transactions;
ANALYZE public.v_egisz_documents_enriched_ui;
ANALYZE public.v_egisz_documents_daily_ui;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
