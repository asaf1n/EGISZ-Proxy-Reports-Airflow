-- ============================================================================
-- 90_views_health_and_finalize.sql — v_health_* views, final GRANT verification,
-- refresh, and ANALYZE.
-- ============================================================================

INSERT INTO public.fact_egisz_documents (document_key, local_uid, document_id, semd_code, source_logid, updated_at)
SELECT DISTINCT ON (document_key)
    document_key,
    local_uid,
    document_id,
    semd_code,
    logid,
    now()
FROM (
    SELECT
        lower(NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '')) AS document_key,
        public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'localUid')) AS local_uid,
        public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')) AS document_id,
        public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) AS semd_code,
        sr.logid
    FROM public.exchangelog_raw sr
    WHERE COALESCE(public.egisz_xml_text(sr.msgtext, 'action'), '') = 'getDocumentFile'
      AND NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL
      AND public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) IS NOT NULL

    UNION ALL

    SELECT
        lower(NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')), '')) AS document_key,
        public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'localUid')) AS local_uid,
        public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')) AS document_id,
        public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) AS semd_code,
        sr.logid
    FROM public.exchangelog_raw sr
    WHERE COALESCE(public.egisz_xml_text(sr.msgtext, 'action'), '') = 'getDocumentFile'
      AND NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')), '') IS NOT NULL
      AND public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) IS NOT NULL
) src
WHERE document_key IS NOT NULL
ORDER BY document_key, logid DESC
ON CONFLICT (document_key) DO UPDATE SET
    local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
    document_id = COALESCE(EXCLUDED.document_id, public.fact_egisz_documents.document_id),
    semd_code = EXCLUDED.semd_code,
    source_logid = GREATEST(public.fact_egisz_documents.source_logid, EXCLUDED.source_logid),
    updated_at = now()
WHERE public.fact_egisz_documents.source_logid IS NULL
   OR public.fact_egisz_documents.source_logid <= EXCLUDED.source_logid;

INSERT INTO public.fact_egisz_channel_errors (
    id, created_at, error_code, message, error_top_type, error_global_subcategory,
    error_group_label_ru, exchangelog_log_id, journal_msgid, egisz_messages_egmid,
    relates_to_hint, local_uid_hint, emdr_id_hint, document_group_key, relates_to_id,
    updated_at
)
SELECT
    r.logid,
    COALESCE(r.createdate, r.loaded_at),
    CASE WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3' ELSE 'PARSE_ERROR' END,
    COALESCE(NULLIF(r.logtext, ''), NULLIF(r.msgtext, ''), '(без текста)'),
    CASE WHEN r.logstate = 3 THEN 'network' ELSE 'async_response' END,
    CASE WHEN r.logstate = 3 THEN 'Сетевая ошибка' ELSE 'Неизвестная ошибка' END,
    CASE WHEN r.logstate = 3 THEN 'Ошибка связи' ELSE 'Неизвестная ошибка' END,
    r.logid,
    r.msgid,
    m.egmid,
    COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext, x.relates_to_message_logtext),
    COALESCE(x.local_uid_msgtext, x.document_id_msgtext, m.document_id),
    x.emdr_id_msgtext,
    COALESCE(
        x.local_uid_msgtext,
        x.document_id_msgtext,
        x.emdr_id_msgtext,
        x.relates_to_message_msgtext,
        x.relates_to_msgtext,
        m.document_id,
        r.msgid,
        r.logid::text
    ),
    COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext),
    now()
FROM public.exchangelog_raw r
LEFT JOIN LATERAL (
    SELECT
        public.egisz_xml_text(r.msgtext, 'relatesToMessage') AS relates_to_message_msgtext,
        public.egisz_xml_text(r.msgtext, 'relatesTo') AS relates_to_msgtext,
        public.egisz_xml_text(r.logtext, 'relatesToMessage') AS relates_to_message_logtext,
        public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_msgtext,
        public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_msgtext,
        public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id_msgtext
) x ON TRUE
LEFT JOIN LATERAL (
    SELECT em.*
    FROM public.fact_egisz_messages em
    WHERE em.document_id_norm IN (
            lower(NULLIF(btrim(x.local_uid_msgtext), '')),
            lower(NULLIF(btrim(x.document_id_msgtext), '')),
            lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
          )
       OR em.msgid_norm = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext))
       OR em.msgid_norm = public.egisz_normalize_message_id(r.msgid)
    ORDER BY
        CASE
            WHEN em.document_id_norm IN (
                lower(NULLIF(btrim(x.local_uid_msgtext), '')),
                lower(NULLIF(btrim(x.document_id_msgtext), '')),
                lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
            ) THEN 0
            WHEN em.msgid_norm = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext)) THEN 1
            ELSE 2
        END,
        em.egmid DESC
    LIMIT 1
) m ON TRUE
WHERE r.logstate = 3
   OR COALESCE(r.msgtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%ошиб%'
ON CONFLICT (id) DO UPDATE SET
    created_at = EXCLUDED.created_at,
    error_code = EXCLUDED.error_code,
    message = EXCLUDED.message,
    error_top_type = EXCLUDED.error_top_type,
    error_global_subcategory = EXCLUDED.error_global_subcategory,
    error_group_label_ru = EXCLUDED.error_group_label_ru,
    exchangelog_log_id = EXCLUDED.exchangelog_log_id,
    journal_msgid = EXCLUDED.journal_msgid,
    egisz_messages_egmid = EXCLUDED.egisz_messages_egmid,
    relates_to_hint = EXCLUDED.relates_to_hint,
    local_uid_hint = EXCLUDED.local_uid_hint,
    emdr_id_hint = EXCLUDED.emdr_id_hint,
    document_group_key = EXCLUDED.document_group_key,
    relates_to_id = EXCLUDED.relates_to_id,
    updated_at = now();

UPDATE public.fact_egisz_transactions f
SET local_uid_semd = public.egisz_clean_text_value(f.local_uid_semd),
    emdr_id = public.egisz_clean_text_value(f.emdr_id),
    doc_number = public.egisz_clean_text_value(f.doc_number),
    org_oid = public.egisz_clean_text_value(f.org_oid)
WHERE f.local_uid_semd IS DISTINCT FROM public.egisz_clean_text_value(f.local_uid_semd)
   OR f.emdr_id IS DISTINCT FROM public.egisz_clean_text_value(f.emdr_id)
   OR f.doc_number IS DISTINCT FROM public.egisz_clean_text_value(f.doc_number)
   OR f.org_oid IS DISTINCT FROM public.egisz_clean_text_value(f.org_oid);

UPDATE public.fact_egisz_transactions f
SET semd_code = k.semd_code,
    semd_name = NULL
FROM public.fact_egisz_documents k
WHERE k.document_key = lower(COALESCE(
        public.egisz_clean_text_value(f.local_uid_semd),
        public.egisz_clean_text_value(f.doc_number)
    ))
  AND (
      f.semd_code IS DISTINCT FROM k.semd_code
      OR f.semd_name IS NOT NULL
  );

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
WITH anchor AS (
    SELECT COALESCE(MAX("Обработано IPS"), now()) AS ref_ts
    FROM public.v_egisz_transactions_enriched_ui
),
fact_24h AS (
    SELECT
        "JID клиники",
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS docs_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_cnt
    FROM public.v_egisz_transactions_enriched_ui, anchor
    WHERE "Обработано IPS" >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY 1
),
queue AS (
    SELECT "JID клиники", COUNT(DISTINCT "localUid СЭМД")::bigint AS queue_cnt
    FROM public.v_rpt_documents_no_response_ui
    GROUP BY 1
)
SELECT
    f."JID клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f."JID клиники") AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.queue_cnt, 0)::bigint AS "В очереди (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.queue_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.queue_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN queue q ON q."JID клиники" = f."JID клиники";

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM public.fact_egisz_messages)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(*) FROM public.fact_egisz_messages WHERE egmid IS NULL)::bigint AS "Без EGMID",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui)::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '24 hours' AND "Отправлено" < now() - INTERVAL '1 hour')::bigint AS "Очередь 1–24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT MAX(egmid) FROM public.fact_egisz_messages) AS "DWH max EGMID",
    (SELECT MAX(created_at) FROM public.fact_egisz_messages) AS "DWH max Sent",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт курсора",
    (SELECT MAX(last_log_id) FROM elt_state) AS "elt_state.last_log_id",
    (SELECT MAX(last_egmid) FROM elt_state) AS "elt_state.last_egmid (курсор EGISZ_MESSAGES)",
    (SELECT MAX(exchangelog_log_id) FROM public.fact_egisz_transactions) AS "DWH max LOGID fact",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
WITH anchor AS (
    SELECT MAX(log_date) AS last_fact_ts FROM public.fact_egisz_transactions
)
SELECT * FROM (
    VALUES
        ('parsed_messages', 'Разложенные сообщения proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM public.fact_egisz_messages), 'строк', 'fact_egisz_messages', 'Контроль поступления EGISZ_MESSAGES в DWH'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT "localUid СЭМД")::numeric FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours'), 'документов', 'fact_egisz_messages без callback-факта', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT "Ключ документа (группировка)")::numeric FROM public.v_rpt_network_errors_detail_ui), 'документов', 'fact_egisz_channel_errors', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки регистрации РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE status = 'error'), 'строк', 'fact_egisz_transactions.status=error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('pending_backlog_24h',
         'Документы в обработке > 24ч (backlog)',
         CASE
             WHEN (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui WHERE "Категория ожидания" = 'просрочено') >= 100 THEN 'red'
             WHEN (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui WHERE "Категория ожидания" = 'просрочено') >= 20  THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM public.v_rpt_documents_no_response_ui WHERE "Категория ожидания" = 'просрочено'),
         'документов > 72ч',
         'v_rpt_documents_no_response_ui (просрочено)',
         'Проверить транспорт клиник в дашборде 03; pending — норма, просрочено — эскалация'),
        ('unknown_high',
         'Доля «Нераспознан» (status=unknown)',
         CASE
             WHEN (SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours' AND status = 'unknown')
                  / NULLIF((SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours'), 0) >= 0.05 THEN 'red'
             WHEN (SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours' AND status = 'unknown')
                  / NULLIF((SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours'), 0) >= 0.01 THEN 'yellow'
             ELSE 'green'
         END,
         ROUND(100.0 * (SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours' AND status = 'unknown')
                     / NULLIF((SELECT COUNT(*)::numeric FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '24 hours'), 0), 2),
         '% (за 24ч)',
         'fact_egisz_transactions.status=unknown / total',
         'Проверить регексп egisz_classify_async_status — возможно, появился новый шаблон ответа РЭМД'),
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
         'fact_egisz_transactions.log_date',
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

REFRESH MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui;
REFRESH MATERIALIZED VIEW public.v_stg_channel_errors_by_document;
ANALYZE public.exchangelog_raw;
ANALYZE public.fact_egisz_messages;
ANALYZE public.fact_egisz_documents;
ANALYZE public.fact_egisz_channel_errors;
ANALYZE public.fact_egisz_transactions;
ANALYZE public.v_egisz_transactions_enriched_ui;
ANALYZE public.v_stg_channel_errors_by_document;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
