-- ============================================================================
-- 70_views_core.sql — v_egisz_transactions_enriched_ui (mat-view) + v_rpt_error_interpretations_ui
-- Source: db/dwh_init.sql, lines [1276..1423).
-- Loaded by db/dwh_init.sql via \i db/parts/70_views_core.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui AS
SELECT
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    t.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    t.message_id AS "MSGID обмена",
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День",
    t.log_date::date AS "День (тренд)",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.status AS "Статус",
    CASE t.status
        WHEN 'success' THEN 'Успех'
        WHEN 'error'   THEN 'Ошибка'
        WHEN 'pending' THEN 'Документы в ожидании'
        WHEN 'unknown' THEN 'Неизвестная ошибка'
        ELSE                'Неизвестная ошибка'
    END AS "Статус (отчёт)",
    t.error_type AS "Тип ошибки",
    t.error_summary AS "Сводка ошибки",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    public.egisz_normalize_semd_code(t.semd_code) AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_clean_text_value(t.semd_name) IS NOT NULL
             AND public.egisz_clean_text_value(t.semd_name) !~ '^\d+$'
             AND public.egisz_clean_text_value(t.semd_name) <> public.egisz_normalize_semd_code(t.semd_code)
            THEN public.egisz_clean_text_value(t.semd_name)
            ELSE NULL
        END,
        CASE
            WHEN public.egisz_normalize_semd_code(t.semd_code) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid)::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid)::text) AS "Наименование клиники",
    t.jid::text AS "JID из журнала (gost, число)",
    o.name AS "Медицинская организация",
    t.org_oid AS "OID организации",
    l.mo_uid AS "OID клиники",
    public.egisz_clean_host(t.callback_url) AS "Хост клиники (VPN ГОСТ)",
    o.inn AS "ИНН клиники",
    l.mo_domen AS "Токен gost (нецифр., для отображения)",
    l.jid::text AS "JID (EGISZ_LICENSES)",
    CASE WHEN t.jid IS NOT NULL AND l.jid IS NOT NULL AND t.jid <> l.jid THEN 'да' ELSE 'нет' END AS "Расхождение источников JID",
    t.creation_date AS "Создание СЭМД",
    public.egisz_extract_jid_from_endpoint(m.reply_to) AS "JID из gost в REPLYTO",
    public.egisz_clean_host(m.reply_to) AS "Токен gost (REPLYTO)",
    t.local_uid_semd AS "localUid СЭМД",
    t.local_uid_semd AS "Идентификатор документа (localUid)",
    t.relates_to_id AS "Связанное сообщение",
    lower(NULLIF(btrim(t.relates_to_id), '')) AS "Связанное сообщение (канон)",
    lower(NULLIF(btrim(t.local_uid_semd), '')) AS "localUid СЭМД (канон)",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.emdr_id AS "Регистрационный номер РЭМД",
    t.doc_number AS "DOCUMENTID",
    t.error_json_text AS "Исходный текст ошибки",
    t.exchangelog_log_id AS transaction_id,
    COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid) AS clinic_id,
    public.egisz_normalize_semd_code(t.semd_code) AS service_id
FROM fact_egisz_transactions t
LEFT JOIN egisz_messages_raw m ON m.egmid = t.egmid
LEFT JOIN LATERAL (
    SELECT candidate.*
    FROM (
        (SELECT dl.*, 0 AS _prio
         FROM dim_licenses dl
         WHERE t.org_oid IS NOT NULL AND dl.mo_uid = t.org_oid
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 1 AS _prio
         FROM dim_licenses dl
         WHERE t.jid IS NOT NULL AND dl.jid = t.jid
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 2 AS _prio
         FROM dim_licenses dl
         WHERE public.egisz_extract_jid_from_endpoint(m.reply_to) IS NOT NULL
           AND dl.jid::text = public.egisz_extract_jid_from_endpoint(m.reply_to)
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 3 AS _prio
         FROM dim_licenses dl
         WHERE public.egisz_clean_host(m.reply_to) IS NOT NULL
           AND public.egisz_clean_host(dl.mo_domen) = public.egisz_clean_host(m.reply_to)
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
    ) candidate
    ORDER BY _prio, modifydate DESC NULLS LAST, id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN public.dim_semd_types st ON st.code = public.egisz_normalize_semd_code(t.semd_code)
LEFT JOIN dim_organizations o ON COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid) = o.jid
WITH NO DATA;

CREATE UNIQUE INDEX ON public.v_egisz_transactions_enriched_ui (transaction_id);
CREATE INDEX ON public.v_egisz_transactions_enriched_ui ("День");
CREATE INDEX ON public.v_egisz_transactions_enriched_ui ("JID клиники");
CREATE INDEX ON public.v_egisz_transactions_enriched_ui ("Статус");
CREATE INDEX ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("localUid СЭМД"), '')));
CREATE INDEX ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("Рег. номер РЭМД (emdrid)"), '')));
CREATE INDEX ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("Связанное сообщение"), '')));

CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui AS
SELECT
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День (тренд)",
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.local_uid_semd AS "localUid СЭМД",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.relates_to_id AS "Связанное сообщение",
    t.jid::text AS "JID клиники",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    t.status AS "Статус",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN COALESCE(NULLIF(t.error_json_text, ''), '(нет текста)')
        ELSE ''
    END AS "Исходный текст ошибки",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN COALESCE(NULLIF(t.error_summary, ''), 'Неизвестная ошибка')
        ELSE ''
    END AS "Интерпретация ошибки",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN t.error_type
        ELSE ''
    END AS "Тип ошибки",
    1::bigint AS "Порядок ошибки"
FROM fact_egisz_transactions t
WHERE t.status = 'error'

UNION ALL

SELECT
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День (тренд)",
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.local_uid_semd AS "localUid СЭМД",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.relates_to_id AS "Связанное сообщение",
    t.jid::text AS "JID клиники",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    t.status AS "Статус",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Исходный текст ошибки",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Интерпретация ошибки",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Тип ошибки",
    NULL::bigint AS "Порядок ошибки"
FROM fact_egisz_transactions t
WHERE t.status <> 'error' OR t.error_summary IS NULL;
