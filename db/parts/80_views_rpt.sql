-- ============================================================================
-- 80_views_rpt.sql — v_rpt_network_errors_detail_ui + v_rpt_documents_no_response_ui + v_rpt_semd_archive_ui + connectivity views
-- Source: db/dwh_init.sql, lines [1506..1744).
-- Loaded by db/dwh_init.sql via \i db/parts/80_views_rpt.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

-- Разбивка ошибок по категории (~10) и конкретному виду (~70) для двойного пирога.
-- Unnest'ит составной error_type (разделитель ' · ') → одна строка = один вид ошибки.
-- Сетевые ошибки из v_stg_channel_network_errors_by_document добавляются отдельно:
-- их created_at маппится в "Обработано IPS" для единого date-фильтра дашборда.
CREATE OR REPLACE VIEW public.v_rpt_error_category_breakdown_ui AS
WITH remd_errors AS (
    SELECT
        t.log_date                                                                    AS "Обработано IPS",
        t.log_date::date                                                              AS "День (тренд)",
        COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id,
                 t.doc_number, t.message_id, t.exchangelog_log_id::text)              AS "Документ (ключ учёта)",
        t.jid::text                                                                   AS "JID клиники",
        public.egisz_normalize_semd_code(t.semd_code)                                AS "Код СЭМД",
        trim(err_item)                                                                AS "Тип ошибки"
    FROM fact_egisz_transactions t
    CROSS JOIN LATERAL unnest(
        string_to_array(
            COALESCE(NULLIF(trim(t.error_type), ''), 'Неизвестная ошибка'),
            ' · '
        )
    ) AS err_item
    WHERE t.status = 'error'
      AND trim(err_item) <> ''
),
network_errors AS (
    SELECT
        n.created_at                AS "Обработано IPS",
        n.created_at::date          AS "День (тренд)",
        n.document_group_key        AS "Документ (ключ учёта)",
        NULL::text                  AS "JID клиники",
        NULL::text                  AS "Код СЭМД",
        'Сетевая ошибка'::text      AS "Тип ошибки"
    FROM public.v_stg_channel_network_errors_by_document n
)
SELECT
    "Обработано IPS",
    "День (тренд)",
    "Документ (ключ учёта)",
    "JID клиники",
    "Код СЭМД",
    "Тип ошибки",
    public.egisz_error_category("Тип ошибки") AS "Категория ошибки"
FROM remd_errors
UNION ALL
SELECT
    "Обработано IPS",
    "День (тренд)",
    "Документ (ключ учёта)",
    "JID клиники",
    "Код СЭМД",
    "Тип ошибки",
    'Ошибки связи'::text AS "Категория ошибки"
FROM network_errors;

COMMENT ON VIEW public.v_rpt_error_category_breakdown_ui IS
'Разбивка ошибок EGISZ-прокси: один ряд = один вид ошибки на документ.
Сетевые ошибки (created_at) и РЭМД-ошибки (log_date) унифицированы в "Обработано IPS"
для единого дашборд-фильтра. Используется картой «Категории ошибок» и «Ошибки по типу».';

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
WITH source_rows AS (
    SELECT
        s.*,
        NULLIF((regexp_match(COALESCE(s.message, ''), 'gost-([0-9]+)', 'i'))[1], '') AS jid_from_text
    FROM public.v_stg_channel_network_errors_by_document s
)
SELECT
    s.created_at AS "Дата создания документа",
    s.exchangelog_log_id::text AS "LOGID журнала (сетевая ошибка)",
    s.journal_msgid AS "MSGID обмена",
    s.egisz_messages_egmid::text AS "EGMID сообщения (строка журнала)",
    s.document_group_key AS "Ключ документа (группировка)",
    s.relates_to_hint AS "relatesToMessage (из текста журнала)",
    s.local_uid_hint AS "localUid / DOCUMENTID (из текста)",
    s.emdr_id_hint AS "emdrId (из текста)",
    public.egisz_clean_host(s.message) AS "Хост клиники (VPN ГОСТ)",
    COALESCE(f."JID клиники", s.jid_from_text) AS "JID клиники",
    COALESCE(f."JID из журнала (gost, число)", s.jid_from_text) AS "JID из журнала (gost, число)",
    COALESCE(f."Наименование клиники", 'Клиника JID: ' || COALESCE(f."JID клиники", s.jid_from_text, '(нет JID)')) AS "Клиника (транспорт)",
    f."Медицинская организация",
    f."Тип СЭМД (код · НСИ)",
    f."Код СЭМД",
    f."Сводка ошибки" AS "Сводка ошибки регистрации",
    s.message AS "Текст сетевой ошибки",
    s.message AS "Сообщение",
    s.error_global_subcategory AS "Подтип ошибки канала",
    CASE WHEN f."Документ (ключ учёта)" IS NULL THEN 'нет' ELSE 'да' END AS "Связанный колбэк найден в аналитике",
    f."LOGID журнала EXCHANGELOG" AS "LOGID записи ответа",
    f."EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)" AS "EGMID записи ответа",
    f."Связанное сообщение" AS "Связанное сообщение (ответ РЭМД)",
    f."Идентификатор документа (localUid)",
    f."Регистрационный номер РЭМД"
FROM source_rows s
LEFT JOIN LATERAL (
    SELECT f.*
    FROM public.v_egisz_transactions_enriched_ui f
    WHERE lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), ''))
       OR lower(NULLIF(btrim(f."Рег. номер РЭМД (emdrid)"), '')) = lower(NULLIF(btrim(s.emdr_id_hint), ''))
       OR lower(NULLIF(btrim(f."Связанное сообщение"), '')) = lower(NULLIF(btrim(s.relates_to_hint), ''))
    ORDER BY
        CASE
            WHEN lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), '')) THEN 0
            WHEN lower(NULLIF(btrim(f."Рег. номер РЭМД (emdrid)"), '')) = lower(NULLIF(btrim(s.emdr_id_hint), '')) THEN 1
            ELSE 2
        END,
        f."Обработано IPS" DESC NULLS LAST
    LIMIT 1
) f ON TRUE;

COMMENT ON VIEW public.v_rpt_network_errors_detail_ui IS
'Техническая витрина ошибок связи proxy_egisz: healthcheck/поддержка клиник, LOGSTATE=3 и строки журнала с привязкой к документу, если её удалось восстановить.';

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
WITH messages AS (
    SELECT
        m.egmid,
        m.created_at,
        m.msgid,
        m.reply_to,
        m.document_id,
        public.egisz_normalize_semd_code(
            COALESCE(public.egisz_xml_text(r.msgtext, 'kind'),
                     public.egisz_xml_text(r.msgtext, 'KIND'))
        ) AS semd_code_resolved,
        public.egisz_clean_text_value(
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'),
                     public.egisz_xml_text(r.msgtext, 'name'),
                     public.egisz_xml_text(r.msgtext, 'documentName'))
        ) AS semd_name_payload,
        public.egisz_normalize_message_id(m.msgid) AS msgid_norm,
        lower(NULLIF(btrim(m.document_id), '')) AS document_id_norm,
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS reply_to_jid,
        public.egisz_clean_host(m.reply_to) AS reply_to_host
    FROM egisz_messages_raw m
    LEFT JOIN LATERAL (
        SELECT er.msgtext
        FROM exchangelog_raw er
        WHERE er.msgid IS NOT NULL
          AND public.egisz_normalize_message_id(er.msgid) = public.egisz_normalize_message_id(m.msgid)
        ORDER BY er.logid DESC
        LIMIT 1
    ) r ON TRUE
),
fact_message_keys AS (
    SELECT DISTINCT public.egisz_normalize_message_id(f.message_id) AS message_key
    FROM fact_egisz_transactions f
    WHERE NULLIF(public.egisz_normalize_message_id(f.message_id), '') IS NOT NULL

    UNION

    SELECT DISTINCT public.egisz_normalize_message_id(f.relates_to_id) AS message_key
    FROM fact_egisz_transactions f
    WHERE NULLIF(public.egisz_normalize_message_id(f.relates_to_id), '') IS NOT NULL
),
fact_document_keys AS (
    SELECT DISTINCT lower(NULLIF(btrim(f.local_uid_semd), '')) AS document_key
    FROM fact_egisz_transactions f
    WHERE lower(NULLIF(btrim(f.local_uid_semd), '')) IS NOT NULL
)
SELECT
    m.created_at AS "Отправлено",
    m.document_id AS "localUid СЭМД",
    m.document_id AS "Идентификатор документа (localUid)",
    m.semd_code_resolved AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_clean_text_value(m.semd_name_payload) IS NOT NULL
             AND public.egisz_clean_text_value(m.semd_name_payload) !~ '^\d+$'
             AND public.egisz_clean_text_value(m.semd_name_payload) <> public.egisz_normalize_semd_code(m.semd_code_resolved)
            THEN public.egisz_clean_text_value(m.semd_name_payload)
            ELSE NULL
        END,
        CASE
            WHEN public.egisz_normalize_semd_code(m.semd_code_resolved) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    public.egisz_semd_type_report_label(m.semd_code_resolved, m.semd_name_payload) AS "Тип СЭМД (код · НСИ)",
    COALESCE(m.reply_to_jid, l.jid)::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(m.reply_to_jid, l.jid)::text) AS "Наименование клиники",
    m.reply_to AS "Связанное сообщение",
    m.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    m.msgid AS "MSGID обмена"
FROM messages m
LEFT JOIN LATERAL (
    SELECT dl.*
    FROM dim_licenses dl
    WHERE (m.reply_to_jid IS NOT NULL AND dl.jid = m.reply_to_jid)
       OR (m.reply_to_host IS NOT NULL AND public.egisz_clean_host(dl.mo_domen) = m.reply_to_host)
    ORDER BY
        CASE
            WHEN m.reply_to_jid IS NOT NULL AND dl.jid = m.reply_to_jid THEN 0
            ELSE 1
        END,
        dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN dim_organizations o ON o.jid = COALESCE(m.reply_to_jid, l.jid)
LEFT JOIN dim_semd_types st ON st.code = public.egisz_normalize_semd_code(m.semd_code_resolved)
LEFT JOIN fact_message_keys fm ON fm.message_key = m.msgid_norm
LEFT JOIN fact_document_keys fd ON fd.document_key = m.document_id_norm
WHERE fm.message_key IS NULL
  AND fd.document_key IS NULL;

CREATE OR REPLACE VIEW public.v_rpt_semd_archive_ui AS
SELECT
    "Обработано IPS" AS "Дата обработки",
    "День (тренд)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "JID клиники",
    "Наименование клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД",
    "Статус",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки"
FROM public.v_egisz_transactions_enriched_ui

UNION ALL

SELECT
    "Отправлено" AS "Дата обработки",
    "Отправлено"::date AS "День (тренд)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "JID клиники",
    "Наименование клиники",
    NULL::text AS "OID организации",
    NULL::text AS "OID клиники",
    COALESCE("localUid СЭМД", "MSGID обмена", "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)") AS "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    NULL::text AS "Рег. номер РЭМД",
    'ожидание ответа' AS "Статус",
    NULL::text AS "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    NULL::timestamptz AS "Создание СЭМД",
    NULL::text AS "Сводка ошибки"
FROM public.v_rpt_documents_no_response_ui;

CREATE OR REPLACE VIEW public.v_rpt_clinic_connectivity_daily_ui AS
WITH success_by_day AS (
    SELECT
        "Обработано IPS"::date AS day,
        NULLIF("JID клиники", '') AS jid,
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')::bigint AS ok_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_remd_cnt
    FROM public.v_egisz_transactions_enriched_ui
    GROUP BY 1, 2
),
network_by_day AS (
    SELECT
        "Дата создания документа"::date AS day,
        NULLIF(COALESCE("JID клиники", "JID из журнала (gost, число)"), '') AS jid,
        MAX("Клиника (транспорт)") AS clinic_name,
        COUNT(DISTINCT "Ключ документа (группировка)")::bigint AS err_cnt
    FROM public.v_rpt_network_errors_detail_ui
    GROUP BY 1, 2
)
SELECT
    COALESCE(s.day, n.day) AS "День",
    COALESCE(s.jid, n.jid) AS "JID клиники (ключ)",
    COALESCE(s.jid, n.jid) AS "JID клиники",
    COALESCE(NULLIF(s.clinic_name, ''), NULLIF(n.clinic_name, ''), 'Клиника JID: ' || COALESCE(s.jid, n.jid)) AS "Наименование клиники",
    COALESCE(s.ok_cnt, 0)::bigint AS "Успешные ответы РЭМД (документов)",
    COALESCE(s.ok_cnt, 0)::bigint AS "Ответы РЭМД: успех (документов)",
    COALESCE(s.err_remd_cnt, 0)::bigint AS "Ответы РЭМД: отказ (документов)",
    COALESCE(n.err_cnt, 0)::bigint AS "Ошибки связи (документов)",
    ROUND(100.0 * COALESCE(s.ok_cnt, 0) / NULLIF(COALESCE(s.ok_cnt, 0) + COALESCE(n.err_cnt, 0), 0), 2) AS "Доступность транспорта (прибл.), %"
FROM success_by_day s
FULL OUTER JOIN network_by_day n ON s.day = n.day AND s.jid = n.jid;

CREATE OR REPLACE VIEW public.v_rpt_connectivity_global_daily_ui AS
SELECT
    "День",
    SUM("Успешные ответы РЭМД (документов)")::bigint AS "Успешные ответы РЭМД (документов)",
    SUM("Ошибки связи (документов)")::bigint AS "Ошибки связи (документов)",
    ROUND(100.0 * SUM("Успешные ответы РЭМД (документов)") / NULLIF(SUM("Успешные ответы РЭМД (документов)") + SUM("Ошибки связи (документов)"), 0), 2) AS "Доступность транспорта (прибл.), %"
FROM public.v_rpt_clinic_connectivity_daily_ui
GROUP BY 1;

