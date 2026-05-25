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
    NULL::text AS "Связанное сообщение",
    m.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    m.msgid AS "MSGID обмена",
    public.egisz_clean_host(m.reply_to) AS "Хост клиники (VPN ГОСТ)"
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
    "Сводка ошибки",
    "Хост клиники (VPN ГОСТ)"
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
    NULL::text AS "Сводка ошибки",
    "Хост клиники (VPN ГОСТ)"
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

CREATE OR REPLACE VIEW public.v_rpt_client_documents_ui AS
WITH fact_source AS (
    SELECT
        f.transaction_id::text AS document_row_id,
        f."Обработано IPS" AS document_ts,
        f."День (тренд)" AS document_day,
        NULLIF(f."JID клиники", '') AS client_jid,
        COALESCE(
            NULLIF(f."Документ (ключ учёта)", ''),
            NULLIF(f."localUid СЭМД", ''),
            NULLIF(f."Рег. номер РЭМД (emdrid)", ''),
            f.transaction_id::text
        ) AS document_key,
        NULLIF(f."Код СЭМД", '') AS semd_code,
        COALESCE(NULLIF(f."Тип СЭМД (код · НСИ)", ''), NULLIF(f."Наименование СЭМД", ''), '(тип СЭМД не определен)') AS document_type,
        CASE
            WHEN f."Статус" = 'success' THEN 'success'
            WHEN f."Статус" = 'error' THEN 'error'
            ELSE 'pending'
        END AS status_code,
        CASE
            WHEN f."Статус" = 'success' THEN 'Успех'
            WHEN f."Статус" = 'error' THEN 'Ошибка'
            ELSE 'В обработке / ждет ответа'
        END AS status_label,
        CASE
            WHEN f."Статус" = 'success' THEN 1
            WHEN f."Статус" = 'error' THEN 3
            ELSE 2
        END AS status_sort,
        COALESCE(NULLIF(f."Сводка ошибки", ''), NULLIF(f."Исходный текст ошибки", ''), '(ошибка без текста)') AS error_text,
        CASE
            WHEN f."Статус" = 'success'
             AND f."Создание СЭМД" IS NOT NULL
             AND f."Обработано IPS" >= f."Создание СЭМД"
            THEN ROUND(EXTRACT(EPOCH FROM (f."Обработано IPS" - f."Создание СЭМД"))::numeric, 0)
            ELSE NULL::numeric
        END AS delivery_seconds,
        r.msgtext AS raw_msgtext,
        COALESCE(
            public.egisz_xml_text(r.msgtext, 'patientName'),
            public.egisz_xml_text(r.msgtext, 'patientFio'),
            public.egisz_xml_text(r.msgtext, 'fio'),
            public.egisz_xml_text(r.msgtext, 'patient'),
            public.egisz_xml_text(r.msgtext, 'PatientName'),
            NULLIF(concat_ws(
                ' ',
                public.egisz_xml_text(r.msgtext, 'familyName'),
                public.egisz_xml_text(r.msgtext, 'givenName'),
                public.egisz_xml_text(r.msgtext, 'patronymic')
            ), '')
        ) AS raw_patient_name,
        COALESCE(
            public.egisz_xml_text(r.msgtext, 'snils'),
            public.egisz_xml_text(r.msgtext, 'SNILS'),
            public.egisz_xml_text(r.msgtext, 'patientSnils')
        ) AS raw_snils,
        COALESCE(
            public.egisz_xml_text(r.msgtext, 'doctorName'),
            public.egisz_xml_text(r.msgtext, 'doctorFio'),
            public.egisz_xml_text(r.msgtext, 'physicianName'),
            public.egisz_xml_text(r.msgtext, 'medicalWorkerName'),
            public.egisz_xml_text(r.msgtext, 'authorName'),
            public.egisz_xml_text(r.msgtext, 'doctor')
        ) AS raw_doctor_name
    FROM public.v_egisz_transactions_enriched_ui f
    LEFT JOIN public.exchangelog_raw r ON r.logid = f.transaction_id
),
pending_source AS (
    SELECT
        'pending:' || COALESCE(NULLIF(p."EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)", ''), p."MSGID обмена", p."localUid СЭМД") AS document_row_id,
        p."Отправлено" AS document_ts,
        p."Отправлено"::date AS document_day,
        NULLIF(p."JID клиники", '') AS client_jid,
        COALESCE(NULLIF(p."localUid СЭМД", ''), NULLIF(p."MSGID обмена", ''), NULLIF(p."EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)", '')) AS document_key,
        NULLIF(p."Код СЭМД", '') AS semd_code,
        COALESCE(NULLIF(p."Тип СЭМД (код · НСИ)", ''), NULLIF(p."Наименование СЭМД", ''), '(тип СЭМД не определен)') AS document_type,
        'pending'::text AS status_code,
        'В обработке / ждет ответа'::text AS status_label,
        2::integer AS status_sort,
        NULL::text AS error_text,
        NULL::numeric AS delivery_seconds,
        NULL::text AS raw_msgtext,
        NULL::text AS raw_patient_name,
        NULL::text AS raw_snils,
        NULL::text AS raw_doctor_name
    FROM public.v_rpt_documents_no_response_ui p
),
source_rows AS (
    SELECT * FROM fact_source
    UNION ALL
    SELECT * FROM pending_source
),
normalized AS (
    SELECT
        s.*,
        regexp_split_to_array(public.egisz_clean_text_value(s.raw_patient_name), '\s+') AS patient_parts,
        regexp_replace(COALESCE(s.raw_snils, ''), '\D', '', 'g') AS snils_digits
    FROM source_rows s
)
SELECT
    document_row_id,
    document_ts,
    document_day,
    client_jid,
    document_key,
    semd_code,
    document_type,
    status_code,
    status_label,
    status_sort,
    error_text,
    delivery_seconds,
    CASE
        WHEN patient_parts IS NULL OR array_length(patient_parts, 1) IS NULL THEN '(нет данных)'
        ELSE substring(patient_parts[1] FROM 1 FOR 1) || '***'
             || CASE WHEN array_length(patient_parts, 1) >= 2 THEN ' ' || substring(patient_parts[2] FROM 1 FOR 1) || '.' ELSE '' END
             || CASE WHEN array_length(patient_parts, 1) >= 3 THEN substring(patient_parts[3] FROM 1 FOR 1) || '.' ELSE '' END
    END AS patient_name_masked,
    CASE
        WHEN length(snils_digits) >= 4 THEN '***-***-*** ' || right(snils_digits, 4)
        WHEN length(snils_digits) >= 2 THEN '***-***-*** ' || right(snils_digits, 2)
        ELSE '(нет данных)'
    END AS snils_masked,
    COALESCE(NULLIF(public.egisz_clean_text_value(raw_doctor_name), ''), '(нет данных)') AS doctor_name,
    -- стабильные surrogate-ID для COUNT DISTINCT в BI-дашборде; raw_* не покидают view.
    CASE
        WHEN COALESCE(NULLIF(btrim(raw_patient_name), ''), '') = ''
         AND COALESCE(NULLIF(snils_digits, ''), '') = '' THEN NULL
        ELSE md5(lower(COALESCE(btrim(raw_patient_name), '')) || '|' || COALESCE(snils_digits, ''))
    END AS patient_hash,
    CASE
        WHEN public.egisz_clean_text_value(raw_doctor_name) IS NULL THEN NULL
        ELSE md5(lower(public.egisz_clean_text_value(raw_doctor_name)))
    END AS doctor_hash
FROM normalized;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_jid_month_ui AS
WITH activity_rows AS (
    SELECT
        date_trunc('month', "Обработано IPS")::date AS month,
        NULLIF("JID клиники", '') AS jid,
        MAX(NULLIF("Наименование клиники", '')) AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS documents_total,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')::bigint AS success_documents,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS error_documents,
        0::bigint AS pending_documents
    FROM public.v_egisz_transactions_enriched_ui
    WHERE "Обработано IPS" IS NOT NULL
      AND NULLIF("JID клиники", '') IS NOT NULL
    GROUP BY date_trunc('month', "Обработано IPS")::date, NULLIF("JID клиники", '')

    UNION ALL

    SELECT
        date_trunc('month', "Отправлено")::date AS month,
        NULLIF("JID клиники", '') AS jid,
        MAX(NULLIF("Наименование клиники", '')) AS clinic_name,
        COUNT(DISTINCT COALESCE(NULLIF("localUid СЭМД", ''), NULLIF("MSGID обмена", ''), NULLIF("EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)", '')))::bigint AS documents_total,
        0::bigint AS success_documents,
        0::bigint AS error_documents,
        COUNT(DISTINCT COALESCE(NULLIF("localUid СЭМД", ''), NULLIF("MSGID обмена", ''), NULLIF("EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)", '')))::bigint AS pending_documents
    FROM public.v_rpt_documents_no_response_ui
    WHERE "Отправлено" IS NOT NULL
      AND NULLIF("JID клиники", '') IS NOT NULL
    GROUP BY date_trunc('month', "Отправлено")::date, NULLIF("JID клиники", '')
),
activity AS (
    SELECT
        month,
        jid,
        CASE WHEN jid ~ '^\d+$' THEN jid::bigint ELSE NULL::bigint END AS jid_bigint,
        MAX(clinic_name) AS clinic_name,
        SUM(documents_total)::bigint AS documents_total,
        SUM(success_documents)::bigint AS success_documents,
        SUM(error_documents)::bigint AS error_documents,
        SUM(pending_documents)::bigint AS pending_documents
    FROM activity_rows
    GROUP BY month, jid
),
cost_month AS (
    SELECT
        client_id,
        date_trunc('month', month)::date AS month,
        SUM(cost) FILTER (WHERE category = 'gateway')::numeric(14, 2) AS gateway_cost,
        SUM(cost) FILTER (WHERE category = 'infra')::numeric(14, 2) AS infra_cost,
        SUM(cost) FILTER (WHERE category = 'l2_support')::numeric(14, 2) AS l2_support_cost,
        SUM(cost) FILTER (WHERE category NOT IN ('gateway', 'infra', 'l2_support', 'cac'))::numeric(14, 2) AS other_cost
    FROM client_costs_monthly
    GROUP BY 1, 2
)
SELECT
    a.month,
    a.jid AS client_id,
    a.jid_bigint,
    COALESCE(NULLIF(c.name, ''), a.clinic_name, 'Клиника JID: ' || a.jid) AS client_name,
    COALESCE(
        NULLIF(c.tier, ''),
        CASE
            WHEN a.documents_total >= 5000 THEN 'Large'
            WHEN a.documents_total >= 1000 THEN 'Medium'
            ELSE 'Small'
        END
    ) AS tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COALESCE(sub.compliance_plus_flag, false) AS compliance_plus_flag,
    true AS active_client,
    10000::numeric(14, 2) AS service_price_monthly,
    10000::numeric(14, 2) AS mrr,
    120000::numeric(14, 2) AS arr,
    10000::numeric(14, 2) AS revenue_accrual,
    COALESCE(cost.gateway_cost, 0)::numeric(14, 2) AS gateway_cost,
    COALESCE(cost.infra_cost, 0)::numeric(14, 2) AS infra_cost,
    COALESCE(cost.l2_support_cost, 0)::numeric(14, 2) AS l2_support_cost,
    COALESCE(cost.other_cost, 0)::numeric(14, 2) AS other_cost,
    (
        COALESCE(cost.gateway_cost, 0)
        + COALESCE(cost.infra_cost, 0)
        + COALESCE(cost.l2_support_cost, 0)
        + COALESCE(cost.other_cost, 0)
    )::numeric(14, 2) AS cogs_total,
    (
        10000
        - COALESCE(cost.gateway_cost, 0)
        - COALESCE(cost.infra_cost, 0)
        - COALESCE(cost.l2_support_cost, 0)
        - COALESCE(cost.other_cost, 0)
    )::numeric(14, 2) AS gross_profit,
    ROUND(
        100.0 * (
            10000
            - COALESCE(cost.gateway_cost, 0)
            - COALESCE(cost.infra_cost, 0)
            - COALESCE(cost.l2_support_cost, 0)
            - COALESCE(cost.other_cost, 0)
        ) / 10000,
        2
    ) AS gross_margin_pct,
    a.documents_total,
    a.success_documents,
    a.error_documents,
    a.pending_documents,
    ROUND(10000.0 / NULLIF(a.documents_total, 0), 2) AS revenue_per_document,
    ROUND((
        COALESCE(cost.gateway_cost, 0)
        + COALESCE(cost.infra_cost, 0)
        + COALESCE(cost.l2_support_cost, 0)
        + COALESCE(cost.other_cost, 0)
    ) / NULLIF(a.documents_total, 0), 2) AS cogs_per_document,
    ROUND((
        10000
        - COALESCE(cost.gateway_cost, 0)
        - COALESCE(cost.infra_cost, 0)
        - COALESCE(cost.l2_support_cost, 0)
        - COALESCE(cost.other_cost, 0)
    ) / NULLIF(a.documents_total, 0), 2) AS gross_profit_per_document
FROM activity a
LEFT JOIN clients c ON c.id = a.jid_bigint
LEFT JOIN LATERAL (
    SELECT bool_or(compliance_plus_flag) AS compliance_plus_flag
    FROM subscriptions s
    WHERE s.client_id = a.jid_bigint
      AND s.started_at < (a.month + INTERVAL '1 month')::date
      AND (s.ended_at IS NULL OR s.ended_at >= a.month)
) sub ON TRUE
LEFT JOIN cost_month cost ON cost.client_id = a.jid_bigint AND cost.month = a.month;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_financial_summary_ui AS
SELECT
    month,
    COUNT(DISTINCT client_id)::bigint AS active_clinics,
    10000::numeric(14, 2) AS service_price_per_jid_month,
    SUM(revenue_accrual)::numeric(14, 2) AS revenue_accrual,
    SUM(gateway_cost)::numeric(14, 2) AS gateway_cost,
    SUM(infra_cost)::numeric(14, 2) AS infra_cost,
    SUM(l2_support_cost)::numeric(14, 2) AS l2_support_cost,
    SUM(other_cost)::numeric(14, 2) AS other_cost,
    SUM(cogs_total)::numeric(14, 2) AS cogs_total,
    SUM(gross_profit)::numeric(14, 2) AS gross_profit,
    ROUND(100.0 * SUM(gross_profit) / NULLIF(SUM(revenue_accrual), 0), 2) AS gross_margin_pct,
    SUM(documents_total)::bigint AS documents_total,
    ROUND(SUM(revenue_accrual) / NULLIF(SUM(documents_total), 0), 2) AS revenue_per_document,
    ROUND(SUM(cogs_total) / NULLIF(SUM(documents_total), 0), 2) AS cogs_per_document,
    ROUND(SUM(gross_profit) / NULLIF(SUM(documents_total), 0), 2) AS gross_profit_per_document
FROM public.v_rpt_service_audit_jid_month_ui
GROUP BY month;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_cost_breakdown_ui AS
SELECT month, tier, region, compliance_plus_flag, 'gateway'::text AS cost_category, gateway_cost AS cost_amount
FROM public.v_rpt_service_audit_jid_month_ui
UNION ALL
SELECT month, tier, region, compliance_plus_flag, 'infra'::text AS cost_category, infra_cost AS cost_amount
FROM public.v_rpt_service_audit_jid_month_ui
UNION ALL
SELECT month, tier, region, compliance_plus_flag, 'l2_support'::text AS cost_category, l2_support_cost AS cost_amount
FROM public.v_rpt_service_audit_jid_month_ui
UNION ALL
SELECT month, tier, region, compliance_plus_flag, 'other'::text AS cost_category, other_cost AS cost_amount
FROM public.v_rpt_service_audit_jid_month_ui;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_client_month_ui AS
WITH months AS (
    SELECT generate_series(
        date_trunc('month', CURRENT_DATE)::date - INTERVAL '23 months',
        date_trunc('month', CURRENT_DATE)::date,
        INTERVAL '1 month'
    )::date AS month
),
client_month AS (
    SELECT
        m.month,
        c.id AS client_id,
        c.name AS client_name,
        COALESCE(NULLIF(MAX(s.tier) FILTER (
            WHERE s.started_at < (m.month + INTERVAL '1 month')::date
              AND (s.ended_at IS NULL OR s.ended_at >= m.month)
        ), ''), c.tier, 'unknown') AS tier,
        COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
        COALESCE(bool_or(s.compliance_plus_flag) FILTER (
            WHERE s.started_at < (m.month + INTERVAL '1 month')::date
              AND (s.ended_at IS NULL OR s.ended_at >= m.month)
        ), false) AS compliance_plus_flag,
        COALESCE(SUM(s.monthly_fee) FILTER (
            WHERE s.started_at < (m.month + INTERVAL '1 month')::date
              AND (s.ended_at IS NULL OR s.ended_at >= m.month)
        ), 0)::numeric(14, 2) AS mrr,
        COUNT(s.id) FILTER (
            WHERE s.started_at < (m.month + INTERVAL '1 month')::date
              AND (s.ended_at IS NULL OR s.ended_at >= m.month)
        ) > 0 AS active_client
    FROM months m
    CROSS JOIN clients c
    LEFT JOIN subscriptions s ON s.client_id = c.id
    GROUP BY m.month, c.id, c.name, c.tier, c.region
),
billing_month AS (
    SELECT
        client_id,
        date_trunc('month', month)::date AS month,
        SUM(amount_billed)::numeric(14, 2) AS amount_billed,
        SUM(amount_paid)::numeric(14, 2) AS amount_paid
    FROM billing
    GROUP BY 1, 2
),
cost_month AS (
    SELECT
        client_id,
        date_trunc('month', month)::date AS month,
        SUM(cost) FILTER (WHERE category = 'gateway')::numeric(14, 2) AS gateway_cost,
        SUM(cost) FILTER (WHERE category = 'infra')::numeric(14, 2) AS infra_cost,
        SUM(cost) FILTER (WHERE category = 'l2_support')::numeric(14, 2) AS l2_support_cost,
        SUM(cost) FILTER (WHERE category NOT IN ('gateway', 'infra', 'l2_support', 'cac'))::numeric(14, 2) AS other_cost
    FROM client_costs_monthly
    GROUP BY 1, 2
),
ticket_month AS (
    SELECT
        client_id,
        date_trunc('month', opened_at)::date AS month,
        COUNT(*)::bigint AS tickets_total,
        COUNT(*) FILTER (WHERE severity = 'P1')::bigint AS p1_tickets,
        AVG(resolution_time_hours) FILTER (WHERE closed_at IS NOT NULL)::numeric(12, 2) AS avg_resolution_hours
    FROM tickets
    GROUP BY 1, 2
),
sed_month AS (
    SELECT
        client_id,
        date_trunc('month', created_at)::date AS month,
        COUNT(*)::bigint AS sed_total,
        COUNT(*) FILTER (WHERE registered_at IS NOT NULL)::bigint AS sed_registered,
        COUNT(*) FILTER (WHERE registered_at IS NULL)::bigint AS sed_failed
    FROM sed_transfers
    GROUP BY 1, 2
)
SELECT
    cm.month,
    cm.client_id,
    cm.client_name,
    cm.tier,
    cm.region,
    cm.compliance_plus_flag,
    cm.active_client,
    cm.mrr,
    (cm.mrr * 12)::numeric(14, 2) AS arr,
    COALESCE(b.amount_billed, 0)::numeric(14, 2) AS amount_billed,
    COALESCE(b.amount_paid, 0)::numeric(14, 2) AS amount_paid,
    COALESCE(cost.gateway_cost, 0)::numeric(14, 2) AS gateway_cost,
    COALESCE(cost.infra_cost, 0)::numeric(14, 2) AS infra_cost,
    COALESCE(cost.l2_support_cost, 0)::numeric(14, 2) AS l2_support_cost,
    COALESCE(cost.other_cost, 0)::numeric(14, 2) AS other_cost,
    (COALESCE(b.amount_paid, 0) - COALESCE(cost.gateway_cost, 0) - COALESCE(cost.infra_cost, 0) - COALESCE(cost.l2_support_cost, 0) - COALESCE(cost.other_cost, 0))::numeric(14, 2) AS gross_profit,
    COALESCE(t.tickets_total, 0)::bigint AS tickets_total,
    COALESCE(t.p1_tickets, 0)::bigint AS p1_tickets,
    t.avg_resolution_hours,
    COALESCE(sed.sed_total, 0)::bigint AS sed_total,
    COALESCE(sed.sed_registered, 0)::bigint AS sed_registered,
    COALESCE(sed.sed_failed, 0)::bigint AS sed_failed
FROM client_month cm
LEFT JOIN billing_month b ON b.client_id = cm.client_id AND b.month = cm.month
LEFT JOIN cost_month cost ON cost.client_id = cm.client_id AND cost.month = cm.month
LEFT JOIN ticket_month t ON t.client_id = cm.client_id AND t.month = cm.month
LEFT JOIN sed_month sed ON sed.client_id = cm.client_id AND sed.month = cm.month;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_mrr_trend_ui AS
SELECT
    month,
    tier,
    region,
    compliance_plus_flag,
    COUNT(DISTINCT client_id)::bigint AS clients,
    10000::numeric(14, 2) AS service_price_per_jid_month,
    SUM(mrr)::numeric(14, 2) AS mrr,
    (SUM(mrr) * 12)::numeric(14, 2) AS arr
FROM public.v_rpt_service_audit_jid_month_ui
GROUP BY month, tier, region, compliance_plus_flag;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_arpu_segments_ui AS
SELECT
    tier,
    region,
    compliance_plus_flag,
    COUNT(DISTINCT client_id)::bigint AS clients,
    10000::numeric(14, 2) AS arpu,
    SUM(mrr)::numeric(14, 2) AS total_mrr,
    SUM(documents_total)::bigint AS documents_total,
    ROUND(SUM(mrr) / NULLIF(SUM(documents_total), 0), 2) AS revenue_per_document
FROM public.v_rpt_service_audit_jid_month_ui
WHERE month = date_trunc('month', CURRENT_DATE)::date
GROUP BY tier, region, compliance_plus_flag;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_gross_margin_ui AS
SELECT
    client_id,
    MAX(jid_bigint) AS jid_bigint,
    MAX(client_name) AS client_name,
    MAX(tier) AS tier,
    MAX(region) AS region,
    bool_or(compliance_plus_flag) AS compliance_plus_flag,
    SUM(revenue_accrual)::numeric(14, 2) AS revenue,
    SUM(gateway_cost)::numeric(14, 2) AS gateway_cost,
    SUM(infra_cost)::numeric(14, 2) AS infra_cost,
    SUM(l2_support_cost)::numeric(14, 2) AS l2_support_cost,
    SUM(other_cost)::numeric(14, 2) AS other_cost,
    SUM(cogs_total)::numeric(14, 2) AS cogs_total,
    SUM(gross_profit)::numeric(14, 2) AS gross_profit,
    ROUND(100.0 * SUM(gross_profit) / NULLIF(SUM(revenue_accrual), 0), 2) AS gross_margin_pct,
    SUM(documents_total)::bigint AS documents_total,
    ROUND(SUM(revenue_accrual) / NULLIF(SUM(documents_total), 0), 2) AS revenue_per_document,
    ROUND(SUM(cogs_total) / NULLIF(SUM(documents_total), 0), 2) AS cogs_per_document
FROM public.v_rpt_service_audit_jid_month_ui
WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
GROUP BY client_id;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_cac_payback_ui AS
WITH first_active AS (
    SELECT client_id, MIN(month) AS first_active_month
    FROM public.v_rpt_service_audit_jid_month_ui
    GROUP BY client_id
),
new_clients AS (
    SELECT client_id
    FROM first_active
    WHERE first_active_month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
),
actual_gp AS (
    SELECT client_id, AVG(NULLIF(gross_profit, 0)) AS monthly_gross_profit
    FROM public.v_rpt_service_audit_jid_month_ui
    WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
    GROUP BY client_id
),
cac AS (
    SELECT client_id::text AS client_id, NULLIF(SUM(cost), 0) AS cac_amount
    FROM client_costs_monthly
    WHERE category = 'cac'
      AND month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
    GROUP BY client_id
)
SELECT
    AVG(COALESCE(cac.cac_amount, 30000.0) / NULLIF(actual_gp.monthly_gross_profit, 0))::numeric(12, 2) AS cac_payback_months,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(cac.cac_amount, 30000.0) / NULLIF(actual_gp.monthly_gross_profit, 0))::numeric(12, 2) AS median_cac_payback_months,
    COUNT(*)::bigint AS new_clients
FROM new_clients nc
LEFT JOIN actual_gp ON actual_gp.client_id = nc.client_id
LEFT JOIN cac ON cac.client_id = nc.client_id;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_ltv_ui AS
WITH gross_margin AS (
    SELECT COALESCE(AVG(gross_margin_pct) / 100.0, 0.52) AS gross_margin_rate
    FROM public.v_rpt_service_audit_gross_margin_ui
    WHERE revenue > 0
),
base AS (
    SELECT COUNT(*)::numeric AS clients_at_risk
    FROM (
        SELECT DISTINCT client_id
        FROM public.v_rpt_service_audit_jid_month_ui
        WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '24 months'
          AND month < date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
    ) x
),
churned AS (
    SELECT COUNT(*)::numeric AS churned_clients
    FROM (
        SELECT old.client_id
        FROM (
            SELECT DISTINCT client_id
            FROM public.v_rpt_service_audit_jid_month_ui
            WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '24 months'
              AND month < date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
        ) old
        LEFT JOIN (
            SELECT DISTINCT client_id
            FROM public.v_rpt_service_audit_jid_month_ui
            WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
        ) cur ON cur.client_id = old.client_id
        WHERE cur.client_id IS NULL
    ) x
)
SELECT
    (churned.churned_clients / NULLIF(base.clients_at_risk, 0))::numeric(12, 4) AS annual_churn_rate,
    10000::numeric(14, 2) AS arpu,
    gross_margin.gross_margin_rate::numeric(6, 4) AS gross_margin_rate,
    (10000 * 12 * gross_margin.gross_margin_rate / NULLIF(churned.churned_clients / NULLIF(base.clients_at_risk, 0), 0))::numeric(14, 2) AS ltv,
    30000::numeric(14, 2) AS default_cac,
    (10000 * 12 * gross_margin.gross_margin_rate / NULLIF(churned.churned_clients / NULLIF(base.clients_at_risk, 0), 0) / 30000.0)::numeric(12, 2) AS ltv_cac_ratio
FROM gross_margin, base, churned;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_mttr_incidents_ui AS
SELECT
    t.severity,
    t.category,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    AVG(t.resolution_time_hours)::numeric(12, 2) AS mttr_hours,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.resolution_time_hours)::numeric(12, 2) AS p95_hours,
    COUNT(*)::bigint AS incident_count
FROM tickets t
JOIN clients c ON c.id = t.client_id
WHERE t.opened_at >= CURRENT_DATE - INTERVAL '90 days'
  AND t.closed_at IS NOT NULL
  AND t.resolution_time_hours IS NOT NULL
GROUP BY t.severity, t.category, c.tier, COALESCE(NULLIF(c.region, ''), 'unknown');

CREATE OR REPLACE VIEW public.v_rpt_service_audit_sla_compliance_ui AS
SELECT
    s.month,
    s.client_id,
    c.name AS client_name,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COALESCE(sub.compliance_plus_flag, false) AS compliance_plus_flag,
    CASE
        WHEN s.mttr_p1_hours <= 8 AND s.response_time_p95_hours <= 4 AND COALESCE(s.uptime_pct, 100) >= 99.5 THEN 'OK'
        WHEN s.mttr_p1_hours <= 12 OR s.response_time_p95_hours <= 6 OR COALESCE(s.uptime_pct, 100) >= 99.0 THEN 'WARN'
        ELSE 'BREACH'
    END AS sla_status,
    s.mttr_p1_hours,
    s.response_time_p95_hours,
    s.uptime_pct
FROM sla_metrics s
JOIN clients c ON c.id = s.client_id
LEFT JOIN LATERAL (
    SELECT bool_or(compliance_plus_flag) AS compliance_plus_flag
    FROM subscriptions x
    WHERE x.client_id = s.client_id
      AND x.started_at < (s.month + INTERVAL '1 month')::date
      AND (x.ended_at IS NULL OR x.ended_at >= s.month)
) sub ON TRUE
WHERE s.month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '6 months';

CREATE OR REPLACE VIEW public.v_rpt_service_audit_problem_clients_ui AS
SELECT
    c.id AS client_id,
    c.name AS client_name,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COUNT(t.id) FILTER (WHERE t.severity = 'P1')::bigint AS p1_tickets,
    COUNT(t.id)::bigint AS total_tickets,
    AVG(t.resolution_time_hours)::numeric(12, 2) AS avg_mttr_hours
FROM clients c
JOIN tickets t ON t.client_id = c.id
WHERE t.opened_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY c.id, c.name, c.tier, COALESCE(NULLIF(c.region, ''), 'unknown')
ORDER BY p1_tickets DESC, total_tickets DESC;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_semd_transfers_ui AS
SELECT
    st.id,
    st.client_id,
    c.name AS client_name,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COALESCE(sub.compliance_plus_flag, false) AS compliance_plus_flag,
    st.document_type,
    st.created_at,
    date_trunc('month', st.created_at)::date AS month,
    st.sent_at,
    st.registered_at,
    st.status,
    st.error_code,
    CASE
        WHEN EXTRACT(ISODOW FROM st.created_at) BETWEEN 1 AND 4 THEN st.created_at + INTERVAL '1 day'
        WHEN EXTRACT(ISODOW FROM st.created_at) = 5 THEN st.created_at + INTERVAL '3 days'
        WHEN EXTRACT(ISODOW FROM st.created_at) = 6 THEN st.created_at + INTERVAL '2 days'
        ELSE st.created_at + INTERVAL '1 day'
    END AS one_business_day_deadline_at,
    st.registered_at IS NOT NULL
      AND st.registered_at <= CASE
          WHEN EXTRACT(ISODOW FROM st.created_at) BETWEEN 1 AND 4 THEN st.created_at + INTERVAL '1 day'
          WHEN EXTRACT(ISODOW FROM st.created_at) = 5 THEN st.created_at + INTERVAL '3 days'
          WHEN EXTRACT(ISODOW FROM st.created_at) = 6 THEN st.created_at + INTERVAL '2 days'
          ELSE st.created_at + INTERVAL '1 day'
      END AS registered_within_one_business_day
FROM sed_transfers st
JOIN clients c ON c.id = st.client_id
LEFT JOIN LATERAL (
    SELECT bool_or(compliance_plus_flag) AS compliance_plus_flag
    FROM subscriptions s
    WHERE s.client_id = st.client_id
      AND s.started_at <= st.created_at::date
      AND (s.ended_at IS NULL OR s.ended_at >= st.created_at::date)
) sub ON TRUE;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_semd_deadline_ui AS
SELECT
    month,
    client_id,
    client_name,
    tier,
    region,
    compliance_plus_flag,
    COUNT(*) FILTER (WHERE registered_within_one_business_day)::bigint AS on_time,
    COUNT(*) FILTER (WHERE registered_at IS NOT NULL AND NOT registered_within_one_business_day)::bigint AS late,
    COUNT(*) FILTER (WHERE registered_at IS NULL)::bigint AS failed,
    COUNT(*)::bigint AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE registered_within_one_business_day) / NULLIF(COUNT(*), 0), 2) AS on_time_pct
FROM public.v_rpt_service_audit_semd_transfers_ui
WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY month, client_id, client_name, tier, region, compliance_plus_flag;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_compliance_score_ui AS
SELECT
    c.id AS client_id,
    c.name AS client_name,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COALESCE(sub.compliance_plus_flag, false) AS compliance_plus_flag,
    COUNT(st.id)::bigint AS transfers_30d,
    (COUNT(st.id) FILTER (WHERE st.registered_within_one_business_day) * 1.0 / NULLIF(COUNT(st.id), 0))::numeric(12, 4) AS on_time_rate,
    CASE
        WHEN COUNT(st.id) = 0 THEN 'N/A'
        WHEN COUNT(st.id) FILTER (WHERE st.registered_within_one_business_day) * 1.0 / NULLIF(COUNT(st.id), 0) >= 0.99 THEN 'A'
        WHEN COUNT(st.id) FILTER (WHERE st.registered_within_one_business_day) * 1.0 / NULLIF(COUNT(st.id), 0) >= 0.95 THEN 'B'
        WHEN COUNT(st.id) FILTER (WHERE st.registered_within_one_business_day) * 1.0 / NULLIF(COUNT(st.id), 0) >= 0.90 THEN 'C'
        ELSE 'D - risk'
    END AS compliance_grade
FROM clients c
LEFT JOIN public.v_rpt_service_audit_semd_transfers_ui st
       ON st.client_id = c.id
      AND st.created_at >= CURRENT_DATE - INTERVAL '30 days'
LEFT JOIN LATERAL (
    SELECT bool_or(compliance_plus_flag) AS compliance_plus_flag
    FROM subscriptions s
    WHERE s.client_id = c.id
      AND (s.ended_at IS NULL OR s.ended_at >= CURRENT_DATE)
) sub ON TRUE
GROUP BY c.id, c.name, c.tier, COALESCE(NULLIF(c.region, ''), 'unknown'), COALESCE(sub.compliance_plus_flag, false);

CREATE OR REPLACE VIEW public.v_rpt_service_audit_semd_error_types_ui AS
SELECT
    COALESCE(NULLIF(error_code, ''), '(no error_code)') AS error_code,
    tier,
    region,
    compliance_plus_flag,
    COUNT(*)::bigint AS occurrences,
    COUNT(DISTINCT client_id)::bigint AS affected_clients,
    AVG(EXTRACT(EPOCH FROM (registered_at - sent_at)) / 3600.0)::numeric(12, 2) AS avg_retry_hours
FROM public.v_rpt_service_audit_semd_transfers_ui
WHERE status = 'failed'
  AND created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY COALESCE(NULLIF(error_code, ''), '(no error_code)'), tier, region, compliance_plus_flag;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_cohort_retention_ui AS
WITH cohorts AS (
    SELECT
        date_trunc('month', signup_date)::date AS cohort_month,
        id AS client_id,
        tier,
        COALESCE(NULLIF(region, ''), 'unknown') AS region
    FROM clients
),
activity AS (
    SELECT
        c.cohort_month,
        date_trunc('month', b.month)::date AS active_month,
        c.tier,
        c.region,
        COUNT(DISTINCT c.client_id)::bigint AS active_clients
    FROM cohorts c
    JOIN billing b ON b.client_id = c.client_id AND b.amount_paid > 0
    GROUP BY c.cohort_month, date_trunc('month', b.month)::date, c.tier, c.region
)
SELECT
    cohort_month,
    active_month,
    tier,
    region,
    ((date_part('year', age(active_month, cohort_month)) * 12) + date_part('month', age(active_month, cohort_month)))::integer AS lifetime_month,
    active_clients,
    (active_clients * 1.0 / NULLIF(FIRST_VALUE(active_clients) OVER (PARTITION BY cohort_month, tier, region ORDER BY active_month), 0))::numeric(12, 4) AS retention_pct
FROM activity;

CREATE OR REPLACE VIEW public.v_rpt_service_audit_churn_risk_ui AS
WITH sed_stats_30d AS (
    SELECT
        client_id,
        COUNT(*) FILTER (WHERE registered_within_one_business_day) * 1.0 / NULLIF(COUNT(*), 0) AS on_time_rate
    FROM public.v_rpt_service_audit_semd_transfers_ui
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY client_id
),
p1 AS (
    SELECT client_id, COUNT(*) AS p1_count
    FROM tickets
    WHERE severity = 'P1'
      AND opened_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY client_id
),
late_payments AS (
    SELECT client_id, COUNT(*) AS late_payment_count
    FROM billing
    WHERE payment_date > month + INTERVAL '7 days'
      AND month >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY client_id
),
active_subscriptions AS (
    SELECT client_id, bool_or(compliance_plus_flag) AS compliance_plus_flag
    FROM subscriptions
    WHERE ended_at IS NULL OR ended_at >= CURRENT_DATE
    GROUP BY client_id
)
SELECT
    c.id AS client_id,
    c.name AS client_name,
    c.tier,
    COALESCE(NULLIF(c.region, ''), 'unknown') AS region,
    COALESCE(active_subscriptions.compliance_plus_flag, false) AS compliance_plus_flag,
    COALESCE(sed_stats_30d.on_time_rate, 0)::numeric(12, 4) AS compliance_rate,
    COALESCE(p1.p1_count, 0)::bigint AS p1_last_90d,
    COALESCE(late_payments.late_payment_count, 0)::bigint AS late_payments_6m,
    ROUND((
        (1 - COALESCE(sed_stats_30d.on_time_rate, 0)) * 40
        + LEAST(COALESCE(p1.p1_count, 0) * 5, 30)
        + LEAST(COALESCE(late_payments.late_payment_count, 0) * 10, 30)
    )::numeric, 2) AS churn_risk_score,
    CASE
        WHEN (
            (1 - COALESCE(sed_stats_30d.on_time_rate, 0)) * 40
            + LEAST(COALESCE(p1.p1_count, 0) * 5, 30)
            + LEAST(COALESCE(late_payments.late_payment_count, 0) * 10, 30)
        ) >= 70 THEN 'high'
        WHEN (
            (1 - COALESCE(sed_stats_30d.on_time_rate, 0)) * 40
            + LEAST(COALESCE(p1.p1_count, 0) * 5, 30)
            + LEAST(COALESCE(late_payments.late_payment_count, 0) * 10, 30)
        ) >= 40 THEN 'medium'
        ELSE 'low'
    END AS churn_risk_band
FROM clients c
LEFT JOIN sed_stats_30d ON sed_stats_30d.client_id = c.id
LEFT JOIN p1 ON p1.client_id = c.id
LEFT JOIN late_payments ON late_payments.client_id = c.id
LEFT JOIN active_subscriptions ON active_subscriptions.client_id = c.id
WHERE c.status = 'active';

CREATE OR REPLACE VIEW public.v_rpt_service_audit_nrr_ui AS
WITH last_year AS (
    SELECT
        client_id,
        MAX(tier) AS tier,
        MAX(region) AS region,
        bool_or(compliance_plus_flag) AS compliance_plus_flag,
        SUM(revenue_accrual) AS revenue_base
    FROM public.v_rpt_service_audit_jid_month_ui
    WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '24 months'
      AND month <  date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
    GROUP BY client_id
),
this_year AS (
    SELECT client_id, SUM(revenue_accrual) AS revenue_current
    FROM public.v_rpt_service_audit_jid_month_ui
    WHERE month >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
      AND month <  date_trunc('month', CURRENT_DATE)::date + INTERVAL '1 month'
    GROUP BY client_id
)
SELECT
    last_year.tier,
    last_year.region,
    last_year.compliance_plus_flag,
    SUM(COALESCE(this_year.revenue_current, 0))::numeric(14, 2) AS revenue_current,
    SUM(last_year.revenue_base)::numeric(14, 2) AS revenue_base,
    (SUM(COALESCE(this_year.revenue_current, 0)) * 1.0 / NULLIF(SUM(last_year.revenue_base), 0))::numeric(12, 4) AS nrr
FROM last_year
LEFT JOIN this_year ON this_year.client_id = last_year.client_id
GROUP BY last_year.tier, last_year.region, last_year.compliance_plus_flag;
