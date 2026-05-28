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
        t.document_key                                                                 AS "Документ (ключ учёта)",
        t.jid::text                                                                   AS "JID клиники",
        public.egisz_normalize_semd_code(COALESCE(d.semd_code, t.semd_code))         AS "Код СЭМД",
        public.egisz_semd_type_report_label(COALESCE(d.semd_code, t.semd_code), t.semd_name) AS "Тип СЭМД (код · НСИ)",
        trim(err_item)                                                                AS "Тип ошибки"
    FROM fact_egisz_transactions t
    LEFT JOIN public.fact_egisz_documents d
      ON d.document_key = t.document_key
    CROSS JOIN LATERAL unnest(
        string_to_array(
            COALESCE(NULLIF(trim(t.error_type), ''), 'Неизвестная ошибка'),
            ' · '
        )
    ) AS err_item
    WHERE t.status = 'error'
      -- после §1 error_type пуст для pending/unknown → этот фильтр осмыслен;
      -- защищает от пустых корзин 'Неизвестная ошибка' в карточках 04 дашборда.
      AND t.error_type IS NOT NULL
      AND t.document_key IS NOT NULL
      AND trim(t.error_type) <> ''
      AND trim(err_item) <> ''
),
network_errors AS (
    SELECT
        n.created_at                AS "Обработано IPS",
        n.created_at::date          AS "День (тренд)",
        n.document_key              AS "Документ (ключ учёта)",
        NULL::text                  AS "JID клиники",
        NULL::text                  AS "Код СЭМД",
        NULL::text                  AS "Тип СЭМД (код · НСИ)",
        'Сетевая ошибка'::text      AS "Тип ошибки"
    FROM public.v_stg_channel_network_errors_by_document n
    WHERE n.document_key IS NOT NULL
)
SELECT
    "Обработано IPS",
    "День (тренд)",
    "Документ (ключ учёта)",
    "JID клиники",
    "Код СЭМД",
    "Тип СЭМД (код · НСИ)",
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
    "Тип СЭМД (код · НСИ)",
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
    s.document_key AS "Документ (ключ учёта)",
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
    ORDER BY
        CASE
            WHEN lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), '')) THEN 0
            ELSE 1
        END,
        f."Обработано IPS" DESC NULLS LAST
    LIMIT 1
) f ON TRUE;

COMMENT ON VIEW public.v_rpt_network_errors_detail_ui IS
'Техническая витрина ошибок связи proxy_egisz: healthcheck/поддержка клиник, LOGSTATE=3 и строки журнала с привязкой к документу, если её удалось восстановить.';

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
WITH source_documents AS (
    SELECT document_key, semd_code
    FROM public.fact_egisz_documents
),
messages_all AS (
    SELECT
        m.egmid,
        m.created_at,
        m.msgid,
        m.reply_to,
        public.egisz_clean_text_value(m.document_id) AS document_id,
        m.document_key,
        source_doc.semd_code AS semd_code_resolved,
        m.msgid_norm,
        m.document_id_norm,
        m.reply_to_jid,
        m.reply_to_host
    FROM public.fact_egisz_messages m
    LEFT JOIN source_documents source_doc
      ON source_doc.document_key = m.document_key
),
messages AS (
    SELECT DISTINCT ON (document_key)
        *
    FROM messages_all
    WHERE document_key IS NOT NULL
    ORDER BY document_key, created_at DESC NULLS LAST, egmid DESC
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
known_document_keys AS (
    SELECT DISTINCT f.document_key
    FROM fact_egisz_transactions f
    WHERE f.document_key IS NOT NULL

    UNION

    SELECT document_key
    FROM public.fact_egisz_documents
    WHERE document_key IS NOT NULL
)
SELECT
    m.created_at AS "Отправлено",
    EXTRACT(EPOCH FROM (now() - m.created_at))/3600.0 AS "Часов в ожидании",
    CASE
        WHEN now() - m.created_at > INTERVAL '72 hours' THEN 'просрочено'
        ELSE 'в обработке'
    END AS "Категория ожидания",
    m.document_id AS "localUid СЭМД",
    m.document_id AS "Идентификатор документа (localUid)",
    m.semd_code_resolved AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_normalize_semd_code(m.semd_code_resolved) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    public.egisz_semd_type_report_label(m.semd_code_resolved, NULL) AS "Тип СЭМД (код · НСИ)",
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
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM public.dim_semd_types dst
    WHERE dst.oid = public.egisz_normalize_semd_code(m.semd_code_resolved)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN fact_message_keys fm ON fm.message_key = m.msgid_norm
LEFT JOIN known_document_keys kd ON kd.document_key = m.document_id_norm
LEFT JOIN public.fact_egisz_transactions fe ON fe.egmid = m.egmid
WHERE fm.message_key IS NULL
  AND kd.document_key IS NULL
  AND fe.egmid IS NULL;

CREATE OR REPLACE VIEW public.v_rpt_documents_ui AS
WITH source_rows AS (
SELECT
    "Обработано IPS" AS "Дата обработки",
    "День (тренд)",
    CASE
        WHEN NULLIF(TRIM("Код СЭМД"), '') IS NOT NULL
        THEN COALESCE(
            NULLIF(TRIM("Наименование СЭМД"), ''),
            NULLIF(TRIM("Тип СЭМД (код · НСИ)"), ''),
            NULLIF(TRIM("Код СЭМД"), '')
        )
        WHEN "Статус" IN ('error', 'unknown')
        THEN 'Документ с ошибкой и не определён код'
        ELSE 'Документ без кода СЭМД'
    END AS "СЭМД (архив)",
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
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Хост клиники (VPN ГОСТ)"
FROM public.v_egisz_transactions_enriched_ui
WHERE "Статус" IN ('success', 'error')
  AND NULLIF(TRIM("Документ (ключ учёта)"), '') IS NOT NULL
),
ranked AS (
    SELECT
        source_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF("Документ (ключ учёта)", '')
            ORDER BY
                NULLIF("LOGID журнала EXCHANGELOG", '')::bigint DESC NULLS LAST,
                NULLIF("EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)", '')::bigint DESC NULLS LAST,
                "Дата обработки" DESC NULLS LAST,
                CASE
                    WHEN "Статус" = 'success' THEN 0
                    WHEN "Статус" = 'error' THEN 1
                    ELSE 2
                END,
                "MSGID обмена" DESC NULLS LAST
        ) AS rn
    FROM source_rows
)
SELECT
    "Дата обработки",
    "День (тренд)",
    "СЭМД (архив)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID",
    "JID клиники",
    "Наименование клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД",
    "Статус",
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Хост клиники (VPN ГОСТ)"
FROM ranked
WHERE rn = 1;

COMMENT ON VIEW public.v_rpt_documents_ui IS
'Единая документная витрина распознанных документов: одна актуальная строка на "Документ (ключ учёта)" только для callback-фактов success/error. Очередь без callback остаётся в v_rpt_documents_no_response_ui и используется дашбордом 03.';

CREATE OR REPLACE VIEW public.v_rpt_semd_archive_ui AS
SELECT
    "Дата обработки",
    "День (тренд)",
    "СЭМД (архив)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID",
    "JID клиники",
    "Наименование клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД",
    "Статус",
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Хост клиники (VPN ГОСТ)"
FROM public.v_rpt_documents_ui;

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
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS err_cnt
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
        NULLIF(f."Документ (ключ учёта)", '') AS document_key,
        NULLIF(f."Код СЭМД", '') AS semd_code,
        COALESCE(NULLIF(f."Тип СЭМД (код · НСИ)", ''), NULLIF(f."Наименование СЭМД", ''), '(тип СЭМД не определен)') AS document_type,
        CASE
            WHEN f."Статус" = 'success' THEN 'success'
            WHEN f."Статус" = 'error' AND f."Тип ошибки" = 'Сетевая ошибка' THEN 'network_error'
            ELSE 'registration_error'
        END AS status_code,
        CASE
            WHEN f."Статус" = 'success' THEN 'Успешный ответ'
            WHEN f."Статус" = 'error' AND f."Тип ошибки" = 'Сетевая ошибка' THEN 'Ошибка связи'
            ELSE 'Ошибка регистрации'
        END AS status_label,
        CASE
            WHEN f."Статус" = 'success' THEN 1
            WHEN f."Статус" = 'error' AND f."Тип ошибки" = 'Сетевая ошибка' THEN 3
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
        f.patient_name_masked,
        f.snils_masked,
        f.doctor_name,
        f.patient_hash,
        f.doctor_hash
    FROM public.v_egisz_transactions_enriched_ui f
    WHERE f."Статус" IN ('success', 'error')
      AND NULLIF(f."Документ (ключ учёта)", '') IS NOT NULL
),
source_rows AS (
    SELECT * FROM fact_source
),
ranked AS (
    SELECT
        source_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(document_key, '')
            ORDER BY
                CASE WHEN document_row_id ~ '^[0-9]+$' THEN document_row_id::bigint END DESC NULLS LAST,
                document_ts DESC NULLS LAST,
                status_sort,
                document_row_id DESC
        ) AS rn
    FROM source_rows
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
    patient_name_masked,
    snils_masked,
    doctor_name,
    patient_hash,
    doctor_hash
FROM ranked
WHERE rn = 1;
