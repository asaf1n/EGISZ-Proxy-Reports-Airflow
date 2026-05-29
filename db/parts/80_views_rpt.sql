-- ============================================================================
-- 80_views_rpt.sql — v_rpt_network_errors_detail_ui + v_rpt_documents_no_response_ui + v_rpt_semd_archive_ui + connectivity views
-- Source: db/dwh_init.sql, lines [1506..1744).
-- Loaded by db/dwh_init.sql via \i db/parts/80_views_rpt.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
SELECT
    COALESCE(d.last_callback_at, d.sent_at, d.updated_at) AS "Дата создания документа",
    COALESCE(d.callback_log_id, d.source_logid)::text AS "LOGID журнала (сетевая ошибка)",
    d.message_id AS "MSGID обмена",
    d.document_key AS "Документ (ключ учёта)",
    d.relates_to_id AS "relatesToMessage (из текста журнала)",
    d.local_uid AS "localUid / DOCUMENTID (из текста)",
    d.emdr_id AS "emdrId (из текста)",
    public.egisz_clean_host(d.error_text) AS "Хост клиники (VPN ГОСТ)",
    d.jid::text AS "JID клиники",
    d.jid::text AS "JID из журнала (gost, число)",
    COALESCE(o.name, 'Клиника JID: ' || COALESCE(d.jid::text, '(нет JID)')) AS "Клиника (транспорт)",
    o.name AS "Медицинская организация",
    public.egisz_semd_type_report_label(d.semd_code, NULL) AS "Тип СЭМД (код · НСИ)",
    public.egisz_normalize_semd_code(d.semd_code) AS "Код СЭМД",
    d.error_summary AS "Сводка ошибки регистрации",
    d.error_text AS "Текст сетевой ошибки",
    d.error_text AS "Сообщение",
    d.error_type AS "Подтип ошибки канала",
    'да'::text AS "Связанный колбэк найден в аналитике",
    COALESCE(d.callback_log_id, d.source_logid)::text AS "LOGID записи ответа",
    d.relates_to_id AS "Связанное сообщение (ответ РЭМД)",
    d.local_uid AS "Идентификатор документа (localUid)",
    d.emdr_id AS "Регистрационный номер РЭМД"
FROM public.fact_egisz_documents d
LEFT JOIN public.dim_organizations o ON d.jid = o.jid
WHERE d.status = 'network_error';

COMMENT ON VIEW public.v_rpt_network_errors_detail_ui IS
'Техническая витрина ошибок связи proxy_egisz: document-grain fact (status=network_error), без отдельной stg-таблицы канала.';

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
SELECT
    d.sent_at AS "Отправлено",
    EXTRACT(EPOCH FROM (now() - d.sent_at))/3600.0 AS "Часов в ожидании",
    CASE
        WHEN now() - d.sent_at > INTERVAL '72 hours' THEN 'просрочено'
        ELSE 'в обработке'
    END AS "Категория ожидания",
    d."localUid СЭМД",
    d."Идентификатор документа (localUid)",
    d."Код СЭМД",
    d."Наименование СЭМД",
    d."Тип СЭМД (код · НСИ)",
    d."JID клиники",
    d."Наименование клиники",
    d."Связанное сообщение",
    d."MSGID обмена",
    d."Хост клиники (VPN ГОСТ)"
FROM public.v_egisz_documents_enriched_ui d
WHERE d.document_status = 'waiting';

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
        WHEN "Статус (код)" IN ('async_error', 'network_error')
        THEN 'Документ с ошибкой и не определён код'
        ELSE 'Документ без кода СЭМД'
    END AS "СЭМД (архив)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "JID клиники",
    "Наименование клиники",
    "ИНН клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД",
    -- Единая нотификация: в колонке «Статус» во всех таблицах/графиках — адаптированный
    -- русский текст (4 значения). «Статус (код)» — машинный код для WHERE/FILTER карточек.
    "Статус (отчёт)" AS "Статус",
    "Статус (код)",
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Исходный текст ошибки",
    "Хост клиники (VPN ГОСТ)"
FROM public.v_egisz_documents_enriched_ui
WHERE NULLIF(TRIM("Документ (ключ учёта)"), '') IS NOT NULL
),
ranked AS (
    SELECT
        source_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF("Документ (ключ учёта)", '')
            ORDER BY
                NULLIF("LOGID журнала EXCHANGELOG", '')::bigint DESC NULLS LAST,
                "Дата обработки" DESC NULLS LAST,
                CASE
                    WHEN "Статус (код)" = 'success' THEN 0
                    WHEN "Статус (код)" IN ('async_error', 'network_error') THEN 1
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
    "ИНН клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД",
    "Статус",
    "Статус (код)",
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Исходный текст ошибки",
    "Хост клиники (VPN ГОСТ)"
FROM ranked
WHERE rn = 1;

COMMENT ON VIEW public.v_rpt_documents_ui IS
'Единая документная витрина: одна актуальная строка на "Документ (ключ учёта)". Документы без localUid не попадают в fact_egisz_documents на этапе transform (getDocumentFile). Очередь без ответа — v_rpt_documents_no_response_ui (дашборд 03).';

-- Разбивка ошибок по категории (~10) и конкретному виду (~70) для двойного пирога.
-- Unnest'ит составной error_type (разделитель ' · ') → одна строка = один вид ошибки.
CREATE OR REPLACE VIEW public.v_rpt_error_category_breakdown_ui AS
WITH remd_errors AS (
    SELECT
        d."Дата обработки" AS "Обработано IPS",
        d."День (тренд)",
        d."Документ (ключ учёта)",
        d."JID клиники",
        d."Код СЭМД",
        d."Тип СЭМД (код · НСИ)",
        trim(err_item)                                                                AS "Тип ошибки"
    FROM public.v_rpt_documents_ui d
    CROSS JOIN LATERAL unnest(
        string_to_array(
            COALESCE(NULLIF(trim(d."Тип ошибки"), ''), 'Неизвестная ошибка'),
            ' · '
        )
    ) AS err_item
    WHERE d."Статус (код)" IN ('async_error', 'network_error')
      AND d."Тип ошибки" IS NOT NULL
      AND d."Документ (ключ учёта)" IS NOT NULL
      AND trim(d."Тип ошибки") <> ''
      AND trim(err_item) <> ''
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
FROM remd_errors;

COMMENT ON VIEW public.v_rpt_error_category_breakdown_ui IS
'Разбивка ошибок EGISZ-прокси: один ряд = один вид ошибки на документ.
Источник — v_rpt_documents_ui («Статус (код)» async_error / network_error).
Используется картой «Категории ошибок» и «Ошибки по типу».';

CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui AS
SELECT
    "Дата обработки" AS "Обработано IPS",
    "День (тренд)",
    "LOGID журнала EXCHANGELOG",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Рег. номер РЭМД" AS "Рег. номер РЭМД (emdrid)",
    "Связанное сообщение",
    "JID клиники",
    "Тип СЭМД (код · НСИ)",
    "Статус",
    CASE
        WHEN "Статус (код)" = 'success' THEN 'Успешно зарегистрирован'
        WHEN "Статус (код)" IN ('async_error', 'network_error')
        THEN COALESCE(NULLIF("Исходный текст ошибки", ''), '(нет текста)')
        ELSE ''
    END AS "Исходный текст ошибки",
    CASE
        WHEN "Статус (код)" = 'success' THEN 'Успешно зарегистрирован'
        WHEN "Статус (код)" IN ('async_error', 'network_error')
        THEN COALESCE(NULLIF("Сводка ошибки", ''), 'Неизвестная ошибка')
        ELSE ''
    END AS "Интерпретация ошибки",
    CASE
        WHEN "Статус (код)" = 'success' THEN 'Успешно зарегистрирован'
        WHEN "Статус (код)" IN ('async_error', 'network_error') THEN "Тип ошибки"
        ELSE ''
    END AS "Тип ошибки",
    CASE
        WHEN "Статус (код)" IN ('async_error', 'network_error') THEN 1::bigint
        ELSE NULL::bigint
    END AS "Порядок ошибки"
FROM public.v_rpt_documents_ui
WHERE "Статус (код)" IN ('success', 'async_error', 'network_error');

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
    "ИНН клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД",
    "Статус",
    "Статус (код)",
    "Тип ошибки",
    "LOGID журнала EXCHANGELOG",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки",
    "Исходный текст ошибки",
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
    FROM public.v_egisz_documents_enriched_ui
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
        f."Статус (код)" AS status_code,
        f."Статус (отчёт)" AS status_label,
        CASE
            WHEN f."Статус (код)" = 'success' THEN 1
            WHEN f."Статус (код)" = 'async_error' THEN 2
            WHEN f."Статус (код)" = 'network_error' THEN 3
            ELSE 4
        END AS status_sort,
        COALESCE(NULLIF(f."Сводка ошибки", ''), NULLIF(f."Исходный текст ошибки", ''), '(ошибка без текста)') AS error_text,
        CASE
            WHEN f."Статус (код)" = 'success'
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
    FROM public.v_egisz_documents_enriched_ui f
    -- Единый документный универсум: включаем «В обработке» (waiting), чтобы итог
    -- «всего документов» в клиентских дашбордах совпадал с операционным. Доли успеха/
    -- ошибок в карточках считаются по финализированным (success/async_error/network_error).
    WHERE NULLIF(f."Документ (ключ учёта)", '') IS NOT NULL
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
