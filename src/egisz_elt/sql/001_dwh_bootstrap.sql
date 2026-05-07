CREATE TABLE IF NOT EXISTS elt_state (
    pipeline text PRIMARY KEY,
    last_log_id bigint DEFAULT 0,
    last_egmid bigint DEFAULT 0,
    updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS egisz_raw (
    logid bigint PRIMARY KEY,
    logdate timestamptz,
    msgid text,
    logstate integer,
    logtext text,
    msgtext text,
    loaded_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS egisz_messages_raw (
    egmid bigint PRIMARY KEY,
    jid integer,
    kind text,
    created_at timestamptz,
    msgid text,
    reply_to text,
    document_id text,
    msgtext text,
    loaded_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_organizations (
    jid integer PRIMARY KEY,
    name text,
    inn text,
    address text,
    updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_licenses (
    id bigint PRIMARY KEY,
    service_type integer,
    jid integer,
    mo_uid text,
    mo_domen text,
    bdate date,
    fdate date,
    kind text,
    modifydate timestamptz,
    updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_egisz_transactions (
    exchangelog_log_id bigint PRIMARY KEY REFERENCES egisz_raw(logid),
    log_date timestamptz,
    message_id text,
    relates_to_id text,
    local_uid_semd text,
    emdr_id text,
    doc_number text,
    org_oid text,
    status text,
    error_message text,
    callback_url text,
    egmid bigint,
    jid integer,
    semd_code text,
    semd_name text,
    error_code text,
    errors_json jsonb DEFAULT '[]'::jsonb,
    processed_at timestamptz DEFAULT now()
);

ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS egmid bigint;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS jid integer;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS semd_code text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS semd_name text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS error_code text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS errors_json jsonb DEFAULT '[]'::jsonb;

CREATE OR REPLACE FUNCTION public.egisz_xml_text(payload text, tag_name text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    safe_tag text;
    match text[];
BEGIN
    IF payload IS NULL OR tag_name IS NULL OR position('<' in payload) = 0 THEN
        RETURN NULL;
    END IF;

    safe_tag := regexp_replace(tag_name, '[^A-Za-z0-9_:-]', '', 'g');
    IF safe_tag = '' THEN
        RETURN NULL;
    END IF;

    match := regexp_match(
        payload,
        '<(?:[A-Za-z0-9_]+:)?' || safe_tag || '(?:\s[^>]*)?>(.*?)</(?:[A-Za-z0-9_]+:)?' || safe_tag || '>',
        'is'
    );
    RETURN NULLIF(btrim(replace(replace(replace(match[1], E'\n', ' '), E'\r', ' '), E'\t', ' ')), '');
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_semd_type_report_label(semd_code text, semd_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN NULLIF(btrim(COALESCE(semd_code, '')), '') IS NULL
             AND NULLIF(btrim(COALESCE(semd_name, '')), '') IS NULL THEN '(неизвестно)'
        WHEN NULLIF(btrim(COALESCE(semd_code, '')), '') IS NULL THEN btrim(semd_name)
        WHEN NULLIF(btrim(COALESCE(semd_name, '')), '') IS NULL THEN btrim(semd_code)
        ELSE btrim(semd_code) || ' · ' || btrim(semd_name)
    END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_type(error_code text, error_message text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN NULLIF(btrim(COALESCE(error_message, '')), '') IS NULL
             AND NULLIF(btrim(COALESCE(error_code, '')), '') IS NULL THEN '(ошибка без деталей)'
        WHEN COALESCE(error_message, '') ILIKE '%network%' OR COALESCE(error_message, '') ILIKE '%connection%'
            THEN 'ошибка связи (транспорт)'
        WHEN COALESCE(error_message, '') ILIKE '%timeout%' OR COALESCE(error_message, '') ILIKE '%timed out%'
            THEN 'таймаут канала'
        WHEN COALESCE(error_message, '') ILIKE '%remd%' OR COALESCE(error_message, '') ILIKE '%рэмд%'
            THEN 'ошибка асинхронного ответа РЭМД'
        WHEN NULLIF(btrim(COALESCE(error_code, '')), '') IS NOT NULL
            THEN btrim(error_code) || ': ' || left(COALESCE(NULLIF(btrim(error_message), ''), 'ошибка без текста'), 180)
        ELSE left(btrim(error_message), 180)
    END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(max_log_id bigint)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
BEGIN
    WITH parsed AS (
        SELECT
            r.logid,
            r.logdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            COALESCE(public.egisz_xml_text(r.msgtext, 'messageId'), r.msgid) AS message_id,
            COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo')) AS relates_to_id,
            COALESCE(public.egisz_xml_text(r.msgtext, 'localUid'), public.egisz_xml_text(r.msgtext, 'DOCUMENTID')) AS local_uid_semd,
            public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id,
            public.egisz_xml_text(r.msgtext, 'documentNumber') AS doc_number,
            public.egisz_xml_text(r.msgtext, 'organization') AS org_oid,
            COALESCE(public.egisz_xml_text(r.msgtext, 'kind'), public.egisz_xml_text(r.msgtext, 'KIND')) AS semd_code,
            COALESCE(public.egisz_xml_text(r.msgtext, 'name'), public.egisz_xml_text(r.msgtext, 'documentName')) AS semd_name,
            public.egisz_xml_text(r.msgtext, 'code') AS error_code,
            public.egisz_xml_text(r.msgtext, 'message') AS xml_message,
            lower(COALESCE(public.egisz_xml_text(r.msgtext, 'status'), '')) AS raw_status,
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload
        FROM egisz_raw r
        WHERE r.logid <= max_log_id
          AND COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
    ),
    enriched AS (
        SELECT
            p.*,
            CASE
                WHEN p.logstate = 3 THEN 'error'
                WHEN p.raw_status LIKE '%success%' THEN 'success'
                WHEN p.raw_status LIKE '%error%' OR COALESCE(p.msgtext, '') ILIKE '%error%' THEN 'error'
                ELSE 'unknown'
            END AS final_status,
            CASE
                WHEN p.logstate = 3 THEN 'Network Error: ' || COALESCE(NULLIF(p.logtext, ''), 'no details')
                ELSE p.xml_message
            END AS final_error_message
        FROM parsed p
    )
    INSERT INTO fact_egisz_transactions (
        exchangelog_log_id,
        log_date,
        message_id,
        relates_to_id,
        local_uid_semd,
        emdr_id,
        doc_number,
        org_oid,
        status,
        error_message,
        callback_url,
        jid,
        semd_code,
        semd_name,
        error_code,
        errors_json,
        processed_at
    )
    SELECT
        e.logid,
        e.logdate,
        e.message_id,
        e.relates_to_id,
        e.local_uid_semd,
        e.emdr_id,
        e.doc_number,
        e.org_oid,
        e.final_status,
        e.final_error_message,
        e.logtext,
        e.jid_from_payload,
        e.semd_code,
        e.semd_name,
        e.error_code,
        CASE
            WHEN e.final_status = 'error' THEN jsonb_build_array(jsonb_build_object('code', e.error_code, 'message', e.final_error_message))
            ELSE '[]'::jsonb
        END,
        now()
    FROM enriched e
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        message_id = EXCLUDED.message_id,
        relates_to_id = EXCLUDED.relates_to_id,
        local_uid_semd = EXCLUDED.local_uid_semd,
        emdr_id = COALESCE(fact_egisz_transactions.emdr_id, EXCLUDED.emdr_id),
        doc_number = EXCLUDED.doc_number,
        org_oid = EXCLUDED.org_oid,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        callback_url = EXCLUDED.callback_url,
        jid = COALESCE(EXCLUDED.jid, fact_egisz_transactions.jid),
        semd_code = EXCLUDED.semd_code,
        semd_name = EXCLUDED.semd_name,
        error_code = EXCLUDED.error_code,
        errors_json = EXCLUDED.errors_json,
        processed_at = now();

    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

CREATE OR REPLACE VIEW public.v_egisz_transactions_enriched_ui AS
SELECT
    t.exchangelog_log_id AS "LOGID журнала EXCHANGELOG",
    t.egmid AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    t.message_id AS "MSGID обмена",
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День (тренд)",
    t.log_date::date AS "День",
    t.log_date::date AS "Дата",
    date_trunc('hour', t.log_date) AS "Час",
    COALESCE(t.local_uid_semd, t.doc_number, t.emdr_id, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    COALESCE(t.local_uid_semd, t.doc_number, t.emdr_id, t.message_id, t.exchangelog_log_id::text) AS "Ключ документа (группировка)",
    t.local_uid_semd AS "localUid СЭМД",
    t.relates_to_id AS "Связанное сообщение",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.status AS "Статус",
    t.errors_json AS "Ошибки JSON",
    public.egisz_error_interpretation_type(t.error_code, t.error_message) AS "Подкатегория ошибки (глобально)",
    COALESCE(NULLIF(t.error_message, ''), '(без ошибки)') AS "Сводка ошибки",
    t.semd_code AS "Код СЭМД",
    t.semd_name AS "Наименование СЭМД",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "СЭМД",
    COALESCE(t.jid, l.jid) AS "JID клиники",
    t.jid AS "JID из журнала (gost, число)",
    t.org_oid AS "OID клиники",
    t.org_oid AS "OID организации",
    o.name AS "Наименование клиники",
    o.name AS "Медицинская организация",
    COALESCE(o.name, 'Клиника JID: ' || COALESCE(t.jid, l.jid)::text) AS "Клиника",
    t.callback_url AS "Клиника (транспорт)",
    CASE WHEN t.local_uid_semd IS NULL THEN 1 ELSE 0 END AS "Без localUid",
    CASE WHEN t.semd_code IS NULL THEN 1 ELSE 0 END AS "Без кода СЭМД",
    CASE WHEN t.status = 'success' THEN 1 ELSE 0 END AS "Успешно",
    CASE WHEN t.status = 'error' THEN 1 ELSE 0 END AS "Ошибок",
    1 AS "Всего"
FROM fact_egisz_transactions t
LEFT JOIN dim_licenses l ON t.org_oid = l.mo_uid OR t.jid = l.jid
LEFT JOIN dim_organizations o ON COALESCE(t.jid, l.jid) = o.jid;

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
SELECT
    "localUid СЭМД",
    "Документ (ключ учёта)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники",
    "Наименование клиники",
    "Обработано IPS" AS "Отправлено",
    "Обработано IPS",
    "Статус",
    CASE
        WHEN "Обработано IPS" IS NULL THEN 'дата неизвестна'
        WHEN now() - "Обработано IPS" < interval '1 hour' THEN 'менее 1 часа'
        WHEN now() - "Обработано IPS" < interval '24 hours' THEN '1–24 часа'
        ELSE 'свыше 24 часов'
    END AS "Срок ожидания"
FROM public.v_egisz_transactions_enriched_ui
WHERE COALESCE("Статус", 'unknown') <> 'success';

CREATE OR REPLACE VIEW public.v_rpt_semd_archive_ui AS
SELECT
    "Обработано IPS" AS "Дата обработки",
    "День (тренд)",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Код СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "Наименование клиники",
    "Связанное сообщение",
    "Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД",
    "Статус",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)"
FROM public.v_egisz_transactions_enriched_ui;

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
SELECT
    "Обработано IPS" AS "Дата создания документа",
    "День",
    "Час",
    "LOGID журнала EXCHANGELOG" AS "LOGID журнала (сетевая ошибка)",
    "LOGID журнала EXCHANGELOG" AS "EXCHANGELOG.LOGID",
    "MSGID обмена" AS "EXCHANGELOG.MSGID",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)" AS "EGISZ_MESSAGES.EGMID",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)" AS "EGMID сообщения (строка журнала)",
    "MSGID обмена",
    "Ключ документа (группировка)",
    "Документ (ключ учёта)",
    "localUid СЭМД" AS "localUid / DOCUMENTID (из текста)",
    "Рег. номер РЭМД (emdrid)" AS "emdrId (из текста)",
    "Связанное сообщение" AS "relatesToMessage (из текста журнала)",
    "Связанное сообщение" AS "Связанное сообщение (ответ РЭМД)",
    "Код СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники",
    "JID из журнала (gost, число)",
    "Наименование клиники",
    "Медицинская организация",
    "Клиника (транспорт)",
    "Сводка ошибки" AS "Текст сетевой ошибки",
    "Сводка ошибки" AS "Сеть (фрагмент)",
    "Сводка ошибки" AS "Сообщение",
    "Сводка ошибки" AS "Сообщение (норм.)",
    "Подкатегория ошибки (глобально)" AS "Подтип ошибки канала",
    "Подкатегория ошибки (глобально)" AS "Подтип",
    "Подкатегория ошибки (глобально)" AS "Интерпретация ошибок регистрации",
    "Ошибки JSON",
    "Статус",
    CASE WHEN "Статус" = 'error' THEN 'Сетевая ошибка' ELSE "Статус" END AS "Тип",
    CASE WHEN "Статус" = 'error' THEN 'да' ELSE 'нет' END AS "Сетевая ошибка",
    CASE WHEN "Связанное сообщение" IS NULL THEN 'нет' ELSE 'да' END AS "Связанный колбэк найден в аналитике",
    NULL::bigint AS "LOGID записи ответа",
    NULL::bigint AS "EGMID записи ответа",
    "localUid СЭМД" AS "Идентификатор документа (localUid)",
    "Рег. номер РЭМД (emdrid)" AS "Регистрационный номер РЭМД"
FROM public.v_egisz_transactions_enriched_ui
WHERE "Статус" = 'error';

CREATE OR REPLACE VIEW public.v_stg_channel_errors_by_document AS
SELECT
    row_number() OVER (ORDER BY "Дата создания документа" DESC NULLS LAST) AS id,
    "Подтип ошибки канала" AS error_code,
    "Сообщение" AS message,
    "Дата создания документа" AS created_at,
    "relatesToMessage (из текста журнала)" AS relates_to_hint,
    "localUid / DOCUMENTID (из текста)" AS local_uid_hint,
    "emdrId (из текста)" AS emdr_id_hint,
    "LOGID журнала (сетевая ошибка)" AS exchangelog_log_id,
    "EXCHANGELOG.MSGID" AS journal_msgid,
    "EGISZ_MESSAGES.EGMID" AS egisz_messages_egmid,
    "Ключ документа (группировка)" AS document_group_key,
    "Связанное сообщение (ответ РЭМД)" AS relates_to_id,
    "Ключ документа (группировка)",
    "LOGID журнала (сетевая ошибка)" AS "EXCHANGELOG.LOGID",
    "EXCHANGELOG.MSGID",
    "EGISZ_MESSAGES.EGMID",
    "localUid / DOCUMENTID (из текста)" AS "localUid (из текста)",
    "emdrId (из текста)",
    "relatesToMessage (из текста журнала)",
    "Связанное сообщение (ответ РЭМД)" AS "relates_to_id (если есть)",
    "Дата создания документа" AS "Создано",
    "Тип",
    "Подтип ошибки канала" AS "Код ошибки",
    "Сообщение",
    row_number() OVER (ORDER BY "Дата создания документа" DESC NULLS LAST) AS "Номер записи",
    COUNT(*) OVER ()::bigint AS "Строк в stg_channel_errors"
FROM public.v_rpt_network_errors_detail_ui;

CREATE OR REPLACE VIEW public.v_stg_channel_network_errors_by_document AS
SELECT * FROM public.v_stg_channel_errors_by_document;

CREATE OR REPLACE VIEW public.v_rpt_connectivity_global_daily_ui AS
SELECT
    "День",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')::bigint AS "Успешные ответы РЭМД (документов)",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS "Ошибки связи (документов)",
    ROUND(
        100.0 * COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')
        / NULLIF(COUNT(DISTINCT "Документ (ключ учёта)"), 0),
        2
    ) AS "Доступность транспорта (прибл.), %"
FROM public.v_egisz_transactions_enriched_ui
GROUP BY 1;

CREATE OR REPLACE VIEW public.v_rpt_clinic_connectivity_daily_ui AS
SELECT
    "День",
    "JID клиники" AS "JID клиники (ключ)",
    "Наименование клиники",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')::bigint AS "Ответы РЭМД: успех (документов)",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS "Ответы РЭМД: отказ (документов)",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS "Ошибки связи (документов)",
    ROUND(
        100.0 * COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')
        / NULLIF(COUNT(DISTINCT "Документ (ключ учёта)"), 0),
        2
    ) AS "Доступность транспорта (прибл.), %"
FROM public.v_egisz_transactions_enriched_ui
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
SELECT
    "JID клиники",
    "Наименование клиники",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Обработано IPS" >= now() - interval '24 hours')::bigint AS "Документов за 24ч",
    COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" <> 'success')::bigint AS "В очереди (документов)",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "Статус" = 'error') / NULLIF(COUNT(*), 0), 2) AS "Доля ошибок, %",
    CASE
        WHEN COUNT(*) FILTER (WHERE "Статус" = 'error') = 0 THEN 'ok'
        WHEN COUNT(*) FILTER (WHERE "Статус" = 'error') < 10 THEN 'warning'
        ELSE 'critical'
    END AS "Уровень здоровья"
FROM public.v_egisz_transactions_enriched_ui
GROUP BY 1, 2;

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM egisz_raw)::bigint AS "Staging: всего строк",
    (SELECT MAX(logid) FROM egisz_raw) AS "Staging max EGMID",
    (SELECT MAX(logdate) FROM egisz_raw) AS "Staging max Sent",
    (SELECT MAX(last_log_id) FROM elt_state) AS "elt_state.last_log_id",
    (SELECT MAX(last_egmid) FROM elt_state) AS "elt_state.last_egmid (курсор EGISZ_MESSAGES)",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт курсора",
    now() - COALESCE((SELECT MAX(logdate) FROM egisz_raw), now()) AS "Возраст",
    (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui)::bigint AS "Очередь всего",
    (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui WHERE now() - "Отправлено" < interval '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui WHERE now() - "Отправлено" >= interval '1 hour' AND now() - "Отправлено" < interval '24 hours')::bigint AS "Очередь 1–24ч",
    (SELECT COUNT(*) FROM public.v_rpt_documents_no_response_ui WHERE now() - "Отправлено" >= interval '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(*) FROM fact_egisz_transactions WHERE egmid IS NULL)::bigint AS "Без EGMID",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
SELECT *
FROM (
    VALUES
        ('raw_rows', 'Staging rows', 'count', (SELECT COUNT(*)::numeric FROM egisz_raw), 'info', 'Проверить загрузку egisz_raw'),
        ('error_rows', 'Ошибки регистрации', 'count', (SELECT COUNT(*)::numeric FROM fact_egisz_transactions WHERE status = 'error'), 'warning', 'Открыть 04 Качество и ошибки'),
        ('pending_docs', 'Очередь без ответа', 'count', (SELECT COUNT(*)::numeric FROM public.v_rpt_documents_no_response_ui), 'warning', 'Открыть 03 Документы без ответа')
) AS v("Код сигнала", "Сигнал", "Единица", "Значение", "Уровень", "Что делать")
CROSS JOIN LATERAL (SELECT 'DWH'::text AS "База расчёта", v."Значение" AS "Количество") s;

CREATE OR REPLACE VIEW public.v_egisz_transactions_full AS
SELECT * FROM public.v_egisz_transactions_enriched_ui;
