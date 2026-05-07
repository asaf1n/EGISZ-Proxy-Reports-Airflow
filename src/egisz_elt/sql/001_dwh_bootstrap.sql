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
    creation_date timestamptz,
    processed_at timestamptz DEFAULT now()
);

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

CREATE OR REPLACE FUNCTION public.safe_cast_timestamptz(p_text text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN p_text::timestamptz;
EXCEPTION WHEN others THEN
    RETURN NULL;
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
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload,
            public.safe_cast_timestamptz(COALESCE(public.egisz_xml_text(r.msgtext, 'creationDateTime'), public.egisz_xml_text(r.msgtext, 'creationDate'))) AS creation_date
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
        exchangelog_log_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id, doc_number, org_oid, status, error_message, callback_url, jid, semd_code, semd_name, error_code, errors_json, creation_date, processed_at
    )
    SELECT
        e.logid, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id, e.doc_number, e.org_oid, e.final_status, e.final_error_message, e.logtext, e.jid_from_payload, e.semd_code, e.semd_name, e.error_code,
        CASE WHEN e.final_status = 'error' THEN jsonb_build_array(jsonb_build_object('code', e.error_code, 'message', e.final_error_message)) ELSE '[]'::jsonb END,
        e.creation_date, now()
    FROM enriched e
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
        log_date = EXCLUDED.log_date, message_id = EXCLUDED.message_id, relates_to_id = EXCLUDED.relates_to_id, local_uid_semd = EXCLUDED.local_uid_semd, emdr_id = COALESCE(fact_egisz_transactions.emdr_id, EXCLUDED.emdr_id), doc_number = EXCLUDED.doc_number, org_oid = EXCLUDED.org_oid, status = EXCLUDED.status, error_message = EXCLUDED.error_message, callback_url = EXCLUDED.callback_url, jid = COALESCE(EXCLUDED.jid, fact_egisz_transactions.jid), semd_code = EXCLUDED.semd_code, semd_name = EXCLUDED.semd_name, error_code = EXCLUDED.error_code, errors_json = EXCLUDED.errors_json, creation_date = EXCLUDED.creation_date, processed_at = now();
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
    t.log_date::date AS "День",
    COALESCE(t.local_uid_semd, t.doc_number, t.emdr_id, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.status AS "Статус",
    public.egisz_error_interpretation_type(t.error_code, t.error_message) AS "Подкатегория ошибки (глобально)",
    COALESCE(NULLIF(t.error_message, ''), '(без ошибки)') AS "Сводка ошибки",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    COALESCE(t.jid, l.jid) AS "JID клиники",
    t.jid AS "JID из журнала (gost, число)",
    o.name AS "Медицинская организация",
    regexp_replace(t.callback_url, '^(?:https?://)?([^/:]+).*$', '\1') AS "Хост клиники (VPN ГОСТ)",
    regexp_replace(t.callback_url, '^(?:https?://)?([^/:]+).*$', '\1') AS "Токен gost (LOGTEXT)",
    o.inn AS "ИНН клиники",
    l.mo_domen AS "Токен gost (нецифр., для отображения)",
    l.jid AS "JID (EGISZ_LICENSES)",
    CASE WHEN t.jid IS NOT NULL AND l.jid IS NOT NULL AND t.jid <> l.jid THEN 'да' ELSE 'нет' END AS "Расхождение источников JID",
    t.creation_date AS "Создание СЭМД",
    NULL::integer AS "JID из gost в REPLYTO",
    NULL::text AS "Токен gost (REPLYTO)"
FROM fact_egisz_transactions t
LEFT JOIN dim_licenses l ON t.org_oid = l.mo_uid OR t.jid = l.jid
LEFT JOIN dim_organizations o ON COALESCE(t.jid, l.jid) = o.jid;

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
SELECT
    "Обработано IPS" AS "Дата создания документа",
    "LOGID журнала EXCHANGELOG" AS "LOGID журнала (сетевая ошибка)",
    "MSGID обмена",
    "Документ (ключ учёта)",
    "Хост клиники (VPN ГОСТ)",
    "Сводка ошибки" AS "Сообщение",
    "Подкатегория ошибки (глобально)" AS "Подтип ошибки канала",
    "Статус"
FROM public.v_egisz_transactions_enriched_ui
WHERE "Статус" = 'error';

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM egisz_raw)::bigint AS "Staging: всего строк",
    (SELECT MAX(logid) FROM egisz_raw) AS "Staging max ID",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
SELECT * FROM (
    VALUES
        ('raw_rows', 'Staging rows', (SELECT COUNT(*)::numeric FROM egisz_raw), 'info'),
        ('error_rows', 'Ошибки регистрации', (SELECT COUNT(*)::numeric FROM fact_egisz_transactions WHERE status = 'error'), 'warning')
) AS v("Код", "Сигнал", "Значение", "Уровень");