CREATE TABLE IF NOT EXISTS elt_state (
    pipeline text PRIMARY KEY,
    last_log_id bigint DEFAULT 0,
    last_egmid bigint DEFAULT 0,
    updated_at timestamptz DEFAULT now()
);

DO $$
BEGIN
    IF to_regclass('public.exchangelog_raw') IS NULL AND to_regclass('public.egisz_raw') IS NOT NULL THEN
        ALTER TABLE public.egisz_raw RENAME TO exchangelog_raw;
    ELSIF to_regclass('public.exchangelog_raw') IS NULL AND to_regclass('public.exchangelog') IS NOT NULL THEN
        ALTER TABLE public.exchangelog RENAME TO exchangelog_raw;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS exchangelog_raw (
    logid bigint PRIMARY KEY,
    logdate timestamptz,
    createdate timestamptz,
    msgid text,
    logstate integer,
    logtext text,
    msgtext text,
    loaded_at timestamptz DEFAULT now()
);

ALTER TABLE exchangelog_raw ADD COLUMN IF NOT EXISTS createdate timestamptz;

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

ALTER TABLE egisz_messages_raw ADD COLUMN IF NOT EXISTS loaded_at timestamptz DEFAULT now();

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
    exchangelog_log_id bigint PRIMARY KEY REFERENCES exchangelog_raw(logid),
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

ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS egmid bigint;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS errors_json jsonb DEFAULT '[]'::jsonb;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS creation_date timestamptz;

CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_msgid ON exchangelog_raw (msgid);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_logstate ON exchangelog_raw (logstate);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_createdate ON exchangelog_raw (createdate);
CREATE INDEX IF NOT EXISTS idx_egisz_messages_msgid ON egisz_messages_raw (msgid);
CREATE INDEX IF NOT EXISTS idx_egisz_messages_document_id ON egisz_messages_raw (document_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_log_date ON fact_egisz_transactions (log_date);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_status ON fact_egisz_transactions (status);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_jid ON fact_egisz_transactions (jid);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_message_id ON fact_egisz_transactions (message_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_local_uid ON fact_egisz_transactions (local_uid_semd);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_relates_to ON fact_egisz_transactions (relates_to_id);

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
    IF match IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN NULLIF(btrim(replace(replace(replace(match[1], E'\n', ' '), E'\r', ' '), E'\t', ' ')), '');
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_normalize_message_id(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(regexp_replace(trim(both '<>' from btrim(COALESCE(value, ''))), '^urn:uuid:', '', 'i'), '');
$$;

CREATE INDEX IF NOT EXISTS idx_egisz_messages_msgid_norm ON egisz_messages_raw (public.egisz_normalize_message_id(msgid));

CREATE OR REPLACE FUNCTION public.safe_cast_timestamptz(p_text text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF NULLIF(btrim(COALESCE(p_text, '')), '') IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN p_text::timestamptz;
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_clean_host(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        regexp_replace(
            btrim(COALESCE(p_text, '')),
            '^(?:https?://)?([^/:?#]+).*$',
            '\1',
            'i'
        ),
        ''
    );
$$;

CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_domen_host ON dim_licenses (public.egisz_clean_host(mo_domen));

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_type(error_code text, error_message text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN NULLIF(btrim(COALESCE(error_message, '')), '') IS NULL
             AND NULLIF(btrim(COALESCE(error_code, '')), '') IS NULL THEN '(ошибка без деталей)'
        WHEN COALESCE(error_message, '') ILIKE '%network%'
             OR COALESCE(error_message, '') ILIKE '%connection%'
             OR COALESCE(error_message, '') ILIKE '%transport%'
             OR COALESCE(error_message, '') ILIKE '%timeout%'
             OR COALESCE(error_message, '') ILIKE '%timed out%'
             OR COALESCE(error_message, '') ILIKE '%соединени%'
             OR COALESCE(error_message, '') ILIKE '%таймаут%'
             THEN 'ошибка связи (транспорт)'
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

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(
    min_log_id bigint,
    max_log_id bigint,
    min_egmid bigint DEFAULT 0,
    max_egmid bigint DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
BEGIN
    WITH changed_messages AS (
        SELECT
            public.egisz_normalize_message_id(msgid) AS msgid,
            lower(NULLIF(btrim(document_id), '')) AS document_id
        FROM egisz_messages_raw
        WHERE egmid > min_egmid
          AND egmid <= max_egmid
    ),
    candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > min_log_id
          AND r.logid <= max_log_id

        UNION

        SELECT f.exchangelog_log_id
        FROM fact_egisz_transactions f
        JOIN changed_messages m
          ON m.msgid IN (f.message_id, f.relates_to_id)
          OR m.document_id = lower(NULLIF(btrim(f.local_uid_semd), ''))
    ),
    raw_parsed AS (
        SELECT
            r.logid,
            r.logdate,
            r.createdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'messageId'), r.msgid)) AS message_id,
            public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo'))) AS relates_to_id,
            public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_xml,
            public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_xml,
            public.egisz_xml_text(r.msgtext, 'kind') AS kind_xml,
            public.egisz_xml_text(r.msgtext, 'KIND') AS kind_upper_xml,
            public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id,
            public.egisz_xml_text(r.msgtext, 'documentNumber') AS doc_number,
            COALESCE(public.egisz_xml_text(r.msgtext, 'organization'), public.egisz_xml_text(r.msgtext, 'organizationOid')) AS org_oid,
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'), public.egisz_xml_text(r.msgtext, 'name'), public.egisz_xml_text(r.msgtext, 'documentName')) AS semd_name,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorCode'), public.egisz_xml_text(r.msgtext, 'code')) AS error_code,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorMessage'), public.egisz_xml_text(r.msgtext, 'message'), public.egisz_xml_text(r.msgtext, 'faultstring')) AS xml_message,
            lower(COALESCE(public.egisz_xml_text(r.msgtext, 'status'), '')) AS raw_status,
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload,
            public.safe_cast_timestamptz(COALESCE(public.egisz_xml_text(r.msgtext, 'creationDateTime'), public.egisz_xml_text(r.msgtext, 'creationDate'))) AS creation_date
        FROM exchangelog_raw r
        JOIN candidate_log_ids c ON c.logid = r.logid
        WHERE COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
    ),
    parsed AS (
        SELECT
            r.logid,
            COALESCE(m.created_at, r.createdate) AS logdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            r.message_id,
            r.relates_to_id,
            COALESCE(r.local_uid_xml, r.document_id_xml, m.document_id) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            COALESCE(r.kind_xml, r.kind_upper_xml, m.kind) AS semd_code,
            r.semd_name,
            r.error_code,
            r.xml_message,
            r.raw_status,
            r.jid_from_payload,
            r.creation_date,
            m.egmid,
            COALESCE(m.jid, m.license_jid) AS message_jid,
            COALESCE(m.kind, m.license_kind) AS message_kind
        FROM raw_parsed r
        LEFT JOIN LATERAL (
            SELECT
                em.*,
                l.jid AS license_jid,
                l.kind AS license_kind
            FROM egisz_messages_raw em
            LEFT JOIN dim_licenses l
              ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
            WHERE public.egisz_normalize_message_id(em.msgid) = r.relates_to_id
               OR public.egisz_normalize_message_id(em.msgid) = r.message_id
               OR lower(NULLIF(btrim(em.document_id), '')) IN (
                    lower(NULLIF(btrim(r.local_uid_xml), '')),
                    lower(NULLIF(btrim(r.document_id_xml), ''))
               )
            ORDER BY
                CASE WHEN public.egisz_normalize_message_id(em.msgid) = r.relates_to_id THEN 0 ELSE 1 END,
                em.egmid DESC
            LIMIT 1
        ) m ON TRUE
    ),
    enriched AS (
        SELECT
            p.*,
            COALESCE(p.jid_from_payload, p.message_jid) AS resolved_jid,
            COALESCE(p.semd_code, p.message_kind) AS resolved_semd_code,
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
        exchangelog_log_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, error_message, callback_url, egmid, jid, semd_code,
        semd_name, error_code, errors_json, creation_date, processed_at
    )
    SELECT
        e.logid, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.final_error_message, e.logtext, e.egmid,
        e.resolved_jid, e.resolved_semd_code, e.semd_name, e.error_code,
        CASE
            WHEN e.final_status = 'error'
                THEN jsonb_build_array(jsonb_build_object('code', e.error_code, 'message', e.final_error_message))
            ELSE '[]'::jsonb
        END,
        e.creation_date, now()
    FROM enriched e
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        message_id = EXCLUDED.message_id,
        relates_to_id = EXCLUDED.relates_to_id,
        local_uid_semd = EXCLUDED.local_uid_semd,
        emdr_id = EXCLUDED.emdr_id,
        doc_number = EXCLUDED.doc_number,
        org_oid = EXCLUDED.org_oid,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        callback_url = EXCLUDED.callback_url,
        egmid = EXCLUDED.egmid,
        jid = EXCLUDED.jid,
        semd_code = EXCLUDED.semd_code,
        semd_name = EXCLUDED.semd_name,
        error_code = EXCLUDED.error_code,
        errors_json = EXCLUDED.errors_json,
        creation_date = EXCLUDED.creation_date,
        processed_at = now();
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(max_log_id bigint)
RETURNS integer
LANGUAGE sql
AS $$
    SELECT public.egisz_transform_raw_to_facts(0, max_log_id, 0, 0);
$$;

UPDATE fact_egisz_transactions
SET
    message_id = public.egisz_normalize_message_id(message_id),
    relates_to_id = public.egisz_normalize_message_id(relates_to_id)
WHERE message_id LIKE 'urn:uuid:%'
   OR message_id LIKE '<urn:uuid:%>'
   OR relates_to_id LIKE 'urn:uuid:%'
   OR relates_to_id LIKE '<urn:uuid:%>';

DROP VIEW IF EXISTS public.v_health_by_clinic_ui;
DROP VIEW IF EXISTS public.v_health_signals_ui;
DROP VIEW IF EXISTS public.v_health_proxy_db_ui;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_network_errors_detail_ui;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document;
DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui;
DROP VIEW IF EXISTS public.v_rpt_documents_no_response_ui;
DROP VIEW IF EXISTS public.v_egisz_transactions_full;
DROP VIEW IF EXISTS public.v_egisz_transactions_enriched_ui;

CREATE OR REPLACE VIEW public.v_egisz_transactions_enriched_ui AS
SELECT
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    t.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    t.message_id AS "MSGID обмена",
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День",
    t.log_date::date AS "День (тренд)",
    COALESCE(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.relates_to_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.status AS "Статус",
    public.egisz_error_interpretation_type(t.error_code, t.error_message) AS "Подкатегория ошибки (глобально)",
    public.egisz_error_interpretation_type(t.error_code, t.error_message) AS "Интерпретация ошибок",
    COALESCE(NULLIF(t.error_message, ''), '(без ошибки)') AS "Сводка ошибки",
    COALESCE(NULLIF(t.error_message, ''), '(без ошибки)') AS "Сводка ошибок",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    t.semd_code AS "Код СЭМД",
    t.semd_name AS "Наименование СЭМД",
    COALESCE(t.jid, l.jid)::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(t.jid, l.jid)::text) AS "Наименование клиники",
    t.jid::text AS "JID из журнала (gost, число)",
    o.name AS "Медицинская организация",
    t.org_oid AS "OID организации",
    l.mo_uid AS "OID клиники",
    public.egisz_clean_host(t.callback_url) AS "Хост клиники (VPN ГОСТ)",
    public.egisz_clean_host(t.callback_url) AS "Токен gost (LOGTEXT)",
    o.inn AS "ИНН клиники",
    l.mo_domen AS "Токен gost (нецифр., для отображения)",
    l.jid::text AS "JID (EGISZ_LICENSES)",
    CASE WHEN t.jid IS NOT NULL AND l.jid IS NOT NULL AND t.jid <> l.jid THEN 'да' ELSE 'нет' END AS "Расхождение источников JID",
    t.creation_date AS "Создание СЭМД",
    NULL::text AS "JID из gost в REPLYTO",
    NULL::text AS "Токен gost (REPLYTO)",
    t.local_uid_semd AS "localUid СЭМД",
    t.local_uid_semd AS "Идентификатор документа (localUid)",
    t.relates_to_id AS "Связанное сообщение",
    lower(NULLIF(btrim(t.relates_to_id), '')) AS "Связанное сообщение (канон)",
    lower(NULLIF(btrim(t.local_uid_semd), '')) AS "localUid СЭМД (канон)",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.emdr_id AS "Регистрационный номер РЭМД",
    t.doc_number AS "DOCUMENTID",
    t.error_code AS "Код ошибки",
    COALESCE(t.errors_json, '[]'::jsonb) AS "Ошибки JSON",
    t.exchangelog_log_id AS transaction_id,
    COALESCE(t.jid, l.jid) AS clinic_id,
    t.semd_code AS service_id
FROM fact_egisz_transactions t
LEFT JOIN LATERAL (
    SELECT dl.*
    FROM dim_licenses dl
    WHERE (t.org_oid IS NOT NULL AND dl.mo_uid = t.org_oid)
       OR (t.jid IS NOT NULL AND dl.jid = t.jid)
    ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN dim_organizations o ON COALESCE(t.jid, l.jid) = o.jid;

COMMENT ON VIEW public.v_egisz_transactions_enriched_ui IS
'Основная UI-витрина ответов РЭМД для бизнес-аналитики сервиса интеграции клиник с ЕГИСЗ. Идентификаторы JID, EGMID и LOGID выводятся как текст, чтобы Metabase не суммировал их как метрики.';

CREATE OR REPLACE VIEW public.v_egisz_transactions_full AS
SELECT * FROM public.v_egisz_transactions_enriched_ui;

CREATE OR REPLACE VIEW public.v_stg_channel_errors_by_document AS
SELECT
    r.logid AS id,
    COALESCE(r.createdate, r.loaded_at) AS created_at,
    CASE WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3' ELSE 'PARSE_ERROR' END AS error_code,
    COALESCE(NULLIF(r.logtext, ''), NULLIF(r.msgtext, ''), '(без текста)') AS message,
    CASE WHEN r.logstate = 3 THEN 'network' ELSE 'async_response' END AS error_top_type,
    CASE WHEN r.logstate = 3 THEN 'ошибка связи (транспорт)' ELSE 'ошибка асинхронного ответа РЭМД' END AS error_global_subcategory,
    CASE WHEN r.logstate = 3 THEN 'Ошибка связи' ELSE 'Ошибка в асинхронном ответе РЭМД' END AS error_group_label_ru,
    r.logid AS exchangelog_log_id,
    r.msgid AS journal_msgid,
    m.egmid AS egisz_messages_egmid,
    COALESCE(
        public.egisz_xml_text(r.msgtext, 'relatesToMessage'),
        public.egisz_xml_text(r.msgtext, 'relatesTo'),
        public.egisz_xml_text(r.logtext, 'relatesToMessage')
    ) AS relates_to_hint,
    COALESCE(
        public.egisz_xml_text(r.msgtext, 'localUid'),
        public.egisz_xml_text(r.msgtext, 'DOCUMENTID'),
        m.document_id
    ) AS local_uid_hint,
    public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id_hint,
    COALESCE(
        public.egisz_xml_text(r.msgtext, 'localUid'),
        public.egisz_xml_text(r.msgtext, 'DOCUMENTID'),
        public.egisz_xml_text(r.msgtext, 'emdrId'),
        public.egisz_xml_text(r.msgtext, 'relatesToMessage'),
        public.egisz_xml_text(r.msgtext, 'relatesTo'),
        m.document_id,
        r.msgid,
        r.logid::text
    ) AS document_group_key,
    COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo')) AS relates_to_id
FROM exchangelog_raw r
LEFT JOIN LATERAL (
    SELECT em.*
    FROM egisz_messages_raw em
    WHERE public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo')))
       OR public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(r.msgid)
    ORDER BY
        CASE
            WHEN public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo'))) THEN 0
            ELSE 1
        END,
        em.egmid DESC
    LIMIT 1
) m ON TRUE
WHERE r.logstate = 3
   OR COALESCE(r.msgtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%ошиб%';

CREATE OR REPLACE VIEW public.v_stg_channel_network_errors_by_document AS
SELECT *
FROM public.v_stg_channel_errors_by_document
WHERE error_top_type = 'network';

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
WITH source_rows AS (
    SELECT
        s.*,
        NULLIF((regexp_match(COALESCE(s.message, ''), 'gost-([0-9]+)', 'i'))[1], '') AS jid_from_text
    FROM public.v_stg_channel_errors_by_document s
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
LEFT JOIN public.v_egisz_transactions_enriched_ui f
       ON lower(NULLIF(btrim(f."Связанное сообщение"), '')) = lower(NULLIF(btrim(s.relates_to_hint), ''))
       OR lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), ''))
       OR lower(NULLIF(btrim(f."Рег. номер РЭМД (emdrid)"), '')) = lower(NULLIF(btrim(s.emdr_id_hint), ''));

COMMENT ON VIEW public.v_rpt_network_errors_detail_ui IS
'Техническая витрина ошибок связи proxy_egisz: healthcheck/поддержка клиник, LOGSTATE=3 и строки журнала с привязкой к документу, если её удалось восстановить.';

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
SELECT
    m.created_at AS "Отправлено",
    m.document_id AS "localUid СЭМД",
    m.document_id AS "Идентификатор документа (localUid)",
    COALESCE(public.egisz_xml_text(m.msgtext, 'kind'), m.kind) AS "Код СЭМД",
    COALESCE(public.egisz_xml_text(m.msgtext, 'documentTypeName'), public.egisz_xml_text(m.msgtext, 'name')) AS "Наименование СЭМД",
    public.egisz_semd_type_report_label(COALESCE(public.egisz_xml_text(m.msgtext, 'kind'), m.kind), COALESCE(public.egisz_xml_text(m.msgtext, 'documentTypeName'), public.egisz_xml_text(m.msgtext, 'name'))) AS "Тип СЭМД (код · НСИ)",
    m.jid::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || m.jid::text) AS "Наименование клиники",
    m.reply_to AS "Связанное сообщение",
    m.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    m.msgid AS "MSGID обмена"
FROM egisz_messages_raw m
LEFT JOIN dim_organizations o ON o.jid = m.jid
LEFT JOIN fact_egisz_transactions f
       ON public.egisz_normalize_message_id(f.message_id) = public.egisz_normalize_message_id(m.msgid)
       OR public.egisz_normalize_message_id(f.relates_to_id) = public.egisz_normalize_message_id(m.msgid)
       OR lower(NULLIF(btrim(f.local_uid_semd), '')) = lower(NULLIF(btrim(m.document_id), ''))
WHERE f.exchangelog_log_id IS NULL;

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

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
WITH fact_24h AS (
    SELECT
        "JID клиники",
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS docs_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_cnt
    FROM public.v_egisz_transactions_enriched_ui
    WHERE "Обработано IPS" >= now() - INTERVAL '24 hours'
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
    (SELECT COUNT(*) FROM exchangelog_raw)::bigint AS "Staging: всего строк",
    (SELECT COUNT(*) FROM egisz_messages_raw WHERE egmid IS NULL)::bigint AS "Без EGMID",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui)::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '24 hours' AND "Отправлено" < now() - INTERVAL '1 hour')::bigint AS "Очередь 1–24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT MAX(egmid) FROM egisz_messages_raw) AS "Staging max EGMID",
    (SELECT MAX(created_at) FROM egisz_messages_raw) AS "Staging max Sent",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт курсора",
    (SELECT MAX(last_log_id) FROM elt_state) AS "elt_state.last_log_id",
    (SELECT MAX(last_egmid) FROM elt_state) AS "elt_state.last_egmid (курсор EGISZ_MESSAGES)",
    (SELECT MAX(logid) FROM exchangelog_raw) AS "Staging max ID",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
SELECT * FROM (
    VALUES
        ('raw_rows', 'Raw-строки proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM exchangelog_raw), 'строк', 'exchangelog_raw', 'Контроль поступления журнала EXCHANGELOG'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT "localUid СЭМД")::numeric FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours'), 'документов', 'egisz_messages_raw без callback-факта', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT "Ключ документа (группировка)")::numeric FROM public.v_rpt_network_errors_detail_ui), 'документов', 'EXCHANGELOG LOGSTATE=3 и журнал ошибок', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки регистрации РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM fact_egisz_transactions WHERE status = 'error'), 'строк', 'fact_egisz_transactions.status=error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");
