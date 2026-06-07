-- ============================================================================
-- 20_functions_parsing.sql — Parsing helpers (xml_text, normalize_message_id, clean_host, ...)
-- Source: db/dwh_init.sql, lines [417..536).
-- Loaded by db/dwh_init.sql via \i db/parts/20_functions_parsing.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

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
    -- NB: inner capture uses `[^<]*` rather than `(.*?)`. In PostgreSQL ARE the
    -- greediness of the entire regex is locked by the FIRST quantifier; the
    -- optional `:?` prefix makes that one greedy and silently turns the
    -- nominally non-greedy `.*?` greedy too, which spilled `<ns2:code>VALIDATION_ERROR</ns2:code>...`
    -- across siblings into a single match. `[^<]*` cannot cross a tag boundary,
    -- so the first matching pair is always returned.
    match := regexp_match(
        payload,
        '<(?:[A-Za-z0-9_]+:)?' || safe_tag || '(?:\s[^>]*)?>([^<]*)</(?:[A-Za-z0-9_]+:)?' || safe_tag || '>',
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

CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_msgid_norm_logid_desc
    ON exchangelog_raw (public.egisz_normalize_message_id(msgid), logid DESC)
    WHERE msgid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_message_id_norm
    ON exchangelog_raw (public.egisz_normalize_message_id(public.egisz_xml_text(msgtext, 'messageId')), logid DESC)
    WHERE public.egisz_xml_text(msgtext, 'messageId') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_relates_to_message_norm
    ON exchangelog_raw (public.egisz_normalize_message_id(public.egisz_xml_text(msgtext, 'relatesToMessage')), logid DESC)
    WHERE public.egisz_xml_text(msgtext, 'relatesToMessage') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_relates_to_norm
    ON exchangelog_raw (public.egisz_normalize_message_id(public.egisz_xml_text(msgtext, 'relatesTo')), logid DESC)
    WHERE public.egisz_xml_text(msgtext, 'relatesTo') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_local_uid_norm
    ON exchangelog_raw (lower(NULLIF(btrim(public.egisz_xml_text(msgtext, 'localUid')), '')), logid DESC)
    WHERE NULLIF(btrim(public.egisz_xml_text(msgtext, 'localUid')), '') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_egisz_message_id_norm ON fact_egisz_transactions (public.egisz_normalize_message_id(message_id));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_relates_to_norm ON fact_egisz_transactions (public.egisz_normalize_message_id(relates_to_id));
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_document_id_norm;

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

CREATE OR REPLACE FUNCTION public.egisz_extract_jid_from_endpoint(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF((regexp_match(COALESCE(p_text, ''), 'gost-([0-9]+)', 'i'))[1], '');
$$;

CREATE OR REPLACE FUNCTION public.egisz_clean_text_value(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        btrim(
            regexp_replace(
                regexp_replace(COALESCE(p_text, ''), '<[^>]+>', ' ', 'g'),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    );
$$;

CREATE OR REPLACE FUNCTION public.egisz_normalize_semd_code(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    WITH normalized AS (
        SELECT public.egisz_clean_text_value(p_text) AS value
    )
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN regexp_match(value, '([0-9]+(?:\.[0-9]+)*)') IS NOT NULL THEN (regexp_match(value, '([0-9]+(?:\.[0-9]+)*)'))[1]
        ELSE split_part(value, ' ', 1)
    END
    FROM normalized;
$$;

-- Канонический ключ учёта документа — ВСЕГДА lower(localUid). localUid выдаёт МИС,
-- он уникален в рамках источника и стабилен на всём жизненном цикле СЭМД.
-- emdrId (рег. номер РЭМД) и OID (код типа в справочнике НСИ / OID организации)
-- НЕ являются ключом: emdrId — атрибут регистрации, OID — классификатор, не идентификатор
-- экземпляра. Колбэк без localUid не порождает новый ключ, а резолвится к существующей
-- строке по relatesToMessage / emdrId (см. egisz_transform_raw_to_facts).
CREATE OR REPLACE FUNCTION public.egisz_document_key(
    p_local_uid text
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT lower(NULLIF(btrim(public.egisz_clean_text_value(p_local_uid)), ''));
$$;

-- Разложение payload EXCHANGELOG: каждый XML-тег и regex-маркер статуса
-- вычисляется ровно один раз; transform и связка документов читают dim_egisz_exchangelog_refs.
CREATE OR REPLACE FUNCTION public.egisz_parse_exchangelog_row(
    p_msgtext text,
    p_msgid text,
    p_logtext text
)
RETURNS TABLE (
    action text,
    exchange_msgid_norm text,
    relates_to_id text,
    local_uid text,
    emdr_id text,
    document_key text,
    kind_xml text,
    doc_number text,
    org_oid text,
    error_code text,
    xml_message text,
    raw_status text,
    document_status text,
    jid_from_payload integer,
    creation_date timestamptz,
    raw_patient_name text,
    raw_snils text,
    raw_doctor_name text,
    has_fault_marker boolean,
    has_register_response boolean,
    has_register_result boolean,
    has_processing_marker boolean,
    has_error_ilike boolean
)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_payload text := COALESCE(p_msgtext, '');
    v_text_blob text := COALESCE(p_logtext, '') || ' ' || v_payload;
    v_action text;
    v_message_id_xml text;
    v_relates_to_message text;
    v_relates_to text;
    v_local_uid_xml text;
    v_kind_xml text;
    v_emdr_id_xml text;
    v_doc_number_xml text;
    v_organization text;
    v_organization_oid text;
    v_error_code_xml text;
    v_code_xml text;
    v_error_message text;
    v_message_xml text;
    v_faultstring text;
    v_status_xml text;
    v_document_status text;
    v_creation_datetime text;
    v_creation_date text;
    v_patient_name text;
    v_patient_fio text;
    v_fio text;
    v_patient text;
    v_patient_name_cap text;
    v_family_name text;
    v_given_name text;
    v_patronymic text;
    v_snils text;
    v_snils_cap text;
    v_patient_snils text;
    v_doctor_name text;
    v_doctor_fio text;
    v_physician_name text;
    v_medical_worker_name text;
    v_author_name text;
    v_doctor text;
BEGIN
    v_action := public.egisz_xml_text(p_msgtext, 'action');
    v_message_id_xml := public.egisz_xml_text(p_msgtext, 'messageId');
    v_relates_to_message := public.egisz_xml_text(p_msgtext, 'relatesToMessage');
    v_relates_to := public.egisz_xml_text(p_msgtext, 'relatesTo');
    v_local_uid_xml := public.egisz_xml_text(p_msgtext, 'localUid');
    v_kind_xml := public.egisz_xml_text(p_msgtext, 'KIND');
    v_emdr_id_xml := public.egisz_xml_text(p_msgtext, 'emdrId');
    v_doc_number_xml := public.egisz_xml_text(p_msgtext, 'documentNumber');
    v_organization := public.egisz_xml_text(p_msgtext, 'organization');
    v_organization_oid := public.egisz_xml_text(p_msgtext, 'organizationOid');
    v_error_code_xml := public.egisz_xml_text(p_msgtext, 'errorCode');
    v_code_xml := public.egisz_xml_text(p_msgtext, 'code');
    v_error_message := public.egisz_xml_text(p_msgtext, 'errorMessage');
    v_message_xml := public.egisz_xml_text(p_msgtext, 'message');
    v_faultstring := public.egisz_xml_text(p_msgtext, 'faultstring');
    v_status_xml := public.egisz_xml_text(p_msgtext, 'status');
    v_document_status := public.egisz_xml_text(p_msgtext, 'documentStatus');
    v_creation_datetime := public.egisz_xml_text(p_msgtext, 'creationDateTime');
    v_creation_date := public.egisz_xml_text(p_msgtext, 'creationDate');
    v_patient_name := public.egisz_xml_text(p_msgtext, 'patientName');
    v_patient_fio := public.egisz_xml_text(p_msgtext, 'patientFio');
    v_fio := public.egisz_xml_text(p_msgtext, 'fio');
    v_patient := public.egisz_xml_text(p_msgtext, 'patient');
    v_patient_name_cap := public.egisz_xml_text(p_msgtext, 'PatientName');
    v_family_name := public.egisz_xml_text(p_msgtext, 'familyName');
    v_given_name := public.egisz_xml_text(p_msgtext, 'givenName');
    v_patronymic := public.egisz_xml_text(p_msgtext, 'patronymic');
    v_snils := public.egisz_xml_text(p_msgtext, 'snils');
    v_snils_cap := public.egisz_xml_text(p_msgtext, 'SNILS');
    v_patient_snils := public.egisz_xml_text(p_msgtext, 'patientSnils');
    v_doctor_name := public.egisz_xml_text(p_msgtext, 'doctorName');
    v_doctor_fio := public.egisz_xml_text(p_msgtext, 'doctorFio');
    v_physician_name := public.egisz_xml_text(p_msgtext, 'physicianName');
    v_medical_worker_name := public.egisz_xml_text(p_msgtext, 'medicalWorkerName');
    v_author_name := public.egisz_xml_text(p_msgtext, 'authorName');
    v_doctor := public.egisz_xml_text(p_msgtext, 'doctor');

    RETURN QUERY
    SELECT
        v_action,
        public.egisz_normalize_message_id(COALESCE(NULLIF(btrim(p_msgid), ''), v_message_id_xml)),
        public.egisz_normalize_message_id(COALESCE(v_relates_to_message, v_relates_to)),
        public.egisz_clean_text_value(v_local_uid_xml),
        public.egisz_clean_text_value(v_emdr_id_xml),
        public.egisz_document_key(v_local_uid_xml),
        v_kind_xml,
        public.egisz_clean_text_value(v_doc_number_xml),
        public.egisz_clean_text_value(COALESCE(v_organization, v_organization_oid)),
        COALESCE(v_error_code_xml, v_code_xml),
        COALESCE(v_error_message, v_message_xml, v_faultstring),
        lower(COALESCE(v_status_xml, '')),
        v_document_status,
        NULLIF((regexp_match(v_text_blob, 'gost-([0-9]+)', 'i'))[1], '')::integer,
        public.safe_cast_timestamptz(COALESCE(v_creation_datetime, v_creation_date)),
        COALESCE(
            v_patient_name,
            v_patient_fio,
            v_fio,
            v_patient,
            v_patient_name_cap,
            NULLIF(concat_ws(' ', v_family_name, v_given_name, v_patronymic), '')
        ),
        COALESCE(v_snils, v_snils_cap, v_patient_snils),
        COALESCE(
            v_doctor_name,
            v_doctor_fio,
            v_physician_name,
            v_medical_worker_name,
            v_author_name,
            v_doctor
        ),
        v_payload ~* '<(ns[0-9]+:)?(error|fault)|<faultstring|<errorCode',
        v_payload ~* 'RegisterDocumentResponse',
        v_payload ~* 'registerDocumentResult',
        v_payload ~* '(в обработке|принято к обработк|документ принят|processing|in[_ ]?progress|queued|accepted)',
        v_payload ILIKE '%error%';
END;
$$;

CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_domen_host ON dim_licenses (public.egisz_clean_host(mo_domen));

-- Финальный статус документа по EXCHANGELOG-сообщению. Ключевое различие синхронного и
-- асинхронного ответов РЭМД (см. README §«Парсинг и классификация»):
--   * Синхронный RegisterDocumentResponse со <status>success</status> подтверждает только
--     приём запроса РЭМД, а не регистрацию документа — трактуется как 'pending' (в обработке).
--   * Регистрация подтверждается ТОЛЬКО асинхронным callback'ом registerDocumentResult с
--     <documentStatus>Зарегистрировано</documentStatus> либо <status>OK</status>.
-- В аналитике остаются только финальные success/error и техническая ошибка LOGSTATE=3.
CREATE OR REPLACE FUNCTION public.egisz_classify_async_status(
    p_logstate               integer,
    p_raw_status             text,
    p_document_status        text,
    p_has_fault_marker       boolean,
    p_has_register_response  boolean,
    p_has_register_result    boolean,
    p_has_processing_marker  boolean,
    p_has_error_ilike        boolean
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_logstate = 3                                                                  THEN 'error'
        WHEN COALESCE(p_raw_status, '') ~* '(error|fail|reject|denied|отказ|ошибк)'          THEN 'error'
        WHEN COALESCE(p_has_fault_marker, false)                                             THEN 'error'
        -- Асинхронное подтверждение регистрации в РЭМД (финальный success).
        WHEN COALESCE(p_document_status, '') ~* 'зарегистр'                                  THEN 'success'
        WHEN COALESCE(p_has_register_result, false)
             AND COALESCE(p_raw_status, '') ~* '^\s*(ok|success)\s*$'                         THEN 'success'
        -- Синхронный приём запроса (RegisterDocumentResponse) — ещё не регистрация.
        WHEN COALESCE(p_has_register_response, false)
             AND COALESCE(p_raw_status, '') ~* '(success|ok)'                                 THEN 'pending'
        WHEN COALESCE(p_raw_status, '') ~* '(processing|in[_-]?progress|inprogress|queued|received|accepted|pending|wait|обработк|принят|получен|ожида)'
                                                                                              THEN 'pending'
        WHEN COALESCE(p_has_processing_marker, false)                                        THEN 'pending'
        -- Колбэк без явного маркера элемента, но со статусом success/ok — регистрация.
        WHEN COALESCE(p_raw_status, '') ~* '^\s*(success|ok)\s*$'                             THEN 'success'
        WHEN COALESCE(p_raw_status, '') LIKE '%error%' OR COALESCE(p_has_error_ilike, false) THEN 'error'
        ELSE 'unknown'
    END;
$$;
