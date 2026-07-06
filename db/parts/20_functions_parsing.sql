-- ============================================================================
-- 20_functions_parsing.sql — Parsing helpers (xml_text, normalize_message_id, clean_host, ...)
-- Source: db/dwh_init.sql, lines [417..536).
-- Loaded by db/dwh_init.sql via \i db/parts/20_functions_parsing.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.xml_text(payload text, tag_name text)
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

CREATE OR REPLACE FUNCTION public.normalize_message_id(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(regexp_replace(trim(both '<>' from btrim(COALESCE(value, ''))), '^urn:uuid:', '', 'i'), '');
$$;

-- Связка цепочки и реквизиты СЭМД читаются из transactions (xml_*, parse-once).
-- на LOGID), а не повторным разбором msgtext в exchangelog_raw. Функциональные индексы по
-- XML-выражениям над msgtext выполняли xml_text на КАЖДОЙ вставке в самый горячий
-- staging-слой и при этом не использовались ни одним запросом — это была чистая
-- write-amplification. JOIN'ы transform идут по PK-полосе logid и по индексам dim/fact ниже.
DROP INDEX IF EXISTS idx_exchangelog_raw_msgid_norm_logid_desc;
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_message_id_norm;
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_relates_to_message_norm;
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_relates_to_norm;
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_local_uid_norm;
DROP INDEX IF EXISTS idx_exchangelog_raw_xml_document_id_norm;

DROP INDEX IF EXISTS idx_transactions_message_id_norm;
DROP INDEX IF EXISTS idx_transactions_relates_to_norm;
CREATE INDEX IF NOT EXISTS idx_transactions_message_id_norm ON transactions (public.normalize_message_id(message_id));
CREATE INDEX IF NOT EXISTS idx_transactions_relates_to_norm ON transactions (public.normalize_message_id(relates_to_id));

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

CREATE OR REPLACE FUNCTION public.clean_host(p_text text)
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

-- Извлекает gost-endpoint (gost-<JID>.<host>...) из LOGTEXT/MSGTEXT для сопоставления
-- с dim_licenses.mo_domen. Число gost-<N> — часть endpoint, не отдельный путь резолва JID.
CREATE OR REPLACE FUNCTION public.extract_gost_endpoint(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        (regexp_match(COALESCE(p_text, ''), '(gost-[0-9]+(?:\.[a-z0-9._-]+(?::[0-9]+)?)?)', 'i'))[1],
        ''
    );
$$;

-- Primary: JID клиники по OID медорганизации (<organization>) через dim_licenses.mo_uid.
-- Один mo_uid может иметь несколько лицензий — берём последнюю по modifydate.
-- DROP перед CREATE: смена типа возврата integer→bigint несовместима с CREATE OR REPLACE (JID > int4).
DROP FUNCTION IF EXISTS public.jid_from_mo_uid(text);
CREATE OR REPLACE FUNCTION public.jid_from_mo_uid(p_org_oid text)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    SELECT dl.jid
    FROM public.dim_licenses dl
    WHERE dl.mo_uid = NULLIF(btrim(p_org_oid), '')
      AND dl.jid IS NOT NULL
    ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1;
$$;

-- Fallback: JID по адресу gost-endpoint через dim_licenses.mo_domen (host).
DROP FUNCTION IF EXISTS public.jid_from_host(text);
CREATE OR REPLACE FUNCTION public.jid_from_host(p_text text)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    WITH endpoint AS (
        SELECT public.extract_gost_endpoint(p_text) AS value
    )
    SELECT dl.jid
    FROM public.dim_licenses dl
    CROSS JOIN endpoint e
    WHERE e.value IS NOT NULL
      AND public.clean_host(dl.mo_domen) = public.clean_host(e.value)
      AND dl.jid IS NOT NULL
    ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1;
$$;

-- Сверка реквизитов JID/OID между источниками (XML, JPERSONS, EGISZ_LICENSES).
CREATE OR REPLACE FUNCTION public.document_source_mismatch(
    p_jid_resolve_method text,
    p_org_oid_xml text,
    p_fir_oid_jpersons text,
    p_mo_uid_license text
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT
        (
            NULLIF(btrim(p_org_oid_xml), '') IS NOT NULL
            AND NULLIF(btrim(p_fir_oid_jpersons), '') IS NOT NULL
            AND btrim(p_org_oid_xml) <> btrim(p_fir_oid_jpersons)
        )
        OR (
            NULLIF(btrim(p_org_oid_xml), '') IS NOT NULL
            AND NULLIF(btrim(p_mo_uid_license), '') IS NOT NULL
            AND btrim(p_org_oid_xml) <> btrim(p_mo_uid_license)
        )
        OR (
            NULLIF(btrim(p_fir_oid_jpersons), '') IS NOT NULL
            AND NULLIF(btrim(p_mo_uid_license), '') IS NOT NULL
            AND btrim(p_fir_oid_jpersons) <> btrim(p_mo_uid_license)
        )
        OR (
            p_jid_resolve_method = 'host'
            AND NULLIF(btrim(p_org_oid_xml), '') IS NOT NULL
            AND (
                (
                    NULLIF(btrim(p_fir_oid_jpersons), '') IS NOT NULL
                    AND btrim(p_org_oid_xml) <> btrim(p_fir_oid_jpersons)
                )
                OR (
                    NULLIF(btrim(p_mo_uid_license), '') IS NOT NULL
                    AND btrim(p_org_oid_xml) <> btrim(p_mo_uid_license)
                )
            )
        );
$$;

-- Единая цепочка резолва JID документа: mo_uid (primary) → host/gost-endpoint (fallback).
DROP FUNCTION IF EXISTS public.resolve_document_jid(text, text);
CREATE OR REPLACE FUNCTION public.resolve_document_jid(p_org_oid text, p_endpoint_text text)
RETURNS TABLE (jid bigint, resolve_method text)
LANGUAGE sql
STABLE
AS $$
    WITH mo AS (
        SELECT public.jid_from_mo_uid(p_org_oid) AS jid
    ),
    ho AS (
        SELECT public.jid_from_host(p_endpoint_text) AS jid
    )
    SELECT
        COALESCE(mo.jid, ho.jid) AS jid,
        CASE
            WHEN mo.jid IS NOT NULL THEN 'mo_uid'
            WHEN ho.jid IS NOT NULL THEN 'host'
        END AS resolve_method
    FROM mo
    CROSS JOIN ho
    WHERE COALESCE(mo.jid, ho.jid) IS NOT NULL;
$$;

CREATE OR REPLACE FUNCTION public.clean_text_value(p_text text)
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

CREATE OR REPLACE FUNCTION public.normalize_semd_code(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    WITH normalized AS (
        SELECT public.clean_text_value(p_text) AS value
    )
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN regexp_match(value, '([0-9]+(?:\.[0-9]+)*)') IS NOT NULL THEN (regexp_match(value, '([0-9]+(?:\.[0-9]+)*)'))[1]
        ELSE split_part(value, ' ', 1)
    END
    FROM normalized;
$$;

-- dwh_id — ключ ЭКЗЕМПЛЯРА/ВЕРСИИ отправки СЭМД: всегда lower(localUid).
-- localUid = CDA ClinicalDocument/id (UUID конкретной версии документа). По правилам РЭМД
-- он ОБЯЗАН меняться при любой правке СЭМД и в ряде сценариев даже при повторной выгрузке
-- без изменений (UpdateCase/UpdateMedRecord) — то есть НЕ стабилен на жизненном цикле
-- документа: корректировка ошибок штатно порождает новый localUid ⇒ новый dwh_id (новый
-- экземпляр), а не переписывает прежний.
-- Стабильный ключ набора версий (CDA setId) в журнал не попадает: тело СЭМД (base64-CDA)
-- шлюзом не сохраняется (см. README §«Версии и идентичность документа»). Поэтому
-- группировка версий в один логический документ ведётся отдельным слоем document_group_id,
-- а не через dwh_id.
-- emdrId (рег. номер РЭМД) и OID (код типа в справочнике НСИ / OID организации) НЕ являются
-- ключом: emdrId — атрибут регистрации, OID — классификатор, не идентификатор экземпляра.
-- Колбэк без localUid не порождает новый ключ, а резолвится к существующей строке по
-- relatesToMessage / emdrId (см. egisz_transform_raw_to_facts).
CREATE OR REPLACE FUNCTION public.dwh_id(
    p_local_uid text
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT lower(NULLIF(btrim(public.clean_text_value(p_local_uid)), ''));
$$;

-- Контур обмена (направление интеграции с ЕГИСЗ) для строки журнала.
-- Первичный признак — wsa:Action payload'а: ИЭМК ходит по IHE XDS.b (urn:ihe:*),
-- всё остальное — РЭМД. Запасной признак — порт сервиса клиники в LOGTEXT:
-- 9921 = ИЭМК, 9945 = РЭМД (конвенция настроек клиник; прочие порты контур
-- не определяют). Порт — условный признак, приоритет всегда у action.
CREATE OR REPLACE FUNCTION public.exchange_contour(p_action text, p_logtext text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_action ILIKE 'urn:ihe%' THEN 'ИЭМК'
        WHEN NULLIF(btrim(COALESCE(p_action, '')), '') IS NOT NULL THEN 'РЭМД'
        WHEN COALESCE(p_logtext, '') ~ ':9921(\D|$)' THEN 'ИЭМК'
        WHEN COALESCE(p_logtext, '') ~ ':9945(\D|$)' THEN 'РЭМД'
        ELSE NULL
    END;
$$;

-- Разложение payload EXCHANGELOG: каждый XML-тег и regex-маркер статуса
-- вычисляется ровно один раз; transform и связка документов читают transactions (xml_*).
-- DROP перед CREATE: jid_from_payload integer→bigint меняет тип возврата (JID > int4).
DROP FUNCTION IF EXISTS public.parse_exchangelog_row(text, text, text);
CREATE OR REPLACE FUNCTION public.parse_exchangelog_row(
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
    dwh_id text,
    kind_xml text,
    doc_number text,
    org_oid text,
    error_code text,
    xml_message text,
    raw_status text,
    document_status text,
    jid_from_payload bigint,
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
    v_faultcode text;
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
    v_action := public.xml_text(p_msgtext, 'action');
    v_message_id_xml := public.xml_text(p_msgtext, 'messageId');
    v_relates_to_message := public.xml_text(p_msgtext, 'relatesToMessage');
    v_relates_to := public.xml_text(p_msgtext, 'relatesTo');
    v_local_uid_xml := public.xml_text(p_msgtext, 'localUid');
    v_kind_xml := public.xml_text(p_msgtext, 'KIND');
    v_emdr_id_xml := public.xml_text(p_msgtext, 'emdrId');
    v_doc_number_xml := public.xml_text(p_msgtext, 'documentNumber');
    v_organization := public.xml_text(p_msgtext, 'organization');
    v_organization_oid := public.xml_text(p_msgtext, 'organizationOid');
    v_error_code_xml := public.xml_text(p_msgtext, 'errorCode');
    v_code_xml := public.xml_text(p_msgtext, 'code');
    -- SOAP-fault без <code>/<errorCode> нёс код только в <faultcode>; значение приходит
    -- с namespace-префиксом ('soap:Server') — оставляем локальную часть в UPPERCASE.
    v_faultcode := NULLIF(upper(regexp_replace(public.xml_text(p_msgtext, 'faultcode'), '^[^:]*:', '')), '');
    v_error_message := public.xml_text(p_msgtext, 'errorMessage');
    v_message_xml := public.xml_text(p_msgtext, 'message');
    v_faultstring := public.xml_text(p_msgtext, 'faultstring');
    v_status_xml := public.xml_text(p_msgtext, 'status');
    v_document_status := public.xml_text(p_msgtext, 'documentStatus');
    v_creation_datetime := public.xml_text(p_msgtext, 'creationDateTime');
    v_creation_date := public.xml_text(p_msgtext, 'creationDate');
    v_patient_name := public.xml_text(p_msgtext, 'patientName');
    v_patient_fio := public.xml_text(p_msgtext, 'patientFio');
    v_fio := public.xml_text(p_msgtext, 'fio');
    v_patient := public.xml_text(p_msgtext, 'patient');
    v_patient_name_cap := public.xml_text(p_msgtext, 'PatientName');
    v_family_name := public.xml_text(p_msgtext, 'familyName');
    v_given_name := public.xml_text(p_msgtext, 'givenName');
    v_patronymic := public.xml_text(p_msgtext, 'patronymic');
    v_snils := public.xml_text(p_msgtext, 'snils');
    v_snils_cap := public.xml_text(p_msgtext, 'SNILS');
    v_patient_snils := public.xml_text(p_msgtext, 'patientSnils');
    v_doctor_name := public.xml_text(p_msgtext, 'doctorName');
    v_doctor_fio := public.xml_text(p_msgtext, 'doctorFio');
    v_physician_name := public.xml_text(p_msgtext, 'physicianName');
    v_medical_worker_name := public.xml_text(p_msgtext, 'medicalWorkerName');
    v_author_name := public.xml_text(p_msgtext, 'authorName');
    v_doctor := public.xml_text(p_msgtext, 'doctor');

    RETURN QUERY
    SELECT
        v_action,
        public.normalize_message_id(COALESCE(NULLIF(btrim(p_msgid), ''), v_message_id_xml)),
        public.normalize_message_id(COALESCE(v_relates_to_message, v_relates_to)),
        public.clean_text_value(v_local_uid_xml),
        public.clean_text_value(v_emdr_id_xml),
        public.dwh_id(v_local_uid_xml),
        v_kind_xml,
        public.clean_text_value(v_doc_number_xml),
        public.clean_text_value(COALESCE(v_organization, v_organization_oid)),
        COALESCE(v_error_code_xml, v_code_xml, v_faultcode),
        COALESCE(v_error_message, v_message_xml, v_faultstring),
        lower(COALESCE(v_status_xml, '')),
        v_document_status,
        NULLIF((regexp_match(v_text_blob, 'gost-([0-9]+)', 'i'))[1], '')::bigint,
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

DROP INDEX IF EXISTS idx_dim_licenses_mo_domen_host;
CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_domen_host ON dim_licenses (public.clean_host(mo_domen));

-- Финальный статус документа по EXCHANGELOG-сообщению. Ключевое различие синхронного и
-- асинхронного ответов РЭМД (см. README §«Парсинг и классификация»):
--   * Синхронный RegisterDocumentResponse со <status>success</status> подтверждает только
--     приём запроса РЭМД, а не регистрацию документа — трактуется как 'pending'.
--   * Регистрация подтверждается ТОЛЬКО асинхронным callback'ом registerDocumentResult с
--     <documentStatus>Зарегистрировано</documentStatus> либо <status>OK</status>.
-- В аналитике остаются только финальные success/error и техническая ошибка LOGSTATE=3.
-- NB: в текущем журнале этого шлюза синхронный ack не наблюдается (маркер RegisterDocumentResponse
-- = 0 строк из ~931k; 'pending' — 2 строки). Документы в неконечном состоянии создаёт ветка
-- getDocumentFile (status='waiting' → метка «Отправлено»). Ветку 'pending' оставляем
-- зарезервированной на случай появления sync-ack — логику не трогаем.
CREATE OR REPLACE FUNCTION public.classify_async_status(
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
