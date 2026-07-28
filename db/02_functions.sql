-- ============================================================================
-- 02_functions.sql — parsing utilities, error rules dictionary, error classification
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: parsing
-- ============================================================================
-- 20_functions_parsing.sql — Parsing helpers (xml_text, normalize_message_id, clean_host, ...)
-- Loaded by db/dwh_init.sql via \i db/02_functions.sql.
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

-- Нормализованные идентификаторы сообщений в transactions не служат ключом поиска:
-- ответ связывается с документом через dim_message_document, а не обратным ходом
-- по журналу.
DROP INDEX IF EXISTS idx_transactions_message_id_norm;
DROP INDEX IF EXISTS idx_transactions_relates_to_norm;

-- Канонический ключ реестра подач. Применяется симметрично: при загрузке
-- EGISZ_MESSAGES.MSGID в dim_message_document и при поиске по relatesToMessage ответа.
-- Шлюз и ЕГИСЗ передают идентификатор в разных написаниях (с дефисами и без,
-- с префиксом urn:uuid:, в разном регистре), поэтому ключ приводится к одному виду.
CREATE OR REPLACE FUNCTION public.message_registry_key(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(upper(replace(public.normalize_message_id(p_value), '-', '')), '');
$$;

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

-- Извлекает адрес обмена (gost-<JID>.<домен>:<порт>) из LOGTEXT/MSGTEXT и REPLY_TO реестра.
-- Имя хоста бывает и числовым (gost-56571), и составным (gost-67136-1), и именованным
-- (gost-sova) — шаблон покрывает все три, иначе адрес обрезается по первому дефису.
CREATE OR REPLACE FUNCTION public.extract_gost_endpoint(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        (regexp_match(
            COALESCE(p_text, ''),
            '(gost-[a-z0-9]+(?:-[a-z0-9]+)*(?:\.[a-z0-9._-]+)?(?::[0-9]+)?)',
            'i'
        ))[1],
        ''
    );
$$;

-- Реестр OID медорганизаций. OID регистрационный, у ЮЛ он один; несколько OID на одном ЮЛ
-- в EGISZ_LICENSES означают отправку дочерних клиник с хоста головного ЮЛ. Поэтому пара
-- берётся по собственному хосту лицензии (gost-<N> адреса совпадает с её же JID), а не по
-- отметке обмена MODIFYDATE, которая тикает при повторной отправке типа СЭМД.
CREATE OR REPLACE VIEW public.dim_clinic_oid AS
SELECT DISTINCT ON (oid) oid, jid
FROM (
    SELECT
        NULLIF(btrim(dl.mo_uid), '') AS oid,
        dl.jid,
        ((regexp_match(COALESCE(dl.mo_domen, ''), 'gost-([0-9]+)'))[1] = dl.jid::text) AS own_host
    FROM public.dim_licenses dl
    WHERE dl.jid IS NOT NULL
      AND NULLIF(btrim(dl.mo_uid), '') IS NOT NULL
) t
ORDER BY oid, own_host DESC NULLS LAST, jid;

COMMENT ON VIEW public.dim_clinic_oid IS
'Реестр OID медорганизаций: OID → ЮЛ. При нескольких кандидатах выигрывает ЮЛ с собственным хостом.';

-- Адрес обмена → ЮЛ. MO_DOMEN лицензии и REPLY_TO реестра подач — один и тот же адрес,
-- поэтому представление нужно только именованным хостам: числовые разбираются из адреса.
CREATE OR REPLACE VIEW public.dim_clinic_endpoint AS
SELECT DISTINCT ON (host) host, jid
FROM (
    SELECT
        public.clean_host(dl.mo_domen) AS host,
        dl.jid,
        ((regexp_match(COALESCE(dl.mo_domen, ''), 'gost-([0-9]+)'))[1] = dl.jid::text) AS own_host
    FROM public.dim_licenses dl
    WHERE dl.jid IS NOT NULL
      AND public.clean_host(dl.mo_domen) IS NOT NULL
) t
ORDER BY host, own_host DESC NULLS LAST, jid;

COMMENT ON VIEW public.dim_clinic_endpoint IS
'Адрес обмена → ЮЛ (MO_DOMEN = REPLY_TO): добор именованных хостов, у которых нет номера в имени.';

-- Основной путь: ЮЛ по OID медорганизации из содержания обмена (<organization>).
-- DROP перед CREATE: смена типа возврата integer→bigint несовместима с CREATE OR REPLACE (JID > int4).
DROP FUNCTION IF EXISTS public.jid_from_mo_uid(text);
CREATE OR REPLACE FUNCTION public.jid_from_mo_uid(p_org_oid text)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    SELECT r.jid
    FROM public.dim_clinic_oid r
    WHERE r.oid = NULLIF(btrim(p_org_oid), '');
$$;

-- Запасной путь: ЮЛ по адресу обмена. Номер в gost-<N> — JID владельца хоста; отправка
-- дочерней клиники с хоста головного ЮЛ разрешается в головное ЮЛ, это допустимо —
-- приоритет остаётся за OID из содержания документа. Номер принимается только как ЮЛ,
-- известное справочнику: иначе адрес породил бы клинику, которой нет в JPERSONS.
DROP FUNCTION IF EXISTS public.jid_from_host(text);
CREATE OR REPLACE FUNCTION public.jid_from_host(p_text text)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    WITH endpoint AS (
        SELECT public.extract_gost_endpoint(p_text) AS value
    )
    SELECT COALESCE(
        (
            SELECT o.jid
            FROM endpoint e
            JOIN public.dim_organizations o
              ON o.jid = (regexp_match(e.value, 'gost-([0-9]+)'))[1]::bigint
        ),
        (
            SELECT r.jid
            FROM public.dim_clinic_endpoint r
            CROSS JOIN endpoint e
            WHERE r.host = public.clean_host(e.value)
        )
    );
$$;

-- Сверка OID документа с реестром ЮЛ ушла из хранимого слоя: сравнивать с JPERSONS.FIR_OID
-- нечего (источник его не заполняет), а сравнение с одной выбранной лицензией давало ложное
-- расхождение там, где OID принадлежал другой лицензии того же ЮЛ. Признак «OID не найден
-- в реестре» считается в rpt_document_versions поверх dim_clinic_oid.
DROP FUNCTION IF EXISTS public.document_source_mismatch(text, text, text, text);

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

-- Коды статуса документа берутся из dim_document_status, а не повторяются литералами
-- в ветвях transform: набор статусов задан справочником в одном месте.
CREATE OR REPLACE FUNCTION public.document_status_nonfinal()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT code
    FROM public.dim_document_status
    WHERE NOT is_final
    ORDER BY sort_order
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION public.document_status_final()
RETURNS SETOF text
LANGUAGE sql
STABLE
AS $$
    SELECT code FROM public.dim_document_status WHERE is_final;
$$;

-- Подсистема ЕГИСЗ, к которой относится строка журнала.
-- Первичный признак — URI вызова, который шлюз пишет в саму запись журнала:
-- /emdr/callback — РЭМД, /ips/callback — ИЭМК. Это реквизит транспорта, он не зависит
-- от разбора payload и заполнен во всех строках, включая сбои связи без тела ответа.
-- Запасные признаки для строк без URI — wsa:Action (ИЭМК ходит по IHE XDS.b, urn:ihe:*)
-- и порт сервиса клиники в LOGTEXT: 9921 — ИЭМК, 9945 — РЭМД.
DROP FUNCTION IF EXISTS public.exchange_contour(text, text);
CREATE OR REPLACE FUNCTION public.egisz_subsystem(
    p_uri text,
    p_action text,
    p_logtext text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN COALESCE(p_uri, '') ILIKE '%/emdr/%' THEN 'РЭМД'
        WHEN COALESCE(p_uri, '') ILIKE '%/ips/%' THEN 'ИЭМК'
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

-- Статус одного EXCHANGELOG-сообщения. Ключевое различие синхронного и асинхронного
-- ответов РЭМД (см. README §«Схема регистрации СЭМД»):
--   * Синхронный RegisterDocumentResponse со <status>success</status> подтверждает только
--     приём запроса на регистрацию (шаг 4 схемы), а не регистрацию документа — 'accepted'.
--   * Регистрация подтверждается ТОЛЬКО асинхронным callback'ом registerDocumentResult с
--     <documentStatus>Зарегистрировано</documentStatus> либо <status>OK</status> (шаг 10).
-- В аналитике остаются только финальные success/error и техническая ошибка LOGSTATE=3.
-- NB: синхронный ответ отдаётся в рамках registerDocument — исходящего вызова МИС→РЭМД,
-- которого в журнале шлюза нет по построению, поэтому ветка 'accepted' недостижима на
-- текущем контракте источника. Оставлена зарезервированной на случай, если шлюз начнёт
-- журналировать исходящую подачу. Документы без ответа создаёт ветка getDocumentFile.
-- 'accepted', а не 'pending': на грейне документа 'pending' — это состояние отправки
-- (dim_sent_state), и совпадение имён на разных грейнах вводило бы в заблуждение.
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
             AND COALESCE(p_raw_status, '') ~* '(success|ok)'                                 THEN 'accepted'
        WHEN COALESCE(p_raw_status, '') ~* '(processing|in[_-]?progress|inprogress|queued|received|accepted|pending|wait|обработк|принят|получен|ожида)'
                                                                                              THEN 'accepted'
        WHEN COALESCE(p_has_processing_marker, false)                                        THEN 'accepted'
        -- Колбэк без явного маркера элемента, но со статусом success/ok — регистрация.
        WHEN COALESCE(p_raw_status, '') ~* '^\s*(success|ok)\s*$'                             THEN 'success'
        WHEN COALESCE(p_raw_status, '') LIKE '%error%' OR COALESCE(p_has_error_ilike, false) THEN 'error'
        ELSE 'unknown'
    END;
$$;

-- ---------------------------------------------------------------- section: error_rules
-- ============================================================================
-- Правила классификации ошибок: dim_error_rules + dim_error_type_group.
-- Loaded by db/dwh_init.sql via \i db/02_functions.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_error_rules (
    rule_code text PRIMARY KEY,
    match_tier integer NOT NULL DEFAULT 3,
    match_code text,
    code_namespace text,
    nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code),
    parent_nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code),
    match_pattern text NOT NULL,
    interpretation text NOT NULL,
    error_category text NOT NULL DEFAULT 'Прочие',
    is_active boolean NOT NULL DEFAULT true,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS error_category text NOT NULL DEFAULT 'Прочие';
ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS match_tier integer NOT NULL DEFAULT 3;
ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS code_namespace text;
ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code);
ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS parent_nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code);

COMMENT ON COLUMN dim_error_rules.match_tier IS
'Ярус матчинга: 1 — код + специфичный текст; 2 — только код (match_pattern = ''(?is).*''); 3 — специфичный текст без кода; 4 — широкий текстовый фолбэк. Первый ярус с совпадением побеждает.';
COMMENT ON COLUMN dim_error_rules.code_namespace IS
'Пространство имён кода: «НСИ 305» — классификатор ФНСИ 1.2.643.5.1.13.13.99.2.305; «IHE XDS» — errorCode контура ИЭМК; «шлюз» — синтетический код интеграционного шлюза. NULL для текстовых ярусов.';
COMMENT ON COLUMN dim_error_rules.nsi_error_code IS
'Мнемоника справочника ФНСИ. Заполнена ровно для code_namespace = «НСИ 305» — внешний ключ не даёт завести правило на несуществующий код.';
COMMENT ON COLUMN dim_error_rules.parent_nsi_error_code IS
'Зонтичная мнемоника ФНСИ, отказ под которой уточняется текстом правила. Заполнена только для ярусов 3–4 контура регистрации: ярус 2 закрывает все коды НСИ 305 кроме VALIDATION_ERROR и RUNTIME_ERROR, поэтому текстовое правило срабатывает ровно под одной из них. NULL для ярусов 1–2 (мнемоника своя) и для контуров ИЭМК/шлюза (код вне НСИ 305).';

-- Сид собирается во временной таблице, чтобы прунинг снимал правила, убранные из
-- исходника: без него словарь в БД накапливал бы строки прошлых редакций и переставал
-- сходиться к декларации.
DROP TABLE IF EXISTS seed_error_rules;
CREATE TEMP TABLE seed_error_rules (
    rule_code text PRIMARY KEY,
    match_tier integer NOT NULL,
    match_code text,
    code_namespace text,
    nsi_error_code text,
    parent_nsi_error_code text,
    match_pattern text NOT NULL,
    interpretation text NOT NULL,
    error_category text NOT NULL
);

-- ------------------------------------------------------------------
-- Ярус 2: только код. Покрывается весь классификатор ФНСИ — правило и наименование
-- типа выводятся из справочника, поэтому завести код, которого нет в НСИ, невозможно.
-- Курируется только пара «категория ↔ формулировка»: категория обязательна, а
-- interpretation задаётся лишь там, где описание справочника непригодно как
-- наименование типа (плейсхолдеры значений в квадратных скобках, опечатки НСИ).
-- VALIDATION_ERROR и RUNTIME_ERROR сознательно вне яруса 2: их описания в НСИ
-- («Ошибка валидации значения», «Непредвиденная ошибка») не несут диагностики,
-- blanket-правило закрыло бы уточняющие ярусы 1 и 3.
-- ------------------------------------------------------------------
INSERT INTO seed_error_rules (rule_code, match_tier, match_code, code_namespace, nsi_error_code, match_pattern, interpretation, error_category)
SELECT
    lower(c.nsi_error_code),
    2,
    c.nsi_error_code,
    'НСИ 305',
    c.nsi_error_code,
    '(?is).*',
    COALESCE(
        m.interpretation,
        btrim(regexp_replace(regexp_replace(c.nsi_error_description, '\s*\[[^\]]*\]', '', 'g'), '\s{2,}', ' ', 'g'))
    ),
    COALESCE(m.error_category, 'Прочие')
FROM dim_nsi_error_code c
LEFT JOIN (VALUES
    ('ACCESS_DENIED', 'Ошибки регистрации в РЭМД', NULL),
    ('ATTRIBUTE_MISMATCH', 'Ошибки регистрации в РЭМД', NULL),
    ('CAN_NOT_ASSOCIATE', 'Ошибки регистрации в РЭМД', NULL),
    ('CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', 'Ошибки ЭП и сертификатов', 'Не удалось построить цепочку сертификатов до аккредитованного удостоверяющего центра'),
    ('CANT_REG_VERSION', 'Ошибки регистрации в РЭМД', NULL),
    ('DIGEST_MISMATCH', 'Ошибки ЭП и сертификатов', 'Хеш-сумма документа, полученного из предоставляющей системы, не соответствует зарегистрированной в РЭМД'),
    ('DISABLED_RMIS', 'Ошибки организации / ИС', NULL),
    ('DOC_DATE_MISMATCH_CERT_NOT_AFTER', 'Ошибки ЭП и сертификатов', NULL),
    ('DOC_DATE_MISMATCH_CERT_NOT_BEFORE', 'Ошибки ЭП и сертификатов', NULL),
    ('INCONSISTENT_DIGESTS', 'Ошибки ЭП и сертификатов', NULL),
    ('INTERNAL_ERROR', 'Технические ошибки РЭМД', NULL),
    ('INVALID_CERT_KEY_USAGE', 'Ошибки ЭП и сертификатов', NULL),
    ('INVALID_CONTENT', 'Ошибки структуры и валидации', NULL),
    ('INVALID_PLUGGABLE_ATTRS', 'Ошибки структуры и валидации', NULL),
    ('MIS_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('MIS_NOT_AVAILABLE', 'Ошибки получения файла ЭМД', NULL),
    ('NO_DOCUMENT_KIND_ON_DATE', 'Ошибки регистрации в РЭМД', NULL),
    ('NO_END_ENTITY_CERTIFICATE', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_RMIS', 'Ошибки организации / ИС', NULL),
    ('NO_ROLE_POLICY_ON_DATE', 'Ошибки регистрации в РЭМД', NULL),
    ('NO_SIGNATURE', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_SNILS', 'Данные пациента', NULL),
    ('NO_SPECIALITY', 'Данные медработника', NULL),
    ('NOT_UNIQUE_ASSOCIATION', 'Ошибки регистрации в РЭМД', NULL),
    ('NOT_UNIQUE_PROVIDED_ID', 'Ошибки регистрации в РЭМД', NULL),
    ('OBJECT_NOT_FOUND', 'Ошибки справочника НСИ', NULL),
    ('ORG_NOT_FOUND_IN_FRMO', 'Ошибки организации / ИС', NULL),
    ('ORG_SIGNATURE_OCCURRENCE_MISMATCH', 'Ошибки ЭП и сертификатов', NULL),
    ('PATIENT_CREATION_ERROR', 'Данные пациента', NULL),
    ('PATIENT_MPI_MISMATCH', 'Данные пациента', NULL),
    ('PATIENT_OCCURRENCE_MISMATCH', 'Данные пациента', NULL),
    ('PERSON_CARD_NOT_FOUND', 'Данные медработника', NULL),
    ('PERSON_NOT_FOUND', 'Данные медработника', NULL),
    ('PERSON_POST_IN_FRMR_MISMATCH', 'Данные медработника', NULL),
    ('PLUGGABLE_ATTRS_OCCURRENCE_MISMATCH', 'Ошибки структуры и валидации', 'Наличие дополнительных атрибутов документа не соответствует требованиям вида документов'),
    ('POSITION_TO_ROLE_MISMATCH', 'Данные медработника', 'Несоответствие должности и роли подписанта'),
    ('REGISTRY_ITEM_NOT_FOUND', 'Ошибки получения файла ЭМД', NULL),
    ('RMIS_REGION_MISMATCH', 'Ошибки организации / ИС', NULL),
    ('ROLE_OCCURRENCE_MISMATCH', 'Ошибки ЭП и сертификатов', NULL),
    ('SIGNATURE_DECODING_ERROR', 'Ошибки ЭП и сертификатов', NULL),
    ('SIGNATURE_VERIFICATION_ERROR', 'Ошибки ЭП и сертификатов', NULL),
    ('SIGNER_ORG_MISMATCH', 'Данные медработника', NULL),
    ('UNKNOWN_ALGORITHM', 'Ошибки ЭП и сертификатов', NULL),
    ('VALUE_MISMATCH_METADATA_AND_CERTIFICATE', 'Ошибки ЭП и сертификатов', NULL),
    ('VALUE_MISMATCH_METADATA_AND_FRMR', 'Данные медработника', NULL),
    ('WRONG_CREATION_DATE', 'Ошибки регистрации в РЭМД', NULL),
    ('WRONG_MESSAGE_ID', 'Ошибки регистрации в РЭМД', NULL),
    ('NO_ORG_ON_DATE', 'Ошибки организации / ИС', NULL),
    ('SIGNATURE_DUPLICATION', 'Ошибки ЭП и сертификатов', NULL),
    ('MULTIPLE_SIGNERS', 'Ошибки ЭП и сертификатов', NULL),
    ('WRONG_SIGNATURE_FORMAT', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_DEPARTMENT', 'Ошибки организации / ИС', NULL),
    ('INVALID_DOC_CONTENT_TYPE', 'Ошибки структуры и валидации', NULL),
    ('FILE_WAS_NOT_SENT', 'Ошибки получения файла ЭМД', NULL),
    ('SERIES_REQUIRED_WRONG_SERVICE_VERSION', 'Ошибки регистрации в РЭМД', NULL),
    ('SERIES_REQUIRED', 'Данные пациента', NULL),
    ('RMIS_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('PATIENT_ALREADY_REGISTERED', 'Данные пациента', NULL),
    ('GET_DOCUMENT_FILE_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('CA_INACCESSIBILITY', 'Ошибки ЭП и сертификатов', 'Адрес OCSP-службы не указан или недоступен, CRL также недоступен'),
    ('PATIENT_NOT_FOUND', 'Данные пациента', NULL),
    ('ADDITIONAL_INFO_REQUIRED', 'Данные пациента', NULL),
    ('NOT_UNIQUE_ITEM', 'Ошибки регистрации в РЭМД', NULL),
    ('AOGUID_NOT_FOUND', 'Данные пациента', NULL),
    ('REGION_CODE_DIFFERENT', 'Данные пациента', 'Регион адресного объекта, переданного в СЭМД, не совпадает с регионом по данным ФИАС'),
    ('HOUSEGUID_NOT_FOUND', 'Данные пациента', NULL),
    ('AOGUID_DIFFERENT', 'Данные пациента', 'Уникальный идентификатор адресного объекта, переданного в СЭМД, не совпадает с адресом по данным ФИАС'),
    ('RESTRICT_NEW_VERSION', 'Ошибки регистрации в РЭМД', NULL),
    ('FRLLO_VALIDATION_ERROR', 'Ошибки ФРЛЛО', 'Неверный формат передаваемого значения (формат или диапазон даты, маска или длина строки)'),
    ('FRLLO_DIC_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_REQUIRED_CITIZEN_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_REQUIRED_IDENTIFY_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_CITIZEN_IDENTIFY_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_CITIZEN_SEARCH_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_RECIPE_POSITION_ERROR', 'Ошибки ФРЛЛО', 'Не передан код назначенной медицинской продукции или передана неоднозначная информация о коде'),
    ('FRLLO_BENEFIT_SOURCE_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_ORGANIZATION_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_CITIZEN_BENEFIT_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_CITIZEN_REGION_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_COMISSION_INFO_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_RECIPE_DATE_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_EXPIRE_DATE_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_RELISE_POSITION_ERROR', 'Ошибки ФРЛЛО', 'Не передан код отпущенной медицинской продукции либо передан неоднозначный код'),
    ('FRLLO_RECIPE_IDENTIFY_ERROR', 'Ошибки ФРЛЛО', 'Отсутствуют сведения о переданном назначении медицинской продукции'),
    ('FRLLO_RELEASE_ORGANIZATION_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_RELISE_DATE_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_RELISE_QTY_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_TRANSPORT_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_SEMD_FLK_ERROR', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_NOT_CORRECT_TYPE', 'Ошибки ФРЛЛО', NULL),
    ('FRLLO_UNKNOWN_SYSTEM', 'Ошибки ФРЛЛО', NULL),
    ('RATE_LIMIT', 'Технические ошибки РЭМД', NULL),
    ('VALSYS_REJECT', 'Технические ошибки РЭМД', NULL),
    ('ASYNC_RESPONSE_TIMEOUT', 'Технические ошибки РЭМД', NULL),
    ('PATIENT_NAME_NOT_FOUND', 'Данные пациента', NULL),
    ('PATIENT_SURNAME_NOT_FOUND', 'Данные пациента', NULL),
    ('DUPLICATE_PATIENT_FOUND', 'Данные пациента', NULL),
    ('IPS_VALIDATION_WARNING', 'Ошибки структуры и валидации', NULL),
    ('XML_VALIDATOR_ERROR', 'Технические ошибки РЭМД', NULL),
    ('SCHEMA_PROCESSING_ERROR', 'Технические ошибки РЭМД', NULL),
    ('XML_VALIDATION_ERROR', 'Ошибки структуры и валидации', NULL),
    ('PERSONAL_SIG_CERT_NOT_ACTUAL_ON_DOC_CREATION_DT', 'Ошибки ЭП и сертификатов', NULL),
    ('INVALID_DOCTOR_FAMILY', 'Данные медработника', 'Фамилия медицинского работника в запросе на регистрацию отличается от фамилии в СЭМД'),
    ('INVALID_DOCTOR_NAME', 'Данные медработника', 'Имя медицинского работника в запросе на регистрацию отличается от имени в СЭМД'),
    ('INVALID_DOCTOR_PATRONYMIC', 'Данные медработника', 'Отчество медицинского работника в запросе на регистрацию отличается от отчества в СЭМД'),
    ('LEGAL_AUTHENTICATOR_NOT_FOUND', 'Данные медработника', NULL),
    ('INVALID_DOCTOR_INFO', 'Данные медработника', NULL),
    ('INVALID_DOCTOR_ID', 'Данные медработника', 'Локальный идентификатор медицинского работника в запросе на регистрацию отличается от идентификатора в СЭМД'),
    ('INVALID_DOCTOR_SNILS', 'Данные медработника', NULL),
    ('INVALID_DICTIONARY_MAPPING', 'Ошибки справочника НСИ', 'Не удалось найти поле, отвечающее за код справочника'),
    ('INVALID_DICTIONARY', 'Ошибки справочника НСИ', 'Для данного вида документа недопустимо использование указанного справочника'),
    ('INVALID_DICTIONARY_OID', 'Ошибки справочника НСИ', 'Справочник с указанным кодом отсутствует'),
    ('INVALID_DICTIONARY_VERSION', 'Ошибки справочника НСИ', 'Версия справочника недопустима для данного вида документа'),
    ('INVALID_ELEMENT_VALUE_CODE', 'Ошибки справочника НСИ', 'Значение с указанным кодом отсутствует в справочнике'),
    ('INVALID_ELEMENT_VALUE_NAME', 'Ошибки справочника НСИ', 'Наименование элемента не соответствует наименованию элемента в НСИ'),
    ('VALSYS_INTERNAL_ERROR', 'Технические ошибки РЭМД', NULL),
    ('RECEPIENT_INFO_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_SNILS_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_FAMILY_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_NAME_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_PATRONYMIC_MISMATCH', 'Данные пациента', NULL),
    ('PERSONAL_SIG_CERT_NOT_ACTUAL_ON_CHECK_DT', 'Ошибки ЭП и сертификатов', NULL),
    ('TIME_EXPIRED_ERROR', 'Ошибки регистрации в РЭМД', NULL),
    ('ORDER_ALREADY_PROCESSED', 'Ошибки регистрации в РЭМД', NULL),
    ('ORDER_NOT_FOUND', 'Ошибки регистрации в РЭМД', NULL)
) AS m(nsi_error_code, error_category, interpretation) ON m.nsi_error_code = c.nsi_error_code
WHERE c.nsi_error_code NOT IN ('VALIDATION_ERROR', 'RUNTIME_ERROR');

INSERT INTO seed_error_rules (rule_code, match_tier, match_code, code_namespace, nsi_error_code, match_pattern, interpretation, error_category)
VALUES
    -- ------------------------------------------------------------------
    -- Ярус 2, контур ИЭМК (IHE XDS.b, ProvideAndRegisterDocumentSet-b): код приходит
    -- в атрибуте errorCode тега RegistryError. match_code хранится UPPERCASE — движок
    -- сравнивает с upper(btrim(code)), смешанный регистр никогда не совпал бы.
    -- ------------------------------------------------------------------
    ('xds_dictionary_validation_code', 2, 'XDSDICTIONARYVALIDATIONERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: данные не соответствуют справочнику НСИ', 'Ошибки ИЭМК'),
    ('xds_cda_validation_code', 2, 'XDS.CDA.VALIDATIONERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки ИЭМК'),
    ('xds_duplicate_unique_id_code', 2, 'XDSDUPLICATEUNIQUEIDINREGISTRY', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: документ уже зарегистрирован', 'Ошибки ИЭМК'),
    ('xds_patient_registration_code', 2, 'XDSPATIENTREGISTRATIONERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),
    ('xds_document_unique_id_code', 2, 'XDSDOCUMENTUNIQUEIDERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: некорректный идентификатор документа', 'Ошибки ИЭМК'),
    ('xds_repository_error_code', 2, 'XDSREPOSITORYERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: внутренняя ошибка репозитория', 'Ошибки ИЭМК'),
    ('xds_cda_processing_code', 2, 'XDS.CDA.PROCESSINGERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка обработки CDA', 'Ошибки ИЭМК'),
    ('xds_replaced_document_org_code', 2, 'XDSREPLACEDDOCUMENTORGANIZATIONERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: замена версии отклонена (другая организация)', 'Ошибки ИЭМК'),
    -- Стандартные коды IHE ITI TF-3 (ebRS) регистрационного пути ITI-41/42, ещё не
    -- встречавшиеся в журнале. Query-коды (XDSStoredQuery*) и warning-код
    -- XDSExtraMetadataNotSaved (severity=Warning при Success) не заводим.
    ('xds_registry_error_code', 2, 'XDSREGISTRYERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: внутренняя ошибка реестра', 'Ошибки ИЭМК'),
    ('xds_registry_not_available_code', 2, 'XDSREGISTRYNOTAVAILABLE', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_registry_busy_code', 2, 'XDSREGISTRYBUSY', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_repository_busy_code', 2, 'XDSREPOSITORYBUSY', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_registry_out_of_resources_code', 2, 'XDSREGISTRYOUTOFRESOURCES', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_repository_out_of_resources_code', 2, 'XDSREPOSITORYOUTOFRESOURCES', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_missing_document_code', 2, 'XDSMISSINGDOCUMENT', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки ИЭМК'),
    ('xds_missing_document_metadata_code', 2, 'XDSMISSINGDOCUMENTMETADATA', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки ИЭМК'),
    ('xds_registry_metadata_error_code', 2, 'XDSREGISTRYMETADATAERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_repository_metadata_error_code', 2, 'XDSREPOSITORYMETADATAERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_patient_id_does_not_match_code', 2, 'XDSPATIENTIDDOESNOTMATCH', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_registry_dup_uid_msg_code', 2, 'XDSREGISTRYDUPLICATEUNIQUEIDINMESSAGE', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки ИЭМК'),
    ('xds_repository_dup_uid_msg_code', 2, 'XDSREPOSITORYDUPLICATEUNIQUEIDINMESSAGE', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки ИЭМК'),
    ('xds_non_identical_hash_code', 2, 'XDSNONIDENTICALHASH', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки ИЭМК'),
    ('xds_non_identical_size_code', 2, 'XDSNONIDENTICALSIZE', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки ИЭМК'),
    ('xds_unknown_patient_id_code', 2, 'XDSUNKNOWNPATIENTID', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),
    ('xds_invalid_document_content_code', 2, 'XDSINVALIDDOCUMENTCONTENT', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки ИЭМК'),
    ('xds_registry_deprecated_doc_code', 2, 'XDSREGISTRYDEPRECATEDDOCUMENTERROR', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: замена версии отклонена (документ уже заменён)', 'Ошибки ИЭМК'),
    ('xds_unknown_repository_id_code', 2, 'XDSUNKNOWNREPOSITORYID', 'IHE XDS', NULL, '(?is).*', 'ИЭМК: неверный идентификатор репозитория', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 2, контур шлюза: LOGSTATE = 3 — сбой транспорта до РЭМД, ответа нет.
    -- Код синтетический, проставляется при разборе журнала (db/03_transform.sql).
    -- ------------------------------------------------------------------
    ('gateway_transport_failure', 2, 'INTEGRATION_LOGSTATE_3', 'шлюз', NULL, '(?is).*', 'Сетевая ошибка', 'Ошибки связи'),

    -- ------------------------------------------------------------------
    -- Ярус 1: код + специфичный текст. Уточняет коды, чьё описание в НСИ покрывает
    -- слишком широкий класс причин.
    -- ------------------------------------------------------------------
    ('signature_metadata_certificate', 1, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', 'НСИ 305', 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    -- Живой текст CRE-126: «Association [RPLC] targetId with unique ID [...] not found
    -- in repository» — замена версии отклонена, заменяемый документ отсутствует.
    ('xds_document_unique_id_rplc', 1, 'XDSDOCUMENTUNIQUEIDERROR', 'IHE XDS', NULL, '(?is)\yRPLC\y|targetId.*not found', 'ИЭМК: заменяемый документ не найден (замена версии)', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 3: специфичный текст без кода. Основной ярус для VALIDATION_ERROR и
    -- RUNTIME_ERROR — по регламенту «Описание выполняемых проверок в РЭМД» причина
    -- этих кодов читается из текста сообщения.
    --
    -- Кросс-валидация запроса и СЭМД (§5.2–5.5 регламента). Значения приходят
    -- в [квадратных скобках] и уникализируют сообщение — нормализуем в тип.
    -- ------------------------------------------------------------------
    ('document_uid_mismatch_request', 3, NULL, NULL, NULL, '(?is)Уникальный идентификатор документа в ЭМД \[.*?\] отличается', 'Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('document_creation_date_mismatch_request', 3, NULL, NULL, NULL, '(?is)Дата создания документа в ЭМД \[.*?\] отличается', 'Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('patient_snils_mismatch_request', 3, NULL, NULL, NULL, '(?is)СНИЛС\s+пациента в ЭМД \[.*?\] отличается', 'СНИЛС пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_fio_mismatch_request', 3, NULL, NULL, NULL, '(?is)(Имя|Фамилия|Отчество) пациента в ЭМД \[.*?\] отличается', 'ФИО пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_birth_mismatch_request', 3, NULL, NULL, NULL, '(?is)Дата рождения пациента в ЭМД \[.*?\] отличается', 'Дата рождения пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('provider_org_mismatch_request', 3, NULL, NULL, NULL, '(?is)не совпадает с СП\s+providerOrganization', 'Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('represented_org_mismatch_request', 3, NULL, NULL, NULL, '(?is)не совпадает с СП\s+representedOrganization', 'Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('custodian_org_mismatch_request', 3, NULL, NULL, NULL, '(?is)не совпадает с СП\s+representedCustodianOrganization', 'Структурное подразделение (representedCustodianOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('org_ogrn_frmo_mismatch', 3, NULL, NULL, NULL, '(?is)ОГРН(ИП)? МО из СЭМД.*не совпадает', 'ОГРН организации из СЭМД не совпадает с ФРМО', 'Ошибки организации / ИС'),
    -- Те же причины приходят и кодом ФНСИ, и текстом внутри VALIDATION_ERROR: трактовка
    -- берётся одна, иначе один дефект дал бы в витрине две строки.
    ('doctor_position_mismatch_frmr', 3, NULL, NULL, NULL, '(?is)Указанная должность сотрудника со СНИЛС \[.*?\] не соответствует', 'Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР', 'Данные медработника'),
    ('doctor_birth_mismatch_frmr', 3, NULL, NULL, NULL, '(?is)Дата рождения сотрудника со СНИЛС \[.*?\] .* не соответствует', 'Переданные данные сотрудника не соответствуют данным, зарегистрированным в ФРМР', 'Данные медработника'),
    ('doctor_fio_mismatch_frmr', 3, NULL, NULL, NULL, '(?is)ФИО сотрудника со СНИЛС \[.*?\] не соответству', 'Переданные данные сотрудника не соответствуют данным, зарегистрированным в ФРМР', 'Данные медработника'),
    ('person_card_absent_text', 3, NULL, NULL, NULL, '(?is)личное дело сотрудника со СНИЛС \[.*?\] .* отсутствует', 'Личное дело сотрудника отсутствует в ФРМР', 'Данные медработника'),
    -- Текстовые двойники правил, привязанных к коду: то же сообщение приходит и без code
    -- (RegistryError без errorCode, записи вне окна хранения журнала). Без них ФИО
    -- медработника из формулировки утекало в наименование типа.
    ('person_card_cert_not_found_text', 3, NULL, NULL, NULL, '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    ('person_not_found_snils_text', 3, NULL, NULL, NULL, '(?is)В ФРМР не найден сотрудник со СНИЛС', 'Сотрудник не найден в ФРМР', 'Данные медработника'),
    ('signer_metadata_cert_mismatch_text', 3, NULL, NULL, NULL, '(?is)Несоответствие данных подписанта в запросе и в сертификате', 'Несоответствие данных (сотрудника либо МО) в сообщении и в сертификате ЭП', 'Ошибки ЭП и сертификатов'),
    ('patient_value_mismatch_gip', 3, NULL, NULL, NULL, '(?is)Указанное значение \[.*?\] .* не соответствует данным ГИП', 'Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП', 'Данные пациента'),
    ('patient_local_id_mismatch_request', 3, NULL, NULL, NULL, '(?is)Локальный идентификатор пациента в ЭМД \[.*?\] отличается', 'Локальный идентификатор пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_gender_mismatch_request', 3, NULL, NULL, NULL, '(?is)Пол пациента в ЭМД \[.*?\] отличается', 'Пол пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_name_invalid_chars', 3, NULL, NULL, NULL, '(?is)Недопустимые символы в имени', 'ФИО пациента содержит недопустимые символы', 'Данные пациента'),
    ('patient_snils_required_text', 3, NULL, NULL, NULL, '(?is)СНИЛС пациента в составе сведений о пациенте обязателен', 'Наличие СНИЛС пациента не соответствует требованиям вида документов', 'Данные пациента'),
    ('recipient_not_found_text', 3, NULL, NULL, NULL, '(?is)Получатель \[.*?\] из запроса на регистрацию сведений не найден', 'Получатель из запроса на регистрацию сведений не найден в СЭМД', 'Данные пациента'),
    ('document_already_registered_text', 3, NULL, NULL, NULL, '(?is)Документ с идентификатором .* уже зарегистрирован', 'Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован', 'Ошибки регистрации в РЭМД'),
    ('document_kind_not_actual_text', 3, NULL, NULL, NULL, '(?is)Вид документов .* не актуален на дату создания', 'Дата создания документа находится вне периода, допустимого для вида документов', 'Ошибки регистрации в РЭМД'),
    ('restrict_new_version_text', 3, NULL, NULL, NULL, '(?is)запрещена регистрация новых версий', 'Для вида документа запрещено регистрировать новую версию', 'Ошибки регистрации в РЭМД'),
    ('org_mismatch_request', 3, NULL, NULL, NULL, '(?is)МО из запроса на регистрацию сведений \[.*?\] не совпадает', 'Организация в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('org_not_linked_rmis', 3, NULL, NULL, NULL, '(?is)не привязана к РМИС', 'Организация не привязана к РМИС', 'Ошибки организации / ИС'),
    ('org_not_actual_frmo_text', 3, NULL, NULL, NULL, '(?is)MO code:.*is not actual', 'МО недействительна на дату создания документа', 'Ошибки организации / ИС'),
    ('department_not_exists_on_date', 3, NULL, NULL, NULL, '(?is)Подразделение с идентификатором \[.*?\] не существовало', 'Подразделение не существовало на дату создания документа', 'Ошибки организации / ИС'),
    ('department_org_mismatch', 3, NULL, NULL, NULL, '(?is)Подразделение с идентификатором \[.*?\] не соответствует организации', 'Подразделение не соответствует организации документа', 'Ошибки организации / ИС'),

    -- Проверка справочных значений (§5.6 регламента): те же проверки приходят как
    -- отдельными кодами ФНСИ, так и текстом внутри VALIDATION_ERROR.
    ('nsi_version_not_allowed_text', 3, NULL, NULL, NULL, '(?is)Справочник OID.*Версия .* недопустима для документа вида', 'Версия справочника недопустима для данного вида документа', 'Ошибки справочника НСИ'),
    ('nsi_version_absent_text', 3, NULL, NULL, NULL, '(?is)Справочник OID.*Версия .* отсутствует для данного справочника', 'Указанная версия отсутствует для данного справочника', 'Ошибки справочника НСИ'),
    ('nsi_element_code_absent_text', 3, NULL, NULL, NULL, '(?is)Справочник OID.*Элемент с кодом .* отсутствует', 'Значение с указанным кодом отсутствует в справочнике', 'Ошибки справочника НСИ'),
    ('nsi_element_name_mismatch_text', 3, NULL, NULL, NULL, '(?is)Наименование элемента .* не соответствует наименованию элемента в НСИ', 'Наименование элемента не соответствует наименованию элемента в НСИ', 'Ошибки справочника НСИ'),

    -- Валидация даты и последовательности подписей (§4.8–4.9 регламента).
    ('signature_mo_date_after_request', 3, NULL, NULL, NULL, '(?is)Дата и время создания подписи МО \[.*?\] не может быть позже', 'Дата подписи МО позже даты поступления запроса на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('signature_mr_date_after_request', 3, NULL, NULL, NULL, '(?is)Дата и время создания подписи медицинского работника \[.*?\] .* не может быть позже', 'Дата подписи медработника позже допустимой', 'Ошибки регистрации в РЭМД'),
    ('signature_creation_time_absent', 3, NULL, NULL, NULL, '(?is)отсутствует атрибут "Дата и время создания"', 'В подписи отсутствует атрибут «Дата и время создания»', 'Ошибки ЭП и сертификатов'),

    -- Ошибки указания адреса (§5.8.3 регламента) — самый массовый класс схематрона.
    -- Разделены по конкретной проверке: «адрес не указан» и «атрибуты типа адреса
    -- заполнены неверно» — разные причины и разные действия клиники.
    ('schematron_addr_type_attribute', 3, NULL, NULL, NULL, '(?is)patientRole/addr/address:Type должен иметь .*атрибута', 'Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям', 'Данные пациента'),
    ('schematron_addr_type_missing', 3, NULL, NULL, NULL, '(?is)patientRole/addr должен иметь .* элемент address:Type', 'Адрес пациента: не указан тип адреса (address:Type)', 'Данные пациента'),
    ('schematron_addr_count', 3, NULL, NULL, NULL, '(?is)patientRole должен иметь .* элемента? addr\y', 'Адрес пациента: недопустимое число элементов addr', 'Данные пациента'),
    ('schematron_addr_part_empty', 3, NULL, NULL, NULL, '(?is)Элемент (state|streetAddressLine|city|district|postalCode|country) должен содержать не пустое', 'Адрес пациента: составляющая адреса не заполнена', 'Данные пациента'),
    ('schematron_addr_fias', 3, NULL, NULL, NULL, '(?is)fias:(Address|AOGUID|HOUSEGUID)', 'Адрес пациента: сведения ФИАС не соответствуют требованиям', 'Данные пациента'),

    -- Ошибки указания отсутствия информации (§5.8.1 регламента): @nullFlavor
    -- проставлен там, где Руководство его не допускает.
    ('schematron_nullflavor_patient_id', 3, NULL, NULL, NULL, '(?is)patientRole/id\S* не должен иметь атрибут @nullFlavor', 'Идентификатор пациента: недопустимый атрибут @nullFlavor', 'Данные пациента'),
    ('schematron_nullflavor_telecom', 3, NULL, NULL, NULL, '(?is)telecom не должен иметь атрибут @nullFlavor', 'Контактные данные: недопустимый атрибут @nullFlavor', 'Ошибки структуры и валидации'),

    -- Ошибки указания связанного документа (§5.8.2 регламента).
    ('schematron_linkdocs', 3, NULL, NULL, NULL, '(?is)\yLINKDOCS?\y', 'Сведения о связанном документе не соответствуют требованиям', 'Ошибки структуры и валидации'),

    -- Контактные данные и реквизиты организации.
    ('schematron_telecom_value', 3, NULL, NULL, NULL, '(?is)Элемент telecom (обязан|должен) содержать один атрибут @value', 'Контактные данные: не заполнен атрибут @value элемента telecom', 'Ошибки структуры и валидации'),
    ('schematron_telecom_required', 3, NULL, NULL, NULL, '(?is)ДОЛЖЕН содержать не менее одного .* элемента telecom', 'Контактные данные: отсутствует обязательный элемент telecom', 'Ошибки структуры и валидации'),
    ('schematron_telecom_format', 3, NULL, NULL, NULL, '(?is)telecom со схемой "tel:"', 'Контактные данные: номер телефона не соответствует требуемому формату', 'Ошибки структуры и валидации'),
    ('schematron_org_props', 3, NULL, NULL, NULL, '(?is)(tmk:ogrn|tmk:inn|Props/Ogrn)', 'Реквизиты организации в СЭМД не заполнены', 'Ошибки организации / ИС'),

    -- Документ, удостоверяющий личность.
    ('schematron_identity_doc', 3, NULL, NULL, NULL, '(?is)(identity:DocInfo|IdentityCardType|identity:IssueDate|Неверный формат номера ДУЛ)', 'Реквизиты документа, удостоверяющего личность, не соответствуют требованиям', 'Данные пациента'),

    -- Значение вне перечня, заданного схематроном (не путать с проверкой по ФНСИ,
    -- которая приходит отдельными кодами INVALID_ELEMENT_VALUE_*).
    ('schematron_allowed_values', 3, NULL, NULL, NULL, '(?is)Допустимые значения для элементов', 'Значение элемента не входит в перечень допустимых', 'Ошибки структуры и валидации'),

    -- Валидация СЭМД по XSD-схеме (§5.7 регламента). Диагностики Xerces разделены
    -- по классу нарушения: структура, атрибут, тип значения, объявление элемента.
    ('xsd_invalid_content', 3, NULL, NULL, NULL, '(?is)(Invalid content was found|content of element .* is not complete)', 'XSD: недопустимый элемент или нарушен порядок элементов', 'Ошибки структуры и валидации'),
    ('xsd_attribute_not_allowed', 3, NULL, NULL, NULL, '(?is)Attribute .* is not allowed to appear', 'XSD: недопустимый атрибут элемента', 'Ошибки структуры и валидации'),
    ('xsd_attribute_required', 3, NULL, NULL, NULL, '(?is)Attribute .* must appear on element', 'XSD: отсутствует обязательный атрибут элемента', 'Ошибки структуры и валидации'),
    ('xsd_datatype_invalid', 3, NULL, NULL, NULL, '(?is)\ycvc-(datatype-valid|minLength-valid|maxLength-valid|pattern-valid|length-valid|enumeration-valid|type\.)', 'XSD: значение не соответствует типу элемента', 'Ошибки структуры и валидации'),
    ('xsd_element_declaration', 3, NULL, NULL, NULL, '(?is)\ycvc-elt\.', 'XSD: не найдено объявление элемента', 'Ошибки структуры и валидации'),
    ('xml_parse_error', 3, NULL, NULL, NULL, '(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)', 'Ошибка разбора XML-структуры документа', 'Ошибки структуры и валидации'),

    -- Недоступность проверяющих подсистем: RUNTIME_ERROR без уточнения выглядел бы
    -- как ошибка данных клиники, хотя причина на стороне РЭМД.
    ('runtime_check_unavailable', 3, NULL, NULL, NULL, '(?is)Не уда(е|ё)тся про(из)?вести проверку', 'Проверяющая подсистема РЭМД недоступна', 'Технические ошибки РЭМД'),
    ('runtime_request_processing', 3, NULL, NULL, NULL, '(?is)Невозможно обработать запрос', 'РЭМД не смог обработать запрос', 'Технические ошибки РЭМД'),
    ('runtime_signature_check', 3, NULL, NULL, NULL, '(?is)Непредвиденная ошибка при проверке подписей', 'Непредвиденная ошибка РЭМД при проверке подписей', 'Технические ошибки РЭМД'),
    ('document_file_storage_error', 3, NULL, NULL, NULL, '(?is)Ошибка получения файла ЭМД из файлового хранилища', 'Ошибка при получении файла документа из предоставляющей системы', 'Ошибки получения файла ЭМД'),
    ('certificate_ca_unavailable_text', 3, NULL, NULL, NULL, '(?is)Удостоверяющий центр сертификата недоступен', 'Адрес OCSP-службы не указан или недоступен, CRL также недоступен', 'Ошибки ЭП и сертификатов'),
    ('xds_replace_target_missing_text', 3, NULL, NULL, NULL, '(?is)targetId with unique ID .* not found in repository', 'ИЭМК: заменяемый документ не найден (замена версии)', 'Ошибки ИЭМК'),
    -- Якорь «срок» обязателен: без него «сертификат … Время ожидания истекло» (недоступность
    -- УЦ) попадало в тип про истёкший сертификат и давало два атома на одном сообщении.
    ('certificate_expired', 3, NULL, NULL, NULL, '(?is)(сертификат.*срок.*ист(ё|е)к|срок.*сертификат.*ист(ё|е)к|истекш\w*.*сертификат|certificate.*expired)', 'Срок действия сертификата ЭП истёк', 'Ошибки ЭП и сертификатов'),
    ('certificate_revoked', 3, NULL, NULL, NULL, '(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)', 'Сертификат ЭП отозван', 'Ошибки ЭП и сертификатов'),
    ('document_revoked_text', 3, NULL, NULL, NULL, '(?is)(аннулирован.*документ|документ.*аннулирован)', 'Документ аннулирован', 'Ошибки регистрации в РЭМД'),
    -- Внутренний код платформы ИЭМК в codeContext («PAT-001; Пациент не определен»):
    -- подстраховка на случай RegistryError без атрибута errorCode.
    ('xds_pat_001_text', 3, NULL, NULL, NULL, '(?is)\yPAT-001\y', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 4: широкий текстовый фолбэк. Применяется, только если ярусы 1–3 молчат.
    -- Держим минимальным: маски вида «любое упоминание СНИЛС/организации» перехватывали
    -- тексты, которые информативнее показать как есть.
    -- ------------------------------------------------------------------
    ('schematron_generic', 4, NULL, NULL, NULL, '(?is)(Ошибка валидации Schematron|схематрон)', 'Ошибка Schematron-валидации', 'Ошибки структуры и валидации'),
    ('transport_network', 4, NULL, NULL, NULL, '(?is)(\ynetwork\y|\yconnection\y|\ytransport\y|соединени|сетевая ошибка)', 'Сетевая ошибка', 'Ошибки связи');

-- ------------------------------------------------------------------
-- Зонтичная мнемоника текстовых ярусов. Ярус 2 закрывает весь классификатор кроме
-- VALIDATION_ERROR и RUNTIME_ERROR, поэтому текстовое правило контура регистрации
-- срабатывает ровно под одной из этих двух мнемоник — она и есть код отказа для типа,
-- у которого своей мнемоники в НСИ 305 нет. Проверено по журналу: у отказов с одним
-- атомом код совпадает с назначенным ниже во всех наблюдённых типах.
-- ------------------------------------------------------------------
UPDATE seed_error_rules SET parent_nsi_error_code = 'VALIDATION_ERROR' WHERE match_tier >= 3;
UPDATE seed_error_rules SET parent_nsi_error_code = 'RUNTIME_ERROR'
WHERE rule_code IN ('runtime_check_unavailable', 'runtime_request_processing', 'runtime_signature_check');
-- Контуры вне НСИ 305: ИЭМК отвечает errorCode IHE XDS, сетевой сбой фиксирует шлюз.
UPDATE seed_error_rules SET parent_nsi_error_code = NULL
WHERE rule_code IN ('xds_pat_001_text', 'xds_replace_target_missing_text', 'transport_network');

INSERT INTO dim_error_rules (rule_code, match_tier, match_code, code_namespace, nsi_error_code, parent_nsi_error_code, match_pattern, interpretation, error_category)
SELECT rule_code, match_tier, match_code, code_namespace, nsi_error_code, parent_nsi_error_code, match_pattern, interpretation, error_category
FROM seed_error_rules
ON CONFLICT (rule_code) DO UPDATE SET
    match_tier = EXCLUDED.match_tier,
    match_code = EXCLUDED.match_code,
    code_namespace = EXCLUDED.code_namespace,
    nsi_error_code = EXCLUDED.nsi_error_code,
    parent_nsi_error_code = EXCLUDED.parent_nsi_error_code,
    match_pattern = EXCLUDED.match_pattern,
    interpretation = EXCLUDED.interpretation,
    error_category = EXCLUDED.error_category,
    is_active = true,
    updated_at = now();

DELETE FROM dim_error_rules r
WHERE NOT EXISTS (SELECT 1 FROM seed_error_rules s WHERE s.rule_code = r.rule_code);

DROP TABLE seed_error_rules;

DO $$
BEGIN
    ALTER TABLE dim_error_rules ADD CONSTRAINT chk_dim_error_rules_match_tier
        CHECK (match_tier BETWEEN 1 AND 4
               AND ((match_tier <= 2) = (match_code IS NOT NULL)));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE dim_error_rules ADD CONSTRAINT chk_dim_error_rules_code_namespace
        CHECK ((match_code IS NOT NULL) = (code_namespace IS NOT NULL)
               AND (code_namespace IS NULL OR code_namespace IN ('НСИ 305', 'IHE XDS', 'шлюз'))
               AND ((code_namespace IS NOT DISTINCT FROM 'НСИ 305') = (nsi_error_code IS NOT NULL)));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Своя и зонтичная мнемоники взаимно исключают друг друга: ярус 1–2 знает код отказа,
-- ярус 3–4 — только зонтик, под которым отказ пришёл.
DO $$
BEGIN
    ALTER TABLE dim_error_rules ADD CONSTRAINT chk_dim_error_rules_parent_code
        CHECK (parent_nsi_error_code IS NULL OR (match_tier >= 3 AND nsi_error_code IS NULL));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_dim_error_rules_match_code ON dim_error_rules (match_code) WHERE match_code IS NOT NULL;

-- ============================================================================
-- dim_error_type_group — ЕДИНЫЙ источник истины «канонический тип → группа»:
-- тип — PK, группа единственная (конфликт «тип → две группы» невозможен).
-- Содержит интерпретации правил + типы, которые движок классификации выдаёт вне
-- таблицы правил. Категорию, зону ответственности и код НСИ витрины берут JOIN'ом.
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_error_type_group (
    error_type text PRIMARY KEY,
    error_category text NOT NULL,
    nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code),
    parent_nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code),
    responsibility text NOT NULL DEFAULT 'смешанная',
    is_retryable boolean NOT NULL DEFAULT false,
    updated_at timestamptz DEFAULT now()
);

-- DEFAULT сохраняем: производный INSERT из dim_error_rules не знает зону/повторяемость
-- новых типов — их выставляет backfill ниже в этом же прогоне.
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS responsibility text NOT NULL DEFAULT 'смешанная';
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS is_retryable boolean NOT NULL DEFAULT false;
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code);
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS parent_nsi_error_code text REFERENCES dim_nsi_error_code (nsi_error_code);
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS code_namespace text;
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS error_code text;

COMMENT ON COLUMN dim_error_type_group.parent_nsi_error_code IS
'Зонтичная мнемоника ФНСИ для типа, распознанного текстовым правилом (своей мнемоники в НСИ 305 у него нет). Отчётный слой показывает код отказа как COALESCE(nsi_error_code, parent_nsi_error_code).';
COMMENT ON COLUMN dim_error_type_group.error_code IS
'Код отказа в своём пространстве имён: мнемоника НСИ 305 для регистрационного пути, errorCode IHE XDS для ИЭМК, синтетический код для шлюза. NULL, если тип рождён правилами с разными кодами — показывать один из них означало бы выдавать частный случай за причину.';
COMMENT ON COLUMN dim_error_type_group.code_namespace IS
'Пространство имён error_code. Отделяет мнемонику ФНСИ 305 от кодов контуров ИЭМК и шлюза, у которых мнемоники в 305 нет по существу.';

DO $$
BEGIN
    ALTER TABLE dim_error_type_group ADD CONSTRAINT chk_dim_error_type_group_responsibility
        CHECK (responsibility IN ('клиника', 'МИС', 'интегратор', 'РЭМД', 'смешанная'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Канонические типы из активных правил. Код НСИ переносится с правила: тип, рождённый
-- уточняющим правилом яруса 1, наследует код уточняемого сообщения; тип, рождённый только
-- текстовым правилом, — зонтичную мнемонику, под которой отказ пришёл. Порядок ярусов в
-- DISTINCT ON отдаёт приоритет кодовому правилу: своя мнемоника точнее зонтичной.
-- Код отказа и его пространство имён считаются по ВСЕМ правилам типа, а не по выбранному
-- DISTINCT ON: один тип бывает исходом нескольких кодов (например «ИЭМК: сервис временно
-- недоступен» — пять кодов XDS*BUSY/OUTOFRESOURCES). Показывать первый по алфавиту значило бы
-- выдавать частный случай за причину, поэтому код проставляется только когда он единственный.
WITH type_code AS (
    SELECT
        r.interpretation,
        CASE WHEN count(DISTINCT c.code) = 1 THEN min(c.code) END AS error_code,
        CASE WHEN count(DISTINCT c.namespace) = 1 THEN min(c.namespace) END AS code_namespace
    FROM dim_error_rules r
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(r.nsi_error_code, r.parent_nsi_error_code, r.match_code) AS code,
            -- Текстовый ярус кода в правиле не несёт, но его зонтичная мнемоника — из НСИ 305.
            CASE
                WHEN COALESCE(r.nsi_error_code, r.parent_nsi_error_code) IS NOT NULL THEN 'НСИ 305'
                ELSE r.code_namespace
            END AS namespace
    ) c
    WHERE r.is_active
    GROUP BY r.interpretation
)
INSERT INTO dim_error_type_group (
    error_type, error_category, nsi_error_code, parent_nsi_error_code, code_namespace, error_code
)
SELECT DISTINCT ON (r.interpretation)
       r.interpretation, r.error_category, r.nsi_error_code, r.parent_nsi_error_code,
       t.code_namespace, t.error_code
FROM dim_error_rules r
JOIN type_code t ON t.interpretation = r.interpretation
WHERE r.is_active
ORDER BY r.interpretation, r.match_tier, r.rule_code
ON CONFLICT (error_type) DO UPDATE SET
    error_category = EXCLUDED.error_category,
    nsi_error_code = EXCLUDED.nsi_error_code,
    parent_nsi_error_code = EXCLUDED.parent_nsi_error_code,
    code_namespace = EXCLUDED.code_namespace,
    error_code = EXCLUDED.error_code,
    updated_at = now();

-- Типы, выдаваемые движком классификации вне таблицы правил.
INSERT INTO dim_error_type_group (error_type, error_category, nsi_error_code, parent_nsi_error_code)
VALUES
    ('Неизвестная ошибка', 'Прочие', NULL, NULL)
ON CONFLICT (error_type) DO UPDATE SET
    error_category = EXCLUDED.error_category,
    nsi_error_code = EXCLUDED.nsi_error_code,
    parent_nsi_error_code = EXCLUDED.parent_nsi_error_code,
    updated_at = now();

-- Прунинг: тип, переставший порождаться правилами, иначе остался бы в словаре
-- и продолжал раздавать категорию строкам витрины.
DELETE FROM dim_error_type_group g
WHERE g.error_type <> 'Неизвестная ошибка'
  AND NOT EXISTS (
      SELECT 1 FROM dim_error_rules r
      WHERE r.is_active AND r.interpretation = g.error_type);

-- ============================================================================
-- Backfill зоны ответственности и повторяемости. Идёт ПОСЛЕ INSERT'ов выше,
-- чтобы типы, впервые появившиеся в этом прогоне, получили значения сразу.
-- Шаг 1 — дефолты по категории, шаг 2 — точечные переопределения по типу.
-- ============================================================================
UPDATE dim_error_type_group g
SET responsibility = v.responsibility, is_retryable = v.is_retryable, updated_at = now()
FROM (VALUES
    ('Технические ошибки РЭМД',      'РЭМД',       true),
    ('Ошибки связи',                 'интегратор', true),
    ('Ошибки получения файла ЭМД',   'МИС',        true),
    ('Ошибки структуры и валидации', 'МИС',        false),
    ('Ошибки справочника НСИ',       'клиника',    false),
    ('Данные пациента',              'клиника',    false),
    ('Данные медработника',          'клиника',    false),
    ('Ошибки ЭП и сертификатов',     'клиника',    false),
    ('Ошибки организации / ИС',      'клиника',    false),
    ('Ошибки регистрации в РЭМД',    'смешанная',  false),
    ('Ошибки ИЭМК',                  'смешанная',  false),
    ('Ошибки ФРЛЛО',                 'МИС',        false),
    ('Прочие',                       'смешанная',  false)
) AS v(error_category, responsibility, is_retryable)
WHERE g.error_category = v.error_category
  AND (g.responsibility IS DISTINCT FROM v.responsibility
       OR g.is_retryable IS DISTINCT FROM v.is_retryable);

UPDATE dim_error_type_group g
SET responsibility = v.responsibility, is_retryable = v.is_retryable, updated_at = now()
FROM (VALUES
    -- Доступность getDocumentFile и регистрационные данные ИС — зона интегратора.
    ('Сервис системы, предоставляющей документ, не доступен', 'интегратор', true),
    ('РМИС/МИС не зарегистрирована в РЭМД',                   'интегратор', false),
    ('РМИС/МИС зарегистрирована в РЭМД но не активна',        'интегратор', false),
    ('Регион организации не соответствует региону РМИС/МИС',  'интегратор', false),
    ('Достигнут защитный лимит, просьба повторить через минуту или позже', 'интегратор', true),
    ('Организация не привязана к РМИС',                       'интегратор', false),
    -- Доступность УЦ и служб проверки статуса сертификата — не зона клиники.
    ('Адрес OCSP-службы не указан или недоступен, CRL также недоступен', 'РЭМД', true),
    ('Удостоверяющий центр сертификата недоступен',           'РЭМД', true),
    ('Проверяющая подсистема РЭМД недоступна',                'РЭМД', true),
    -- Внутренняя ошибка ГИП при создании пациента лечится повтором.
    ('Внутренняя ошибка ГИП при создании пациента',           'РЭМД', true),
    -- Запрос на регистрацию и его метаописание формирует МИС.
    ('Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию', 'МИС', false),
    ('Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию', 'МИС', false),
    ('СНИЛС пациента в ЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('ФИО пациента в ЭМД не совпадает с запросом на регистрацию',   'МИС', false),
    ('Дата рождения пациента в ЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Структурное подразделение (representedCustodianOrganization) в СЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Дата подписи МО позже даты поступления запроса на регистрацию', 'МИС', false),
    ('Дата подписи медработника позже допустимой',            'МИС', false),
    ('Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован', 'МИС', false),
    ('Из предоставляющей РМИС/МИС передан документ, метаописание которого не соответствует зарегистрированному', 'МИС', false),
    ('Дата создания документа больше даты регистрации',        'МИС', false),
    ('Асинхронный запрос файла ЭМД с указанным messageID не найден', 'МИС', false),
    -- Подпись формирует и упаковывает МИС/крипто-прослойка, не клиника.
    ('Ошибка декодирования ЭП',                                'МИС', false),
    ('Неподдерживаемый формат ЭП',                             'МИС', false),
    -- ИЭМК: технические сбои федеральной стороны лечатся повтором.
    ('ИЭМК: внутренняя ошибка репозитория', 'РЭМД', true),
    ('ИЭМК: внутренняя ошибка реестра',     'РЭМД', true),
    ('ИЭМК: сервис временно недоступен',    'РЭМД', true),
    ('ИЭМК: ошибка обработки CDA',          'РЭМД', true),
    ('ИЭМК: данные не соответствуют справочнику НСИ', 'клиника', false),
    ('ИЭМК: пациент не определён',          'клиника', false),
    ('ИЭМК: ошибка валидации структуры CDA', 'МИС', false),
    ('ИЭМК: документ уже зарегистрирован',  'МИС', false),
    ('ИЭМК: некорректный идентификатор документа', 'МИС', false),
    ('ИЭМК: заменяемый документ не найден (замена версии)', 'МИС', false),
    ('ИЭМК: замена версии отклонена (документ уже заменён)', 'МИС', false),
    ('ИЭМК: состав пакета не согласован (документы/метаданные)', 'МИС', false),
    ('ИЭМК: ошибка метаданных документа',   'МИС', false),
    ('ИЭМК: дублирующийся идентификатор в пакете', 'МИС', false),
    ('ИЭМК: повторная загрузка с изменённым содержимым', 'МИС', false),
    ('ИЭМК: неверный идентификатор репозитория', 'интегратор', false)
) AS v(error_type, responsibility, is_retryable)
WHERE g.error_type = v.error_type
  AND (g.responsibility IS DISTINCT FROM v.responsibility
       OR g.is_retryable IS DISTINCT FROM v.is_retryable);

-- ---------------------------------------------------------------- section: error_functions
-- ============================================================================
-- 40_functions_errors.sql — Error classification functions + xml_error_items + build_errors_json + semd_type_report_label
-- Loaded by db/dwh_init.sql via \i db/02_functions.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

-- DROP перед CREATE: sep — обязательный параметр, а CREATE OR REPLACE не убирает DEFAULT.
DROP FUNCTION IF EXISTS public.error_join_deduped(text[], text);
CREATE FUNCTION public.error_join_deduped(parts text[], sep text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    deduped text[] := ARRAY[]::text[];
    p text;
BEGIN
    IF parts IS NULL OR COALESCE(array_length(parts, 1), 0) = 0 THEN
        RETURN NULL;
    END IF;

    FOREACH p IN ARRAY parts
    LOOP
        IF p IS NULL OR btrim(p) = '' OR p = ANY (deduped) THEN
            CONTINUE;
        END IF;
        deduped := array_append(deduped, p);
    END LOOP;

    IF COALESCE(array_length(deduped, 1), 0) = 0 THEN
        RETURN NULL;
    END IF;

    RETURN array_to_string(deduped, sep);
END;
$$;

-- Ярусный матчинг: правила проверяются по возрастанию match_tier, первый ярус,
-- давший хотя бы одно совпадение, закрывает поиск (совпадения внутри яруса — все).
-- Ярус 2 (только код) матчится и на пустом message: '' ~* '(?is).*' истинно, поэтому
-- item с пустым текстом и известным кодом классифицируется правилом, а не «Код: X».
CREATE OR REPLACE FUNCTION public.error_matching_rule_labels(p_code text, p_message text)
RETURNS text[]
LANGUAGE sql
STABLE
AS $$
    WITH incoming AS (
        SELECT
            upper(btrim(COALESCE(p_code, ''))) AS c,
            btrim(COALESCE(p_message, '')) AS m
    ),
    -- РЭМД отдаёт часть мнемоник в написании, отличном от справочника (RECIPIENT_* против
    -- RECEPIENT_*). Синоним разрешается до сопоставления, поэтому правило заводится один
    -- раз — на каноничный код ФНСИ.
    normalized AS (
        SELECT COALESCE(a.nsi_error_code, i.c) AS c, i.m
        FROM incoming i
        LEFT JOIN public.dim_nsi_error_code_alias a ON a.alias = i.c
    ),
    matched AS (
        SELECT r.match_tier, r.rule_code, r.interpretation
        FROM normalized n
        JOIN public.dim_error_rules r ON r.is_active
        WHERE CASE r.match_tier
            WHEN 1 THEN n.c <> '' AND r.match_code = n.c AND n.m <> '' AND n.m ~* r.match_pattern
            WHEN 2 THEN n.c <> '' AND r.match_code = n.c AND n.m ~* r.match_pattern
            ELSE n.m <> '' AND n.m ~* r.match_pattern
        END
    ),
    -- Дедуп интерпретаций внутри выигравшего яруса: два правила яруса с одним типом
    -- (напр. общий и уточнённый schematron-паттерн) не должны давать атом дважды.
    winning AS (
        SELECT m.interpretation, min(m.rule_code) AS rule_code
        FROM matched m
        WHERE m.match_tier = (SELECT min(match_tier) FROM matched)
        GROUP BY m.interpretation
    )
    SELECT COALESCE(
        array_agg(r.interpretation ORDER BY r.rule_code),
        ARRAY[]::text[]
    )
    FROM winning r;
$$;

-- Атомарные типы для одного <item>. Порядок разбора повторяет логику регламента
-- «Описание выполняемых проверок в РЭМД»: сперва правило (код, затем уточняющий текст),
-- при его отсутствии — наименование кода по классификатору ФНСИ, и лишь затем сама
-- формулировка отказа. Заглушка возможна только когда нет ни кода, ни текста.
CREATE OR REPLACE FUNCTION public.error_item_atoms(p_code text, p_message text)
RETURNS text[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    c text;
    m text;
    rule_labels text[];
    nsi_label text;
BEGIN
    c := upper(btrim(COALESCE(p_code, '')));
    m := btrim(COALESCE(p_message, ''));

    IF m = '' AND c = '' THEN
        RETURN ARRAY[]::text[];
    END IF;

    -- Правила первыми, в том числе при пустом message: code-only правила яруса 2
    -- закрывают item без текста каноническим типом.
    rule_labels := public.error_matching_rule_labels(c, m);
    IF COALESCE(array_length(rule_labels, 1), 0) > 0 THEN
        RETURN rule_labels;
    END IF;

    -- Текст информативнее кода: VALIDATION_ERROR и RUNTIME_ERROR намеренно не покрыты
    -- ярусом 2, их причина читается только из формулировки.
    IF m <> '' THEN
        RETURN ARRAY[public.remd_error_type(m)];
    END IF;

    -- Отказ без текста: наименование берётся из классификатора ФНСИ по коду.
    SELECT btrim(regexp_replace(regexp_replace(g.nsi_error_description, '\s*\[[^\]]*\]', '', 'g'), '\s{2,}', ' ', 'g'))
    INTO nsi_label
    FROM public.dim_nsi_error_code g
    LEFT JOIN public.dim_nsi_error_code_alias a ON a.nsi_error_code = g.nsi_error_code
    WHERE g.nsi_error_code = c OR a.alias = c
    LIMIT 1;

    IF nsi_label IS NOT NULL THEN
        RETURN ARRAY[nsi_label];
    END IF;

    -- Код вне классификатора и без текста — остаток, по которому строится health-сигнал
    -- непокрытых причин.
    RETURN ARRAY['Код: ' || c];
END;
$$;

-- Нормализация payload ошибок (object|array|прочее → jsonb-массив). Общий вход для
-- всех построчных свёрток ниже — устраняет дублирование CTE normalized.
CREATE OR REPLACE FUNCTION public.error_payload_array(p_errors jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE jsonb_typeof(COALESCE(p_errors, '[]'::jsonb))
        WHEN 'array' THEN COALESCE(p_errors, '[]'::jsonb)
        WHEN 'object' THEN jsonb_build_array(COALESCE(p_errors, '{}'::jsonb))
        ELSE '[]'::jsonb
    END;
$$;

-- Плоская таксономия error_types: каждый <item> → атомы (error_item_atoms),
-- уникальные дедуплицируются и склеиваются через ' · ' (порядок детерминирован:
-- позиция item, затем тип — детерминизм важен для идемпотентности transform).
CREATE OR REPLACE FUNCTION public.error_classify(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        public.error_join_deduped(
            array_agg(btrim(atom) ORDER BY o, btrim(atom))
                FILTER (WHERE NULLIF(btrim(atom), '') IS NOT NULL
                          AND btrim(atom) <> 'Неизвестная ошибка'),
            ' · '
        ),
        'Неизвестная ошибка'
    )
    FROM jsonb_array_elements(public.error_payload_array(p_errors)) WITH ORDINALITY AS x(e, o)
    CROSS JOIN LATERAL unnest(public.error_item_atoms(e->>'code', e->>'message')) AS atom;
$$;

-- Исходные тексты <message> каждого <item>, уникальные через ' · ' в порядке появления.
CREATE OR REPLACE FUNCTION public.error_messages_row(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT public.error_join_deduped(
        array_agg(btrim(e->>'message') ORDER BY o),
        ' · '
    )
    FROM jsonb_array_elements(public.error_payload_array(p_errors)) WITH ORDINALITY AS x(e, o);
$$;

CREATE OR REPLACE FUNCTION public.xml_error_items(payload text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    item_xml text;
    item_code text;
    item_message text;
    result jsonb := '[]'::jsonb;
BEGIN
    IF payload IS NULL OR position('<' in payload) = 0 THEN
        RETURN result;
    END IF;

    FOR item_xml IN
        SELECT part
        FROM regexp_split_to_table(payload, '<(?:[A-Za-z0-9_]+:)?item(?:\s[^>]*)?>', 'i') AS part
    LOOP
        item_code := public.xml_text(item_xml, 'code');
        item_message := public.xml_text(item_xml, 'message');
        IF NULLIF(btrim(COALESCE(item_code, '')), '') IS NOT NULL
           OR NULLIF(btrim(COALESCE(item_message, '')), '') IS NOT NULL THEN
            result := result || jsonb_build_array(jsonb_build_object('code', item_code, 'message', item_message));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;

-- Ошибки контура ИЭМК (IHE XDS.b): RegistryResponse несёт их АТРИБУТАМИ тега
-- <rs:RegistryError errorCode="…" codeContext="…"/>, а не элементами <item>.
-- Атрибуты извлекаются независимо из attr-строки каждого тега — порядок и наличие
-- severity/location не важны. Значение в "" не может содержать сырую кавычку,
-- поэтому [^"]* безопасен; XML-сущности декодируем после захвата (&amp; — последним,
-- иначе двойное декодирование &amp;quot; → ").
CREATE OR REPLACE FUNCTION public.xml_registry_errors(payload text)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    WITH tags AS (
        SELECT t.m[1] AS attrs, t.ord
        FROM regexp_matches(
                 COALESCE(payload, ''),
                 '<(?:[A-Za-z0-9_.-]+:)?RegistryError\y([^>]*?)/?>',
                 'gi'
             ) WITH ORDINALITY AS t(m, ord)
    ),
    parsed AS (
        SELECT
            NULLIF(btrim((regexp_match(attrs, 'errorCode\s*=\s*"([^"]*)"', 'i'))[1]), '') AS code,
            NULLIF(btrim(
                replace(replace(replace(replace(replace(
                    COALESCE((regexp_match(attrs, 'codeContext\s*=\s*"([^"]*)"', 'i'))[1], ''),
                    '&quot;', '"'), '&apos;', ''''), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
            ), '') AS message,
            ord
        FROM tags
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object('code', code, 'message', message) ORDER BY ord)
            FILTER (WHERE code IS NOT NULL OR message IS NOT NULL),
        '[]'::jsonb
    )
    FROM parsed;
$$;

CREATE OR REPLACE FUNCTION public.build_errors_json(
    p_status text,
    p_error_code text,
    p_error_message text,
    p_msgtext text
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    WITH xml_items AS (
        SELECT public.xml_error_items(p_msgtext) AS items
    ),
    registry_items AS (
        -- strpos-гард: regexp-скан payload только когда RegistryError вообще присутствует.
        SELECT CASE
            WHEN strpos(COALESCE(p_msgtext, ''), 'RegistryError') > 0
            THEN public.xml_registry_errors(p_msgtext)
            ELSE '[]'::jsonb
        END AS items
    )
    SELECT CASE
        WHEN p_status <> 'error' THEN '[]'::jsonb
        WHEN jsonb_array_length(x.items) > 0 THEN x.items
        WHEN jsonb_array_length(g.items) > 0 THEN g.items
        WHEN NULLIF(btrim(COALESCE(p_error_code, '')), '') IS NOT NULL
          OR NULLIF(btrim(COALESCE(p_error_message, '')), '') IS NOT NULL
          THEN jsonb_build_array(jsonb_build_object('code', p_error_code, 'message', p_error_message))
        ELSE '[]'::jsonb
    END
    FROM xml_items x
    CROSS JOIN registry_items g;
$$;

-- Сворачивает формулировки LOGSTATE=3 в канонический тип: URL, gost-endpoint,
-- UUID и IP не должны раздувать кардинальность топов на дашбордах 02/04.
CREATE OR REPLACE FUNCTION public.network_error_type(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(
            left(
                btrim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            btrim(COALESCE(p_text, '')),
                                            'https?://[^\s<>"'',;]+',
                                            '<endpoint>',
                                            'gi'
                                        ),
                                        '(?i)gost-[0-9]+\.[a-z0-9._-]+(?::[0-9]+)?',
                                        '<gost-endpoint>',
                                        'g'
                                    ),
                                    '(?i)(?:<urn:uuid:|<uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}>?',
                                    '<uuid>',
                                    'g'
                                ),
                                '\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?',
                                '<ip>',
                                'g'
                            ),
                            '\[[^\]]{1,200}\]',
                            '[…]',
                            'g'
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ),
                220
            ),
            ''
        ),
        '(без текста)'
    );
$$;

-- Тип для сообщения, не покрытого правилами: показываем формулировку РЭМД, сняв с неё
-- значения конкретного документа. Без этого СНИЛС, OID и идентификаторы из [скобок]
-- превращали бы каждый отказ в отдельный тип. Полный текст остаётся в error_text —
-- поле поиска по ошибкам не нормализуется.
CREATE OR REPLACE FUNCTION public.remd_error_type(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(
            left(
                btrim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                -- Хвост «Путь: /ClinicalDocument[1]/…» описывает место
                                                -- в документе, а не причину: в наименовании типа он
                                                -- только мешает, в error_text сохраняется целиком.
                                                regexp_replace(btrim(COALESCE(p_text, '')), '(?is)\s*Путь:\s*/.*$', '', 'g'),
                                                'https?://[^\s<>"'',;]+',
                                                '<endpoint>',
                                                'gi'
                                            ),
                                            '(?i)(?:<urn:uuid:|<uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}>?',
                                            '<uuid>',
                                            'g'
                                        ),
                                        '\[[^\]]{0,200}\]',
                                        '[…]',
                                        'g'
                                    ),
                                    -- Номер правила схематрона задаётся Руководством по виду СЭМД:
                                    -- один и тот же дефект нумеруется по-разному в разных видах.
                                    '(?i)\yУ\d+(?:[-.]\d+)+',
                                    '<правило>',
                                    'g'
                                ),
                                '\y\d+(?:\.\d+){3,}\y',
                                '<oid>',
                                'g'
                            ),
                            '\y\d{6,}\y',
                            '<значение>',
                            'g'
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ),
                220
            ),
            ''
        ),
        '(без текста)'
    );
$$;
