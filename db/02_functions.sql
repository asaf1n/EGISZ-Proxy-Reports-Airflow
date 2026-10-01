-- ============================================================================
-- 02_functions.sql — parsing utilities, error rules dictionary, error classification
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: parsing
-- ============================================================================
-- 20_functions_parsing.sql — Parsing helpers (xml_text, normalize_message_id, clean_host, ...)
-- Loaded by db/dwh_init.sql via \i db/02_functions.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- ============================================================================

-- Пояс отчётного календаря. Литерала пояса в отчётном слое нет: границы недель и месяцев
-- берут значение отсюда, поэтому календарь витрин не может разойтись с календарём BI.
--
-- Значение читается из настройки роли конвейера (ALTER ROLE egisz SET timezone,
-- 01_schema), а не из пояса сессии. Разница существенна: REFRESH материализованного
-- представления пересчитывает границы периодов, и запуск обновления из сессии с другим
-- поясом сдвинул бы уже закрытые недели. Настройка роли одна на контур, поэтому закрытый
-- период считается одинаково, кем бы ни было запущено обновление.
--
-- Пояс сессии остаётся резервом: он покрывает контур, где пин роли не выставлен, и там же
-- даёт Metabase его собственный report-timezone.
--
-- STABLE, а не IMMUTABLE: значение постоянно внутри запроса, но задано конфигурацией.
-- Годится для материализованных представлений; в выражение индекса не ставится.
CREATE OR REPLACE FUNCTION serving_egisz.report_timezone()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (
            SELECT split_part(cfg, '=', 2)
            FROM pg_catalog.pg_db_role_setting s
            CROSS JOIN LATERAL unnest(s.setconfig) AS cfg
            WHERE split_part(cfg, '=', 1) = 'TimeZone'
              AND s.setrole <> 0
              AND s.setdatabase IN (
                  0,
                  (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = current_database())
              )
            -- Детерминированный отбор при нескольких ролях с пином: сначала настройка,
            -- заданная для этой базы, затем настройка текущего пользователя.
            ORDER BY (s.setdatabase <> 0) DESC, (s.setrole = current_user::regrole::oid) DESC, s.setrole
            LIMIT 1
        ),
        current_setting('TimeZone')
    );
$$;

COMMENT ON FUNCTION serving_egisz.report_timezone() IS
'Пояс отчётного календаря: настройка timezone роли конвейера, резервно — пояс сессии. Границы недель и месяцев считаются через него, литералом пояс в SQL не задаётся.';

CREATE OR REPLACE FUNCTION stg_egisz.xml_text(payload text, tag_name text)
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

CREATE OR REPLACE FUNCTION stg_egisz.normalize_message_id(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(regexp_replace(trim(both '<>' from btrim(COALESCE(value, ''))), '^urn:uuid:', '', 'i'), '');
$$;

-- Канонический ключ реестра подач. Применяется симметрично: к MSGID подачи
-- (stg_egisz.message_registry) и к relatesToMessage ответа при поиске подачи.
-- Шлюз и ЕГИСЗ передают идентификатор в разных написаниях (с дефисами и без,
-- с префиксом urn:uuid:, в разном регистре), поэтому ключ приводится к одному виду.
CREATE OR REPLACE FUNCTION stg_egisz.message_registry_key(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(upper(replace(stg_egisz.normalize_message_id(p_value), '-', '')), '');
$$;

CREATE OR REPLACE FUNCTION stg_egisz.clean_host(p_text text)
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
CREATE OR REPLACE FUNCTION stg_egisz.extract_gost_endpoint(p_text text)
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

-- Реестр OID медорганизаций. Первичный источник OID — справочник ЮЛ:
-- dim_organizations.fir_oid наполняется из НСИ организаций. Лицензии остаются
-- запасным источником для определения ЮЛ по хосту обмена, а не по OID документа.
CREATE OR REPLACE VIEW mart_egisz.dim_clinic_oid AS
SELECT DISTINCT ON (oid) oid, jid
FROM (
    SELECT
        NULLIF(btrim(o.fir_oid), '') AS oid,
        o.jid
    FROM mart_egisz.dim_organizations o
    WHERE o.jid IS NOT NULL
      AND NULLIF(btrim(o.fir_oid), '') IS NOT NULL
) t
ORDER BY oid, jid;

COMMENT ON VIEW mart_egisz.dim_clinic_oid IS
'Реестр OID медорганизаций: OID → ЮЛ из dim_organizations.fir_oid; host/лицензии используются только запасным резолвом.';

-- Адрес обмена → ЮЛ. MO_DOMEN лицензии и REPLY_TO реестра подач — один и тот же адрес,
-- поэтому представление нужно только именованным хостам: числовые разбираются из адреса.
CREATE OR REPLACE VIEW mart_egisz.dim_clinic_endpoint AS
SELECT DISTINCT ON (host) host, jid
FROM (
    SELECT
        stg_egisz.clean_host(dl.mo_domen) AS host,
        dl.jid,
        ((regexp_match(COALESCE(dl.mo_domen, ''), 'gost-([0-9]+)'))[1] = dl.jid::text) AS own_host
    FROM mart_egisz.dim_licenses dl
    WHERE dl.jid IS NOT NULL
      AND stg_egisz.clean_host(dl.mo_domen) IS NOT NULL
) t
ORDER BY host, own_host DESC NULLS LAST, jid;

COMMENT ON VIEW mart_egisz.dim_clinic_endpoint IS
'Адрес обмена → ЮЛ (MO_DOMEN = REPLY_TO): добор именованных хостов, у которых нет номера в имени.';

-- Разрешение OID руководства по реализации: основной OID и синонимы из НСИ 638 в одном реестре.
-- При совпадении выигрывает основной OID: загрузчик такое пересечение сейчас отвергает,
-- но порядок разрешения не должен зависеть от этой проверки.
CREATE OR REPLACE VIEW mart_egisz.dim_semd_guide_oid AS
SELECT DISTINCT ON (published_oid) published_oid, guide_oid, is_alias
FROM (
    SELECT g.oid, g.oid, false
    FROM mart_egisz.dim_nsi_semd_guide g
    UNION ALL
    SELECT a.alias_oid, a.guide_oid, true
    FROM mart_egisz.dim_nsi_semd_guide_alias a
) t (published_oid, guide_oid, is_alias)
ORDER BY published_oid, is_alias;

COMMENT ON VIEW mart_egisz.dim_semd_guide_oid IS
'Реестр OID руководств по реализации: published_oid (dim_nsi_semd_guide.oid либо dim_nsi_semd_guide_alias.alias_oid) → guide_oid (dim_nsi_semd_guide.oid). Точка входа — dim_semd_types.ig_oid.';

-- Единая цепочка резолва JID документа.
-- Основной путь: ЮЛ по OID медорганизации из содержания обмена (<organization>).
-- Запасной путь: ЮЛ по адресу обмена. Номер в gost-<N> — JID владельца хоста; отправка
-- дочерней клиники с хоста головного ЮЛ разрешается в головное ЮЛ, это допустимо —
-- приоритет остаётся за OID из содержания документа. Номер принимается только как ЮЛ,
-- известное справочнику: иначе адрес породил бы клинику, которой нет в JPERSONS.
CREATE OR REPLACE FUNCTION mart_egisz.resolve_document_jid(p_org_oid text, p_endpoint_text text)
RETURNS TABLE (jid bigint, resolve_method text)
LANGUAGE sql
STABLE
AS $$
    WITH endpoint AS (
        SELECT stg_egisz.extract_gost_endpoint(p_endpoint_text) AS value
    ),
    mo AS (
        SELECT (
            SELECT r.jid
            FROM mart_egisz.dim_clinic_oid r
            WHERE r.oid = NULLIF(btrim(p_org_oid), '')
        ) AS jid
    ),
    ho AS (
        SELECT COALESCE(
            (
                SELECT o.jid
                FROM endpoint e
                JOIN mart_egisz.dim_organizations o
                  ON o.jid = (regexp_match(e.value, 'gost-([0-9]+)'))[1]::bigint
            ),
            (
                SELECT r.jid
                FROM mart_egisz.dim_clinic_endpoint r
                CROSS JOIN endpoint e
                WHERE r.host = stg_egisz.clean_host(e.value)
            )
        ) AS jid
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

CREATE OR REPLACE FUNCTION stg_egisz.clean_text_value(p_text text)
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

CREATE OR REPLACE FUNCTION stg_egisz.normalize_semd_code(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    WITH normalized AS (
        SELECT stg_egisz.clean_text_value(p_text) AS value
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
-- экземпляр), без перезаписи существующего dwh_id.
-- Стабильный ключ набора версий (CDA setId) в журнал не попадает: тело СЭМД (base64-CDA)
-- шлюзом не сохраняется. Поэтому
-- группировка версий в один логический документ ведётся отдельным слоем document_group_id,
-- а не через dwh_id.
-- emdrId (рег. номер РЭМД) и OID (код типа в справочнике НСИ / OID организации) НЕ являются
-- ключом: emdrId — атрибут регистрации, OID — классификатор, не идентификатор экземпляра.
-- Колбэк без localUid не порождает новый ключ, а резолвится к существующей строке по
-- relatesToMessage / emdrId (см. egisz_transform_raw_to_facts).
CREATE OR REPLACE FUNCTION stg_egisz.dwh_id(
    p_local_uid text
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT lower(NULLIF(btrim(stg_egisz.clean_text_value(p_local_uid)), ''));
$$;

-- Коды статуса документа берутся из dim_document_status, а не повторяются литералами
-- в ветвях transform: набор статусов задан справочником в одном месте.
CREATE OR REPLACE FUNCTION mart_egisz.document_status_nonfinal()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT code
    FROM mart_egisz.dim_document_status
    WHERE NOT is_final
    ORDER BY sort_order
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION mart_egisz.document_status_final()
RETURNS SETOF text
LANGUAGE sql
STABLE
AS $$
    SELECT code FROM mart_egisz.dim_document_status WHERE is_final;
$$;

-- Очередь обработки на момент времени. Обе функции —
-- единственное определение членства и возраста: отчётный слой подставляет now(),
-- срез на прошлый момент — правую границу периода.
--
-- Членство: документ отправлен не позже момента, а ответ к этому моменту ещё не пришёл —
-- либо его нет вовсе, либо он наступил позже. Границей служит отметка ПЕРВОГО ответа:
-- last_callback_at перезаписывается каждым повторным коллбэком, и документ, отвеченный
-- за секунды, числился бы в очереди до последнего повтора.
CREATE OR REPLACE FUNCTION serving_egisz.is_pending_at(
    p_first_sent_at timestamptz,
    p_first_callback_at timestamptz,
    p_anchor timestamptz
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT p_first_sent_at IS NOT NULL
       AND p_anchor IS NOT NULL
       AND p_first_sent_at <= p_anchor
       AND (
           p_first_callback_at IS NULL
           OR p_first_callback_at > p_anchor
       );
$$;

-- Ступень — первая по sort_order, чья граница покрывает возраст ожидания; терминальная
-- (max_age_minutes IS NULL) замыкает лестницу и ловит в том числе отправки без
-- first_sent_at: без известного момента запроса файла возраст не определён. Пороги
-- остаются данными справочника — функции читают dim_pending_segments, поэтому STABLE.
--
-- Единственное определение — табличная функция: SQL-функция, возвращающая множество,
-- подставляется планировщиком в запрос, а скалярный вызов на каждой строке стоил бы
-- запуска отдельного исполнителя (для 180 тыс. документов — около двух секунд). Отчётный
-- слой соединяет её через LATERAL, точечные запросы читают скалярную обёртку.
CREATE OR REPLACE FUNCTION serving_egisz.pending_segment_at(
    p_first_sent_at timestamptz,
    p_anchor timestamptz
) RETURNS SETOF mart_egisz.dim_pending_segments
LANGUAGE sql
STABLE
AS $$
    SELECT (t.segment).*
    FROM (
        SELECT (array_agg(s ORDER BY s.sort_order))[1] AS segment
        FROM mart_egisz.dim_pending_segments s
        WHERE s.max_age_minutes IS NULL
           OR (
               p_first_sent_at IS NOT NULL
               AND p_anchor IS NOT NULL
               AND EXTRACT(EPOCH FROM (p_anchor - p_first_sent_at)) / 60.0 <= s.max_age_minutes
           )
    ) t;
$$;

CREATE OR REPLACE FUNCTION serving_egisz.pending_segment_code_at(
    p_first_sent_at timestamptz,
    p_anchor timestamptz
) RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT code FROM serving_egisz.pending_segment_at(p_first_sent_at, p_anchor);
$$;

-- Подсистема ЕГИСЗ, к которой относится строка журнала.
-- Первичный признак — URI вызова, который шлюз пишет в саму запись журнала:
-- /emdr/callback — РЭМД, /ips/callback — ИЭМК. Это реквизит транспорта, он не зависит
-- от разбора payload и заполнен во всех строках, включая сбои связи без тела ответа.
-- Запасные признаки для строк без URI — wsa:Action (ИЭМК ходит по IHE XDS.b, urn:ihe:*)
-- и порт сервиса клиники в LOGTEXT: 9921 — ИЭМК, 9945 — РЭМД.
CREATE OR REPLACE FUNCTION stg_egisz.egisz_subsystem(
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

-- Реестр подач в разобранном виде: ключ реестра по MSGID подачи и localUid документа.
-- ИЭМК localUid не использует — подачу на его порт (egisz_subsystem по REPLYTO) документ
-- не определяет. Выражения индексов ниже повторяют выражения колонок: по ним transform
-- ищет подачу, и без совпадения индекс не применяется.
CREATE OR REPLACE VIEW stg_egisz.message_registry AS
SELECT
    m.egmid,
    stg_egisz.message_registry_key(m.msgid) AS msgid,
    CASE
        WHEN stg_egisz.egisz_subsystem(NULL, NULL, m.replyto) = 'ИЭМК' THEN NULL
        ELSE stg_egisz.dwh_id(m.documentid)
    END AS document_uid,
    m.replyto AS reply_to,
    m.createdate AS created_at
FROM raw_egisz.egisz_messages m;

COMMENT ON VIEW stg_egisz.message_registry IS
'Реестр подач: строка EGISZ_MESSAGES по EGMID с ключом реестра (msgid) и localUid документа (document_uid, пуст для ИЭМК).';

CREATE INDEX IF NOT EXISTS idx_egisz_messages_registry_key
    ON raw_egisz.egisz_messages (stg_egisz.message_registry_key(msgid), egmid DESC)
    WHERE stg_egisz.message_registry_key(msgid) IS NOT NULL;
-- Ключ relatesToMessage у разобранных сообщений: детализация «реестр без DOCUMENTID» идёт от
-- небольшого числа подач без документа к их ответам, а не от каждого ответа к реестру.
-- Выражение повторяет ключ реестра: без совпадения индекс не применяется.
CREATE INDEX IF NOT EXISTS idx_exchange_messages_relates_to_key
    ON stg_egisz.exchange_messages (stg_egisz.message_registry_key(relates_to_msgid))
    WHERE relates_to_msgid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_egisz_messages_document_uid
    ON raw_egisz.egisz_messages ((
        CASE
            WHEN stg_egisz.egisz_subsystem(NULL, NULL, replyto) = 'ИЭМК' THEN NULL
            ELSE stg_egisz.dwh_id(documentid)
        END
    ));

-- Разложение payload EXCHANGELOG: каждый XML-тег и regex-маркер статуса
-- вычисляется ровно один раз; transform и связка документов читают stg_egisz.exchange_messages (xml_*).
CREATE OR REPLACE FUNCTION stg_egisz.parse_exchangelog_row(
    p_msgtext text,
    p_msgid text,
    p_logtext text
)
RETURNS TABLE (
    action text,
    msgid text,
    relates_to_msgid text,
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
    has_fault_marker boolean,
    mentions_error boolean
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
BEGIN
    v_action := stg_egisz.xml_text(p_msgtext, 'action');
    v_message_id_xml := stg_egisz.xml_text(p_msgtext, 'messageId');
    v_relates_to_message := stg_egisz.xml_text(p_msgtext, 'relatesToMessage');
    v_relates_to := stg_egisz.xml_text(p_msgtext, 'relatesTo');
    v_local_uid_xml := stg_egisz.xml_text(p_msgtext, 'localUid');
    v_kind_xml := stg_egisz.xml_text(p_msgtext, 'KIND');
    v_emdr_id_xml := stg_egisz.xml_text(p_msgtext, 'emdrId');
    v_doc_number_xml := stg_egisz.xml_text(p_msgtext, 'documentNumber');
    v_organization := stg_egisz.xml_text(p_msgtext, 'organization');
    v_organization_oid := stg_egisz.xml_text(p_msgtext, 'organizationOid');
    v_error_code_xml := stg_egisz.xml_text(p_msgtext, 'errorCode');
    v_code_xml := stg_egisz.xml_text(p_msgtext, 'code');
    -- SOAP-fault без <code>/<errorCode> нёс код только в <faultcode>; значение приходит
    -- с namespace-префиксом ('soap:Server') — оставляем локальную часть в UPPERCASE.
    v_faultcode := NULLIF(upper(regexp_replace(stg_egisz.xml_text(p_msgtext, 'faultcode'), '^[^:]*:', '')), '');
    v_error_message := stg_egisz.xml_text(p_msgtext, 'errorMessage');
    v_message_xml := stg_egisz.xml_text(p_msgtext, 'message');
    v_faultstring := stg_egisz.xml_text(p_msgtext, 'faultstring');
    v_status_xml := stg_egisz.xml_text(p_msgtext, 'status');
    v_document_status := stg_egisz.xml_text(p_msgtext, 'documentStatus');
    v_creation_datetime := stg_egisz.xml_text(p_msgtext, 'creationDateTime');
    v_creation_date := stg_egisz.xml_text(p_msgtext, 'creationDate');

    RETURN QUERY
    SELECT
        v_action,
        stg_egisz.normalize_message_id(COALESCE(NULLIF(btrim(p_msgid), ''), v_message_id_xml)),
        stg_egisz.normalize_message_id(COALESCE(v_relates_to_message, v_relates_to)),
        stg_egisz.clean_text_value(v_local_uid_xml),
        stg_egisz.clean_text_value(v_emdr_id_xml),
        stg_egisz.dwh_id(v_local_uid_xml),
        v_kind_xml,
        stg_egisz.clean_text_value(v_doc_number_xml),
        stg_egisz.clean_text_value(COALESCE(v_organization, v_organization_oid)),
        COALESCE(v_error_code_xml, v_code_xml, v_faultcode),
        COALESCE(v_error_message, v_message_xml, v_faultstring),
        lower(COALESCE(v_status_xml, '')),
        v_document_status,
        NULLIF((regexp_match(v_text_blob, 'gost-([0-9]+)', 'i'))[1], '')::bigint,
        NULLIF(btrim(COALESCE(v_creation_datetime, v_creation_date)), '')::timestamptz,
        v_payload ~* '<(ns[0-9]+:)?(error|fault)|<faultstring|<errorCode',
        v_payload ILIKE '%error%';
END;
$$;

CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_domen_host ON mart_egisz.dim_licenses (stg_egisz.clean_host(mo_domen));

-- Исход асинхронного ответа. По «Описанию выполняемых проверок в РЭМД» асинхронный ответ
-- содержит подтверждение регистрации СЭМД с регистрационными сведениями либо сведения об
-- отказе в регистрации; асинхронный ответ ИЭМК несёт статус RegistryResponse. Исход
-- читается из тела ответа при любом LOGSTATE: сбой доставки ответа в МИС исход регистрации
-- не меняет. У сообщения, которое не является асинхронным ответом, исхода нет (NULL);
-- асинхронный ответ с нераспознанным исходом тоже получает NULL и виден в контроле качества.
CREATE OR REPLACE FUNCTION stg_egisz.classify_async_status(
    p_source_action text,
    p_raw_status text,
    p_document_status text,
    p_has_fault_marker boolean,
    p_mentions_error boolean,
    p_registry_response_status text
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_source_action = 'sendRegisterDocumentResult' THEN CASE
            WHEN COALESCE(p_raw_status, '') ~* '(error|fail|reject|denied|отказ|ошибк)' THEN 'error'
            WHEN COALESCE(p_has_fault_marker, false)                                  THEN 'error'
            WHEN COALESCE(p_document_status, '') ~* 'зарегистр'                       THEN 'success'
            WHEN COALESCE(p_raw_status, '') ~* '^\s*(ok|success)\s*$'                 THEN 'success'
            WHEN COALESCE(p_mentions_error, false)                                    THEN 'error'
        END
        WHEN p_source_action LIKE 'urn:ihe:%AsyncResponse' THEN CASE
            WHEN p_registry_response_status = 'Success'                            THEN 'success'
            WHEN p_registry_response_status IN ('Failure', 'PartialSuccess')       THEN 'error'
        END
    END;
$$;

-- ---------------------------------------------------------------- section: error_rules
-- ============================================================================
-- Справочники обработки ошибок (mart_egisz): правила, категории, типы.
-- Вид, категория и тип ошибки определены в README, раздел «Классификация ошибок».
-- ============================================================================

-- Правило двух родов. Классификация относит элемент асинхронного ответа к типу ошибки
-- по коду и тексту. Нормализация — упорядоченный шаг, по которому тип строится из текста,
-- когда правило классификации не нашлось; различия источников задаются здесь данными, а
-- не отдельными функциями. Шаги нормализации с признаком masks_personal_data скрывают
-- персональные данные и составляют маскирование текста ошибки для выдачи
-- (mart_egisz.mask_error_text).
CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_rules (
    rule_code text PRIMARY KEY,
    rule_kind text NOT NULL,
    error_kind text,
    apply_order integer,
    match_tier integer,
    match_code text,
    nsi_error_code text REFERENCES mart_egisz.dim_nsi_error_code (nsi_error_code),
    match_pattern text NOT NULL,
    match_flags text NOT NULL DEFAULT '',
    replacement text,
    nsi_dictionary_pattern text,
    interpretation text,
    error_category text,
    updated_at timestamptz DEFAULT now(),
    masks_personal_data boolean NOT NULL DEFAULT false,
    CONSTRAINT chk_dim_error_rules_kind CHECK (
        (rule_kind = 'нормализация'
            AND apply_order IS NOT NULL AND replacement IS NOT NULL
            AND (error_kind IS NULL OR error_kind IN ('Ошибка связи', 'Ошибка асинхронного ответа'))
            AND match_tier IS NULL AND match_code IS NULL AND nsi_error_code IS NULL
            AND nsi_dictionary_pattern IS NULL AND interpretation IS NULL AND error_category IS NULL)
        OR (rule_kind = 'классификация'
            AND error_kind = 'Ошибка асинхронного ответа'
            AND apply_order IS NULL AND replacement IS NULL AND match_flags = ''
            AND match_tier BETWEEN 1 AND 4
            AND (match_tier <= 2) = (match_code IS NOT NULL)
            AND interpretation IS NOT NULL AND error_category IS NOT NULL
            AND NOT masks_personal_data)
    )
);

COMMENT ON TABLE mart_egisz.dim_error_rules IS
'Правила обработки ошибок. Строка — одно правило: классификация (код и текст элемента асинхронного ответа → тип и категория) либо шаг нормализации текста в тип (порядок, шаблон, замена). Справочник правил, сид — db/02_functions.sql.';
COMMENT ON COLUMN mart_egisz.dim_error_rules.error_kind IS
'Вид ошибки, к которому применяется правило. У шага нормализации NULL означает оба вида.';
COMMENT ON COLUMN mart_egisz.dim_error_rules.masks_personal_data IS
'Шаг нормализации скрывает персональные данные (ФИО, СНИЛС, дата рождения, ДУЛ, идентификатор пациента, субъект сертификата, e-mail). Только такие шаги применяет маскирование текста ошибки для выдачи mart_egisz.mask_error_text.';
COMMENT ON COLUMN mart_egisz.dim_error_rules.match_tier IS
'Ярус классификации: 1 — код и специфичный текст; 2 — только код; 3 — специфичный текст без кода; 4 — широкий текстовый фолбэк. Первый ярус с совпадением побеждает, внутри яруса — правило с меньшим rule_code.';
COMMENT ON COLUMN mart_egisz.dim_error_rules.nsi_error_code IS
'Мнемоника НСИ 305 правила, привязанного к коду классификатора. Внешний ключ не даёт завести правило на несуществующий код.';
COMMENT ON COLUMN mart_egisz.dim_error_rules.nsi_dictionary_pattern IS
'Регулярное выражение, извлекающее OID справочника ФНСИ из текста элемента (первая группа захвата). Захватывается справочник, а не версия и код элемента: они принадлежат отдельному документу.';

CREATE INDEX IF NOT EXISTS idx_dim_error_rules_match_code
    ON mart_egisz.dim_error_rules (match_code) WHERE match_code IS NOT NULL;

-- Сид собирается во временной таблице, чтобы прунинг снимал правила, убранные из
-- исходника: без него словарь в БД накапливал бы строки прошлых редакций.
DROP TABLE IF EXISTS seed_error_rules;
CREATE TEMP TABLE seed_error_rules (LIKE mart_egisz.dim_error_rules INCLUDING DEFAULTS);

-- ------------------------------------------------------------------
-- Нормализация. Тип ошибки без правила — текст элемента, в котором значения конкретного
-- документа заменены обозначениями. Шаги применяются по apply_order к виду из error_kind.
-- Снятие служебной обёртки ответов ИЭМК и ФРМСС идёт до замены значений: иначе значения
-- скрыли бы формулировку вместе с вложенными скобками. Реквизит в «Указанное значение
-- [Имя пациента] …» — указание, что именно не совпало с ГИП, поэтому шаг скобок его
-- не трогает. Граница слова и регистр для кириллицы заданы явными классами: под
-- lc_ctype = C \y и (?i) рядом с кириллицей не срабатывают.
-- ------------------------------------------------------------------
INSERT INTO seed_error_rules (rule_code, rule_kind, error_kind, apply_order, match_pattern, match_flags, replacement)
VALUES
    ('mask_iemk_rule_prefix', 'нормализация', 'Ошибка асинхронного ответа', 10, '^\[[A-Z]+-[0-9]+\]:\s*[A-Z]+-[0-9]+;\s*', '', ''),
    ('mask_iemk_patient_tail', 'нормализация', 'Ошибка асинхронного ответа', 20, ';\s*Patient\(.*$', '', ''),
    ('mask_iemk_patient_brackets', 'нормализация', 'Ошибка асинхронного ответа', 30, '^(Пациент не определен:\s*)\[(.*)\]$', '', '\1\2'),
    ('mask_check_digit', 'нормализация', 'Ошибка асинхронного ответа', 40, 'контрольное число [0-9]+', 'g', 'контрольное число'),
    -- ИЭМК пишет формат СНИЛС регулярным выражением; в типе — словами.
    ('describe_snils_format', 'нормализация', 'Ошибка асинхронного ответа', 45, 'формату \\d\{11\}', '', 'формату (11 цифр)'),
    ('mask_frmss_wrapper', 'нормализация', 'Ошибка асинхронного ответа', 50, '(?s)^(Ошибки валидации в ФРМСС):\s*\[code:\s*([A-Za-z_]+),\s*description:\s*(.*)\]\.?\s*$', '', '\1 (\2): \3'),
    ('mask_error_uid', 'нормализация', 'Ошибка асинхронного ответа', 60, ',?\s*уникальный идентификатор ошибки:\s*\S+\s*$', '', ''),
    -- Хвост с реквизитами сертификата (субъект, серийный номер, e-mail) принадлежит экземпляру.
    ('mask_certificate_tail', 'нормализация', 'Ошибка асинхронного ответа', 80, '(?is)\s*:?\s*(Validation failed|PKUP of the certificate|serial:|subject:).*$', '', ''),
    ('trim_spaces', 'нормализация', NULL, 100, '^ +| +$', 'g', ''),
    -- «Путь: /ClinicalDocument[1]/…» описывает место в документе, а не причину.
    ('mask_document_path', 'нормализация', 'Ошибка асинхронного ответа', 110, '(?is)\s*Путь:\s*/.*$', 'g', ''),
    ('mask_quoted_value', 'нормализация', 'Ошибка асинхронного ответа', 120, '''[^'']{0,200}''', 'g', '''[…]'''),
    ('mask_url', 'нормализация', NULL, 140, 'https?://[^\s<>"'',;]+', 'gi', '<endpoint>'),
    ('mask_gost_host', 'нормализация', 'Ошибка связи', 150, '(?i)gost-[0-9]+\.[a-z0-9._-]+(?::[0-9]+)?', 'g', '<gost-endpoint>'),
    ('mask_uuid', 'нормализация', NULL, 160, '(?i)(?:<urn:uuid:|<uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}>?', 'g', '<uuid>'),
    ('mask_ip', 'нормализация', 'Ошибка связи', 170, '\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?', 'g', '<ip>'),
    ('mask_bracket_value', 'нормализация', NULL, 180, '(?<!^Указанное значение )\[[^\]]{0,200}\]|(?<=^Указанное значение )\[(?![А-Яа-яЁё :0-9]{1,40}\])[^\]]{0,200}\]', 'g', '[…]'),
    -- Номер правила схематрона задаётся Руководством по виду СЭМД: один дефект нумеруется
    -- по-разному в разных видах.
    ('mask_rule_number', 'нормализация', 'Ошибка асинхронного ответа', 190, '(^|[^0-9A-Za-zА-Яа-яЁё])[Уу]\d+(?:[-.]\d+)+', 'g', '\1<правило>'),
    ('mask_oid', 'нормализация', 'Ошибка асинхронного ответа', 200, '\y\d+(?:\.\d+){3,}\y', 'g', '<oid>'),
    ('mask_long_number', 'нормализация', 'Ошибка асинхронного ответа', 210, '\y\d{6,}\y', 'g', '<значение>'),
    ('collapse_spaces', 'нормализация', NULL, 220, '\s+', 'g', ' '),
    ('trim_spaces_final', 'нормализация', NULL, 230, '^ +| +$', 'g', ''),
    ('cut_length', 'нормализация', NULL, 240, '^(.{220}).+$', '', '\1'),
    ('mask_date', 'нормализация', 'Ошибка асинхронного ответа', 250, '[0-9]{4}-[0-9]{2}-[0-9]{2}([T ][0-9:.]+Z?)?', 'g', '[…]'),
    -- Вложенные скобки источника «[[…]]» после замены значений оставляют лишнюю «]».
    ('collapse_masked_brackets', 'нормализация', 'Ошибка асинхронного ответа', 260, '\[…\]\]+', 'g', '[…]');

-- ------------------------------------------------------------------
-- Шаги, скрывающие персональные данные (masks_personal_data). Ими же маскируется текст
-- ошибки для выдачи (mart_egisz.mask_error_text), поэтому шаг заменяет только значение
-- человека и оставляет формулировку, адреса и реквизиты документа. Значение в квадратных
-- скобках узнаётся по реквизиту перед ним: СНИЛС, сравнение с ГИП и ФРМР, сравнение
-- реквизитов пациента в ЭМД и в запросе, подписант в метаданных и в сертификате. Шаблон
-- начинается с литерала: общий шаблон с перечнем реквизитов в начале проверяется на
-- порядок дольше. В нормализации результат шага совпадает с последующей заменой значения
-- в скобках; СНИЛС в типе обозначается псевдонимом <snils>.
-- ------------------------------------------------------------------
INSERT INTO seed_error_rules (rule_code, rule_kind, error_kind, apply_order, match_pattern, match_flags, replacement, masks_personal_data)
VALUES
    ('mask_series_number', 'нормализация', 'Ошибка асинхронного ответа', 70, '(номером|серией) [0-9]+', 'g', '\1 […]', true),
    ('mask_certificate_subject', 'нормализация', 'Ошибка асинхронного ответа', 72, '(subject:? )(?:(?!\s+issuer:).)+', 'g', '\1[…]', true),
    -- СНИЛС заменяется псевдонимом <snils>: тип называет, какой реквизит не прошёл проверку.
    ('mask_signer_snils', 'нормализация', 'Ошибка асинхронного ответа', 71, '(SNILS )\[[^\]]{0,200}\]( в метаданных и )\[[^\]]{0,200}\]', 'g', '\1<snils>\2<snils>', true),
    ('mask_patient_snils', 'нормализация', 'Ошибка асинхронного ответа', 73, '^(СНИЛС пациента в ЭМД )\[[^\]]{0,200}\]( отличается от СНИЛС пациента в запросе на регистрацию сведений )\[[^\]]{0,200}\]', '', '\1<snils>\2<snils>', true),
    ('mask_snils_value', 'нормализация', 'Ошибка асинхронного ответа', 74, '(СНИЛС(?: сотрудника| пациента)? ?)\[[^\]]{0,200}\]', 'g', '\1<snils>', true),
    ('mask_snils_entity', 'нормализация', 'Ошибка асинхронного ответа', 75, '(СНИЛС сотрудника &lt;)[0-9 -]{11,14}(&gt;)', 'g', '\1<snils>\2', true),
    -- «Дата рождения сотрудника со СНИЛС <snils> ([дата])».
    ('mask_birth_date_after_snils', 'нормализация', 'Ошибка асинхронного ответа', 76, '(СНИЛС <snils> \()\[[^\]]{0,200}\]', 'g', '\1[…]', true),
    ('mask_patient_value', 'нормализация', 'Ошибка асинхронного ответа', 77, '(пациента в (?:ЭМД|запросе на регистрацию сведений) )\[[^\]]{0,200}\]', 'g', '\1[…]', true),
    ('mask_registry_person_value', 'нормализация', 'Ошибка асинхронного ответа', 78, '(данным (?:ГИП|ФРМР) )\[[^\]]{0,200}\]', 'g', '\1[…]', true),
    -- РЭМД называет владельца сертификата его СНИЛС без обрамления.
    ('mask_certificate_holder_snils', 'нормализация', 'Ошибка асинхронного ответа', 79, '(сертификата недоступен: )[0-9]{11}(?![0-9])', 'g', '\1<snils>', true),
    ('mask_specified_snils', 'нормализация', 'Ошибка асинхронного ответа', 81, '^(Указанное значение \[СНИЛС\] )\[[^\]]{0,200}\]( не соответствует данным ГИП )\[[^\]]{0,200}\]', '', '\1<snils>\2<snils>', true),
    ('mask_specified_value', 'нормализация', 'Ошибка асинхронного ответа', 82, '^(Указанное значение \[[^\]]{1,40}\] )\[[^\]]{0,200}\]', '', '\1[…]', true),
    ('mask_name_value', 'нормализация', 'Ошибка асинхронного ответа', 84, '(^(?:Фамилия|Имя|Отчество) |от (?:фамилии|имени|отчества) )\[[^\]]{0,200}\]', 'g', '\1[…]', true),
    ('mask_signer_value', 'нормализация', 'Ошибка асинхронного ответа', 86, '((?:GIVEN_NAME|SURNAME|MIDDLE_NAME|SNILS) )\[[^\]]{0,200}\]( в метаданных и )\[[^\]]{0,200}\]', 'g', '\1[…]\2[…]', true),
    -- Получатель сведений РЭМД обозначен своим СНИЛС.
    ('mask_recipient_snils', 'нормализация', 'Ошибка асинхронного ответа', 87, '^(Получатель )\[[^\]]{0,200}\]( из запроса на регистрацию сведений)', '', '\1<snils>\2', true),
    ('mask_person_identifier', 'нормализация', 'Ошибка асинхронного ответа', 88, '(^По локальному id |patientId: |ДУЛ\. Номер )\[[^\]]{0,200}\]', 'g', '\1[…]', true),
    ('mask_fio_snils', 'нормализация', 'Ошибка асинхронного ответа', 90, ':[^:()]+\([Сс][Нн][Ии][Лл][Сс]:[^)]*\)', 'g', ': […] (СНИЛС: <snils>)', true),
    ('mask_email', 'нормализация', 'Ошибка асинхронного ответа', 130, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', 'g', '<e-mail>', true);

-- ------------------------------------------------------------------
-- Классификация, ярус 2: только код. Покрывается весь классификатор НСИ 305: правило
-- и наименование типа выводятся из справочника, поэтому завести код вне НСИ нельзя.
-- Курируется пара «категория ↔ формулировка»; interpretation задаётся лишь там, где
-- описание справочника непригодно как наименование типа. VALIDATION_ERROR и
-- RUNTIME_ERROR вне яруса 2: их описания не несут диагностики, причина читается из текста.
-- ------------------------------------------------------------------
INSERT INTO seed_error_rules (rule_code, rule_kind, error_kind, match_tier, match_code, nsi_error_code, match_pattern, interpretation, error_category)
SELECT
    lower(c.nsi_error_code),
    'классификация',
    'Ошибка асинхронного ответа',
    2,
    c.nsi_error_code,
    c.nsi_error_code,
    '(?is).*',
    COALESCE(
        m.interpretation,
        btrim(regexp_replace(regexp_replace(c.nsi_error_description, '\s*\[[^\]]*\]', '', 'g'), '\s{2,}', ' ', 'g'))
    ),
    m.error_category
FROM mart_egisz.dim_nsi_error_code c
JOIN (VALUES
    ('ACCESS_DENIED', 'Ошибки регистрации', NULL),
    ('ATTRIBUTE_MISMATCH', 'Ошибки регистрации', NULL),
    ('CAN_NOT_ASSOCIATE', 'Ошибки регистрации', NULL),
    ('CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', 'Ошибки ЭП и сертификатов', 'Не удалось построить цепочку сертификатов до аккредитованного удостоверяющего центра'),
    ('CANT_REG_VERSION', 'Ошибки регистрации', NULL),
    ('DIGEST_MISMATCH', 'Ошибки ЭП и сертификатов', 'Хеш-сумма документа, полученного из предоставляющей системы, не соответствует зарегистрированной в РЭМД'),
    ('DISABLED_RMIS', 'Ошибки организации / ИС', NULL),
    ('DOC_DATE_MISMATCH_CERT_NOT_AFTER', 'Ошибки ЭП и сертификатов', NULL),
    ('DOC_DATE_MISMATCH_CERT_NOT_BEFORE', 'Ошибки ЭП и сертификатов', NULL),
    ('INCONSISTENT_DIGESTS', 'Ошибки ЭП и сертификатов', NULL),
    ('INTERNAL_ERROR', 'Технические ошибки ЕГИСЗ', NULL),
    ('INVALID_CERT_KEY_USAGE', 'Ошибки ЭП и сертификатов', NULL),
    ('INVALID_CONTENT', 'Ошибки структуры и валидации', NULL),
    ('INVALID_PLUGGABLE_ATTRS', 'Ошибки структуры и валидации', NULL),
    ('MIS_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('MIS_NOT_AVAILABLE', 'Ошибки получения файла ЭМД', NULL),
    ('NO_DOCUMENT_KIND_ON_DATE', 'Ошибки регистрации', NULL),
    ('NO_END_ENTITY_CERTIFICATE', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_RMIS', 'Ошибки организации / ИС', NULL),
    ('NO_ROLE_POLICY_ON_DATE', 'Ошибки регистрации', NULL),
    ('NO_SIGNATURE', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_SNILS', 'Данные пациента', NULL),
    ('NO_SPECIALITY', 'Данные медработника', NULL),
    ('NOT_UNIQUE_ASSOCIATION', 'Ошибки регистрации', NULL),
    ('NOT_UNIQUE_PROVIDED_ID', 'Ошибки регистрации', NULL),
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
    ('WRONG_CREATION_DATE', 'Ошибки регистрации', NULL),
    ('WRONG_MESSAGE_ID', 'Ошибки регистрации', NULL),
    ('NO_ORG_ON_DATE', 'Ошибки организации / ИС', NULL),
    ('SIGNATURE_DUPLICATION', 'Ошибки ЭП и сертификатов', NULL),
    ('MULTIPLE_SIGNERS', 'Ошибки ЭП и сертификатов', NULL),
    ('WRONG_SIGNATURE_FORMAT', 'Ошибки ЭП и сертификатов', NULL),
    ('NO_DEPARTMENT', 'Ошибки организации / ИС', NULL),
    ('INVALID_DOC_CONTENT_TYPE', 'Ошибки структуры и валидации', NULL),
    ('FILE_WAS_NOT_SENT', 'Ошибки получения файла ЭМД', NULL),
    ('SERIES_REQUIRED_WRONG_SERVICE_VERSION', 'Ошибки регистрации', NULL),
    ('SERIES_REQUIRED', 'Данные пациента', NULL),
    ('RMIS_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('PATIENT_ALREADY_REGISTERED', 'Данные пациента', NULL),
    ('GET_DOCUMENT_FILE_ERROR', 'Ошибки получения файла ЭМД', NULL),
    ('CA_INACCESSIBILITY', 'Ошибки ЭП и сертификатов', 'Адрес OCSP-службы не указан или недоступен, CRL также недоступен'),
    ('PATIENT_NOT_FOUND', 'Данные пациента', NULL),
    ('ADDITIONAL_INFO_REQUIRED', 'Данные пациента', NULL),
    ('NOT_UNIQUE_ITEM', 'Ошибки регистрации', NULL),
    ('AOGUID_NOT_FOUND', 'Данные пациента', NULL),
    ('REGION_CODE_DIFFERENT', 'Данные пациента', 'Регион адресного объекта, переданного в СЭМД, не совпадает с регионом по данным ФИАС'),
    ('HOUSEGUID_NOT_FOUND', 'Данные пациента', NULL),
    ('AOGUID_DIFFERENT', 'Данные пациента', 'Уникальный идентификатор адресного объекта, переданного в СЭМД, не совпадает с адресом по данным ФИАС'),
    ('RESTRICT_NEW_VERSION', 'Ошибки регистрации', NULL),
    ('FRLLO_VALIDATION_ERROR', 'Ошибки структуры и валидации', 'Неверный формат передаваемого значения (формат или диапазон даты, маска или длина строки)'),
    ('FRLLO_DIC_ERROR', 'Ошибки справочника НСИ', NULL),
    ('FRLLO_REQUIRED_CITIZEN_ERROR', 'Данные пациента', NULL),
    ('FRLLO_REQUIRED_IDENTIFY_ERROR', 'Данные пациента', NULL),
    ('FRLLO_CITIZEN_IDENTIFY_ERROR', 'Данные пациента', NULL),
    ('FRLLO_CITIZEN_SEARCH_ERROR', 'Данные пациента', NULL),
    ('FRLLO_RECIPE_POSITION_ERROR', 'Ошибки структуры и валидации', 'Не передан код назначенной медицинской продукции или передана неоднозначная информация о коде'),
    ('FRLLO_BENEFIT_SOURCE_ERROR', 'Ошибки организации / ИС', NULL),
    ('FRLLO_ORGANIZATION_ERROR', 'Ошибки организации / ИС', NULL),
    ('FRLLO_CITIZEN_BENEFIT_ERROR', 'Данные пациента', NULL),
    ('FRLLO_CITIZEN_REGION_ERROR', 'Данные пациента', NULL),
    ('FRLLO_COMISSION_INFO_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_RECIPE_DATE_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_EXPIRE_DATE_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_RELISE_POSITION_ERROR', 'Ошибки структуры и валидации', 'Не передан код отпущенной медицинской продукции либо передан неоднозначный код'),
    ('FRLLO_RECIPE_IDENTIFY_ERROR', 'Ошибки структуры и валидации', 'Отсутствуют сведения о переданном назначении медицинской продукции'),
    ('FRLLO_RELEASE_ORGANIZATION_ERROR', 'Ошибки организации / ИС', NULL),
    ('FRLLO_RELISE_DATE_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_RELISE_QTY_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_TRANSPORT_ERROR', 'Ошибки регистрации', NULL),
    ('FRLLO_SEMD_FLK_ERROR', 'Ошибки структуры и валидации', NULL),
    ('FRLLO_NOT_CORRECT_TYPE', 'Ошибки регистрации', NULL),
    ('FRLLO_UNKNOWN_SYSTEM', 'Ошибки организации / ИС', NULL),
    ('RATE_LIMIT', 'Технические ошибки ЕГИСЗ', NULL),
    ('VALSYS_REJECT', 'Технические ошибки ЕГИСЗ', NULL),
    ('ASYNC_RESPONSE_TIMEOUT', 'Технические ошибки ЕГИСЗ', NULL),
    ('PATIENT_NAME_NOT_FOUND', 'Данные пациента', NULL),
    ('PATIENT_SURNAME_NOT_FOUND', 'Данные пациента', NULL),
    ('DUPLICATE_PATIENT_FOUND', 'Данные пациента', NULL),
    ('IPS_VALIDATION_WARNING', 'Ошибки структуры и валидации', NULL),
    ('XML_VALIDATOR_ERROR', 'Технические ошибки ЕГИСЗ', NULL),
    ('SCHEMA_PROCESSING_ERROR', 'Технические ошибки ЕГИСЗ', NULL),
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
    ('VALSYS_INTERNAL_ERROR', 'Технические ошибки ЕГИСЗ', NULL),
    ('RECEPIENT_INFO_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_SNILS_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_FAMILY_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_NAME_MISMATCH', 'Данные пациента', NULL),
    ('RECEPIENT_PATRONYMIC_MISMATCH', 'Данные пациента', NULL),
    ('PERSONAL_SIG_CERT_NOT_ACTUAL_ON_CHECK_DT', 'Ошибки ЭП и сертификатов', NULL),
    ('TIME_EXPIRED_ERROR', 'Ошибки регистрации', NULL),
    ('ORDER_ALREADY_PROCESSED', 'Ошибки регистрации', NULL),
    ('ORDER_NOT_FOUND', 'Ошибки регистрации', NULL)
) AS m(nsi_error_code, error_category, interpretation) ON m.nsi_error_code = c.nsi_error_code
WHERE c.nsi_error_code NOT IN ('VALIDATION_ERROR', 'RUNTIME_ERROR');

-- ------------------------------------------------------------------
-- Классификация, ярусы 1–4 и коды IHE XDS контура ИЭМК. Ярус 1 уточняет код текстом;
-- ярус 3 — специфичный текст без кода, основной для VALIDATION_ERROR и RUNTIME_ERROR:
-- по регламенту «Описание выполняемых проверок в РЭМД» их причина читается из текста.
-- Значения в [квадратных скобках] уникализируют сообщение и в тип не попадают.
-- Коды IHE XDS хранятся UPPERCASE: движок сравнивает с upper(btrim(code)).
-- ------------------------------------------------------------------
INSERT INTO seed_error_rules (rule_code, rule_kind, error_kind, match_tier, match_code, nsi_error_code, match_pattern, interpretation, error_category)
SELECT v.rule_code, 'классификация', 'Ошибка асинхронного ответа', v.match_tier, v.match_code, v.nsi_error_code, v.match_pattern, v.interpretation, v.error_category
FROM (VALUES
    ('xds_dictionary_validation_code', 2, 'XDSDICTIONARYVALIDATIONERROR', NULL, '(?is).*', 'ИЭМК: данные не соответствуют справочнику НСИ', 'Ошибки справочника НСИ'),
    ('xds_cda_validation_code', 2, 'XDS.CDA.VALIDATIONERROR', NULL, '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки структуры и валидации'),
    ('xds_duplicate_unique_id_code', 2, 'XDSDUPLICATEUNIQUEIDINREGISTRY', NULL, '(?is).*', 'ИЭМК: документ уже зарегистрирован', 'Ошибки регистрации'),
    ('xds_patient_registration_code', 2, 'XDSPATIENTREGISTRATIONERROR', NULL, '(?is).*', 'ИЭМК: пациент не определён', 'Данные пациента'),
    ('xds_document_unique_id_code', 2, 'XDSDOCUMENTUNIQUEIDERROR', NULL, '(?is).*', 'ИЭМК: некорректный идентификатор документа', 'Ошибки регистрации'),
    ('xds_repository_error_code', 2, 'XDSREPOSITORYERROR', NULL, '(?is).*', 'ИЭМК: внутренняя ошибка репозитория', 'Технические ошибки ЕГИСЗ'),
    ('xds_cda_processing_code', 2, 'XDS.CDA.PROCESSINGERROR', NULL, '(?is).*', 'ИЭМК: ошибка обработки CDA', 'Технические ошибки ЕГИСЗ'),
    ('xds_replaced_document_org_code', 2, 'XDSREPLACEDDOCUMENTORGANIZATIONERROR', NULL, '(?is).*', 'ИЭМК: замена версии отклонена (другая организация)', 'Ошибки регистрации'),
    ('xds_registry_error_code', 2, 'XDSREGISTRYERROR', NULL, '(?is).*', 'ИЭМК: внутренняя ошибка реестра', 'Технические ошибки ЕГИСЗ'),
    ('xds_registry_not_available_code', 2, 'XDSREGISTRYNOTAVAILABLE', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Технические ошибки ЕГИСЗ'),
    ('xds_registry_busy_code', 2, 'XDSREGISTRYBUSY', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Технические ошибки ЕГИСЗ'),
    ('xds_repository_busy_code', 2, 'XDSREPOSITORYBUSY', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Технические ошибки ЕГИСЗ'),
    ('xds_registry_out_of_resources_code', 2, 'XDSREGISTRYOUTOFRESOURCES', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Технические ошибки ЕГИСЗ'),
    ('xds_repository_out_of_resources_code', 2, 'XDSREPOSITORYOUTOFRESOURCES', NULL, '(?is).*', 'ИЭМК: сервис временно недоступен', 'Технические ошибки ЕГИСЗ'),
    ('xds_missing_document_code', 2, 'XDSMISSINGDOCUMENT', NULL, '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки структуры и валидации'),
    ('xds_missing_document_metadata_code', 2, 'XDSMISSINGDOCUMENTMETADATA', NULL, '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки структуры и валидации'),
    ('xds_registry_metadata_error_code', 2, 'XDSREGISTRYMETADATAERROR', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки структуры и валидации'),
    ('xds_repository_metadata_error_code', 2, 'XDSREPOSITORYMETADATAERROR', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки структуры и валидации'),
    ('xds_patient_id_does_not_match_code', 2, 'XDSPATIENTIDDOESNOTMATCH', NULL, '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки структуры и валидации'),
    ('xds_registry_dup_uid_msg_code', 2, 'XDSREGISTRYDUPLICATEUNIQUEIDINMESSAGE', NULL, '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки регистрации'),
    ('xds_repository_dup_uid_msg_code', 2, 'XDSREPOSITORYDUPLICATEUNIQUEIDINMESSAGE', NULL, '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки регистрации'),
    ('xds_non_identical_hash_code', 2, 'XDSNONIDENTICALHASH', NULL, '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки регистрации'),
    ('xds_non_identical_size_code', 2, 'XDSNONIDENTICALSIZE', NULL, '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки регистрации'),
    ('xds_unknown_patient_id_code', 2, 'XDSUNKNOWNPATIENTID', NULL, '(?is).*', 'ИЭМК: пациент не определён', 'Данные пациента'),
    ('xds_invalid_document_content_code', 2, 'XDSINVALIDDOCUMENTCONTENT', NULL, '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки структуры и валидации'),
    ('xds_registry_deprecated_doc_code', 2, 'XDSREGISTRYDEPRECATEDDOCUMENTERROR', NULL, '(?is).*', 'ИЭМК: замена версии отклонена (документ уже заменён)', 'Ошибки регистрации'),
    ('xds_unknown_repository_id_code', 2, 'XDSUNKNOWNREPOSITORYID', NULL, '(?is).*', 'ИЭМК: неверный идентификатор репозитория', 'Ошибки организации / ИС'),
    ('signature_metadata_certificate', 1, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    ('xds_document_unique_id_rplc', 1, 'XDSDOCUMENTUNIQUEIDERROR', NULL, '(?is)\yRPLC\y|targetId.*not found', 'ИЭМК: заменяемый документ не найден (замена версии)', 'Ошибки регистрации'),
    ('document_uid_mismatch_request', 3, NULL, NULL, '(?is)Уникальный идентификатор документа в ЭМД \[.*?\] отличается', 'Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию', 'Ошибки регистрации'),
    ('document_creation_date_mismatch_request', 3, NULL, NULL, '(?is)Дата создания документа в ЭМД \[.*?\] отличается', 'Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию', 'Ошибки регистрации'),
    ('patient_snils_mismatch_request', 3, NULL, NULL, '(?is)СНИЛС\s+пациента в ЭМД \[.*?\] отличается', 'СНИЛС пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_fio_mismatch_request', 3, NULL, NULL, '(?is)(Имя|Фамилия|Отчество) пациента в ЭМД \[.*?\] отличается', 'ФИО пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_birth_mismatch_request', 3, NULL, NULL, '(?is)Дата рождения пациента в ЭМД \[.*?\] отличается', 'Дата рождения пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('provider_org_mismatch_request', 3, NULL, NULL, '(?is)не совпадает с СП\s+providerOrganization', 'Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации'),
    ('represented_org_mismatch_request', 3, NULL, NULL, '(?is)не совпадает с СП\s+representedOrganization', 'Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации'),
    ('custodian_org_mismatch_request', 3, NULL, NULL, '(?is)не совпадает с СП\s+representedCustodianOrganization', 'Структурное подразделение (representedCustodianOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации'),
    ('org_ogrn_frmo_mismatch', 3, NULL, NULL, '(?is)ОГРН(ИП)? МО из СЭМД.*не совпадает', 'ОГРН организации из СЭМД не совпадает с ФРМО', 'Ошибки организации / ИС'),
    ('doctor_position_mismatch_frmr', 3, NULL, NULL, '(?is)Указанная должность сотрудника со СНИЛС \[.*?\] не соответствует', 'Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР', 'Данные медработника'),
    ('doctor_birth_mismatch_frmr', 3, NULL, NULL, '(?is)Дата рождения сотрудника со СНИЛС \[.*?\] .* не соответствует', 'Переданные данные сотрудника не соответствуют данным, зарегистрированным в ФРМР', 'Данные медработника'),
    ('doctor_fio_mismatch_frmr', 3, NULL, NULL, '(?is)ФИО сотрудника со СНИЛС \[.*?\] не соответству', 'Переданные данные сотрудника не соответствуют данным, зарегистрированным в ФРМР', 'Данные медработника'),
    ('person_card_absent_text', 3, NULL, NULL, '(?is)личное дело сотрудника со СНИЛС \[.*?\] .* отсутствует', 'Личное дело сотрудника отсутствует в ФРМР', 'Данные медработника'),
    ('person_card_cert_not_found_text', 3, NULL, NULL, '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    ('person_not_found_snils_text', 3, NULL, NULL, '(?is)В ФРМР не найден сотрудник со СНИЛС', 'Сотрудник не найден в ФРМР', 'Данные медработника'),
    ('signer_metadata_cert_mismatch_text', 3, NULL, NULL, '(?is)Несоответствие данных подписанта в запросе и в сертификате', 'Несоответствие данных (сотрудника либо МО) в сообщении и в сертификате ЭП', 'Ошибки ЭП и сертификатов'),
    ('patient_value_mismatch_gip', 3, NULL, NULL, '(?is)Указанное значение \[.*?\] .* не соответствует данным ГИП', 'Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП', 'Данные пациента'),
    ('patient_local_id_mismatch_request', 3, NULL, NULL, '(?is)Локальный идентификатор пациента в ЭМД \[.*?\] отличается', 'Локальный идентификатор пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_gender_mismatch_request', 3, NULL, NULL, '(?is)Пол пациента в ЭМД \[.*?\] отличается', 'Пол пациента в ЭМД не совпадает с запросом на регистрацию', 'Данные пациента'),
    ('patient_name_invalid_chars', 3, NULL, NULL, '(?is)Недопустимые символы в имени', 'ФИО пациента содержит недопустимые символы', 'Данные пациента'),
    ('patient_snils_required_text', 3, NULL, NULL, '(?is)СНИЛС пациента в составе сведений о пациенте обязателен', 'Наличие СНИЛС пациента не соответствует требованиям вида документов', 'Данные пациента'),
    ('recipient_not_found_text', 3, NULL, NULL, '(?is)Получатель \[.*?\] из запроса на регистрацию сведений не найден', 'Получатель из запроса на регистрацию сведений не найден в СЭМД', 'Данные пациента'),
    ('document_already_registered_text', 3, NULL, NULL, '(?is)Документ с идентификатором .* уже зарегистрирован', 'Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован', 'Ошибки регистрации'),
    ('document_kind_not_actual_text', 3, NULL, NULL, '(?is)Вид документов .* не актуален на дату создания', 'Дата создания документа находится вне периода, допустимого для вида документов', 'Ошибки регистрации'),
    ('restrict_new_version_text', 3, NULL, NULL, '(?is)запрещена регистрация новых версий', 'Для вида документа запрещено регистрировать новую версию', 'Ошибки регистрации'),
    ('org_mismatch_request', 3, NULL, NULL, '(?is)МО из запроса на регистрацию сведений \[.*?\] не совпадает', 'Организация в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации'),
    ('org_not_linked_rmis', 3, NULL, NULL, '(?is)не привязана к РМИС', 'Организация не привязана к РМИС', 'Ошибки организации / ИС'),
    ('org_not_actual_frmo_text', 3, NULL, NULL, '(?is)MO code:.*is not actual', 'МО недействительна на дату создания документа', 'Ошибки организации / ИС'),
    ('department_not_exists_on_date', 3, NULL, NULL, '(?is)Подразделение с идентификатором \[.*?\] не существовало', 'Подразделение не существовало на дату создания документа', 'Ошибки организации / ИС'),
    ('department_org_mismatch', 3, NULL, NULL, '(?is)Подразделение с идентификатором \[.*?\] не соответствует организации', 'Подразделение не соответствует организации документа', 'Ошибки организации / ИС'),
    ('nsi_version_not_allowed_text', 3, NULL, NULL, '(?is)Справочник OID.*Версия .* недопустима для документа вида', 'Версия справочника недопустима для данного вида документа', 'Ошибки справочника НСИ'),
    ('nsi_version_absent_text', 3, NULL, NULL, '(?is)Справочник OID.*Версия .* отсутствует для данного справочника', 'Указанная версия отсутствует для данного справочника', 'Ошибки справочника НСИ'),
    ('nsi_element_code_absent_text', 3, NULL, NULL, '(?is)Справочник OID.*Элемент с кодом .* отсутствует', 'Значение с указанным кодом отсутствует в справочнике', 'Ошибки справочника НСИ'),
    ('nsi_element_name_mismatch_text', 3, NULL, NULL, '(?is)Наименование элемента .* не соответствует наименованию элемента в НСИ', 'Наименование элемента не соответствует наименованию элемента в НСИ', 'Ошибки справочника НСИ'),
    ('signature_mo_date_after_request', 3, NULL, NULL, '(?is)Дата и время создания подписи МО \[.*?\] не может быть позже', 'Дата подписи МО позже даты поступления запроса на регистрацию', 'Ошибки регистрации'),
    ('signature_mr_date_after_request', 3, NULL, NULL, '(?is)Дата и время создания подписи медицинского работника \[.*?\] .* не может быть позже', 'Дата подписи медработника позже допустимой', 'Ошибки регистрации'),
    ('signature_creation_time_absent', 3, NULL, NULL, '(?is)отсутствует атрибут "Дата и время создания"', 'В подписи отсутствует атрибут «Дата и время создания»', 'Ошибки ЭП и сертификатов'),
    ('schematron_addr_type_attribute', 3, NULL, NULL, '(?is)patientRole/addr/address:Type должен иметь .*атрибута', 'Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям', 'Данные пациента'),
    ('schematron_addr_type_missing', 3, NULL, NULL, '(?is)patientRole/addr должен иметь .* элемент address:Type', 'Адрес пациента: не указан тип адреса (address:Type)', 'Данные пациента'),
    ('schematron_addr_count', 3, NULL, NULL, '(?is)patientRole должен иметь .* элемента? addr\y', 'Адрес пациента: недопустимое число элементов addr', 'Данные пациента'),
    ('schematron_addr_part_empty', 3, NULL, NULL, '(?is)Элемент (state|streetAddressLine|city|district|postalCode|country) должен содержать не пустое', 'Адрес пациента: составляющая адреса не заполнена', 'Данные пациента'),
    ('schematron_addr_fias', 3, NULL, NULL, '(?is)fias:(Address|AOGUID|HOUSEGUID)', 'Адрес пациента: сведения ФИАС не соответствуют требованиям', 'Данные пациента'),
    ('schematron_nullflavor_patient_id', 3, NULL, NULL, '(?is)patientRole/id\S* не должен иметь атрибут @nullFlavor', 'Идентификатор пациента: недопустимый атрибут @nullFlavor', 'Данные пациента'),
    ('schematron_nullflavor_telecom', 3, NULL, NULL, '(?is)telecom не должен иметь атрибут @nullFlavor', 'Контактные данные: недопустимый атрибут @nullFlavor', 'Ошибки структуры и валидации'),
    ('schematron_linkdocs', 3, NULL, NULL, '(?is)\yLINKDOCS?\y', 'Сведения о связанном документе не соответствуют требованиям', 'Ошибки структуры и валидации'),
    ('schematron_telecom_value', 3, NULL, NULL, '(?is)Элемент telecom (обязан|должен) содержать один атрибут @value', 'Контактные данные: не заполнен атрибут @value элемента telecom', 'Ошибки структуры и валидации'),
    ('schematron_telecom_required', 3, NULL, NULL, '(?is)ДОЛЖЕН содержать не менее одного .* элемента telecom', 'Контактные данные: отсутствует обязательный элемент telecom', 'Ошибки структуры и валидации'),
    ('schematron_telecom_format', 3, NULL, NULL, '(?is)telecom со схемой "tel:"', 'Контактные данные: номер телефона не соответствует требуемому формату', 'Ошибки структуры и валидации'),
    ('schematron_org_props', 3, NULL, NULL, '(?is)(tmk:ogrn|tmk:inn|Props/Ogrn)', 'Реквизиты организации в СЭМД не заполнены', 'Ошибки организации / ИС'),
    ('schematron_identity_doc', 3, NULL, NULL, '(?is)(identity:DocInfo|IdentityCardType|identity:IssueDate|Неверный формат номера ДУЛ)', 'Реквизиты документа, удостоверяющего личность, не соответствуют требованиям', 'Данные пациента'),
    ('schematron_allowed_values', 3, NULL, NULL, '(?is)Допустимые значения для элементов', 'Значение элемента не входит в перечень допустимых', 'Ошибки структуры и валидации'),
    ('xsd_invalid_content', 3, NULL, NULL, '(?is)(Invalid content was found|content of element .* is not complete)', 'XSD: недопустимый элемент или нарушен порядок элементов', 'Ошибки структуры и валидации'),
    ('xsd_attribute_not_allowed', 3, NULL, NULL, '(?is)Attribute .* is not allowed to appear', 'XSD: недопустимый атрибут элемента', 'Ошибки структуры и валидации'),
    ('xsd_attribute_required', 3, NULL, NULL, '(?is)Attribute .* must appear on element', 'XSD: отсутствует обязательный атрибут элемента', 'Ошибки структуры и валидации'),
    ('xsd_datatype_invalid', 3, NULL, NULL, '(?is)\ycvc-(datatype-valid|minLength-valid|maxLength-valid|pattern-valid|length-valid|enumeration-valid|type\.)', 'XSD: значение не соответствует типу элемента', 'Ошибки структуры и валидации'),
    ('xsd_element_declaration', 3, NULL, NULL, '(?is)\ycvc-elt\.', 'XSD: не найдено объявление элемента', 'Ошибки структуры и валидации'),
    ('xml_parse_error', 3, NULL, NULL, '(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)', 'Ошибка разбора XML-структуры документа', 'Ошибки структуры и валидации'),
    ('runtime_check_unavailable', 3, NULL, NULL, '(?is)Не уда(е|ё)тся про(из)?вести проверку', 'Проверяющая подсистема РЭМД недоступна', 'Технические ошибки ЕГИСЗ'),
    ('runtime_request_processing', 3, NULL, NULL, '(?is)Невозможно обработать запрос', 'РЭМД не смог обработать запрос', 'Технические ошибки ЕГИСЗ'),
    ('runtime_signature_check', 3, NULL, NULL, '(?is)Непредвиденная ошибка при проверке подписей', 'Непредвиденная ошибка РЭМД при проверке подписей', 'Технические ошибки ЕГИСЗ'),
    ('document_file_storage_error', 3, NULL, NULL, '(?is)Ошибка получения файла ЭМД из файлового хранилища', 'Ошибка при получении файла документа из предоставляющей системы', 'Ошибки получения файла ЭМД'),
    ('certificate_ca_unavailable_text', 3, NULL, NULL, '(?is)Удостоверяющий центр сертификата недоступен', 'Адрес OCSP-службы не указан или недоступен, CRL также недоступен', 'Ошибки ЭП и сертификатов'),
    ('xds_replace_target_missing_text', 3, NULL, NULL, '(?is)targetId with unique ID .* not found in repository', 'ИЭМК: заменяемый документ не найден (замена версии)', 'Ошибки регистрации'),
    ('certificate_expired', 3, NULL, NULL, '(?is)(сертификат.*срок.*ист(ё|е)к|срок.*сертификат.*ист(ё|е)к|истекш\w*.*сертификат|certificate.*expired)', 'Срок действия сертификата ЭП истёк', 'Ошибки ЭП и сертификатов'),
    ('certificate_revoked', 3, NULL, NULL, '(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)', 'Сертификат ЭП отозван', 'Ошибки ЭП и сертификатов'),
    ('document_revoked_text', 3, NULL, NULL, '(?is)(аннулирован.*документ|документ.*аннулирован)', 'Документ аннулирован', 'Ошибки регистрации'),
    ('xds_pat_001_text', 3, NULL, NULL, '(?is)\yPAT-001\y', 'ИЭМК: пациент не определён', 'Данные пациента'),
    ('schematron_generic', 4, NULL, NULL, '(?is)(Ошибка валидации Schematron|схематрон)', 'Ошибка Schematron-валидации', 'Ошибки структуры и валидации')
) AS v(rule_code, match_tier, match_code, nsi_error_code, match_pattern, interpretation, error_category);

-- ------------------------------------------------------------------
-- Справочник, к которому относится отказ, задаётся на класс целиком. Регистр задан
-- явно: (?i) под lc_ctype = C рядом с кириллицей не работает.
-- ------------------------------------------------------------------
UPDATE seed_error_rules
SET nsi_dictionary_pattern = '(?:Справочник OID|Запись справочника) \[([0-9.]+)'
WHERE error_category = 'Ошибки справочника НСИ';

-- Schematron называет справочник атрибутом codeSystem; класс в другой категории, поэтому
-- шаблон задаётся правилу.
UPDATE seed_error_rules
SET nsi_dictionary_pattern = 'codeSystem=''([0-9.]+)'''
WHERE rule_code = 'schematron_allowed_values';

INSERT INTO mart_egisz.dim_error_rules (
    rule_code, rule_kind, error_kind, apply_order, match_tier, match_code, nsi_error_code,
    match_pattern, match_flags, replacement, nsi_dictionary_pattern, interpretation, error_category,
    masks_personal_data
)
SELECT rule_code, rule_kind, error_kind, apply_order, match_tier, match_code, nsi_error_code,
       match_pattern, match_flags, replacement, nsi_dictionary_pattern, interpretation, error_category,
       masks_personal_data
FROM seed_error_rules
ON CONFLICT (rule_code) DO UPDATE SET
    rule_kind = EXCLUDED.rule_kind,
    error_kind = EXCLUDED.error_kind,
    apply_order = EXCLUDED.apply_order,
    match_tier = EXCLUDED.match_tier,
    match_code = EXCLUDED.match_code,
    nsi_error_code = EXCLUDED.nsi_error_code,
    match_pattern = EXCLUDED.match_pattern,
    match_flags = EXCLUDED.match_flags,
    replacement = EXCLUDED.replacement,
    nsi_dictionary_pattern = EXCLUDED.nsi_dictionary_pattern,
    interpretation = EXCLUDED.interpretation,
    error_category = EXCLUDED.error_category,
    masks_personal_data = EXCLUDED.masks_personal_data,
    updated_at = now()
WHERE (mart_egisz.dim_error_rules.rule_kind, mart_egisz.dim_error_rules.error_kind,
       mart_egisz.dim_error_rules.apply_order, mart_egisz.dim_error_rules.match_tier,
       mart_egisz.dim_error_rules.match_code, mart_egisz.dim_error_rules.nsi_error_code,
       mart_egisz.dim_error_rules.match_pattern, mart_egisz.dim_error_rules.match_flags,
       mart_egisz.dim_error_rules.replacement, mart_egisz.dim_error_rules.nsi_dictionary_pattern,
       mart_egisz.dim_error_rules.interpretation, mart_egisz.dim_error_rules.error_category,
       mart_egisz.dim_error_rules.masks_personal_data)
  IS DISTINCT FROM
      (EXCLUDED.rule_kind, EXCLUDED.error_kind, EXCLUDED.apply_order, EXCLUDED.match_tier,
       EXCLUDED.match_code, EXCLUDED.nsi_error_code, EXCLUDED.match_pattern, EXCLUDED.match_flags,
       EXCLUDED.replacement, EXCLUDED.nsi_dictionary_pattern, EXCLUDED.interpretation,
       EXCLUDED.error_category, EXCLUDED.masks_personal_data);

DELETE FROM mart_egisz.dim_error_rules r
WHERE NOT EXISTS (SELECT 1 FROM seed_error_rules s WHERE s.rule_code = r.rule_code);

DROP TABLE seed_error_rules;

-- ============================================================================
-- Категории ошибок: зона ответственности (кто устраняет причину) и признак повтора
-- (лечится ли повторной отправкой) по умолчанию для типов категории. У вида
-- «Ошибка связи» категорий нет, его строка задаёт значения для всех его типов.
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_category (
    error_kind text NOT NULL CHECK (error_kind IN ('Ошибка связи', 'Ошибка асинхронного ответа')),
    error_category text,
    responsibility text NOT NULL CHECK (responsibility IN ('клиника', 'МИС', 'интегратор', 'РЭМД', 'смешанная')),
    is_retryable boolean NOT NULL,
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT uq_dim_error_category UNIQUE NULLS NOT DISTINCT (error_kind, error_category),
    CONSTRAINT chk_dim_error_category_kind CHECK ((error_kind = 'Ошибка связи') = (error_category IS NULL))
);

COMMENT ON TABLE mart_egisz.dim_error_category IS
'Категории ошибок. Строка — категория вида «Ошибка асинхронного ответа» либо вид «Ошибка связи» целиком (категория пуста): зона ответственности и признак повтора, которые наследуют её типы.';

INSERT INTO mart_egisz.dim_error_category (error_kind, error_category, responsibility, is_retryable)
VALUES
    ('Ошибка связи',               NULL,                           'интегратор', true),
    ('Ошибка асинхронного ответа', 'Технические ошибки ЕГИСЗ',     'РЭМД',       true),
    ('Ошибка асинхронного ответа', 'Ошибки получения файла ЭМД',   'МИС',        true),
    ('Ошибка асинхронного ответа', 'Ошибки структуры и валидации', 'МИС',        false),
    ('Ошибка асинхронного ответа', 'Ошибки справочника НСИ',       'клиника',    false),
    ('Ошибка асинхронного ответа', 'Данные пациента',              'клиника',    false),
    ('Ошибка асинхронного ответа', 'Данные медработника',          'клиника',    false),
    ('Ошибка асинхронного ответа', 'Ошибки ЭП и сертификатов',     'клиника',    false),
    ('Ошибка асинхронного ответа', 'Ошибки организации / ИС',      'клиника',    false),
    ('Ошибка асинхронного ответа', 'Ошибки регистрации',           'смешанная',  false),
    ('Ошибка асинхронного ответа', 'Прочие',                       'смешанная',  false)
ON CONFLICT ON CONSTRAINT uq_dim_error_category DO UPDATE SET
    responsibility = EXCLUDED.responsibility,
    is_retryable = EXCLUDED.is_retryable,
    updated_at = now()
WHERE (mart_egisz.dim_error_category.responsibility, mart_egisz.dim_error_category.is_retryable)
      IS DISTINCT FROM (EXCLUDED.responsibility, EXCLUDED.is_retryable);

DELETE FROM mart_egisz.dim_error_category c
WHERE NOT EXISTS (
    SELECT 1 FROM (VALUES
        ('Ошибка связи', NULL::text),
        ('Ошибка асинхронного ответа', 'Технические ошибки ЕГИСЗ'),
        ('Ошибка асинхронного ответа', 'Ошибки получения файла ЭМД'),
        ('Ошибка асинхронного ответа', 'Ошибки структуры и валидации'),
        ('Ошибка асинхронного ответа', 'Ошибки справочника НСИ'),
        ('Ошибка асинхронного ответа', 'Данные пациента'),
        ('Ошибка асинхронного ответа', 'Данные медработника'),
        ('Ошибка асинхронного ответа', 'Ошибки ЭП и сертификатов'),
        ('Ошибка асинхронного ответа', 'Ошибки организации / ИС'),
        ('Ошибка асинхронного ответа', 'Ошибки регистрации'),
        ('Ошибка асинхронного ответа', 'Прочие')
    ) AS v(error_kind, error_category)
    WHERE v.error_kind = c.error_kind AND v.error_category IS NOT DISTINCT FROM c.error_category);

-- ============================================================================
-- Типы ошибок. Строка — одна нормализованная ошибка. Типы правил заводит этот сид;
-- тип без правила (нормализованный текст элемента) заводит разбор журнала
-- при первом появлении, с категорией «Прочие» либо без категории у вида «Ошибка связи».
-- rule_code пуст у типов без правила: они живут, пока на них ссылаются элементы, и
-- снимаются пересчётом ошибок.
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_type (
    error_type text PRIMARY KEY,
    error_kind text NOT NULL,
    error_category text,
    nsi_error_code text REFERENCES mart_egisz.dim_nsi_error_code (nsi_error_code),
    rule_code text REFERENCES mart_egisz.dim_error_rules (rule_code) ON DELETE CASCADE,
    responsibility text NOT NULL,
    is_retryable boolean NOT NULL,
    updated_at timestamptz DEFAULT now(),
    -- Внешний ключ по паре (вид, категория) при пустой категории не проверяется
    -- (MATCH SIMPLE), поэтому пустая категория вида «Ошибка связи» закрыта условием.
    CONSTRAINT fk_dim_error_type_category FOREIGN KEY (error_kind, error_category)
        REFERENCES mart_egisz.dim_error_category (error_kind, error_category),
    CONSTRAINT chk_dim_error_type_category CHECK ((error_kind = 'Ошибка связи') = (error_category IS NULL))
);

COMMENT ON TABLE mart_egisz.dim_error_type IS
'Типы ошибок. Строка — одна нормализованная ошибка: вид, категория (у вида «Ошибка связи» пуста), мнемоника НСИ 305 у типа, привязанного к коду, зона ответственности и признак повтора.';
COMMENT ON COLUMN mart_egisz.dim_error_type.rule_code IS
'Правило классификации, задающее тип. Пусто у типа без правила: его наименование — нормализованный текст элемента.';

-- Тип правила наследует код уточняемого сообщения: при нескольких правилах одного типа
-- приоритет у нижнего яруса.
INSERT INTO mart_egisz.dim_error_type (
    error_type, error_kind, error_category, nsi_error_code, rule_code, responsibility, is_retryable
)
SELECT DISTINCT ON (r.interpretation)
    r.interpretation, r.error_kind, r.error_category, r.nsi_error_code, r.rule_code,
    c.responsibility, c.is_retryable
FROM mart_egisz.dim_error_rules r
JOIN mart_egisz.dim_error_category c
  ON c.error_kind = r.error_kind AND c.error_category = r.error_category
WHERE r.rule_kind = 'классификация'
ORDER BY r.interpretation, r.match_tier, r.rule_code
ON CONFLICT (error_type) DO UPDATE SET
    error_kind = EXCLUDED.error_kind,
    error_category = EXCLUDED.error_category,
    nsi_error_code = EXCLUDED.nsi_error_code,
    rule_code = EXCLUDED.rule_code,
    responsibility = EXCLUDED.responsibility,
    is_retryable = EXCLUDED.is_retryable,
    updated_at = now()
WHERE (mart_egisz.dim_error_type.error_kind, mart_egisz.dim_error_type.error_category,
       mart_egisz.dim_error_type.nsi_error_code, mart_egisz.dim_error_type.rule_code)
      IS DISTINCT FROM
      (EXCLUDED.error_kind, EXCLUDED.error_category, EXCLUDED.nsi_error_code, EXCLUDED.rule_code);

-- Тип правила, чьё наименование в правилах больше не встречается, снимается: элементы,
-- которые на него ссылаются, приводит к текущим правилам пересчёт ошибок.
DELETE FROM mart_egisz.dim_error_type t
WHERE t.rule_code IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM mart_egisz.dim_error_rules r
      WHERE r.rule_kind = 'классификация' AND r.interpretation = t.error_type);

-- Типы без правила наследуют значения категории.
UPDATE mart_egisz.dim_error_type t
SET responsibility = c.responsibility, is_retryable = c.is_retryable, updated_at = now()
FROM mart_egisz.dim_error_category c
WHERE t.rule_code IS NULL
  AND c.error_kind = t.error_kind
  AND c.error_category IS NOT DISTINCT FROM t.error_category
  AND (t.responsibility, t.is_retryable) IS DISTINCT FROM (c.responsibility, c.is_retryable);

-- Точечные исключения из значений категории.
UPDATE mart_egisz.dim_error_type t
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
WHERE t.error_type = v.error_type
  AND (t.responsibility, t.is_retryable) IS DISTINCT FROM (v.responsibility, v.is_retryable);

-- ---------------------------------------------------------------- section: error_functions
-- ============================================================================
-- Функции ошибок: извлечение элементов сообщения журнала и их классификация.
-- ============================================================================

-- Функция общего разбора элементов заменена разбором по источникам; снятие приводит к
-- этому состоянию базу, где она осталась.

-- Разбор ошибок сообщения журнала по источникам. У каждого источника своя схема ответа,
-- поэтому функции не объединяют результаты: общую форму собирает mart_egisz.exchangelog_errors.
--   Ошибка связи — шлюз не доставил сообщение (LOGSTATE = 3): исходный текст шлюза и код
--   из него (код сокета Windows либо код ответа HTTP).
--   Ответ РЭМД — элементы <item> (code, message) в разделах errors и registrationWarnings;
--   предупреждения приходят в успешном ответе вместе с регистрационным номером.
--   Ответ ИЭМК — атрибуты IHE RegistryError: errorCode, codeContext, severity, location.
CREATE OR REPLACE FUNCTION stg_egisz.network_error_code(p_logtext text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT (regexp_match(COALESCE(p_logtext, ''), '(?:Socket error |Error code: )([0-9]+)'))[1];
$$;

-- Раздел элемента — последний открытый перед ним тег errors или registrationWarnings;
-- элемент вне раздела получает пустой раздел.
CREATE OR REPLACE FUNCTION stg_egisz.remd_error_items(p_msgtext text)
RETURNS TABLE (item_no integer, section text, code text, message text)
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    part text;
    part_code text;
    part_text text;
    opened text;
    current_section text;
    is_first boolean := true;
    n integer := 0;
BEGIN
    IF position('<' in COALESCE(p_msgtext, '')) = 0 THEN
        RETURN;
    END IF;
    FOR part IN
        SELECT s FROM regexp_split_to_table(p_msgtext, '<(?:[A-Za-z0-9_]+:)?item(?:\s[^>]*)?>', 'i') AS s
    LOOP
        IF NOT is_first THEN
            part_code := stg_egisz.xml_text(part, 'code');
            part_text := stg_egisz.xml_text(part, 'message');
            IF NULLIF(btrim(COALESCE(part_code, '')), '') IS NOT NULL
               OR NULLIF(btrim(COALESCE(part_text, '')), '') IS NOT NULL THEN
                n := n + 1;
                item_no := n; section := current_section; code := part_code; message := part_text;
                RETURN NEXT;
            END IF;
        END IF;
        is_first := false;
        opened := NULL;
        SELECT t.m[1] INTO opened
        FROM regexp_matches(part, '<(?:[A-Za-z0-9_]+:)?(errors|registrationWarnings)(?:\s[^>]*)?>', 'gi')
            WITH ORDINALITY AS t(m, ord)
        ORDER BY t.ord DESC
        LIMIT 1;
        IF opened IS NOT NULL THEN
            current_section := CASE lower(opened) WHEN 'errors' THEN 'errors' ELSE 'registrationWarnings' END;
        END IF;
    END LOOP;
END;
$$;

-- Значение атрибута в "" не может содержать сырую кавычку, XML-сущности декодируются после
-- захвата (&amp; последним).
CREATE OR REPLACE FUNCTION stg_egisz.xml_attribute(p_tag text, p_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT NULLIF(btrim(
        replace(replace(replace(replace(replace(
            COALESCE((regexp_match(p_tag, '\y' || p_name || '\s*=\s*"([^"]*)"', 'i'))[1], ''),
            '&quot;', '"'), '&apos;', ''''), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
    ), '');
$$;

CREATE OR REPLACE FUNCTION stg_egisz.ihe_error_items(p_msgtext text)
RETURNS TABLE (item_no integer, error_code text, code_context text, severity text, location text)
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    tag text;
    n integer := 0;
BEGIN
    IF strpos(COALESCE(p_msgtext, ''), 'RegistryError') = 0 THEN
        RETURN;
    END IF;
    FOR tag IN
        SELECT m[1] FROM regexp_matches(p_msgtext, '<(?:[A-Za-z0-9_.-]+:)?RegistryError\y([^>]*?)/?>', 'gi') AS m
    LOOP
        error_code := stg_egisz.xml_attribute(tag, 'errorCode');
        code_context := stg_egisz.xml_attribute(tag, 'codeContext');
        IF error_code IS NOT NULL OR code_context IS NOT NULL THEN
            n := n + 1;
            item_no := n;
            severity := stg_egisz.xml_attribute(tag, 'severity');
            location := stg_egisz.xml_attribute(tag, 'location');
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

-- Маскирование текста ошибки для выдачи поддержке: шаги нормализации своего вида с
-- признаком masks_personal_data по apply_order. Заменяются только персональные данные;
-- длина текста, адрес сервиса клиники и реквизиты документа сохраняются.
CREATE OR REPLACE FUNCTION mart_egisz.mask_error_text(
    p_error_kind text,
    p_error_text text
)
RETURNS text
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_step record;
    v_masked text := p_error_text;
BEGIN
    IF v_masked IS NULL THEN
        RETURN NULL;
    END IF;
    FOR v_step IN
        SELECT r.match_pattern, r.replacement, r.match_flags
        FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'нормализация'
          AND r.masks_personal_data
          AND (r.error_kind IS NULL OR r.error_kind = p_error_kind)
        ORDER BY r.apply_order
    LOOP
        v_masked := regexp_replace(v_masked, v_step.match_pattern, v_step.replacement, v_step.match_flags);
    END LOOP;
    RETURN v_masked;
END;
$$;

COMMENT ON FUNCTION mart_egisz.mask_error_text(text, text) IS
'Текст ошибки для выдачи: исходный текст, в котором шаги нормализации mart_egisz.dim_error_rules с признаком masks_personal_data для вида p_error_kind (Ошибка связи либо Ошибка асинхронного ответа) по apply_order заменили персональные данные обозначениями. Остальной текст не меняется.';

-- Тип элемента ошибки. Для асинхронного ответа — наименование первого совпавшего правила
-- классификации: ярусы по возрастанию, внутри яруса меньший rule_code; синоним кода из
-- dim_nsi_error_code_alias разрешается до сравнения. Без правила и для ошибки связи тип —
-- текст, нормализованный шагами нормализации своего вида: значения документа и персональные
-- данные заменены обозначениями. Пустой текст без правила типа не получает: такой элемент
-- виден в контроле качества, а не скрыт подставленным наименованием.
CREATE OR REPLACE FUNCTION stg_egisz.classify_error(
    p_error_kind text,
    p_error_code text,
    p_error_text text
)
RETURNS TABLE (error_type text, nsi_dictionary_oid text)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_code text := upper(btrim(COALESCE(p_error_code, '')));
    v_text text := btrim(COALESCE(p_error_text, ''));
    v_interpretation text;
    v_tier integer;
    v_step record;
BEGIN
    IF p_error_kind = 'Ошибка асинхронного ответа' THEN
        SELECT COALESCE((SELECT a.nsi_error_code FROM mart_egisz.dim_nsi_error_code_alias a
                         WHERE a.alias = v_code), v_code)
        INTO v_code;
        FOR v_tier IN 1..4 LOOP
            SELECT r.interpretation
            INTO v_interpretation
            FROM mart_egisz.dim_error_rules r
            WHERE r.rule_kind = 'классификация'
              AND r.match_tier = v_tier
              AND CASE v_tier
                  WHEN 1 THEN v_code <> '' AND r.match_code = v_code
                              AND v_text <> '' AND v_text ~* r.match_pattern
                  WHEN 2 THEN v_code <> '' AND r.match_code = v_code AND v_text ~* r.match_pattern
                  ELSE v_text <> '' AND v_text ~* r.match_pattern
              END
            ORDER BY r.rule_code
            LIMIT 1;
            IF v_interpretation IS NOT NULL THEN
                error_type := v_interpretation;
                SELECT (regexp_match(COALESCE(p_error_text, ''), r.nsi_dictionary_pattern))[1]
                INTO nsi_dictionary_oid
                FROM mart_egisz.dim_error_rules r
                WHERE r.rule_kind = 'классификация'
                  AND r.interpretation = v_interpretation
                  AND r.nsi_dictionary_pattern IS NOT NULL
                  AND COALESCE(p_error_text, '') ~ r.nsi_dictionary_pattern
                ORDER BY r.match_tier, r.rule_code
                LIMIT 1;
                RETURN NEXT;
                RETURN;
            END IF;
        END LOOP;
    END IF;

    v_text := COALESCE(p_error_text, '');
    FOR v_step IN
        SELECT r.match_pattern, r.replacement, r.match_flags
        FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'нормализация'
          AND (r.error_kind IS NULL OR r.error_kind = p_error_kind)
        ORDER BY r.apply_order
    LOOP
        v_text := regexp_replace(v_text, v_step.match_pattern, v_step.replacement, v_step.match_flags);
    END LOOP;
    error_type := NULLIF(v_text, '');
    nsi_dictionary_oid := NULL;
    RETURN NEXT;
END;
$$;
