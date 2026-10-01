-- ============================================================================
-- 03_transform.sql — raw journal -> exchange_messages -> documents
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: transform
-- ============================================================================
-- 50_transform.sql — transform_raw_to_facts
-- Loaded by db/dwh_init.sql via \i db/03_transform.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- ============================================================================

-- recompute_document_attributes — в 70_views_core.sql

-- Слой версий/логического документа.
-- Пересобирает document_group_id / version / цепочку / is_current_version для групп,
-- затронутых батчем (p_dwh_ids); p_dwh_ids = NULL — полный пересчёт (обслуживание).
--
-- Ключ логического документа = (jid + semd_code + doc_number), где doc_number = PROTOCOLID
-- (номер протокола/ИБ в МИС). Пара (jid, doc_number) несёт ровно ОДИН semd_code — это ключ
-- ДОКУМЕНТА, а localUid меняется при каждой правке/ре-выгрузке ⇒ несколько localUid на
-- (jid, semd_code, doc_number) = версии одного документа. Провенанс в
-- document_group_confidence: 'doc_number' (сгруппировано) | 'singleton'. Защитный c_cap:
-- группы крупнее порога не считаем версиями (страховка от клиник, переиспользующих счётчик
-- протокола) — остаются singleton и видны в health_versions.
CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_versions(p_dwh_ids text[] DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    affected integer := 0;
    c_cap constant integer := 50;  -- макс. версий в группе
BEGIN
    -- Шаг 0: documents.doc_number наполняется из exchange_messages (PROTOCOLID не хранится в
    -- documents при INSERT). Только затронутые dwh_id (или весь архив при p_dwh_ids=NULL).
    UPDATE mart_egisz.documents d
    SET doc_number = src.docnum
    FROM (
        SELECT
            t.dwh_id,
            COALESCE(
                max(NULLIF(btrim(t.doc_number), '')),
                max(NULLIF(btrim(t.xml_doc_number), ''))
            ) AS docnum
        FROM stg_egisz.exchange_messages t
        WHERE t.dwh_id IS NOT NULL
          AND (p_dwh_ids IS NULL OR t.dwh_id = ANY (p_dwh_ids))
        GROUP BY t.dwh_id
    ) src
    WHERE d.dwh_id = src.dwh_id
      AND src.docnum IS NOT NULL
      AND d.doc_number IS DISTINCT FROM src.docnum;

    WITH seed AS (
        SELECT
            d.dwh_id,
            d.jid,
            lower(btrim(d.semd_code)) AS semd_norm,
            lower(btrim(d.doc_number)) AS docnum_norm,
            d.document_group_id
        FROM mart_egisz.documents d
        WHERE p_dwh_ids IS NULL OR d.dwh_id = ANY (p_dwh_ids)
    ),
    -- Пересчёт затрагивает не только переданные экземпляры, но и их соседей по группе:
    -- по новому ключу (jid + код СЭМД + номер документа) и по ранее сохранённой группе,
    -- из которой экземпляр мог уйти. При p_dwh_ids = NULL первая ветка уже даёт весь
    -- архив, поэтому соседние ветки не выполняются.
    member_ids AS (
        SELECT s.dwh_id FROM seed s

        UNION

        SELECT d.dwh_id
        FROM seed s
        JOIN mart_egisz.documents d
          ON d.jid = s.jid
         AND lower(btrim(d.semd_code)) = s.semd_norm
         AND lower(btrim(d.doc_number)) = s.docnum_norm
        WHERE p_dwh_ids IS NOT NULL
          AND s.jid IS NOT NULL
          AND s.semd_norm IS NOT NULL
          AND s.docnum_norm IS NOT NULL

        UNION

        SELECT d.dwh_id
        FROM seed s
        JOIN mart_egisz.documents d ON d.document_group_id = s.document_group_id
        WHERE p_dwh_ids IS NOT NULL
          AND s.document_group_id IS NOT NULL
    ),
    keyed AS (
        SELECT
            d.dwh_id,
            CASE
                WHEN d.jid IS NOT NULL
                     AND NULLIF(btrim(d.semd_code), '') IS NOT NULL
                     AND NULLIF(btrim(d.doc_number), '') IS NOT NULL
                    THEN 'd:' || d.jid || '|' || lower(btrim(d.semd_code)) || '|' || lower(btrim(d.doc_number))
                ELSE 'one:' || d.dwh_id
            END AS grp_key,
            CASE
                WHEN d.jid IS NOT NULL
                     AND NULLIF(btrim(d.semd_code), '') IS NOT NULL
                     AND NULLIF(btrim(d.doc_number), '') IS NOT NULL THEN 'doc_number'
                ELSE 'singleton'
            END AS conf,
            d.status, d.registered_at, d.last_callback_at, d.first_sent_at, d.request_logid
        FROM mart_egisz.documents d
        JOIN member_ids m ON m.dwh_id = d.dwh_id
    ),
    ranked AS (
        SELECT
            k.*,
            count(*) OVER (PARTITION BY k.grp_key) AS grp_size,
            -- Порядок версий: первая отправка = 1.
            row_number() OVER (
                PARTITION BY k.grp_key
                ORDER BY COALESCE(k.first_sent_at, '-infinity'::timestamptz), k.request_logid, k.dwh_id
            ) AS vnum,
            -- Текущая версия: success; при его отсутствии последнее событие.
            row_number() OVER (
                PARTITION BY k.grp_key
                ORDER BY
                    (CASE WHEN k.status = 'success' THEN 1 ELSE 0 END) DESC,
                    COALESCE(k.last_callback_at, k.registered_at, k.first_sent_at, '-infinity'::timestamptz) DESC,
                    k.request_logid DESC, k.dwh_id DESC
            ) AS cur_rank
        FROM keyed k
    ),
    final AS (
        SELECT
            r.*,
            -- Реальная группа: 2..c_cap версий с doc_number-ключом. Крупнее cap — страховка
            -- от переиспользованного счётчика протокола: трактуем как singleton.
            (r.conf = 'doc_number' AND r.grp_size > 1 AND r.grp_size <= c_cap) AS is_real_group,
            LAG(r.dwh_id)  OVER (PARTITION BY r.grp_key ORDER BY r.vnum) AS prev_dwh,
            LEAD(r.dwh_id) OVER (PARTITION BY r.grp_key ORDER BY r.vnum) AS next_dwh
        FROM ranked r
    )
    UPDATE mart_egisz.documents d SET
        document_group_id         = CASE WHEN f.is_real_group THEN f.grp_key ELSE d.dwh_id END,
        document_group_confidence = CASE WHEN f.is_real_group THEN f.conf ELSE 'singleton' END,
        semd_version_number       = CASE WHEN f.is_real_group THEN f.vnum ELSE 1 END,
        supersedes_dwh_id         = CASE WHEN f.is_real_group THEN f.prev_dwh ELSE NULL END,
        superseded_by_dwh_id      = CASE WHEN f.is_real_group THEN f.next_dwh ELSE NULL END,
        is_current_version        = CASE WHEN f.is_real_group THEN (f.cur_rank = 1) ELSE TRUE END
    FROM final f
    WHERE d.dwh_id = f.dwh_id
      AND (
            d.document_group_id         IS DISTINCT FROM (CASE WHEN f.is_real_group THEN f.grp_key ELSE d.dwh_id END)
         OR d.document_group_confidence IS DISTINCT FROM (CASE WHEN f.is_real_group THEN f.conf ELSE 'singleton' END)
         OR d.semd_version_number       IS DISTINCT FROM (CASE WHEN f.is_real_group THEN f.vnum ELSE 1 END)
         OR d.supersedes_dwh_id         IS DISTINCT FROM (CASE WHEN f.is_real_group THEN f.prev_dwh ELSE NULL END)
         OR d.superseded_by_dwh_id      IS DISTINCT FROM (CASE WHEN f.is_real_group THEN f.next_dwh ELSE NULL END)
         OR d.is_current_version        IS DISTINCT FROM (CASE WHEN f.is_real_group THEN (f.cur_rank = 1) ELSE TRUE END)
      );
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

-- Разбор окна журнала (from_logid, to_logid] в разобранные сообщения и документы.
--
-- Правила связки ответа с документом:
--   getDocumentFile — документ РЭМД по localUid из payload;
--   ответ РЭМД — relatesToMessage -> stg_egisz.message_registry.document_uid;
--   ответ ИЭМК — relatesToMessage без document_uid;
--   повторный ответ РЭМД — dwh_id по emdrId.
--
-- Окно строго ограничено (from_logid, to_logid]: связывание не зависит от префикса
-- журнала, поэтому отсечение партиций по createdate работает на каждом батче.
CREATE OR REPLACE FUNCTION mart_egisz.transform_raw_to_facts(
    from_logid bigint,
    to_logid bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    affected integer := 0;
    inserted_rows integer := 0;
    registry_no_document_rows integer := 0;
    unlinked_rows integer := 0;
    skipped_no_clinic integer := 0;
    raw_cd_min timestamptz;
    raw_cd_max timestamptz;
BEGIN
    -- raw_egisz.exchangelog партиционирована по createdate; transform фильтрует по logid.
    -- Узкий диапазон createdate по батчу включает partition pruning.
    SELECT
        MIN(r.createdate) - interval '1 day',
        MAX(r.createdate) + interval '1 day'
    INTO raw_cd_min, raw_cd_max
    FROM raw_egisz.exchangelog r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid;

    raw_cd_min := COALESCE(raw_cd_min, '-infinity'::timestamptz);
    raw_cd_max := COALESCE(raw_cd_max, 'infinity'::timestamptz);

    -- Разложение payload: каждый LOGID парсится один раз, результат — в exchange_messages (xml_*).
    -- Анти-джойн идёт по egisz_exchangelog_parse_attempts, а не по exchange_messages.xml_parsed_at:
    -- строки без реквизитов не проходят фильтр вставки, и маркер только на вставленных
    -- строках заставлял перепарсивать их при каждом повторном проходе окна.
    WITH parse_targets AS (
        SELECT r.logid, r.createdate, r._loaded_at, r.msgid, r.msgtext, r.logtext, r.uri
        FROM raw_egisz.exchangelog r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND NOT EXISTS (
              SELECT 1
              FROM etl_meta.egisz_exchangelog_parse_attempts pa
              WHERE pa.logid = r.logid
          )
    )
    INSERT INTO stg_egisz.exchange_messages (
        logid, log_date,
        msgid, relates_to_msgid,
        xml_dwh_id, xml_local_uid, xml_emdr_id,
        source_action, egisz_subsystem, jid, xml_semd_code, xml_doc_number, xml_org_oid,
        xml_error_code, xml_message, xml_raw_status, xml_document_status,
        xml_creation_date,
        xml_has_fault_marker, xml_mentions_error,
        xml_parsed_at, loaded_at
    )
    SELECT
        t.logid,
        COALESCE(t.createdate, t._loaded_at, now()) AS log_date,
        p.msgid,
        p.relates_to_msgid,
        p.dwh_id,
        p.local_uid,
        p.emdr_id,
        p.action,
        stg_egisz.egisz_subsystem(t.uri, p.action, t.logtext),
        rj.jid,
        p.kind_xml,
        p.doc_number,
        p.org_oid,
        p.error_code,
        p.xml_message,
        p.raw_status,
        p.document_status,
        p.creation_date,
        p.has_fault_marker,
        p.mentions_error,
        now(),
        now()
    FROM parse_targets t
    CROSS JOIN LATERAL stg_egisz.parse_exchangelog_row(t.msgtext, t.msgid, t.logtext) p
    -- jid запроса getDocumentFile фиксируется при парсинге: один resolve на строку за всю
    -- её жизнь вместо повторного разбора payload регулярным выражением на чтении.
    LEFT JOIN LATERAL (
        SELECT res.jid
        FROM mart_egisz.resolve_document_jid(
            p.org_oid,
            COALESCE(t.logtext, '') || ' ' || COALESCE(t.msgtext, '')
        ) res
        WHERE COALESCE(p.action, '') = 'getDocumentFile'
    ) rj ON TRUE
    WHERE (
          p.msgid IS NOT NULL
          OR NULLIF(btrim(t.msgid), '') IS NOT NULL
          OR NULLIF(btrim(p.local_uid), '') IS NOT NULL
          OR NULLIF(btrim(p.emdr_id), '') IS NOT NULL
          OR COALESCE(p.action, '') = 'getDocumentFile'
      )
    ON CONFLICT (logid, log_date) DO UPDATE SET
        msgid = COALESCE(EXCLUDED.msgid, stg_egisz.exchange_messages.msgid),
        relates_to_msgid = COALESCE(EXCLUDED.relates_to_msgid, stg_egisz.exchange_messages.relates_to_msgid),
        xml_dwh_id = COALESCE(EXCLUDED.xml_dwh_id, stg_egisz.exchange_messages.xml_dwh_id),
        xml_local_uid = COALESCE(EXCLUDED.xml_local_uid, stg_egisz.exchange_messages.xml_local_uid),
        xml_emdr_id = COALESCE(EXCLUDED.xml_emdr_id, stg_egisz.exchange_messages.xml_emdr_id),
        source_action = COALESCE(EXCLUDED.source_action, stg_egisz.exchange_messages.source_action),
        egisz_subsystem = COALESCE(EXCLUDED.egisz_subsystem, stg_egisz.exchange_messages.egisz_subsystem),
        jid = COALESCE(stg_egisz.exchange_messages.jid, EXCLUDED.jid),
        xml_semd_code = COALESCE(EXCLUDED.xml_semd_code, stg_egisz.exchange_messages.xml_semd_code),
        xml_doc_number = COALESCE(EXCLUDED.xml_doc_number, stg_egisz.exchange_messages.xml_doc_number),
        xml_org_oid = COALESCE(EXCLUDED.xml_org_oid, stg_egisz.exchange_messages.xml_org_oid),
        xml_error_code = COALESCE(EXCLUDED.xml_error_code, stg_egisz.exchange_messages.xml_error_code),
        xml_message = COALESCE(EXCLUDED.xml_message, stg_egisz.exchange_messages.xml_message),
        xml_raw_status = COALESCE(EXCLUDED.xml_raw_status, stg_egisz.exchange_messages.xml_raw_status),
        xml_document_status = COALESCE(EXCLUDED.xml_document_status, stg_egisz.exchange_messages.xml_document_status),
        xml_creation_date = COALESCE(EXCLUDED.xml_creation_date, stg_egisz.exchange_messages.xml_creation_date),
        xml_has_fault_marker = COALESCE(EXCLUDED.xml_has_fault_marker, stg_egisz.exchange_messages.xml_has_fault_marker),
        xml_mentions_error = COALESCE(EXCLUDED.xml_mentions_error, stg_egisz.exchange_messages.xml_mentions_error),
        xml_parsed_at = COALESCE(EXCLUDED.xml_parsed_at, stg_egisz.exchange_messages.xml_parsed_at),
        loaded_at = now();

    -- Фиксация попытки парсинга по всему просканированному диапазону, независимо от того,
    -- прошла ли строка фильтр вставки. Строго после INSERT выше: его анти-джойн должен
    -- видеть состояние маркера до этого батча.
    INSERT INTO etl_meta.egisz_exchangelog_parse_attempts (logid)
    SELECT r.logid
    FROM raw_egisz.exchangelog r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND r.createdate >= raw_cd_min
      AND r.createdate < raw_cd_max
      AND NOT EXISTS (
          SELECT 1
          FROM etl_meta.egisz_exchangelog_parse_attempts pa
          WHERE pa.logid = r.logid
      )
    ON CONFLICT (logid) DO NOTHING;

    -- ------------------------------------------------------------------
    -- Ветка запроса: getDocumentFile создаёт документ РЭМД при наличии localUid
    -- и записи EGISZ_MESSAGES по тому же document_uid.
    -- ------------------------------------------------------------------
    WITH batch_document_ids AS (
        SELECT DISTINCT tx.xml_dwh_id
        FROM stg_egisz.exchange_messages tx
        WHERE tx.source_action = 'getDocumentFile'
          AND tx.logid > from_logid
          AND tx.logid <= to_logid
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
          AND NULLIF(btrim(tx.xml_emdr_id), '') IS NULL
          AND tx.xml_dwh_id IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM stg_egisz.message_registry m
              WHERE m.document_uid = tx.xml_dwh_id
          )
    ),
    -- Реквизиты документа агрегируются по getDocumentFile текущего батча.
    document_attributes AS (
        SELECT
            tx.xml_dwh_id AS dwh_id,
            (array_agg(tx.xml_local_uid ORDER BY gr.logid)
                FILTER (WHERE NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL))[1] AS local_uid,
            (array_agg(stg_egisz.normalize_semd_code(tx.xml_semd_code) ORDER BY gr.logid)
                FILTER (WHERE stg_egisz.normalize_semd_code(tx.xml_semd_code) IS NOT NULL))[1] AS semd_code,
            (array_agg(tx.xml_org_oid ORDER BY gr.logid)
                FILTER (WHERE NULLIF(btrim(tx.xml_org_oid), '') IS NOT NULL))[1] AS org_oid,
            (array_agg(
                COALESCE(NULLIF(btrim(gr.logtext), ''), '')
                || ' '
                || COALESCE(NULLIF(btrim(gr.msgtext), ''), '')
                ORDER BY gr.logid
            ) FILTER (
                WHERE NULLIF(btrim(COALESCE(gr.logtext, '') || COALESCE(gr.msgtext, '')), '') IS NOT NULL
            ))[1] AS endpoint_text,
            min(COALESCE(gr.createdate, gr.logdate)) AS sent_at,
            (array_agg(gr.logid ORDER BY COALESCE(gr.createdate, gr.logdate), gr.logid))[1] AS request_logid,
            (array_agg(tx.msgid ORDER BY COALESCE(gr.createdate, gr.logdate), gr.logid)
                FILTER (WHERE tx.msgid IS NOT NULL))[1] AS sent_msgid
        FROM stg_egisz.exchange_messages tx
        JOIN batch_document_ids bd ON bd.xml_dwh_id = tx.xml_dwh_id
        JOIN raw_egisz.exchangelog gr ON gr.logid = tx.logid
            AND gr.createdate >= raw_cd_min
            AND gr.createdate < raw_cd_max
        WHERE COALESCE(tx.source_action, '') = 'getDocumentFile'
          AND gr.logid > from_logid
          AND gr.logid <= to_logid
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
          AND NULLIF(btrim(tx.xml_emdr_id), '') IS NULL
        GROUP BY tx.xml_dwh_id
    ),
    document_resolved AS (
        SELECT
            a.*,
            r.jid AS resolved_jid,
            r.resolve_method
        FROM document_attributes a
        -- reply_to реестра подач содержит endpoint клиники.
        JOIN LATERAL (
            SELECT m.reply_to
            FROM stg_egisz.message_registry m
            WHERE m.document_uid = a.dwh_id
            ORDER BY m.egmid DESC
            LIMIT 1
        ) reg ON TRUE
        LEFT JOIN LATERAL mart_egisz.resolve_document_jid(
            a.org_oid,
            COALESCE(a.endpoint_text, '') || ' ' || COALESCE(reg.reply_to, '')
        ) r ON TRUE
    )
    -- Запрос файла — шаг регистрации, а не её исход: документ получает нефинальный статус.
    -- Сбой доставки запроса статус не меняет; его элемент хранится в разобранном сообщении.
    INSERT INTO mart_egisz.documents (
        dwh_id, local_uid, semd_code,
        status, first_sent_at, request_logid, msgid,
        jid, org_oid, jid_resolve_method,
        updated_at
    )
    SELECT
        a.dwh_id,
        a.local_uid,
        a.semd_code,
        mart_egisz.document_status_nonfinal(),
        a.sent_at,
        a.request_logid,
        a.sent_msgid,
        a.resolved_jid,
        a.org_oid,
        a.resolve_method,
        now()
    FROM document_resolved a
    WHERE a.dwh_id IS NOT NULL
      AND a.local_uid IS NOT NULL
      -- Код СЭМД не требуется: он дозагружается ниже из соседних сообщений документа.
      -- Клиника обязательна — без неё экземпляр не отображается ни в одном срезе.
      AND a.resolved_jid IS NOT NULL
    ON CONFLICT (dwh_id) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, mart_egisz.documents.local_uid),
        semd_code = COALESCE(EXCLUDED.semd_code, mart_egisz.documents.semd_code),
        first_sent_at = LEAST(
            COALESCE(mart_egisz.documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, mart_egisz.documents.first_sent_at)
        ),
        status = CASE
            WHEN mart_egisz.documents.status IN (SELECT mart_egisz.document_status_final())
            THEN mart_egisz.documents.status
            ELSE EXCLUDED.status
        END,
        jid = COALESCE(EXCLUDED.jid, mart_egisz.documents.jid),
        org_oid = COALESCE(EXCLUDED.org_oid, mart_egisz.documents.org_oid),
        jid_resolve_method = CASE
            WHEN mart_egisz.documents.jid_resolve_method = 'mo_uid'
            THEN mart_egisz.documents.jid_resolve_method
            ELSE COALESCE(EXCLUDED.jid_resolve_method, mart_egisz.documents.jid_resolve_method)
        END,
        msgid = CASE
            WHEN mart_egisz.documents.status IN (SELECT mart_egisz.document_status_final())
            THEN mart_egisz.documents.msgid
            ELSE COALESCE(EXCLUDED.msgid, mart_egisz.documents.msgid)
        END,
        request_logid = CASE
            WHEN mart_egisz.documents.first_sent_at IS NULL THEN EXCLUDED.request_logid
            WHEN EXCLUDED.first_sent_at IS NULL THEN mart_egisz.documents.request_logid
            WHEN EXCLUDED.first_sent_at < mart_egisz.documents.first_sent_at THEN EXCLUDED.request_logid
            WHEN EXCLUDED.first_sent_at = mart_egisz.documents.first_sent_at THEN LEAST(
                COALESCE(mart_egisz.documents.request_logid, EXCLUDED.request_logid),
                COALESCE(EXCLUDED.request_logid, mart_egisz.documents.request_logid)
            )
            ELSE mart_egisz.documents.request_logid
        END,
        updated_at = now();

    -- Отправки, по которым клиника не разрешилась ни payload'ом, ни реестром: в documents
    -- они не попадают (нечем атрибутировать), но их число возвращается вызывающему.
    SELECT count(*) INTO skipped_no_clinic
    FROM stg_egisz.exchange_messages tx
    WHERE tx.source_action = 'getDocumentFile'
      AND tx.logid > from_logid
      AND tx.logid <= to_logid
      AND tx.xml_dwh_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM mart_egisz.documents d WHERE d.dwh_id = tx.xml_dwh_id);

    -- ------------------------------------------------------------------
    -- Ветка ответа: исход асинхронного ответа, элементы ошибки и привязка к документу.
    -- ------------------------------------------------------------------
    -- Пакет разбирается один раз: связанные сообщения обновляются вставкой ниже,
    -- несвязанные получают исход и элементы ошибки отдельным обновлением, которое не
    -- трогает реквизиты связывания.
    DROP TABLE IF EXISTS pg_temp.batch_responses;
    CREATE TEMP TABLE batch_responses AS
    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM raw_egisz.exchangelog r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
    ),
    raw_parsed AS (
        SELECT
            r.logid,
            r.logdate,
            r.createdate,
            r.logstate,
            r.logtext,
            r.msgtext,
            tx.source_action,
            tx.msgid AS msgid,
            tx.relates_to_msgid,
            tx.xml_local_uid AS local_uid_xml,
            tx.xml_dwh_id AS dwh_id_xml,
            tx.xml_semd_code AS kind_xml,
            tx.xml_emdr_id AS emdr_id,
            tx.xml_doc_number AS doc_number,
            tx.xml_org_oid AS org_oid,
            tx.xml_error_code AS error_code,
            tx.xml_message,
            tx.xml_raw_status AS raw_status,
            tx.xml_creation_date AS creation_date,
            tx.xml_document_status AS document_status,
            tx.xml_has_fault_marker AS has_fault_marker,
            tx.xml_mentions_error AS mentions_error,
            -- Статус асинхронного ответа ИЭМК передаётся атрибутом RegistryResponse.
            substring(r.msgtext from 'ResponseStatusType:([A-Za-z]+)') AS registry_response_status
        FROM raw_egisz.exchangelog r
        JOIN candidate_log_ids c ON c.logid = r.logid
        JOIN stg_egisz.exchange_messages tx ON tx.logid = r.logid
        WHERE r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND tx.xml_parsed_at IS NOT NULL
          -- getDocumentFile — это отправка, её обрабатывает ветка выше; сюда она попадает
          -- только сбоем доставки (LOGSTATE=3), чтобы сохранить элемент ошибки связи.
          AND (
              COALESCE(tx.source_action, '') <> 'getDocumentFile'
              OR r.logstate = 3
          )
          AND (
              r.logstate = 3
              OR stg_egisz.normalize_message_id(r.msgid) IS NOT NULL
              OR tx.msgid IS NOT NULL
              OR tx.relates_to_msgid IS NOT NULL
              OR NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_emdr_id), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_doc_number), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_semd_code), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_raw_status), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_error_code), '') IS NOT NULL
              OR NULLIF(btrim(tx.xml_message), '') IS NOT NULL
          )
    ),
    parsed AS (
        SELECT
            r.logid,
            r.createdate AS logdate,
            r.logstate,
            r.logtext,
            r.msgtext,
            r.source_action,
            r.msgid,
            r.relates_to_msgid,
            -- Запрос файла уже зарегистрированного ЭМД (getDocumentFile с emdrId) — не шаг
            -- регистрации: с документом его сбой доставки не связывается.
            CASE
                WHEN r.source_action = 'getDocumentFile' AND NULLIF(btrim(r.emdr_id), '') IS NOT NULL THEN NULL
                ELSE COALESCE(r.dwh_id_xml, msg_ref.dwh_id, emdr_ref.dwh_id)
            END AS dwh_id,
            CASE
                WHEN r.dwh_id_xml IS NOT NULL THEN 'payload_local_uid'
                WHEN msg_ref.dwh_id IS NOT NULL THEN 'message_registry'
                WHEN msg_ref.has_registry THEN 'message_registry_no_document'
                WHEN emdr_ref.dwh_id IS NOT NULL THEN 'emdr_id'
                ELSE 'unlinked'
            END AS link_method,
            COALESCE(r.local_uid_xml, msg_ref.local_uid) AS local_uid_semd,
            msg_ref.reply_to AS registry_reply_to,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            stg_egisz.normalize_semd_code(r.kind_xml) AS semd_code,
            r.error_code,
            r.xml_message,
            r.raw_status,
            r.creation_date,
            r.document_status,
            r.has_fault_marker,
            r.mentions_error,
            r.registry_response_status,
            src_doc.semd_code AS source_document_semd_code
        FROM raw_parsed r
        -- Ответ РЭМД: relatesToMessage -> document_uid реестра подач.
        LEFT JOIN LATERAL (
            SELECT
                stg_egisz.dwh_id(m.document_uid) AS dwh_id,
                m.document_uid AS local_uid,
                m.reply_to,
                true AS has_registry
            FROM stg_egisz.message_registry m
            WHERE r.relates_to_msgid IS NOT NULL
              AND m.msgid = stg_egisz.message_registry_key(r.relates_to_msgid)
            ORDER BY (m.document_uid IS NOT NULL) DESC, m.egmid DESC NULLS LAST
            LIMIT 1
        ) msg_ref ON TRUE
        -- Повторный ответ РЭМД: dwh_id по emdrId.
        LEFT JOIN LATERAL (
            SELECT fd.dwh_id
            FROM mart_egisz.documents fd
            WHERE r.emdr_id IS NOT NULL
              AND lower(NULLIF(btrim(fd.emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
            ORDER BY fd.last_callback_at DESC NULLS LAST, fd.request_logid DESC NULLS LAST
            LIMIT 1
        ) emdr_ref ON TRUE
        LEFT JOIN mart_egisz.documents src_doc
          ON src_doc.dwh_id = COALESCE(r.dwh_id_xml, msg_ref.dwh_id, emdr_ref.dwh_id)
    ),
    enriched AS (
        SELECT
            p.*,
            res.jid AS resolved_jid,
            res.resolve_method AS resolved_method,
            COALESCE(
                p.semd_code,
                p.source_document_semd_code
            ) AS resolved_semd_code,
            stg_egisz.classify_async_status(
                p.source_action,
                p.raw_status,
                p.document_status,
                p.has_fault_marker,
                p.mentions_error,
                p.registry_response_status
            ) AS outcome,
            CASE WHEN p.logstate = 3 THEN p.logtext ELSE p.xml_message END AS message_text
        FROM parsed p
        LEFT JOIN LATERAL mart_egisz.resolve_document_jid(
            p.org_oid,
            COALESCE(p.logtext, '') || ' ' || COALESCE(p.msgtext, '') || ' ' || COALESCE(p.registry_reply_to, '')
        ) res ON TRUE
    )
    SELECT e.*
    FROM enriched e;

    INSERT INTO stg_egisz.exchange_messages (
        logid, dwh_id, log_date, msgid, relates_to_msgid, local_uid_semd, emdr_id,
        doc_number, org_oid, status, message, jid, jid_resolve_method, semd_code,
        creation_date, loaded_at, link_method
    )
    SELECT
        e.logid, e.dwh_id, e.logdate, e.msgid, e.relates_to_msgid, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.outcome, e.message_text,
        e.resolved_jid, e.resolved_method, e.resolved_semd_code,
        e.creation_date, now(), e.link_method
    FROM pg_temp.batch_responses e
    WHERE (e.outcome IS NOT NULL OR e.logstate = 3)
      AND e.dwh_id IS NOT NULL
    ON CONFLICT (logid, log_date) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        dwh_id = EXCLUDED.dwh_id,
        msgid = EXCLUDED.msgid,
        relates_to_msgid = EXCLUDED.relates_to_msgid,
        local_uid_semd = EXCLUDED.local_uid_semd,
        emdr_id = EXCLUDED.emdr_id,
        doc_number = EXCLUDED.doc_number,
        org_oid = EXCLUDED.org_oid,
        status = EXCLUDED.status,
        message = EXCLUDED.message,
        jid = EXCLUDED.jid,
        jid_resolve_method = EXCLUDED.jid_resolve_method,
        semd_code = EXCLUDED.semd_code,
        creation_date = EXCLUDED.creation_date,
        loaded_at = now(),
        link_method = EXCLUDED.link_method;
    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    affected := affected + inserted_rows;

    -- Сообщение без связи с документом тоже хранит исход: сбой доставки и отказ видны в
    -- разрезе периода независимо от того, найден ли документ.
    UPDATE stg_egisz.exchange_messages tx
    SET status = e.outcome,
        message = e.message_text,
        loaded_at = now()
    FROM pg_temp.batch_responses e
    WHERE tx.logid = e.logid
      AND tx.log_date = e.logdate
      AND e.dwh_id IS NULL
      AND (e.outcome IS NOT NULL OR e.logstate = 3)
      AND (tx.status, tx.message) IS DISTINCT FROM (e.outcome, e.message_text);
    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    affected := affected + inserted_rows;

    DROP TABLE pg_temp.batch_responses;

    -- Ошибки разбираются после записи исхода: элементы ответа ищутся только в ответе с
    -- распознанным исходом.
    PERFORM stg_egisz.parse_exchangelog_errors(from_logid, to_logid);

    -- РЭМД-ответ с MSGID в EGISZ_MESSAGES и пустым DOCUMENTID.
    WITH registry_match AS (
        SELECT
            tx.logid,
            tx.log_date,
            reg.reply_to
        FROM stg_egisz.exchange_messages tx
        JOIN LATERAL (
            SELECT
                m.document_uid,
                m.reply_to
            FROM stg_egisz.message_registry m
            WHERE m.msgid = stg_egisz.message_registry_key(tx.relates_to_msgid)
            ORDER BY (m.document_uid IS NOT NULL) DESC, m.egmid DESC NULLS LAST
            LIMIT 1
        ) reg ON TRUE
        WHERE tx.logid > from_logid
          AND tx.logid <= to_logid
          AND tx.log_date >= raw_cd_min
          AND tx.log_date < raw_cd_max
          AND tx.dwh_id IS NULL
          AND tx.relates_to_msgid IS NOT NULL
          AND tx.egisz_subsystem IS DISTINCT FROM 'ИЭМК'
          AND tx.link_method IS DISTINCT FROM 'message_registry_no_document'
          AND reg.document_uid IS NULL
    ),
    registry_no_document AS (
        SELECT
            rm.logid,
            rm.log_date,
            r.jid,
            r.resolve_method
        FROM registry_match rm
        LEFT JOIN LATERAL mart_egisz.resolve_document_jid(NULL::text, COALESCE(rm.reply_to, '')) r ON TRUE
    )
    UPDATE stg_egisz.exchange_messages tx
    SET
        link_method = 'message_registry_no_document',
        jid = reg.jid,
        jid_resolve_method = reg.resolve_method,
        loaded_at = now()
    FROM registry_no_document reg
    WHERE tx.logid = reg.logid
      AND tx.log_date = reg.log_date;
    GET DIAGNOSTICS registry_no_document_rows = ROW_COUNT;
    affected := affected + registry_no_document_rows;

    -- Ответы без связи по payload, EGISZ_MESSAGES и emdrId.
    UPDATE stg_egisz.exchange_messages tx
    SET link_method = 'unlinked'
    WHERE tx.logid > from_logid
      AND tx.logid <= to_logid
      AND tx.log_date >= raw_cd_min
      AND tx.log_date < raw_cd_max
      AND tx.dwh_id IS NULL
      AND tx.relates_to_msgid IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM stg_egisz.message_registry m
          WHERE m.msgid = stg_egisz.message_registry_key(tx.relates_to_msgid)
      )
      AND tx.link_method IS DISTINCT FROM 'unlinked';
    GET DIAGNOSTICS unlinked_rows = ROW_COUNT;

    -- ------------------------------------------------------------------
    -- Перенос исхода на грейн документа: статус выставляет только асинхронный ответ.
    -- Сбой доставки статус не меняет — его элемент хранится в разобранном сообщении.
    -- ------------------------------------------------------------------
    INSERT INTO mart_egisz.documents (
        dwh_id, local_uid, emdr_id, semd_code,
        status, msgid, relates_to_msgid,
        result_logid, document_created_at, registered_at,
        first_callback_at, last_callback_at, last_status, jid, org_oid, jid_resolve_method,
        updated_at
    )
    SELECT DISTINCT ON (f.dwh_id)
        f.dwh_id,
        stg_egisz.clean_text_value(f.local_uid_semd),
        stg_egisz.clean_text_value(f.emdr_id),
        stg_egisz.normalize_semd_code(f.semd_code),
        CASE f.status WHEN 'success' THEN 'success' ELSE 'async_error' END,
        stg_egisz.clean_text_value(f.msgid),
        stg_egisz.clean_text_value(f.relates_to_msgid),
        f.logid,
        f.creation_date,
        CASE WHEN f.status = 'success' THEN f.log_date ELSE NULL::timestamptz END,
        -- DISTINCT ON оставляет последний ответ документа, поэтому первый берётся окном:
        -- оконные функции считаются до отбора строки.
        MIN(f.log_date) OVER (PARTITION BY f.dwh_id),
        f.log_date,
        f.status,
        f.jid,
        f.org_oid,
        f.jid_resolve_method,
        now()
    FROM stg_egisz.exchange_messages f
    WHERE f.logid > from_logid
      AND f.logid <= to_logid
      AND f.dwh_id IS NOT NULL
      AND f.status IN ('success', 'error')
    ORDER BY f.dwh_id, f.log_date DESC NULLS LAST, f.logid DESC
    ON CONFLICT (dwh_id) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, mart_egisz.documents.local_uid),
        emdr_id = COALESCE(EXCLUDED.emdr_id, mart_egisz.documents.emdr_id),
        semd_code = COALESCE(EXCLUDED.semd_code, mart_egisz.documents.semd_code),
        status = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(mart_egisz.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.status
            ELSE mart_egisz.documents.status
        END,
        msgid = COALESCE(EXCLUDED.msgid, mart_egisz.documents.msgid),
        relates_to_msgid = COALESCE(EXCLUDED.relates_to_msgid, mart_egisz.documents.relates_to_msgid),
        result_logid = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(mart_egisz.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.result_logid
            ELSE mart_egisz.documents.result_logid
        END,
        document_created_at = COALESCE(EXCLUDED.document_created_at, mart_egisz.documents.document_created_at),
        registered_at = COALESCE(EXCLUDED.registered_at, mart_egisz.documents.registered_at),
        first_callback_at = LEAST(
            COALESCE(mart_egisz.documents.first_callback_at, EXCLUDED.first_callback_at),
            COALESCE(EXCLUDED.first_callback_at, mart_egisz.documents.first_callback_at)
        ),
        last_callback_at = GREATEST(COALESCE(mart_egisz.documents.last_callback_at, '-infinity'::timestamptz), COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)),
        last_status = COALESCE(EXCLUDED.last_status, mart_egisz.documents.last_status),
        jid = COALESCE(mart_egisz.documents.jid, EXCLUDED.jid),
        org_oid = COALESCE(EXCLUDED.org_oid, mart_egisz.documents.org_oid),
        jid_resolve_method = CASE
            WHEN mart_egisz.documents.jid_resolve_method = 'mo_uid'
            THEN mart_egisz.documents.jid_resolve_method
            ELSE COALESCE(EXCLUDED.jid_resolve_method, mart_egisz.documents.jid_resolve_method)
        END,
        updated_at = now();

    -- Ответ может прийти без KIND, а тип СЭМД уже известен из отправки.
    -- Только документы, затронутые в этой транзакции: O(батч), не O(архив).
    WITH batch_docs AS (
        SELECT d.dwh_id
        FROM mart_egisz.documents d
        WHERE d.updated_at = transaction_timestamp()
          AND NULLIF(btrim(d.semd_code), '') IS NULL
    )
    UPDATE mart_egisz.documents d
    SET
        semd_code = src.semd_code,
        updated_at = now()
    FROM (
        SELECT DISTINCT ON (t.dwh_id)
            t.dwh_id,
            stg_egisz.normalize_semd_code(t.semd_code) AS semd_code
        FROM stg_egisz.exchange_messages t
        INNER JOIN batch_docs b ON b.dwh_id = t.dwh_id
        WHERE NULLIF(btrim(t.semd_code), '') IS NOT NULL
        ORDER BY t.dwh_id, t.log_date DESC NULLS LAST, t.logid DESC
    ) src
    WHERE d.dwh_id = src.dwh_id;

    -- Число подач документа в ЕГИСЗ по реестру: повторная подача не меняет localUid,
    -- поэтому счётчик показывает, сколько раз документ отправлялся до текущего исхода.
    UPDATE mart_egisz.documents d
    SET attempt_count = src.attempts,
        updated_at = now()
    FROM (
        SELECT m.document_uid AS dwh_id, count(*)::integer AS attempts
        FROM stg_egisz.message_registry m
        WHERE EXISTS (
            SELECT 1 FROM mart_egisz.documents b
            WHERE b.dwh_id = m.document_uid
              AND b.updated_at = transaction_timestamp()
        )
        GROUP BY m.document_uid
    ) src
    WHERE d.dwh_id = src.dwh_id
      AND d.attempt_count IS DISTINCT FROM src.attempts;

    -- Инкрементальное сопровождение document_attributes по dwh_id из батча.
    PERFORM mart_egisz.recompute_document_attributes(
        ARRAY(
            SELECT d.dwh_id::text
            FROM mart_egisz.documents d
            WHERE d.updated_at = transaction_timestamp()
        )
    );

    -- Пересбор слоя версий для групп, затронутых батчем.
    PERFORM mart_egisz.recompute_document_versions(
        ARRAY(
            SELECT d.dwh_id::text
            FROM mart_egisz.documents d
            WHERE d.updated_at = transaction_timestamp()
        )
    );

    RETURN jsonb_build_object(
        'transformed', affected,
        'unlinked', unlinked_rows,
        'sends_without_clinic', skipped_no_clinic
    );
END;
$$;

-- Разбор ошибок разобранных строк журнала обмена диапазона LOGID по источникам: ошибка связи,
-- элементы ответа РЭМД, элементы ответа ИЭМК. Элементы ответа ищутся только в строке с
-- распознанным исходом. Ответ об ошибке без элементов сохраняет код и текст ответа
-- элементом источника своего контура. Одинаковые элементы классифицируются один раз; тип
-- без правила заводится в справочнике типов при первом появлении: категория «Прочие» у
-- асинхронного ответа, без категории у ошибки связи. Вызывается приёмом для пакета и
-- повторным разбором журнала по участкам LOGID.
CREATE OR REPLACE FUNCTION stg_egisz.parse_exchangelog_errors(p_from_logid bigint, p_to_logid bigint)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    updated integer := 0;
    cd_min timestamptz;
    cd_max timestamptz;
BEGIN
    SELECT MIN(r.createdate), MAX(r.createdate)
    INTO cd_min, cd_max
    FROM raw_egisz.exchangelog r
    WHERE r.logid > p_from_logid
      AND r.logid <= p_to_logid;
    IF cd_min IS NULL THEN
        RETURN 0;
    END IF;

    -- Сообщения диапазона, у которых ошибки есть или были: прежние значения снимаются,
    -- если источник их больше не даёт. Payload в набор не копируется — он большой.
    DROP TABLE IF EXISTS pg_temp.exchangelog_error_scope;
    CREATE TEMP TABLE exchangelog_error_scope AS
    SELECT tx.logid, tx.log_date
    FROM stg_egisz.exchange_messages tx
    JOIN raw_egisz.exchangelog r
      ON r.logid = tx.logid
     AND r.createdate = tx.log_date
     AND r.createdate >= cd_min
     AND r.createdate <= cd_max
    WHERE tx.logid > p_from_logid
      AND tx.logid <= p_to_logid
      AND tx.log_date >= cd_min
      AND tx.log_date <= cd_max
      AND (r.logstate = 3
           OR tx.status IS NOT NULL
           OR tx.network_error_text IS NOT NULL
           OR tx.remd_errors IS NOT NULL
           OR tx.ihe_errors IS NOT NULL);
    ANALYZE pg_temp.exchangelog_error_scope;

    DROP TABLE IF EXISTS pg_temp.exchangelog_error_items;
    CREATE TEMP TABLE exchangelog_error_items AS
    SELECT s.logid, s.log_date, i.*
    FROM pg_temp.exchangelog_error_scope s
    JOIN stg_egisz.exchange_messages tx ON tx.logid = s.logid AND tx.log_date = s.log_date
    JOIN raw_egisz.exchangelog r
      ON r.logid = s.logid
     AND r.createdate = s.log_date
     AND r.createdate >= cd_min
     AND r.createdate <= cd_max
    CROSS JOIN LATERAL (
        WITH remd AS (
            SELECT * FROM stg_egisz.remd_error_items(r.msgtext) WHERE tx.status IS NOT NULL
        ),
        ihe AS (
            SELECT * FROM stg_egisz.ihe_error_items(r.msgtext) WHERE tx.status IS NOT NULL
        )
        SELECT 'network'::text AS source, 0 AS item_no, 'Ошибка связи'::text AS error_kind,
               stg_egisz.network_error_code(r.logtext) AS error_code, r.logtext AS error_text,
               NULL::text AS section, NULL::text AS severity, NULL::text AS location
        WHERE r.logstate = 3
        UNION ALL
        SELECT 'remd', m.item_no, 'Ошибка асинхронного ответа', m.code, m.message, m.section, NULL, NULL
        FROM remd m
        UNION ALL
        SELECT 'ihe', h.item_no, 'Ошибка асинхронного ответа', h.error_code, h.code_context, NULL, h.severity, h.location
        FROM ihe h
        UNION ALL
        SELECT CASE WHEN tx.egisz_subsystem = 'ИЭМК' THEN 'ihe' ELSE 'remd' END, 1,
               'Ошибка асинхронного ответа', tx.xml_error_code, tx.xml_message, NULL, NULL, NULL
        WHERE tx.status = 'error'
          AND COALESCE(r.msgtext, '') <> ''
          AND (NULLIF(btrim(COALESCE(tx.xml_error_code, '')), '') IS NOT NULL
               OR NULLIF(btrim(COALESCE(tx.xml_message, '')), '') IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM remd)
          AND NOT EXISTS (SELECT 1 FROM ihe)
    ) i;
    ANALYZE pg_temp.exchangelog_error_items;

    DROP TABLE IF EXISTS pg_temp.exchangelog_error_types;
    CREATE TEMP TABLE exchangelog_error_types AS
    SELECT k.error_kind, k.error_code, k.error_text, c.error_type, c.nsi_dictionary_oid
    FROM (SELECT DISTINCT error_kind, error_code, error_text FROM pg_temp.exchangelog_error_items) k
    CROSS JOIN LATERAL stg_egisz.classify_error(k.error_kind, k.error_code, k.error_text) c;
    ANALYZE pg_temp.exchangelog_error_types;

    INSERT INTO mart_egisz.dim_error_type (error_type, error_kind, error_category, responsibility, is_retryable)
    SELECT DISTINCT ON (t.error_type)
        t.error_type, t.error_kind, c.error_category, c.responsibility, c.is_retryable
    FROM pg_temp.exchangelog_error_types t
    JOIN mart_egisz.dim_error_category c
      ON c.error_kind = t.error_kind
     AND c.error_category IS NOT DISTINCT FROM
         CASE WHEN t.error_kind = 'Ошибка связи' THEN NULL ELSE 'Прочие' END
    WHERE t.error_type IS NOT NULL
    ORDER BY t.error_type
    ON CONFLICT (error_type) DO NOTHING;

    -- Значения собираются во временную таблицу со статистикой: соединение наборов без
    -- статистики планировщик сводил к перебору пар. Сравнение массивов считает NULL
    -- равными и соединяется хешем.
    DROP TABLE IF EXISTS pg_temp.exchangelog_error_parsed;
    CREATE TEMP TABLE exchangelog_error_parsed AS
    SELECT
        s.logid,
        s.log_date,
        max(i.error_code) FILTER (WHERE i.source = 'network') AS network_error_code,
        max(i.error_text) FILTER (WHERE i.source = 'network') AS network_error_text,
        max(t.error_type) FILTER (WHERE i.source = 'network') AS network_error_type,
        jsonb_agg(jsonb_build_object(
            'item_no', i.item_no, 'section', i.section, 'code', i.error_code, 'message', i.error_text,
            'error_type', t.error_type, 'nsi_dictionary_oid', t.nsi_dictionary_oid
        ) ORDER BY i.item_no) FILTER (WHERE i.source = 'remd') AS remd_errors,
        jsonb_agg(jsonb_build_object(
            'item_no', i.item_no, 'error_code', i.error_code, 'code_context', i.error_text,
            'severity', i.severity, 'location', i.location,
            'error_type', t.error_type, 'nsi_dictionary_oid', t.nsi_dictionary_oid
        ) ORDER BY i.item_no) FILTER (WHERE i.source = 'ihe') AS ihe_errors
    FROM pg_temp.exchangelog_error_scope s
    LEFT JOIN pg_temp.exchangelog_error_items i ON i.logid = s.logid AND i.log_date = s.log_date
    LEFT JOIN pg_temp.exchangelog_error_types t
      ON ARRAY[t.error_kind, t.error_code, t.error_text] = ARRAY[i.error_kind, i.error_code, i.error_text]
    GROUP BY s.logid, s.log_date;
    ANALYZE pg_temp.exchangelog_error_parsed;

    UPDATE stg_egisz.exchange_messages tx
    SET network_error_code = p.network_error_code,
        network_error_text = p.network_error_text,
        network_error_type = p.network_error_type,
        remd_errors = p.remd_errors,
        ihe_errors = p.ihe_errors
    FROM pg_temp.exchangelog_error_parsed p
    WHERE tx.logid = p.logid
      AND tx.log_date = p.log_date
      AND tx.log_date >= cd_min
      AND tx.log_date <= cd_max
      AND (tx.network_error_code, tx.network_error_text, tx.network_error_type, tx.remd_errors, tx.ihe_errors)
          IS DISTINCT FROM (p.network_error_code, p.network_error_text, p.network_error_type, p.remd_errors, p.ihe_errors);
    GET DIAGNOSTICS updated = ROW_COUNT;

    DROP TABLE pg_temp.exchangelog_error_parsed;
    DROP TABLE pg_temp.exchangelog_error_types;
    DROP TABLE pg_temp.exchangelog_error_items;
    DROP TABLE pg_temp.exchangelog_error_scope;
    RETURN updated;
END;
$$;


-- Приведение ошибок к текущим правилам: после изменения правил или шагов маскирования
-- типы в разобранных сообщениях пересчитываются по уникальным элементам (вид, код,
-- исходный текст) каждого источника. Тип без правила заводится в справочнике, тип без
-- правила, на который больше не ссылается ни один элемент, снимается. Запускается вручную
-- задачей DAG обслуживания; приём на это время ставится на паузу.
CREATE OR REPLACE FUNCTION stg_egisz.reclassify_errors()
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    updated integer := 0;
    step_rows integer := 0;
BEGIN
    DROP TABLE IF EXISTS pg_temp.reclassified;
    CREATE TEMP TABLE reclassified AS
    SELECT k.error_kind, k.error_code, k.error_text, c.error_type, c.nsi_dictionary_oid
    FROM (
        SELECT 'Ошибка связи'::text AS error_kind, t.network_error_code AS error_code, t.network_error_text AS error_text
        FROM stg_egisz.exchange_messages t
        WHERE t.network_error_text IS NOT NULL
        UNION
        SELECT 'Ошибка асинхронного ответа', e.code, e.message
        FROM stg_egisz.exchange_messages t
        CROSS JOIN LATERAL jsonb_to_recordset(t.remd_errors) AS e(code text, message text)
        WHERE t.remd_errors IS NOT NULL
        UNION
        SELECT 'Ошибка асинхронного ответа', e.error_code, e.code_context
        FROM stg_egisz.exchange_messages t
        CROSS JOIN LATERAL jsonb_to_recordset(t.ihe_errors) AS e(error_code text, code_context text)
        WHERE t.ihe_errors IS NOT NULL
    ) k
    CROSS JOIN LATERAL stg_egisz.classify_error(k.error_kind, k.error_code, k.error_text) c;
    -- Временные таблицы автоанализ не обрабатывает; без статистики план соединений слеп.
    ANALYZE pg_temp.reclassified;

    INSERT INTO mart_egisz.dim_error_type (error_type, error_kind, error_category, responsibility, is_retryable)
    SELECT DISTINCT ON (r.error_type)
        r.error_type, r.error_kind, c.error_category, c.responsibility, c.is_retryable
    FROM pg_temp.reclassified r
    JOIN mart_egisz.dim_error_category c
      ON c.error_kind = r.error_kind
     AND c.error_category IS NOT DISTINCT FROM
         CASE WHEN r.error_kind = 'Ошибка связи' THEN NULL ELSE 'Прочие' END
    WHERE r.error_type IS NOT NULL
    ORDER BY r.error_type
    ON CONFLICT (error_type) DO NOTHING;

    UPDATE stg_egisz.exchange_messages t
    SET network_error_type = r.error_type
    FROM pg_temp.reclassified r
    WHERE t.network_error_text IS NOT NULL
      AND r.error_kind = 'Ошибка связи'
      AND ARRAY[r.error_code, r.error_text] = ARRAY[t.network_error_code, t.network_error_text]
      AND t.network_error_type IS DISTINCT FROM r.error_type;
    GET DIAGNOSTICS step_rows = ROW_COUNT;
    updated := updated + step_rows;

    -- Элементы разворачиваются в отдельный набор до соединения со справочником: внутри
    -- LATERAL соединение уходило во вложенный цикл и перебирало справочник для каждого
    -- сообщения. Сравнение массивов считает NULL равными и соединяется хешем.
    WITH elements AS MATERIALIZED (
        SELECT t.logid, t.log_date, e.ord, e.item
        FROM stg_egisz.exchange_messages t
        CROSS JOIN LATERAL jsonb_array_elements(t.remd_errors) WITH ORDINALITY AS e(item, ord)
        WHERE t.remd_errors IS NOT NULL
    ),
    rebuilt AS (
        SELECT el.logid, el.log_date,
               jsonb_agg(el.item || jsonb_build_object('error_type', r.error_type, 'nsi_dictionary_oid', r.nsi_dictionary_oid)
                         ORDER BY el.ord) AS items
        FROM elements el
        JOIN pg_temp.reclassified r
          ON r.error_kind = 'Ошибка асинхронного ответа'
         AND ARRAY[r.error_code, r.error_text] = ARRAY[el.item ->> 'code', el.item ->> 'message']
        GROUP BY el.logid, el.log_date
    )
    UPDATE stg_egisz.exchange_messages t
    SET remd_errors = n.items
    FROM rebuilt n
    WHERE t.logid = n.logid
      AND t.log_date = n.log_date
      AND t.remd_errors IS DISTINCT FROM n.items;
    GET DIAGNOSTICS step_rows = ROW_COUNT;
    updated := updated + step_rows;

    WITH elements AS MATERIALIZED (
        SELECT t.logid, t.log_date, e.ord, e.item
        FROM stg_egisz.exchange_messages t
        CROSS JOIN LATERAL jsonb_array_elements(t.ihe_errors) WITH ORDINALITY AS e(item, ord)
        WHERE t.ihe_errors IS NOT NULL
    ),
    rebuilt AS (
        SELECT el.logid, el.log_date,
               jsonb_agg(el.item || jsonb_build_object('error_type', r.error_type, 'nsi_dictionary_oid', r.nsi_dictionary_oid)
                         ORDER BY el.ord) AS items
        FROM elements el
        JOIN pg_temp.reclassified r
          ON r.error_kind = 'Ошибка асинхронного ответа'
         AND ARRAY[r.error_code, r.error_text] = ARRAY[el.item ->> 'error_code', el.item ->> 'code_context']
        GROUP BY el.logid, el.log_date
    )
    UPDATE stg_egisz.exchange_messages t
    SET ihe_errors = n.items
    FROM rebuilt n
    WHERE t.logid = n.logid
      AND t.log_date = n.log_date
      AND t.ihe_errors IS DISTINCT FROM n.items;
    GET DIAGNOSTICS step_rows = ROW_COUNT;
    updated := updated + step_rows;

    DELETE FROM mart_egisz.dim_error_type d
    WHERE d.rule_code IS NULL
      AND NOT EXISTS (SELECT 1 FROM pg_temp.reclassified r WHERE r.error_type = d.error_type);

    DROP TABLE pg_temp.reclassified;
    RETURN updated;
END;
$$;
