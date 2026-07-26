-- ============================================================================
-- 03_transform.sql — raw journal -> transactions -> documents
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: transform
-- ============================================================================
-- 50_transform.sql — transform_raw_to_facts
-- Loaded by db/dwh_init.sql via \i db/03_transform.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

-- reconcile_document_attributes — в 70_views_core.sql

-- Слой версий/логического документа (README §«Версии и идентичность документа»).
-- Пересобирает document_group_id / version / цепочку / is_current_version для групп,
-- затронутых батчем (p_dwh_ids); p_dwh_ids = NULL — полный пересчёт (обслуживание).
--
-- Ключ логического документа = (jid + semd_code + doc_number), где doc_number = PROTOCOLID
-- (номер протокола/ИБ в МИС). Пара (jid, doc_number) несёт ровно ОДИН semd_code — это ключ
-- ДОКУМЕНТА, а localUid меняется при каждой правке/ре-выгрузке ⇒ несколько localUid на
-- (jid, semd_code, doc_number) = версии одного документа. Провенанс в
-- document_group_confidence: 'doc_number' (сгруппировано) | 'singleton'. Защитный c_cap:
-- группы крупнее порога не считаем версиями (страховка от клиник, переиспользующих счётчик
-- протокола) — остаются singleton и видны в rpt_health_versions.
CREATE OR REPLACE FUNCTION public.recompute_document_versions(p_dwh_ids text[] DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    c_cap constant integer := 50;  -- макс. версий в группе
BEGIN
    -- Шаг 0: documents.doc_number наполняется из transactions (PROTOCOLID не хранится в
    -- documents при INSERT). Только затронутые dwh_id (или весь архив при p_dwh_ids=NULL).
    UPDATE public.documents d
    SET doc_number = src.docnum
    FROM (
        SELECT
            t.dwh_id,
            COALESCE(
                max(NULLIF(btrim(t.doc_number), '')),
                max(NULLIF(btrim(t.xml_doc_number), ''))
            ) AS docnum
        FROM public.transactions t
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
        FROM public.documents d
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
        JOIN public.documents d
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
        JOIN public.documents d ON d.document_group_id = s.document_group_id
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
        FROM public.documents d
        JOIN member_ids m ON m.dwh_id = d.dwh_id
    ),
    ranked AS (
        SELECT
            k.*,
            count(*) OVER (PARTITION BY k.grp_key) AS grp_size,
            -- Порядок версий: старейшая отправка = 1.
            row_number() OVER (
                PARTITION BY k.grp_key
                ORDER BY COALESCE(k.first_sent_at, '-infinity'::timestamptz), k.request_logid, k.dwh_id
            ) AS vnum,
            -- Текущая версия: зарегистрированный success приоритетнее, иначе последнее событие.
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
    UPDATE public.documents d SET
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

-- Разбор окна журнала (from_logid, to_logid] в факты.
--
-- Классы сообщений и правила привязки к документу (README §«Связывание сообщений»):
--   getDocumentFile          — localUid лежит в самом payload;
--   ответ РЭМД/ИЭМК        — localUid в ответе нет, есть relatesToMessage; документ
--                              находится через реестр подач dim_message_document;
--   повторный ответ        — подтверждающий путь по emdrId уже собранных документов.
-- Применённое правило пишется в transactions.link_method, непривязанные ответы
-- помечаются 'unlinked' и попадают в сигналы здоровья, а не теряются молча.
--
-- Окно строго ограничено (from_logid, to_logid]: связывание не зависит от префикса
-- журнала, поэтому отсечение партиций по createdate работает на каждом батче.
DROP FUNCTION IF EXISTS public.transform_raw_to_facts(bigint, bigint, bigint);
DROP FUNCTION IF EXISTS public.transform_raw_to_facts(bigint, bigint);
CREATE OR REPLACE FUNCTION public.transform_raw_to_facts(
    from_logid bigint,
    to_logid bigint
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    inserted_rows integer := 0;
    unlinked_rows integer := 0;
    skipped_no_clinic integer := 0;
    raw_cd_min timestamptz;
    raw_cd_max timestamptz;
BEGIN
    -- exchangelog_raw партиционирована по createdate; transform фильтрует по logid.
    -- Узкий диапазон createdate по батчу включает partition pruning.
    SELECT
        MIN(r.createdate) - interval '1 day',
        MAX(r.createdate) + interval '1 day'
    INTO raw_cd_min, raw_cd_max
    FROM exchangelog_raw r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid;

    raw_cd_min := COALESCE(raw_cd_min, '-infinity'::timestamptz);
    raw_cd_max := COALESCE(raw_cd_max, 'infinity'::timestamptz);

    -- Разложение payload: каждый LOGID парсится один раз, результат — в transactions (xml_*).
    -- Анти-джойн идёт по exchangelog_parse_attempts, а не по transactions.xml_parsed_at:
    -- строки без реквизитов не проходят фильтр вставки, и маркер только на вставленных
    -- строках заставлял перепарсивать их при каждом повторном проходе окна.
    WITH parse_targets AS (
        SELECT r.logid, r.createdate, r.loaded_at, r.msgid, r.msgtext, r.logtext, r.uri
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND NOT EXISTS (
              SELECT 1
              FROM public.exchangelog_parse_attempts pa
              WHERE pa.logid = r.logid
          )
    )
    INSERT INTO public.transactions (
        logid, log_date,
        source_msgid, source_message_id_norm,
        xml_dwh_id, xml_local_uid, xml_emdr_id,
        source_action, egisz_subsystem, jid, xml_relates_to_id, xml_semd_code, xml_doc_number, xml_org_oid,
        xml_error_code, xml_message, xml_raw_status, xml_document_status,
        xml_creation_date,
        xml_patient_name, xml_snils, xml_doctor_name,
        xml_has_fault_marker, xml_has_register_response, xml_has_register_result,
        xml_has_processing_marker, xml_has_error_ilike,
        xml_parsed_at, loaded_at
    )
    SELECT
        t.logid,
        COALESCE(t.createdate, t.loaded_at, now()) AS log_date,
        t.msgid,
        p.exchange_msgid_norm,
        p.dwh_id,
        p.local_uid,
        p.emdr_id,
        p.action,
        public.egisz_subsystem(t.uri, p.action, t.logtext),
        rj.jid,
        p.relates_to_id,
        p.kind_xml,
        p.doc_number,
        p.org_oid,
        p.error_code,
        p.xml_message,
        p.raw_status,
        p.document_status,
        p.creation_date,
        p.raw_patient_name,
        p.raw_snils,
        p.raw_doctor_name,
        p.has_fault_marker,
        p.has_register_response,
        p.has_register_result,
        p.has_processing_marker,
        p.has_error_ilike,
        now(),
        now()
    FROM parse_targets t
    CROSS JOIN LATERAL public.parse_exchangelog_row(t.msgtext, t.msgid, t.logtext) p
    -- jid запроса getDocumentFile фиксируется при парсинге: один resolve на строку за всю
    -- её жизнь вместо повторного разбора payload регулярным выражением на чтении.
    LEFT JOIN LATERAL (
        SELECT res.jid
        FROM public.resolve_document_jid(
            p.org_oid,
            COALESCE(t.logtext, '') || ' ' || COALESCE(t.msgtext, '')
        ) res
        WHERE COALESCE(p.action, '') = 'getDocumentFile'
    ) rj ON TRUE
    WHERE (
          p.exchange_msgid_norm IS NOT NULL
          OR NULLIF(btrim(t.msgid), '') IS NOT NULL
          OR NULLIF(btrim(p.local_uid), '') IS NOT NULL
          OR NULLIF(btrim(p.emdr_id), '') IS NOT NULL
          OR COALESCE(p.action, '') = 'getDocumentFile'
      )
    ON CONFLICT (logid, log_date) DO UPDATE SET
        source_msgid = COALESCE(EXCLUDED.source_msgid, public.transactions.source_msgid),
        source_message_id_norm = COALESCE(EXCLUDED.source_message_id_norm, public.transactions.source_message_id_norm),
        xml_dwh_id = COALESCE(EXCLUDED.xml_dwh_id, public.transactions.xml_dwh_id),
        xml_local_uid = COALESCE(EXCLUDED.xml_local_uid, public.transactions.xml_local_uid),
        xml_emdr_id = COALESCE(EXCLUDED.xml_emdr_id, public.transactions.xml_emdr_id),
        source_action = COALESCE(EXCLUDED.source_action, public.transactions.source_action),
        egisz_subsystem = COALESCE(EXCLUDED.egisz_subsystem, public.transactions.egisz_subsystem),
        jid = COALESCE(public.transactions.jid, EXCLUDED.jid),
        xml_relates_to_id = COALESCE(EXCLUDED.xml_relates_to_id, public.transactions.xml_relates_to_id),
        xml_semd_code = COALESCE(EXCLUDED.xml_semd_code, public.transactions.xml_semd_code),
        xml_doc_number = COALESCE(EXCLUDED.xml_doc_number, public.transactions.xml_doc_number),
        xml_org_oid = COALESCE(EXCLUDED.xml_org_oid, public.transactions.xml_org_oid),
        xml_error_code = COALESCE(EXCLUDED.xml_error_code, public.transactions.xml_error_code),
        xml_message = COALESCE(EXCLUDED.xml_message, public.transactions.xml_message),
        xml_raw_status = COALESCE(EXCLUDED.xml_raw_status, public.transactions.xml_raw_status),
        xml_document_status = COALESCE(EXCLUDED.xml_document_status, public.transactions.xml_document_status),
        xml_creation_date = COALESCE(EXCLUDED.xml_creation_date, public.transactions.xml_creation_date),
        xml_patient_name = COALESCE(EXCLUDED.xml_patient_name, public.transactions.xml_patient_name),
        xml_snils = COALESCE(EXCLUDED.xml_snils, public.transactions.xml_snils),
        xml_doctor_name = COALESCE(EXCLUDED.xml_doctor_name, public.transactions.xml_doctor_name),
        xml_has_fault_marker = COALESCE(EXCLUDED.xml_has_fault_marker, public.transactions.xml_has_fault_marker),
        xml_has_register_response = COALESCE(EXCLUDED.xml_has_register_response, public.transactions.xml_has_register_response),
        xml_has_register_result = COALESCE(EXCLUDED.xml_has_register_result, public.transactions.xml_has_register_result),
        xml_has_processing_marker = COALESCE(EXCLUDED.xml_has_processing_marker, public.transactions.xml_has_processing_marker),
        xml_has_error_ilike = COALESCE(EXCLUDED.xml_has_error_ilike, public.transactions.xml_has_error_ilike),
        xml_parsed_at = COALESCE(EXCLUDED.xml_parsed_at, public.transactions.xml_parsed_at),
        loaded_at = now();

    -- Фиксация попытки парсинга по всему просканированному диапазону, независимо от того,
    -- прошла ли строка фильтр вставки. Строго после INSERT выше: его анти-джойн должен
    -- видеть состояние маркера до этого батча.
    INSERT INTO public.exchangelog_parse_attempts (logid)
    SELECT r.logid
    FROM exchangelog_raw r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND r.createdate >= raw_cd_min
      AND r.createdate < raw_cd_max
      AND NOT EXISTS (
          SELECT 1
          FROM public.exchangelog_parse_attempts pa
          WHERE pa.logid = r.logid
      )
    ON CONFLICT (logid) DO NOTHING;

    -- ------------------------------------------------------------------
    -- Ветка запроса: getDocumentFile создаёт экземпляр документа.
    -- localUid лежит в payload, клиника — из OID организации, gost-endpoint запроса
    -- или, если то и другое не разрешилось, из reply_to реестра подач.
    -- ------------------------------------------------------------------
    WITH batch_document_ids AS (
        SELECT DISTINCT tx.xml_dwh_id
        FROM public.transactions tx
        WHERE tx.source_action = 'getDocumentFile'
          AND tx.logid > from_logid
          AND tx.logid <= to_logid
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
          AND tx.xml_dwh_id IS NOT NULL
    ),
    -- Минимальный набор реквизитов (localUid + JID + KIND) может приходить разными
    -- сообщениями одного документа: недостающие поля дозагружаются последующими
    -- батчами через COALESCE в ON CONFLICT.
    document_attributes AS (
        SELECT
            tx.xml_dwh_id AS dwh_id,
            (array_agg(tx.xml_local_uid ORDER BY gr.logid)
                FILTER (WHERE NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL))[1] AS local_uid,
            (array_agg(public.normalize_semd_code(tx.xml_semd_code) ORDER BY gr.logid)
                FILTER (WHERE public.normalize_semd_code(tx.xml_semd_code) IS NOT NULL))[1] AS semd_code,
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
            max(gr.logid) AS request_logid,
            bool_or(gr.logstate = 3) AS has_network_error,
            max(gr.logid) FILTER (WHERE gr.logstate = 3) AS network_logid,
            max(COALESCE(gr.createdate, gr.logdate)) FILTER (WHERE gr.logstate = 3) AS network_at,
            (array_agg(COALESCE(NULLIF(btrim(gr.logtext), ''), NULLIF(btrim(gr.msgtext), ''), 'Сетевая ошибка') ORDER BY gr.logid DESC)
                FILTER (WHERE gr.logstate = 3))[1] AS network_message
        FROM public.transactions tx
        JOIN batch_document_ids bd ON bd.xml_dwh_id = tx.xml_dwh_id
        JOIN public.exchangelog_raw gr ON gr.logid = tx.logid
            AND gr.createdate >= raw_cd_min
            AND gr.createdate < raw_cd_max
        WHERE COALESCE(tx.source_action, '') = 'getDocumentFile'
          AND gr.logid > from_logid
          AND gr.logid <= to_logid
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
        GROUP BY tx.xml_dwh_id
    ),
    document_resolved AS (
        SELECT
            a.*,
            r.jid AS resolved_jid,
            r.resolve_method
        FROM document_attributes a
        -- reply_to реестра подач содержит endpoint клиники (gost-<jid>) и остаётся
        -- единственным источником клиники, когда в payload нет ни OID, ни адреса.
        LEFT JOIN LATERAL (
            SELECT m.reply_to
            FROM public.dim_message_document m
            WHERE m.document_uid = a.dwh_id
            ORDER BY m.source_egmid DESC
            LIMIT 1
        ) reg ON TRUE
        LEFT JOIN LATERAL public.resolve_document_jid(
            a.org_oid,
            COALESCE(a.endpoint_text, '') || ' ' || COALESCE(reg.reply_to, '')
        ) r ON TRUE
    )
    INSERT INTO public.documents (
        dwh_id, local_uid, semd_code,
        status, first_sent_at, request_logid,
        result_logid, last_callback_at, jid, org_oid, jid_resolve_method,
        error_types, error_text,
        updated_at
    )
    SELECT
        a.dwh_id,
        a.local_uid,
        a.semd_code,
        CASE WHEN a.has_network_error THEN 'network_error' ELSE public.document_status_nonfinal() END,
        a.sent_at,
        a.request_logid,
        CASE WHEN a.has_network_error THEN a.network_logid END,
        CASE WHEN a.has_network_error THEN a.network_at END,
        a.resolved_jid,
        a.org_oid,
        a.resolve_method,
        CASE WHEN a.has_network_error THEN 'Сетевая ошибка' END,
        CASE WHEN a.has_network_error THEN a.network_message END,
        now()
    FROM document_resolved a
    WHERE a.dwh_id IS NOT NULL
      AND a.local_uid IS NOT NULL
      -- Код СЭМД не требуется: он дозагружается ниже из соседних сообщений документа.
      -- Клиника обязательна — без неё экземпляр не отображается ни в одном срезе.
      AND (a.has_network_error OR a.resolved_jid IS NOT NULL)
    ON CONFLICT (dwh_id) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.documents.local_uid),
        semd_code = COALESCE(EXCLUDED.semd_code, public.documents.semd_code),
        first_sent_at = LEAST(
            COALESCE(public.documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, public.documents.first_sent_at)
        ),
        status = CASE
            WHEN public.documents.status IN (SELECT public.document_status_final())
            THEN public.documents.status
            ELSE EXCLUDED.status
        END,
        result_logid = COALESCE(EXCLUDED.result_logid, public.documents.result_logid),
        last_callback_at = COALESCE(EXCLUDED.last_callback_at, public.documents.last_callback_at),
        jid = COALESCE(EXCLUDED.jid, public.documents.jid),
        org_oid = COALESCE(EXCLUDED.org_oid, public.documents.org_oid),
        jid_resolve_method = CASE
            WHEN public.documents.jid_resolve_method = 'mo_uid'
            THEN public.documents.jid_resolve_method
            ELSE COALESCE(EXCLUDED.jid_resolve_method, public.documents.jid_resolve_method)
        END,
        error_types = COALESCE(EXCLUDED.error_types, public.documents.error_types),
        error_text = COALESCE(EXCLUDED.error_text, public.documents.error_text),
        -- request_logid — LOGID отправки; наибольший из известных. Ветка ответа эту
        -- колонку не трогает, поэтому пара request_msgid ↔ relates_to_msgid остаётся целой.
        request_logid = GREATEST(
            COALESCE(public.documents.request_logid, 0),
            COALESCE(EXCLUDED.request_logid, 0)
        ),
        updated_at = now();

    -- Отправки, по которым клиника не разрешилась ни payload'ом, ни реестром: в documents
    -- они не попадают (нечем атрибутировать), но их число возвращается вызывающему.
    SELECT count(*) INTO skipped_no_clinic
    FROM public.transactions tx
    WHERE tx.source_action = 'getDocumentFile'
      AND tx.logid > from_logid
      AND tx.logid <= to_logid
      AND tx.xml_dwh_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM public.documents d WHERE d.dwh_id = tx.xml_dwh_id);

    -- ------------------------------------------------------------------
    -- Ветка ответа: классификация ответа и привязка к документу.
    -- ------------------------------------------------------------------
    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
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
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            tx.source_message_id_norm AS message_id,
            tx.xml_relates_to_id AS relates_to_id,
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
            tx.xml_patient_name AS raw_patient_name,
            tx.xml_snils AS raw_snils,
            tx.xml_doctor_name AS raw_doctor_name,
            tx.xml_document_status AS document_status,
            tx.xml_has_fault_marker AS has_fault_marker,
            tx.xml_has_register_response AS has_register_response,
            tx.xml_has_register_result AS has_register_result,
            tx.xml_has_processing_marker AS has_processing_marker,
            tx.xml_has_error_ilike AS has_error_ilike
        FROM exchangelog_raw r
        JOIN candidate_log_ids c ON c.logid = r.logid
        JOIN public.transactions tx ON tx.logid = r.logid
        WHERE r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND tx.xml_parsed_at IS NOT NULL
          -- getDocumentFile — это отправка, её обрабатывает ветка выше; сюда она попадает
          -- только сбоем связи (LOGSTATE=3), который является исходом отправки.
          AND (
              COALESCE(tx.source_action, '') <> 'getDocumentFile'
              OR r.logstate = 3
          )
          AND (
              r.logstate = 3
              OR public.normalize_message_id(r.msgid) IS NOT NULL
              OR tx.source_message_id_norm IS NOT NULL
              OR tx.xml_relates_to_id IS NOT NULL
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
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            r.message_id,
            r.relates_to_id,
            COALESCE(
                r.dwh_id_xml,
                msg_ref.dwh_id,
                emdr_ref.dwh_id
            ) AS dwh_id,
            CASE
                WHEN r.dwh_id_xml IS NOT NULL THEN 'payload_local_uid'
                WHEN msg_ref.dwh_id IS NOT NULL THEN 'message_registry'
                WHEN emdr_ref.dwh_id IS NOT NULL THEN 'emdr_id'
                ELSE 'unlinked'
            END AS link_method,
            COALESCE(r.local_uid_xml, msg_ref.local_uid) AS local_uid_semd,
            msg_ref.reply_to AS registry_reply_to,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.normalize_semd_code(r.kind_xml) AS semd_code,
            CASE
                WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3'
                ELSE r.error_code
            END AS error_code,
            r.xml_message,
            r.raw_status,
            r.creation_date,
            r.raw_patient_name,
            r.raw_snils,
            r.raw_doctor_name,
            r.document_status,
            r.has_fault_marker,
            r.has_register_response,
            r.has_register_result,
            r.has_processing_marker,
            r.has_error_ilike,
            src_doc.semd_code AS source_document_semd_code
        FROM raw_parsed r
        -- Штатный ключ: relatesToMessage ответа → идентификатор подачи → localUid.
        LEFT JOIN LATERAL (
            SELECT
                public.dwh_id(m.document_uid) AS dwh_id,
                m.document_uid AS local_uid,
                m.reply_to
            FROM public.dim_message_document m
            WHERE r.relates_to_id IS NOT NULL
              AND m.msgid = public.message_registry_key(r.relates_to_id)
            LIMIT 1
        ) msg_ref ON TRUE
        -- Подтверждающий путь: повторный или поздний ответ по уже известному emdrId.
        LEFT JOIN LATERAL (
            SELECT fd.dwh_id
            FROM public.documents fd
            WHERE r.emdr_id IS NOT NULL
              AND lower(NULLIF(btrim(fd.emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
            ORDER BY fd.last_callback_at DESC NULLS LAST, fd.request_logid DESC NULLS LAST
            LIMIT 1
        ) emdr_ref ON TRUE
        LEFT JOIN public.documents src_doc
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
            public.classify_async_status(
                p.logstate,
                p.raw_status,
                p.document_status,
                p.has_fault_marker,
                p.has_register_response,
                p.has_register_result,
                p.has_processing_marker,
                p.has_error_ilike
            ) AS final_status,
            CASE
                WHEN p.logstate = 3 THEN 'Сетевая ошибка: ' || COALESCE(NULLIF(p.logtext, ''), 'нет деталей')
                ELSE p.xml_message
            END AS event_message
        FROM parsed p
        LEFT JOIN LATERAL public.resolve_document_jid(
            p.org_oid,
            COALESCE(p.logtext, '') || ' ' || COALESCE(p.msgtext, '') || ' ' || COALESCE(p.registry_reply_to, '')
        ) res ON TRUE
    ),
    with_errors AS (
        SELECT
            e.*,
            -- errors_json нужен только для error-строк; для success/pending это всегда '[]',
            -- поэтому не гоняем разбор по payload'у успешных ответов.
            CASE
                WHEN e.final_status = 'error'
                THEN public.build_errors_json(e.final_status, e.error_code, e.event_message, e.msgtext)
                ELSE '[]'::jsonb
            END AS built_errors_json
        FROM enriched e
    ),
    -- Классификация отказа дорогая: на каждый <item> идёт регекс-скан правил
    -- dim_error_rules, и эта работа повторяется для одинаковых payload'ов внутри батча.
    -- Считаем классификацию один раз на уникальный errors_json и приклеиваем обратно.
    error_dict AS (
        SELECT DISTINCT built_errors_json
        FROM with_errors
        WHERE final_status = 'error'
    ),
    error_interp AS (
        SELECT
            built_errors_json,
            public.error_classify(built_errors_json) AS error_type_dict,
            public.error_messages_row(built_errors_json) AS error_messages_dict
        FROM error_dict
    ),
    with_bi_fields AS (
        SELECT
            e.*,
            ei.error_type_dict,
            ei.error_messages_dict,
            regexp_split_to_array(public.clean_text_value(e.raw_patient_name), '\s+') AS patient_parts,
            regexp_replace(COALESCE(e.raw_snils, ''), '\D', '', 'g') AS snils_digits,
            public.clean_text_value(e.raw_doctor_name) AS doctor_name_clean
        FROM with_errors e
        LEFT JOIN error_interp ei ON ei.built_errors_json = e.built_errors_json
    )
    INSERT INTO transactions (
        logid, dwh_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, message, jid, jid_resolve_method, semd_code,
        error_code, creation_date, loaded_at, link_method,
        error_type, error_json_text,
        patient_name_masked, snils_masked, doctor_name, patient_hash, doctor_hash
    )
    SELECT
        e.logid, e.dwh_id, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.event_message,
        e.resolved_jid, e.resolved_method, e.resolved_semd_code, e.error_code,
        e.creation_date, now(), e.link_method,
        CASE
            WHEN e.final_status = 'error' AND e.logstate = 3 THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error'   THEN e.error_type_dict
            ELSE NULL  -- success/pending/unknown: видимость через status, error_type не заполняется
        END,
        e.error_messages_dict,
        CASE
            WHEN e.patient_parts IS NULL OR array_length(e.patient_parts, 1) IS NULL THEN '(нет данных)'
            ELSE substring(e.patient_parts[1] FROM 1 FOR 1) || '***'
                 || CASE WHEN array_length(e.patient_parts, 1) >= 2 THEN ' ' || substring(e.patient_parts[2] FROM 1 FOR 1) || '.' ELSE '' END
                 || CASE WHEN array_length(e.patient_parts, 1) >= 3 THEN substring(e.patient_parts[3] FROM 1 FOR 1) || '.' ELSE '' END
        END,
        CASE
            WHEN length(e.snils_digits) >= 4 THEN '***-***-*** ' || right(e.snils_digits, 4)
            WHEN length(e.snils_digits) >= 2 THEN '***-***-*** ' || right(e.snils_digits, 2)
            ELSE '(нет данных)'
        END,
        COALESCE(NULLIF(e.doctor_name_clean, ''), '(нет данных)'),
        CASE
            WHEN COALESCE(NULLIF(btrim(e.raw_patient_name), ''), '') = ''
             AND COALESCE(NULLIF(e.snils_digits, ''), '') = '' THEN NULL
            ELSE md5(lower(COALESCE(btrim(e.raw_patient_name), '')) || '|' || COALESCE(e.snils_digits, ''))
        END,
        CASE
            WHEN e.doctor_name_clean IS NULL THEN NULL
            ELSE md5(lower(e.doctor_name_clean))
        END
    FROM with_bi_fields e
    WHERE e.final_status IN ('success', 'error')
      AND e.dwh_id IS NOT NULL
    ON CONFLICT (logid, log_date) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        dwh_id = EXCLUDED.dwh_id,
        message_id = EXCLUDED.message_id,
        relates_to_id = EXCLUDED.relates_to_id,
        local_uid_semd = EXCLUDED.local_uid_semd,
        emdr_id = EXCLUDED.emdr_id,
        doc_number = EXCLUDED.doc_number,
        org_oid = EXCLUDED.org_oid,
        status = EXCLUDED.status,
        message = EXCLUDED.message,
        jid = EXCLUDED.jid,
        jid_resolve_method = EXCLUDED.jid_resolve_method,
        semd_code = EXCLUDED.semd_code,
        error_code = EXCLUDED.error_code,
        creation_date = EXCLUDED.creation_date,
        loaded_at = now(),
        link_method = EXCLUDED.link_method,
        error_type = EXCLUDED.error_type,
        error_json_text = EXCLUDED.error_json_text,
        patient_name_masked = EXCLUDED.patient_name_masked,
        snils_masked = EXCLUDED.snils_masked,
        doctor_name = EXCLUDED.doctor_name,
        patient_hash = EXCLUDED.patient_hash,
        doctor_hash = EXCLUDED.doctor_hash;
    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    affected := affected + inserted_rows;

    -- Ответы, не связавшиеся ни одним правилом, помечаются явно: без метки они
    -- неотличимы от неразобранных строк и деградация привязки остаётся незаметной.
    UPDATE public.transactions tx
    SET link_method = 'unlinked'
    WHERE tx.logid > from_logid
      AND tx.logid <= to_logid
      AND tx.dwh_id IS NULL
      AND tx.xml_relates_to_id IS NOT NULL
      AND tx.link_method IS DISTINCT FROM 'unlinked';
    GET DIAGNOSTICS unlinked_rows = ROW_COUNT;

    -- ------------------------------------------------------------------
    -- Перенос исхода на грейн документа. Ветка создаёт запись, если отправки в журнале
    -- не было: ЕГИСЗ отклоняет часть документов до запроса файла, и единственный след
    -- такого документа — сам ответ.
    -- ------------------------------------------------------------------
    INSERT INTO public.documents (
        dwh_id, local_uid, emdr_id, semd_code,
        status, result_msgid, relates_to_msgid,
        result_logid, document_created_at, registered_at,
        last_callback_at, last_status, jid, org_oid, jid_resolve_method,
        error_types, error_text,
        patient_hash, doctor_hash, updated_at
    )
    SELECT DISTINCT ON (f.dwh_id)
        f.dwh_id,
        public.clean_text_value(f.local_uid_semd),
        public.clean_text_value(f.emdr_id),
        public.normalize_semd_code(f.semd_code),
        CASE
            WHEN f.status = 'success' THEN 'success'
            WHEN f.status = 'error' AND f.error_type = 'Сетевая ошибка' THEN 'network_error'
            WHEN f.status = 'error' THEN 'async_error'
            ELSE public.document_status_nonfinal()
        END,
        public.clean_text_value(f.message_id),
        public.clean_text_value(f.relates_to_id),
        f.logid,
        f.creation_date,
        CASE WHEN f.status = 'success' THEN f.log_date ELSE NULL::timestamptz END,
        f.log_date,
        f.status,
        f.jid,
        f.org_oid,
        f.jid_resolve_method,
        f.error_type,
        NULLIF(btrim(f.error_json_text), ''),
        f.patient_hash,
        f.doctor_hash,
        now()
    FROM public.transactions f
    WHERE f.logid > from_logid
      AND f.logid <= to_logid
      AND f.dwh_id IS NOT NULL
    ORDER BY f.dwh_id, f.log_date DESC NULLS LAST, f.logid DESC
    ON CONFLICT (dwh_id) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.documents.local_uid),
        emdr_id = COALESCE(EXCLUDED.emdr_id, public.documents.emdr_id),
        semd_code = COALESCE(EXCLUDED.semd_code, public.documents.semd_code),
        status = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.status
            ELSE public.documents.status
        END,
        result_msgid = COALESCE(EXCLUDED.result_msgid, public.documents.result_msgid),
        relates_to_msgid = COALESCE(EXCLUDED.relates_to_msgid, public.documents.relates_to_msgid),
        result_logid = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.result_logid
            ELSE public.documents.result_logid
        END,
        document_created_at = COALESCE(EXCLUDED.document_created_at, public.documents.document_created_at),
        registered_at = COALESCE(EXCLUDED.registered_at, public.documents.registered_at),
        last_callback_at = GREATEST(COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz), COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)),
        last_status = COALESCE(EXCLUDED.last_status, public.documents.last_status),
        jid = COALESCE(public.documents.jid, EXCLUDED.jid),
        org_oid = COALESCE(EXCLUDED.org_oid, public.documents.org_oid),
        jid_resolve_method = CASE
            WHEN public.documents.jid_resolve_method = 'mo_uid'
            THEN public.documents.jid_resolve_method
            ELSE COALESCE(EXCLUDED.jid_resolve_method, public.documents.jid_resolve_method)
        END,
        error_types = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_types
            ELSE public.documents.error_types
        END,
        error_text = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_text
            ELSE public.documents.error_text
        END,
        patient_hash = COALESCE(EXCLUDED.patient_hash, public.documents.patient_hash),
        doctor_hash = COALESCE(EXCLUDED.doctor_hash, public.documents.doctor_hash),
        updated_at = now();

    -- Ответ может прийти без KIND, а тип СЭМД уже известен из отправки.
    -- Только документы, затронутые в этой транзакции: O(батч), не O(архив).
    WITH batch_docs AS (
        SELECT d.dwh_id
        FROM public.documents d
        WHERE d.updated_at = transaction_timestamp()
          AND NULLIF(btrim(d.semd_code), '') IS NULL
    )
    UPDATE public.documents d
    SET
        semd_code = src.semd_code,
        updated_at = now()
    FROM (
        SELECT DISTINCT ON (t.dwh_id)
            t.dwh_id,
            public.normalize_semd_code(t.semd_code) AS semd_code
        FROM public.transactions t
        INNER JOIN batch_docs b ON b.dwh_id = t.dwh_id
        WHERE NULLIF(btrim(t.semd_code), '') IS NOT NULL
        ORDER BY t.dwh_id, t.log_date DESC NULLS LAST, t.logid DESC
    ) src
    WHERE d.dwh_id = src.dwh_id;

    -- Число подач документа в ЕГИСЗ по реестру: повторная подача не меняет localUid,
    -- поэтому счётчик показывает, сколько раз документ отправлялся до текущего исхода.
    UPDATE public.documents d
    SET attempt_count = src.attempts,
        updated_at = now()
    FROM (
        SELECT m.document_uid AS dwh_id, count(*)::integer AS attempts
        FROM public.dim_message_document m
        WHERE EXISTS (
            SELECT 1 FROM public.documents b
            WHERE b.dwh_id = m.document_uid
              AND b.updated_at = transaction_timestamp()
        )
        GROUP BY m.document_uid
    ) src
    WHERE d.dwh_id = src.dwh_id
      AND d.attempt_count IS DISTINCT FROM src.attempts;

    -- Инкрементальное сопровождение document_attributes по dwh_id из батча.
    PERFORM public.reconcile_document_attributes(
        ARRAY(
            SELECT d.dwh_id::text
            FROM public.documents d
            WHERE d.updated_at = transaction_timestamp()
        )
    );

    -- Пересбор слоя версий для групп, затронутых батчем.
    PERFORM public.recompute_document_versions(
        ARRAY(
            SELECT d.dwh_id::text
            FROM public.documents d
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
