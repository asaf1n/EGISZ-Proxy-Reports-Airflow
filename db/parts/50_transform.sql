-- ============================================================================
-- 50_transform.sql — egisz_transform_raw_to_facts
-- Source: db/dwh_init.sql, lines [1055..1254).
-- Loaded by db/dwh_init.sql via \i db/parts/50_transform.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

DROP FUNCTION IF EXISTS public.reconcile_document_attributes_ui();
DROP FUNCTION IF EXISTS public.reconcile_document_attributes(text[]);
DROP FUNCTION IF EXISTS public.transform_raw_to_facts(bigint, bigint);
DROP FUNCTION IF EXISTS public.transform_raw_to_facts(bigint, bigint, bigint);
-- backfill_semd_codes() удалён: backfill типа СЭМД делает inline-блок batch_docs
-- внутри transform_raw_to_facts (O(батч)); отдельная O(архив)-функция не вызывалась.
DROP FUNCTION IF EXISTS public.backfill_semd_codes();

-- reconcile_document_attributes — в 70_views_core.sql

-- Слой версий/логического документа (README §«Версии и идентичность документа»).
-- Пересобирает document_group_id / version / цепочку / is_current_version для групп,
-- затронутых батчем (p_dwh_ids); p_dwh_ids = NULL — полный пересчёт (обслуживание).
--
-- Ключ логического документа = (jid + semd_code + doc_number), где doc_number = PROTOCOLID
-- (номер протокола/ИБ в МИС). Проверено на базе: пара (jid, doc_number) всегда несёт ровно
-- ОДИН semd_code — это ключ ДОКУМЕНТА, а localUid меняется при каждой правке/ре-выгрузке
-- ⇒ несколько localUid на (jid, semd_code, doc_number) = версии одного документа.
-- Провенанс в document_group_confidence: 'doc_number' (сгруппировано) | 'singleton' (нет
-- doc_number / уникальный документ). Защитный c_cap: группы крупнее порога не считаем
-- версиями (страховка от клиник, переиспользующих счётчик протокола) — остаются singleton
-- и видны в rpt_health_versions.
CREATE OR REPLACE FUNCTION public.recompute_document_versions(p_dwh_ids text[] DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    c_cap constant integer := 50;  -- макс. версий в группе; max по базе = 7
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

    WITH keyed AS (
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
            END AS conf
        FROM public.documents d
    ),
    affected_keys AS (
        SELECT DISTINCT k.grp_key
        FROM keyed k
        WHERE p_dwh_ids IS NULL OR k.dwh_id = ANY (p_dwh_ids)
    ),
    members AS (
        SELECT
            k.dwh_id, k.grp_key, k.conf,
            d.status, d.registered_at, d.last_callback_at, d.first_sent_at, d.request_logid
        FROM keyed k
        JOIN affected_keys ak ON ak.grp_key = k.grp_key
        JOIN public.documents d ON d.dwh_id = k.dwh_id
    ),
    ranked AS (
        SELECT
            m.*,
            count(*) OVER (PARTITION BY m.grp_key) AS grp_size,
            -- Порядок версий: старейшая отправка = 1.
            row_number() OVER (
                PARTITION BY m.grp_key
                ORDER BY COALESCE(m.first_sent_at, '-infinity'::timestamptz), m.request_logid, m.dwh_id
            ) AS vnum,
            -- Текущая версия: зарегистрированный success приоритетнее, иначе последнее событие.
            row_number() OVER (
                PARTITION BY m.grp_key
                ORDER BY
                    (CASE WHEN m.status = 'success' THEN 1 ELSE 0 END) DESC,
                    COALESCE(m.last_callback_at, m.registered_at, m.first_sent_at, '-infinity'::timestamptz) DESC,
                    m.request_logid DESC, m.dwh_id DESC
            ) AS cur_rank
        FROM members m
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

CREATE OR REPLACE FUNCTION public.transform_raw_to_facts(
    from_logid bigint,
    to_logid bigint,
    p_lookback_logids bigint DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    inserted_rows integer := 0;
    raw_cd_min timestamptz;
    raw_cd_max timestamptz;
    -- 0 = auto: look back across the LOGID span being transformed (forward extract).
    -- Reconcile passes explicit lookback (window low LOGID) to link late callbacks
    -- with earlier getDocumentFile rows anywhere in the journal prefix.
    lookback_logids bigint := GREATEST(
        COALESCE(NULLIF(p_lookback_logids, 0), to_logid - from_logid),
        1
    );
BEGIN
    -- exchangelog_raw партиционирована по createdate; transform фильтрует по logid.
    -- Узкий диапазон createdate по батчу включает partition pruning (см. idx_exchangelog_raw_logid).
    SELECT
        MIN(r.createdate) - interval '7 days',
        MAX(r.createdate) + interval '1 day'
    INTO raw_cd_min, raw_cd_max
    FROM exchangelog_raw r
    WHERE r.logid > GREATEST(from_logid - lookback_logids, 0)
      AND r.logid <= to_logid;

    raw_cd_min := COALESCE(raw_cd_min, '-infinity'::timestamptz);
    raw_cd_max := COALESCE(raw_cd_max, 'infinity'::timestamptz);

    -- Разложение payload: каждый LOGID парсится один раз в transactions (xml_*).
    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
    ),
    batch_min AS (
        SELECT COALESCE(MIN(c.logid), from_logid) AS min_logid
        FROM candidate_log_ids c
    ),
    parse_targets AS (
        SELECT r.logid, r.createdate, r.loaded_at, r.msgid, r.msgtext, r.logtext
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND NOT EXISTS (
              SELECT 1
              FROM public.transactions tx
              WHERE tx.logid = r.logid
                AND tx.xml_parsed_at IS NOT NULL
          )

        UNION

        SELECT r.logid, r.createdate, r.loaded_at, r.msgid, r.msgtext, r.logtext
        FROM exchangelog_raw r
        CROSS JOIN batch_min bm
        WHERE r.logid >= GREATEST(bm.min_logid - lookback_logids, 0)
          AND r.logid <= from_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
          AND NOT EXISTS (
              SELECT 1
              FROM public.transactions tx
              WHERE tx.logid = r.logid
                AND tx.xml_parsed_at IS NOT NULL
          )
    )
    INSERT INTO public.transactions (
        logid, log_date,
        source_msgid, source_message_id_norm,
        xml_dwh_id, xml_local_uid, xml_emdr_id,
        source_action, xml_relates_to_id, xml_semd_code, xml_doc_number, xml_org_oid,
        xml_error_code, xml_message, xml_raw_status, xml_document_status,
        xml_jid, xml_creation_date,
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
        p.relates_to_id,
        p.kind_xml,
        p.doc_number,
        p.org_oid,
        p.error_code,
        p.xml_message,
        p.raw_status,
        p.document_status,
        p.jid_from_payload,
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
        xml_relates_to_id = COALESCE(EXCLUDED.xml_relates_to_id, public.transactions.xml_relates_to_id),
        xml_semd_code = COALESCE(EXCLUDED.xml_semd_code, public.transactions.xml_semd_code),
        xml_doc_number = COALESCE(EXCLUDED.xml_doc_number, public.transactions.xml_doc_number),
        xml_org_oid = COALESCE(EXCLUDED.xml_org_oid, public.transactions.xml_org_oid),
        xml_error_code = COALESCE(EXCLUDED.xml_error_code, public.transactions.xml_error_code),
        xml_message = COALESCE(EXCLUDED.xml_message, public.transactions.xml_message),
        xml_raw_status = COALESCE(EXCLUDED.xml_raw_status, public.transactions.xml_raw_status),
        xml_document_status = COALESCE(EXCLUDED.xml_document_status, public.transactions.xml_document_status),
        xml_jid = COALESCE(EXCLUDED.xml_jid, public.transactions.xml_jid),
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

    WITH document_source_rows AS (
        SELECT tx.logid
        FROM public.transactions tx
        WHERE COALESCE(tx.source_action, '') = 'getDocumentFile'
          AND tx.logid > from_logid
          AND tx.logid <= to_logid
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
    ),
    -- Минимальный набор реквизитов ЭМД (localUid + JID + KIND) может приходить
    -- разными getDocumentFile-сообщениями одного документа. Собираем реквизиты по
    -- dwh_id из окна батча и небольшого lookback назад, чтобы недостающие поля
    -- дозагружались по мере поступления, а запись об ЭМД появлялась при их полном наборе.
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
        JOIN public.exchangelog_raw gr ON gr.logid = tx.logid
            AND gr.createdate >= raw_cd_min
            AND gr.createdate < raw_cd_max
        WHERE COALESCE(tx.source_action, '') = 'getDocumentFile'
          AND gr.logid <= to_logid
          AND gr.logid >= GREATEST(
                (SELECT COALESCE(MIN(logid), from_logid) FROM document_source_rows) - lookback_logids,
                0
              )
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
        GROUP BY tx.xml_dwh_id
        HAVING max(gr.logid) > from_logid
    ),
    document_resolved AS (
        SELECT
            a.*,
            r.jid AS resolved_jid,
            r.resolve_method
        FROM document_attributes a
        LEFT JOIN LATERAL public.resolve_document_jid(a.org_oid, a.endpoint_text) r ON TRUE
    )
    INSERT INTO public.documents (
        dwh_id, local_uid, semd_code,
        status, first_sent_at, request_logid,
        result_logid, last_callback_at, jid, org_oid, jid_resolve_method,
        error_types, error_text, error_summary,
        updated_at
    )
    SELECT
        a.dwh_id,
        a.local_uid,
        a.semd_code,
        CASE WHEN a.has_network_error THEN 'network_error' ELSE 'waiting' END,
        a.sent_at,
        a.request_logid,
        CASE WHEN a.has_network_error THEN a.network_logid END,
        CASE WHEN a.has_network_error THEN a.network_at END,
        a.resolved_jid,
        a.org_oid,
        a.resolve_method,
        CASE WHEN a.has_network_error THEN 'Сетевая ошибка' END,
        CASE WHEN a.has_network_error THEN a.network_message END,
        CASE WHEN a.has_network_error THEN 'Сетевая ошибка' END,
        now()
    FROM document_resolved a
    WHERE a.dwh_id IS NOT NULL
      AND a.local_uid IS NOT NULL
      AND (
          a.has_network_error
          OR (a.resolved_jid IS NOT NULL AND a.semd_code IS NOT NULL)
      )
    ON CONFLICT (dwh_id) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.documents.local_uid),
        semd_code = COALESCE(EXCLUDED.semd_code, public.documents.semd_code),
        first_sent_at = LEAST(
            COALESCE(public.documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, public.documents.first_sent_at)
        ),
        status = CASE
            WHEN public.documents.status IN ('success', 'async_error', 'network_error')
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
        error_summary = COALESCE(EXCLUDED.error_summary, public.documents.error_summary),
        request_logid = GREATEST(public.documents.request_logid, EXCLUDED.request_logid),
        updated_at = now()
    WHERE public.documents.request_logid IS NULL
       OR public.documents.request_logid <= EXCLUDED.request_logid;

    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
          AND r.createdate >= raw_cd_min
          AND r.createdate < raw_cd_max
    ),
    gdf_events AS (
        SELECT
            gr.logid,
            res.jid,
            tx.xml_dwh_id AS dwh_id,
            tx.xml_local_uid AS local_uid
        FROM public.transactions tx
        JOIN public.exchangelog_raw gr ON gr.logid = tx.logid
            AND gr.createdate >= raw_cd_min
            AND gr.createdate < raw_cd_max
        LEFT JOIN LATERAL public.resolve_document_jid(
            tx.xml_org_oid,
            COALESCE(gr.logtext, '') || ' ' || COALESCE(gr.msgtext, '')
        ) res ON TRUE
        WHERE COALESCE(tx.source_action, '') = 'getDocumentFile'
          AND gr.logid <= to_logid
          AND gr.logid >= GREATEST(
                (SELECT COALESCE(MIN(c.logid), from_logid) FROM candidate_log_ids c) - lookback_logids,
                0
              )
          AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL
          AND res.jid IS NOT NULL
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
            tx.xml_jid AS jid_from_payload,
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
                exch_ref.dwh_id,
                emdr_ref.dwh_id,
                gdf_ref.dwh_id
            ) AS dwh_id,
            COALESCE(r.local_uid_xml, exch_ref.local_uid, gdf_ref.local_uid) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.normalize_semd_code(r.kind_xml) AS semd_code,
            NULL::text AS semd_name,
            CASE
                WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3'
                ELSE r.error_code
            END AS error_code,
            r.xml_message,
            r.raw_status,
            r.jid_from_payload,
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
        LEFT JOIN LATERAL (
            SELECT c.dwh_id, c.local_uid
            FROM (
                SELECT
                    0 AS priority,
                    link_tx.xml_dwh_id AS dwh_id,
                    link_tx.xml_local_uid AS local_uid
                FROM public.transactions link_tx
                WHERE r.relates_to_id IS NOT NULL
                  AND link_tx.source_message_id_norm = r.relates_to_id
                  AND link_tx.xml_dwh_id IS NOT NULL

                UNION ALL

                SELECT
                    1,
                    link_tx.xml_dwh_id,
                    link_tx.xml_local_uid
                FROM public.transactions link_tx
                WHERE r.emdr_id IS NOT NULL
                  AND lower(NULLIF(btrim(link_tx.xml_emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
                  AND NULLIF(btrim(link_tx.xml_local_uid), '') IS NOT NULL
                  AND link_tx.xml_dwh_id IS NOT NULL

            ) c
            WHERE c.dwh_id IS NOT NULL
            ORDER BY c.priority, c.dwh_id
            LIMIT 1
        ) exch_ref ON TRUE
        LEFT JOIN LATERAL (
            SELECT fd.dwh_id
            FROM public.documents fd
            WHERE r.emdr_id IS NOT NULL
              AND lower(NULLIF(btrim(fd.emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
            ORDER BY fd.last_callback_at DESC NULLS LAST, fd.request_logid DESC NULLS LAST
            LIMIT 1
        ) emdr_ref ON TRUE
        LEFT JOIN LATERAL (
            SELECT g.dwh_id, g.local_uid
            FROM gdf_events g
            LEFT JOIN LATERAL public.resolve_document_jid(
                r.org_oid,
                COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, '')
            ) cb_jid ON TRUE
            WHERE cb_jid.jid IS NOT NULL
              AND g.jid = cb_jid.jid
              AND g.logid < r.logid
            ORDER BY g.logid DESC
            LIMIT 1
        ) gdf_ref ON TRUE
        LEFT JOIN public.documents src_doc
          ON src_doc.dwh_id = COALESCE(
                r.dwh_id_xml,
                exch_ref.dwh_id,
                emdr_ref.dwh_id,
                gdf_ref.dwh_id
            )
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
            COALESCE(p.logtext, '') || ' ' || COALESCE(p.msgtext, '')
        ) res ON TRUE
    ),
    with_errors AS (
        SELECT
            e.*,
            -- errors_json нужен только для error-строк; для success/pending это всегда '[]',
            -- поэтому не гоняем egisz_xml_error_items по payload'у успешных ответов.
            CASE
                WHEN e.final_status = 'error'
                THEN public.build_errors_json(e.final_status, e.error_code, e.event_message, e.msgtext)
                ELSE '[]'::jsonb
            END AS built_errors_json
        FROM enriched e
    ),
    -- Интерпретация отказа РЭМД дорогая: на каждый <item> идёт регекс-скан 80 правил
    -- dim_error_rules, и эта работа повторяется для одинаковых payload'ов
    -- внутри батча. Считаем интерпретацию один раз на уникальный errors_json и приклеиваем
    -- обратно по равенству jsonb — результат построчно идентичен прежнему.
    error_dict AS (
        SELECT DISTINCT built_errors_json
        FROM with_errors
        WHERE final_status = 'error'
    ),
    error_interp AS (
        SELECT
            built_errors_json,
            public.error_classify(built_errors_json) AS error_type_dict,
            public.error_interpretation_row(built_errors_json) AS error_summary_dict,
            public.error_messages_row(built_errors_json) AS error_messages_dict
        FROM error_dict
    ),
    with_bi_fields AS (
        SELECT
            e.*,
            ei.error_type_dict,
            ei.error_summary_dict,
            ei.error_messages_dict,
            regexp_split_to_array(public.clean_text_value(e.raw_patient_name), '\s+') AS patient_parts,
            regexp_replace(COALESCE(e.raw_snils, ''), '\D', '', 'g') AS snils_digits,
            public.clean_text_value(e.raw_doctor_name) AS doctor_name_clean
        FROM with_errors e
        LEFT JOIN error_interp ei ON ei.built_errors_json = e.built_errors_json
    )
    INSERT INTO transactions (
        logid, dwh_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, message, callback_url, jid, jid_resolve_method, semd_code,
        semd_name, error_code, creation_date, loaded_at,
        error_type, error_json_text, error_summary,
        patient_name_masked, snils_masked, doctor_name, patient_hash, doctor_hash
    )
    SELECT
        e.logid, e.dwh_id, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.event_message, e.logtext,
        e.resolved_jid, e.resolved_method, e.resolved_semd_code, e.semd_name, e.error_code,
        e.creation_date, now(),
        CASE
            WHEN e.final_status = 'error' AND e.logstate = 3 THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error'   THEN e.error_type_dict
            ELSE NULL  -- success/pending/unknown: видимость через status, error_type не заполняется
        END,
        e.error_messages_dict,
        CASE
            WHEN e.final_status = 'error' AND e.logstate = 3 THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error'   THEN e.error_summary_dict
            ELSE NULL
        END,
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
        callback_url = EXCLUDED.callback_url,
        jid = EXCLUDED.jid,
        jid_resolve_method = EXCLUDED.jid_resolve_method,
        semd_code = EXCLUDED.semd_code,
        semd_name = EXCLUDED.semd_name,
        error_code = EXCLUDED.error_code,
        creation_date = EXCLUDED.creation_date,
        loaded_at = now(),
        error_type = EXCLUDED.error_type,
        error_json_text = EXCLUDED.error_json_text,
        error_summary = EXCLUDED.error_summary,
        patient_name_masked = EXCLUDED.patient_name_masked,
        snils_masked = EXCLUDED.snils_masked,
        doctor_name = EXCLUDED.doctor_name,
        patient_hash = EXCLUDED.patient_hash,
        doctor_hash = EXCLUDED.doctor_hash;
    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    affected := affected + inserted_rows;

    INSERT INTO public.documents (
        dwh_id, local_uid, emdr_id, semd_code,
        status, result_msgid, relates_to_msgid,
        result_logid, request_logid, document_created_at, registered_at,
        last_callback_at, last_status, jid, org_oid, jid_resolve_method,
        error_types, error_text, error_summary,
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
            ELSE 'waiting'
        END,
        public.clean_text_value(f.message_id),
        public.clean_text_value(f.relates_to_id),
        f.logid,
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
        NULLIF(btrim(f.error_summary), ''),
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
        request_logid = GREATEST(COALESCE(public.documents.request_logid, 0), COALESCE(EXCLUDED.request_logid, 0)),
        last_callback_at = GREATEST(COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz), COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)),
        last_status = COALESCE(EXCLUDED.last_status, public.documents.last_status),
        jid = COALESCE(EXCLUDED.jid, public.documents.jid),
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
        error_summary = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_summary
            ELSE public.documents.error_summary
        END,
        patient_hash = COALESCE(EXCLUDED.patient_hash, public.documents.patient_hash),
        doctor_hash = COALESCE(EXCLUDED.doctor_hash, public.documents.doctor_hash),
        updated_at = now();

    -- Колбэк может прийти без KIND в XML, а тип СЭМД уже известен из getDocumentFile (gdf).
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

    -- Инкрементальное сопровождение document_attributes по dwh_id из батча.
    PERFORM public.reconcile_document_attributes(
        ARRAY(
            SELECT d.dwh_id::text
            FROM public.documents d
            WHERE d.updated_at = transaction_timestamp()
        )
    );

    -- Пересбор слоя версий (document_group_id / is_current_version / цепочка) для групп,
    -- затронутых батчем. Детерминирован и идемпотентен (пишет только при изменении).
    PERFORM public.recompute_document_versions(
        ARRAY(
            SELECT d.dwh_id::text
            FROM public.documents d
            WHERE d.updated_at = transaction_timestamp()
        )
    );

    RETURN affected;
END;
$$;
