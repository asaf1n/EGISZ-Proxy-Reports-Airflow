-- ============================================================================
-- 50_transform.sql — egisz_transform_raw_to_facts
-- Source: db/dwh_init.sql, lines [1055..1254).
-- Loaded by db/dwh_init.sql via \i db/parts/50_transform.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

DROP FUNCTION IF EXISTS public.egisz_transform_raw_to_facts(bigint, bigint);

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(
    from_logid bigint,
    to_logid bigint
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    inserted_rows integer := 0;
BEGIN
    INSERT INTO public.dim_egisz_exchangelog_refs (
        logid, created_at, exchange_msgid, exchange_msgid_norm,
        local_uid, emdr_id, document_key, updated_at
    )
    SELECT
        r.logid,
        COALESCE(r.createdate, r.loaded_at) AS created_at,
        r.msgid,
        public.egisz_normalize_message_id(
            COALESCE(NULLIF(btrim(r.msgid), ''), public.egisz_xml_text(r.msgtext, 'messageId'))
        ) AS exchange_msgid_norm,
        public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'localUid')) AS local_uid,
        public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'emdrId')) AS emdr_id,
        public.egisz_document_key(public.egisz_xml_text(r.msgtext, 'localUid')) AS document_key,
        now()
    FROM public.exchangelog_raw r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND (
          public.egisz_normalize_message_id(COALESCE(NULLIF(btrim(r.msgid), ''), public.egisz_xml_text(r.msgtext, 'messageId'))) IS NOT NULL
          OR NULLIF(btrim(r.msgid), '') IS NOT NULL
          OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '') IS NOT NULL
          OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'emdrId')), '') IS NOT NULL
      )
    ON CONFLICT (logid) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        exchange_msgid = EXCLUDED.exchange_msgid,
        exchange_msgid_norm = EXCLUDED.exchange_msgid_norm,
        local_uid = COALESCE(EXCLUDED.local_uid, public.dim_egisz_exchangelog_refs.local_uid),
        emdr_id = COALESCE(EXCLUDED.emdr_id, public.dim_egisz_exchangelog_refs.emdr_id),
        document_key = COALESCE(EXCLUDED.document_key, public.dim_egisz_exchangelog_refs.document_key),
        updated_at = now();

    WITH document_source_rows AS (
        SELECT sr.logid
        FROM public.exchangelog_raw sr
        WHERE COALESCE(public.egisz_xml_text(sr.msgtext, 'action'), '') = 'getDocumentFile'
          AND sr.logid > from_logid
          AND sr.logid <= to_logid
          AND NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL
    ),
    -- Минимальный набор реквизитов ЭМД (localUid + JID + KIND) может приходить
    -- разными getDocumentFile-сообщениями одного документа. Собираем реквизиты по
    -- document_key из окна батча и небольшого lookback назад, чтобы недостающие поля
    -- дозагружались по мере поступления, а запись об ЭМД появлялась при их полном наборе.
    document_attributes AS (
        SELECT
            public.egisz_document_key(public.egisz_xml_text(gr.msgtext, 'localUid')) AS document_key,
            (array_agg(public.egisz_clean_text_value(public.egisz_xml_text(gr.msgtext, 'localUid')) ORDER BY gr.logid)
                FILTER (WHERE NULLIF(btrim(public.egisz_xml_text(gr.msgtext, 'localUid')), '') IS NOT NULL))[1] AS local_uid,
            (array_agg(public.egisz_normalize_semd_code(public.egisz_xml_text(gr.msgtext, 'KIND')) ORDER BY gr.logid)
                FILTER (WHERE public.egisz_normalize_semd_code(public.egisz_xml_text(gr.msgtext, 'KIND')) IS NOT NULL))[1] AS semd_code,
            (array_agg(NULLIF((regexp_match(COALESCE(gr.logtext, '') || ' ' || COALESCE(gr.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer ORDER BY gr.logid)
                FILTER (WHERE NULLIF((regexp_match(COALESCE(gr.logtext, '') || ' ' || COALESCE(gr.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '') IS NOT NULL))[1] AS jid,
            min(COALESCE(gr.createdate, gr.logdate)) AS sent_at,
            max(gr.logid) AS source_logid,
            bool_or(gr.logstate = 3) AS has_network_error,
            max(gr.logid) FILTER (WHERE gr.logstate = 3) AS network_logid,
            max(COALESCE(gr.createdate, gr.logdate)) FILTER (WHERE gr.logstate = 3) AS network_at,
            (array_agg(COALESCE(NULLIF(btrim(gr.logtext), ''), NULLIF(btrim(gr.msgtext), ''), 'Сетевая ошибка') ORDER BY gr.logid DESC)
                FILTER (WHERE gr.logstate = 3))[1] AS network_message
        FROM public.exchangelog_raw gr
        WHERE COALESCE(public.egisz_xml_text(gr.msgtext, 'action'), '') = 'getDocumentFile'
          AND gr.logid <= to_logid
          AND gr.logid >= GREATEST(
                (SELECT COALESCE(MIN(logid), from_logid) FROM document_source_rows) - 500,
                0
              )
          AND NULLIF(btrim(public.egisz_xml_text(gr.msgtext, 'localUid')), '') IS NOT NULL
        GROUP BY 1
        -- только документы, по которым было сообщение в текущем батче
        HAVING max(gr.logid) > from_logid
    )
    INSERT INTO public.fact_egisz_documents (
        document_key, local_uid, semd_code,
        status, status_category, sent_at, first_sent_at, source_logid,
        callback_log_id, last_callback_at, jid, error_type, error_summary, error_text,
        updated_at
    )
    SELECT
        a.document_key,
        a.local_uid,
        a.semd_code,
        CASE WHEN a.has_network_error THEN 'network_error' ELSE 'waiting' END,
        CASE WHEN a.has_network_error THEN 'error' ELSE 'waiting' END,
        a.sent_at,
        a.sent_at,
        a.source_logid,
        CASE WHEN a.has_network_error THEN a.network_logid END,
        CASE WHEN a.has_network_error THEN a.network_at END,
        a.jid,
        CASE WHEN a.has_network_error THEN 'Сетевая ошибка' END,
        CASE WHEN a.has_network_error THEN LEFT(a.network_message, 500) END,
        CASE WHEN a.has_network_error THEN a.network_message END,
        now()
    FROM document_attributes a
    WHERE a.document_key IS NOT NULL
      AND a.local_uid IS NOT NULL
      AND (
          a.has_network_error
          OR (a.jid IS NOT NULL AND a.semd_code IS NOT NULL)
      )
    ON CONFLICT (document_key) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
        semd_code = COALESCE(EXCLUDED.semd_code, public.fact_egisz_documents.semd_code),
        first_sent_at = LEAST(
            COALESCE(public.fact_egisz_documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, public.fact_egisz_documents.first_sent_at)
        ),
        sent_at = LEAST(
            COALESCE(public.fact_egisz_documents.sent_at, EXCLUDED.sent_at),
            COALESCE(EXCLUDED.sent_at, public.fact_egisz_documents.sent_at)
        ),
        status = CASE
            WHEN public.fact_egisz_documents.status IN ('success', 'async_error', 'network_error')
            THEN public.fact_egisz_documents.status
            ELSE EXCLUDED.status
        END,
        status_category = CASE
            WHEN public.fact_egisz_documents.status_category IN ('success', 'error')
            THEN public.fact_egisz_documents.status_category
            ELSE EXCLUDED.status_category
        END,
        callback_log_id = COALESCE(EXCLUDED.callback_log_id, public.fact_egisz_documents.callback_log_id),
        last_callback_at = COALESCE(EXCLUDED.last_callback_at, public.fact_egisz_documents.last_callback_at),
        jid = COALESCE(EXCLUDED.jid, public.fact_egisz_documents.jid),
        error_type = COALESCE(EXCLUDED.error_type, public.fact_egisz_documents.error_type),
        error_summary = COALESCE(EXCLUDED.error_summary, public.fact_egisz_documents.error_summary),
        error_text = COALESCE(EXCLUDED.error_text, public.fact_egisz_documents.error_text),
        source_logid = GREATEST(public.fact_egisz_documents.source_logid, EXCLUDED.source_logid),
        updated_at = now()
    WHERE public.fact_egisz_documents.source_logid IS NULL
       OR public.fact_egisz_documents.source_logid <= EXCLUDED.source_logid;

    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
    ),
    gdf_events AS (
        SELECT
            gr.logid,
            NULLIF((regexp_match(COALESCE(gr.logtext, '') || ' ' || COALESCE(gr.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid,
            public.egisz_document_key(public.egisz_xml_text(gr.msgtext, 'localUid')) AS document_key,
            public.egisz_clean_text_value(public.egisz_xml_text(gr.msgtext, 'localUid')) AS local_uid
        FROM public.exchangelog_raw gr
        WHERE COALESCE(public.egisz_xml_text(gr.msgtext, 'action'), '') = 'getDocumentFile'
          AND gr.logid <= to_logid
          AND gr.logid >= GREATEST(
                (SELECT COALESCE(MIN(c.logid), from_logid) FROM candidate_log_ids c) - 500,
                0
              )
          AND NULLIF(btrim(public.egisz_xml_text(gr.msgtext, 'localUid')), '') IS NOT NULL
          AND NULLIF((regexp_match(COALESCE(gr.logtext, '') || ' ' || COALESCE(gr.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '') IS NOT NULL
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
            public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'localUid')) AS local_uid_xml,
            public.egisz_document_key(public.egisz_xml_text(r.msgtext, 'localUid')) AS document_key_xml,
            public.egisz_xml_text(r.msgtext, 'KIND') AS kind_xml,
            public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'emdrId')) AS emdr_id,
            public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'documentNumber')) AS doc_number,
            public.egisz_clean_text_value(COALESCE(public.egisz_xml_text(r.msgtext, 'organization'), public.egisz_xml_text(r.msgtext, 'organizationOid'))) AS org_oid,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorCode'), public.egisz_xml_text(r.msgtext, 'code')) AS error_code,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorMessage'), public.egisz_xml_text(r.msgtext, 'message'), public.egisz_xml_text(r.msgtext, 'faultstring')) AS xml_message,
            lower(COALESCE(public.egisz_xml_text(r.msgtext, 'status'), '')) AS raw_status,
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload,
            public.safe_cast_timestamptz(COALESCE(public.egisz_xml_text(r.msgtext, 'creationDateTime'), public.egisz_xml_text(r.msgtext, 'creationDate'))) AS creation_date,
            COALESCE(
                public.egisz_xml_text(r.msgtext, 'patientName'),
                public.egisz_xml_text(r.msgtext, 'patientFio'),
                public.egisz_xml_text(r.msgtext, 'fio'),
                public.egisz_xml_text(r.msgtext, 'patient'),
                public.egisz_xml_text(r.msgtext, 'PatientName'),
                NULLIF(concat_ws(
                    ' ',
                    public.egisz_xml_text(r.msgtext, 'familyName'),
                    public.egisz_xml_text(r.msgtext, 'givenName'),
                    public.egisz_xml_text(r.msgtext, 'patronymic')
                ), '')
            ) AS raw_patient_name,
            COALESCE(
                public.egisz_xml_text(r.msgtext, 'snils'),
                public.egisz_xml_text(r.msgtext, 'SNILS'),
                public.egisz_xml_text(r.msgtext, 'patientSnils')
            ) AS raw_snils,
            COALESCE(
                public.egisz_xml_text(r.msgtext, 'doctorName'),
                public.egisz_xml_text(r.msgtext, 'doctorFio'),
                public.egisz_xml_text(r.msgtext, 'physicianName'),
                public.egisz_xml_text(r.msgtext, 'medicalWorkerName'),
                public.egisz_xml_text(r.msgtext, 'authorName'),
                public.egisz_xml_text(r.msgtext, 'doctor')
            ) AS raw_doctor_name
        FROM exchangelog_raw r
        JOIN candidate_log_ids c ON c.logid = r.logid
        WHERE (
              COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
              OR r.logstate = 3
          )
          AND (
              r.logstate = 3
              OR public.egisz_normalize_message_id(r.msgid) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'messageId')) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesToMessage')) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesTo')) IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '') IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'emdrId')), '') IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'documentNumber')), '') IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'KIND')), '') IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'status')), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(public.egisz_xml_text(r.msgtext, 'errorCode'), public.egisz_xml_text(r.msgtext, 'code'))), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(public.egisz_xml_text(r.msgtext, 'errorMessage'), public.egisz_xml_text(r.msgtext, 'message'), public.egisz_xml_text(r.msgtext, 'faultstring'))), '') IS NOT NULL
          )
    ),
    source_documents AS (
        SELECT document_key, semd_code
        FROM public.fact_egisz_documents
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
                r.document_key_xml,
                exch_ref.document_key,
                emdr_ref.document_key,
                gdf_ref.document_key
            ) AS document_key,
            COALESCE(r.local_uid_xml, exch_ref.local_uid, gdf_ref.local_uid) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.egisz_normalize_semd_code(r.kind_xml) AS semd_code,
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
            src_doc.semd_code AS source_document_semd_code
        FROM raw_parsed r
        LEFT JOIN LATERAL (
            SELECT c.document_key, c.local_uid
            FROM (
                SELECT
                    0 AS priority,
                    ref.document_key,
                    ref.local_uid
                FROM public.dim_egisz_exchangelog_refs ref
                WHERE r.relates_to_id IS NOT NULL
                  AND ref.exchange_msgid_norm = r.relates_to_id
                  AND ref.document_key IS NOT NULL

                UNION ALL

                SELECT
                    1,
                    ref.document_key,
                    ref.local_uid
                FROM public.dim_egisz_exchangelog_refs ref
                WHERE r.emdr_id IS NOT NULL
                  AND lower(NULLIF(btrim(ref.emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
                  AND NULLIF(btrim(ref.local_uid), '') IS NOT NULL
                  AND ref.document_key IS NOT NULL

            ) c
            WHERE c.document_key IS NOT NULL
            ORDER BY c.priority, c.document_key
            LIMIT 1
        ) exch_ref ON TRUE
        LEFT JOIN LATERAL (
            SELECT fd.document_key
            FROM public.fact_egisz_documents fd
            WHERE r.emdr_id IS NOT NULL
              AND lower(NULLIF(btrim(fd.emdr_id), '')) = lower(NULLIF(btrim(r.emdr_id), ''))
            ORDER BY fd.last_callback_at DESC NULLS LAST, fd.source_logid DESC NULLS LAST
            LIMIT 1
        ) emdr_ref ON TRUE
        LEFT JOIN LATERAL (
            SELECT g.document_key, g.local_uid
            FROM gdf_events g
            WHERE r.jid_from_payload IS NOT NULL
              AND g.jid = r.jid_from_payload
              AND g.logid < r.logid
            ORDER BY g.logid DESC
            LIMIT 1
        ) gdf_ref ON TRUE
        LEFT JOIN source_documents src_doc
          ON src_doc.document_key = COALESCE(
                r.document_key_xml,
                exch_ref.document_key,
                emdr_ref.document_key,
                gdf_ref.document_key
            )
    ),
    enriched AS (
        SELECT
            p.*,
            p.jid_from_payload AS resolved_jid,
            COALESCE(
                p.semd_code,
                p.source_document_semd_code
            ) AS resolved_semd_code,
            public.egisz_classify_async_status(p.logstate, p.raw_status, p.msgtext, p.logtext) AS final_status,
            CASE
                WHEN p.logstate = 3 THEN 'Сетевая ошибка: ' || COALESCE(NULLIF(p.logtext, ''), 'нет деталей')
                ELSE p.xml_message
            END AS event_message
        FROM parsed p
    ),
    with_errors AS (
        SELECT
            e.*,
            -- errors_json нужен только для error-строк; для success/pending это всегда '[]',
            -- поэтому не гоняем egisz_xml_error_items по payload'у успешных ответов.
            CASE
                WHEN e.final_status = 'error'
                THEN public.egisz_build_errors_json(e.final_status, e.error_code, e.event_message, e.msgtext)
                ELSE '[]'::jsonb
            END AS built_errors_json
        FROM enriched e
    ),
    -- Интерпретация отказа РЭМД дорогая: на каждый <item> идёт регекс-скан 80 правил
    -- egisz_error_interpretation_rules, и эта работа повторяется для одинаковых payload'ов
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
            public.egisz_error_classify(built_errors_json) AS error_type_dict,
            public.egisz_error_interpretation_row(built_errors_json) AS error_summary_dict,
            public.egisz_error_messages_row(built_errors_json) AS error_messages_dict
        FROM error_dict
    ),
    with_bi_fields AS (
        SELECT
            e.*,
            ei.error_type_dict,
            ei.error_summary_dict,
            ei.error_messages_dict,
            regexp_split_to_array(public.egisz_clean_text_value(e.raw_patient_name), '\s+') AS patient_parts,
            regexp_replace(COALESCE(e.raw_snils, ''), '\D', '', 'g') AS snils_digits,
            public.egisz_clean_text_value(e.raw_doctor_name) AS doctor_name_clean
        FROM with_errors e
        LEFT JOIN error_interp ei ON ei.built_errors_json = e.built_errors_json
    )
    INSERT INTO fact_egisz_transactions (
        exchangelog_log_id, document_key, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, message, callback_url, jid, semd_code,
        semd_name, error_code, creation_date, processed_at,
        error_type, error_summary, error_json_text,
        patient_name_masked, snils_masked, doctor_name, patient_hash, doctor_hash
    )
    SELECT
        e.logid, e.document_key, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.event_message, e.logtext,
        e.resolved_jid, e.resolved_semd_code, e.semd_name, e.error_code,
        e.creation_date, now(),
        CASE
            WHEN e.final_status = 'error' AND e.logstate = 3 THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error'   THEN e.error_type_dict
            ELSE NULL  -- success/pending/unknown: видимость через status, error_type не заполняется
        END,
        e.error_summary_dict,
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
      AND e.document_key IS NOT NULL
    ON CONFLICT (exchangelog_log_id, log_date) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        document_key = EXCLUDED.document_key,
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
        semd_code = EXCLUDED.semd_code,
        semd_name = EXCLUDED.semd_name,
        error_code = EXCLUDED.error_code,
        creation_date = EXCLUDED.creation_date,
        processed_at = now(),
        error_type = EXCLUDED.error_type,
        error_summary = EXCLUDED.error_summary,
        error_json_text = EXCLUDED.error_json_text,
        patient_name_masked = EXCLUDED.patient_name_masked,
        snils_masked = EXCLUDED.snils_masked,
        doctor_name = EXCLUDED.doctor_name,
        patient_hash = EXCLUDED.patient_hash,
        doctor_hash = EXCLUDED.doctor_hash;
    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    affected := affected + inserted_rows;

    INSERT INTO public.fact_egisz_documents (
        document_key, local_uid, emdr_id, semd_code,
        status, status_category, message_id, relates_to_id,
        callback_log_id, source_logid, document_created_at, registered_at,
        last_callback_at, last_status, jid, error_type, error_summary, error_text,
        patient_hash, doctor_hash, updated_at
    )
    SELECT DISTINCT ON (f.document_key)
        f.document_key,
        public.egisz_clean_text_value(f.local_uid_semd),
        public.egisz_clean_text_value(f.emdr_id),
        public.egisz_normalize_semd_code(f.semd_code),
        CASE
            WHEN f.status = 'success' THEN 'success'
            WHEN f.status = 'error' AND f.error_type = 'Сетевая ошибка' THEN 'network_error'
            WHEN f.status = 'error' THEN 'async_error'
            ELSE 'waiting'
        END,
        CASE WHEN f.status = 'success' THEN 'success' WHEN f.status = 'error' THEN 'error' ELSE 'waiting' END,
        public.egisz_clean_text_value(f.message_id),
        public.egisz_clean_text_value(f.relates_to_id),
        f.exchangelog_log_id,
        f.exchangelog_log_id,
        f.creation_date,
        CASE WHEN f.status = 'success' THEN f.log_date ELSE NULL::timestamptz END,
        f.log_date,
        f.status,
        f.jid,
        f.error_type,
        f.error_summary,
        COALESCE(NULLIF(btrim(f.error_json_text), ''), f.message),
        f.patient_hash,
        f.doctor_hash,
        now()
    FROM public.fact_egisz_transactions f
    WHERE f.exchangelog_log_id > from_logid
      AND f.exchangelog_log_id <= to_logid
      AND f.document_key IS NOT NULL
    ORDER BY f.document_key, f.log_date DESC NULLS LAST, f.exchangelog_log_id DESC
    ON CONFLICT (document_key) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
        emdr_id = COALESCE(EXCLUDED.emdr_id, public.fact_egisz_documents.emdr_id),
        semd_code = COALESCE(EXCLUDED.semd_code, public.fact_egisz_documents.semd_code),
        status = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.status
            ELSE public.fact_egisz_documents.status
        END,
        status_category = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.status_category
            ELSE public.fact_egisz_documents.status_category
        END,
        message_id = COALESCE(EXCLUDED.message_id, public.fact_egisz_documents.message_id),
        relates_to_id = COALESCE(EXCLUDED.relates_to_id, public.fact_egisz_documents.relates_to_id),
        callback_log_id = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.callback_log_id
            ELSE public.fact_egisz_documents.callback_log_id
        END,
        document_created_at = COALESCE(EXCLUDED.document_created_at, public.fact_egisz_documents.document_created_at),
        registered_at = COALESCE(EXCLUDED.registered_at, public.fact_egisz_documents.registered_at),
        source_logid = GREATEST(COALESCE(public.fact_egisz_documents.source_logid, 0), COALESCE(EXCLUDED.source_logid, 0)),
        last_callback_at = GREATEST(COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz), COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)),
        last_status = COALESCE(EXCLUDED.last_status, public.fact_egisz_documents.last_status),
        jid = COALESCE(EXCLUDED.jid, public.fact_egisz_documents.jid),
        error_type = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_type
            ELSE public.fact_egisz_documents.error_type
        END,
        error_summary = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_summary
            ELSE public.fact_egisz_documents.error_summary
        END,
        error_text = CASE
            WHEN COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)
               >= COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz)
            THEN EXCLUDED.error_text
            ELSE public.fact_egisz_documents.error_text
        END,
        patient_hash = COALESCE(EXCLUDED.patient_hash, public.fact_egisz_documents.patient_hash),
        doctor_hash = COALESCE(EXCLUDED.doctor_hash, public.fact_egisz_documents.doctor_hash),
        updated_at = now();

    -- Колбэк может прийти без KIND в XML, а тип СЭМД уже известен из getDocumentFile (gdf).
    UPDATE public.fact_egisz_documents d
    SET
        semd_code = src.semd_code,
        updated_at = now()
    FROM (
        SELECT DISTINCT ON (t.document_key)
            t.document_key,
            public.egisz_normalize_semd_code(t.semd_code) AS semd_code
        FROM public.fact_egisz_transactions t
        WHERE t.document_key IS NOT NULL
          AND NULLIF(btrim(t.semd_code), '') IS NOT NULL
        ORDER BY t.document_key, t.log_date DESC NULLS LAST, t.exchangelog_log_id DESC
    ) src
    WHERE d.document_key = src.document_key
      AND NULLIF(btrim(d.semd_code), '') IS NULL;

    -- Инкрементальное сопровождение обогащённой витрины: пересобираем строки только по
    -- document_key, реально изменённым в этой транзакции (updated_at = now()), вместо
    -- полного REFRESH MATERIALIZED VIEW каждые 5 минут — стоимость O(батч), а не O(архив).
    -- Защита от полумигрированной БД: работаем, только если витрина уже persistent-таблица
    -- и источник доступен; иначе первичное наполнение делает dwh_init (90_..._finalize.sql).
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = 'v_egisz_documents_enriched_ui'
          AND c.relkind = 'r'
    ) AND to_regclass('public.v_egisz_documents_enriched_src') IS NOT NULL THEN
        DELETE FROM public.v_egisz_documents_enriched_ui e
        WHERE e."Документ (ключ учёта)" IN (
            SELECT d.document_key FROM public.fact_egisz_documents d WHERE d.updated_at = now()
        );

        INSERT INTO public.v_egisz_documents_enriched_ui
        SELECT s.* FROM public.v_egisz_documents_enriched_src s
        WHERE s."Документ (ключ учёта)" IN (
            SELECT d.document_key FROM public.fact_egisz_documents d WHERE d.updated_at = now()
        );
    END IF;

    RETURN affected;
END;
$$;
