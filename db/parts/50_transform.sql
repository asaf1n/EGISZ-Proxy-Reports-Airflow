-- ============================================================================
-- 50_transform.sql — egisz_transform_raw_to_facts
-- Source: db/dwh_init.sql, lines [1055..1254).
-- Loaded by db/dwh_init.sql via \i db/parts/50_transform.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

DROP FUNCTION IF EXISTS public.egisz_transform_raw_to_facts(bigint, bigint, bigint, bigint);
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
        local_uid, document_id, emdr_id, document_key, updated_at
    )
    SELECT
        r.logid,
        COALESCE(r.createdate, r.loaded_at) AS created_at,
        r.msgid,
        public.egisz_normalize_message_id(
            COALESCE(public.egisz_xml_text(r.msgtext, 'messageId'), r.msgid)
        ) AS exchange_msgid_norm,
        public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'localUid')) AS local_uid,
        public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')) AS document_id,
        public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'emdrId')) AS emdr_id,
        public.egisz_document_key(
            public.egisz_xml_text(r.msgtext, 'localUid'),
            public.egisz_xml_text(r.msgtext, 'DOCUMENTID'),
            public.egisz_xml_text(r.msgtext, 'emdrId')
        ) AS document_key,
        now()
    FROM public.exchangelog_raw r
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND (
          public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'messageId'), r.msgid)) IS NOT NULL
          OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '') IS NOT NULL
          OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')), '') IS NOT NULL
          OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'emdrId')), '') IS NOT NULL
      )
    ON CONFLICT (logid) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        exchange_msgid = EXCLUDED.exchange_msgid,
        exchange_msgid_norm = EXCLUDED.exchange_msgid_norm,
        local_uid = COALESCE(EXCLUDED.local_uid, public.dim_egisz_exchangelog_refs.local_uid),
        document_id = COALESCE(EXCLUDED.document_id, public.dim_egisz_exchangelog_refs.document_id),
        emdr_id = COALESCE(EXCLUDED.emdr_id, public.dim_egisz_exchangelog_refs.emdr_id),
        document_key = COALESCE(EXCLUDED.document_key, public.dim_egisz_exchangelog_refs.document_key),
        updated_at = now();

    WITH document_source_rows AS (
        SELECT sr.*
        FROM public.exchangelog_raw sr
        WHERE COALESCE(public.egisz_xml_text(sr.msgtext, 'action'), '') = 'getDocumentFile'
          AND sr.logid > from_logid
          AND sr.logid <= to_logid
    )
    INSERT INTO public.fact_egisz_documents (
        document_key, local_uid, document_id, semd_code,
        status, status_category, sent_at, first_sent_at, source_logid, updated_at
    )
    SELECT DISTINCT ON (document_key)
        document_key,
        local_uid,
        document_id,
        semd_code,
        'waiting',
        'waiting',
        sent_at,
        sent_at,
        logid,
        now()
    FROM (
        SELECT
            public.egisz_document_key(
                public.egisz_xml_text(sr.msgtext, 'localUid'),
                public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')
            ) AS document_key,
            public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'localUid')) AS local_uid,
            public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')) AS document_id,
            public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) AS semd_code,
            COALESCE(sr.createdate, sr.logdate) AS sent_at,
            sr.logid
        FROM document_source_rows sr
        WHERE NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL
          AND public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) IS NOT NULL
    ) src
    WHERE document_key IS NOT NULL
    ORDER BY document_key, logid DESC
    ON CONFLICT (document_key) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
        document_id = COALESCE(EXCLUDED.document_id, public.fact_egisz_documents.document_id),
        semd_code = EXCLUDED.semd_code,
        first_sent_at = LEAST(
            COALESCE(public.fact_egisz_documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, public.fact_egisz_documents.first_sent_at)
        ),
        sent_at = LEAST(
            COALESCE(public.fact_egisz_documents.sent_at, EXCLUDED.sent_at),
            COALESCE(EXCLUDED.sent_at, public.fact_egisz_documents.sent_at)
        ),
        status = CASE
            WHEN public.fact_egisz_documents.status IN ('success', 'registration_error', 'network_error')
            THEN public.fact_egisz_documents.status
            ELSE EXCLUDED.status
        END,
        status_category = CASE
            WHEN public.fact_egisz_documents.status_category IN ('success', 'error')
            THEN public.fact_egisz_documents.status_category
            ELSE EXCLUDED.status_category
        END,
        source_logid = GREATEST(public.fact_egisz_documents.source_logid, EXCLUDED.source_logid),
        updated_at = now()
    WHERE public.fact_egisz_documents.source_logid IS NULL
       OR public.fact_egisz_documents.source_logid <= EXCLUDED.source_logid;

    INSERT INTO public.fact_egisz_channel_errors (
        id, created_at, error_code, message, error_top_type, error_global_subcategory,
        error_group_label_ru, exchangelog_log_id, journal_msgid,
        relates_to_hint, local_uid_hint, emdr_id_hint, document_key, jid, relates_to_id,
        updated_at
    )
    SELECT
        r.logid AS id,
        COALESCE(r.createdate, r.loaded_at) AS created_at,
        CASE WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3' ELSE 'PARSE_ERROR' END AS error_code,
        COALESCE(NULLIF(r.logtext, ''), NULLIF(r.msgtext, ''), '(без текста)') AS message,
        CASE WHEN r.logstate = 3 THEN 'network' ELSE 'async_response' END AS error_top_type,
        CASE WHEN r.logstate = 3 THEN 'Сетевая ошибка' ELSE 'Неизвестная ошибка' END AS error_global_subcategory,
        CASE WHEN r.logstate = 3 THEN 'Ошибка связи' ELSE 'Неизвестная ошибка' END AS error_group_label_ru,
        r.logid AS exchangelog_log_id,
        r.msgid AS journal_msgid,
        COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext, x.relates_to_message_logtext) AS relates_to_hint,
        COALESCE(x.local_uid_msgtext, x.document_id_msgtext) AS local_uid_hint,
        x.emdr_id_msgtext AS emdr_id_hint,
        public.egisz_document_key(x.local_uid_msgtext, x.document_id_msgtext, x.emdr_id_msgtext) AS document_key,
        NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid,
        COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext) AS relates_to_id,
        now()
    FROM public.exchangelog_raw r
    LEFT JOIN LATERAL (
        SELECT
            public.egisz_xml_text(r.msgtext, 'relatesToMessage') AS relates_to_message_msgtext,
            public.egisz_xml_text(r.msgtext, 'relatesTo') AS relates_to_msgtext,
            public.egisz_xml_text(r.logtext, 'relatesToMessage') AS relates_to_message_logtext,
            public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_msgtext,
            public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_msgtext,
            public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id_msgtext
    ) x ON TRUE
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND (
          r.logstate = 3
          OR COALESCE(r.msgtext, '') ILIKE '%error%'
          OR COALESCE(r.logtext, '') ILIKE '%error%'
          OR COALESCE(r.logtext, '') ILIKE '%ошиб%'
      )
      AND public.egisz_document_key(x.local_uid_msgtext, x.document_id_msgtext, x.emdr_id_msgtext) IS NOT NULL
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        error_code = EXCLUDED.error_code,
        message = EXCLUDED.message,
        error_top_type = EXCLUDED.error_top_type,
        error_global_subcategory = EXCLUDED.error_global_subcategory,
        error_group_label_ru = EXCLUDED.error_group_label_ru,
        exchangelog_log_id = EXCLUDED.exchangelog_log_id,
        journal_msgid = EXCLUDED.journal_msgid,
        relates_to_hint = EXCLUDED.relates_to_hint,
        local_uid_hint = EXCLUDED.local_uid_hint,
        emdr_id_hint = EXCLUDED.emdr_id_hint,
        document_key = EXCLUDED.document_key,
        jid = EXCLUDED.jid,
        relates_to_id = EXCLUDED.relates_to_id,
        updated_at = now();

    WITH candidate_log_ids AS (
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid
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
            public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')) AS document_id_xml,
            public.egisz_document_key(
                public.egisz_xml_text(r.msgtext, 'localUid'),
                public.egisz_xml_text(r.msgtext, 'DOCUMENTID'),
                public.egisz_xml_text(r.msgtext, 'emdrId')
            ) AS document_key_xml,
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
        WHERE COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
          AND (
              r.logstate = 3
              OR public.egisz_normalize_message_id(r.msgid) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'messageId')) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesToMessage')) IS NOT NULL
              OR public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesTo')) IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '') IS NOT NULL
              OR NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')), '') IS NOT NULL
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
                emdr_ref.document_key
            ) AS document_key,
            COALESCE(r.local_uid_xml, exch_ref.local_uid) AS local_uid_semd,
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

                UNION ALL

                SELECT
                    2,
                    ref.document_key,
                    ref.local_uid
                FROM public.dim_egisz_exchangelog_refs ref
                WHERE r.document_id_xml IS NOT NULL
                  AND lower(NULLIF(btrim(ref.document_id), '')) = lower(NULLIF(btrim(r.document_id_xml), ''))
                  AND ref.document_key IS NOT NULL

                UNION ALL

                SELECT
                    3,
                    public.egisz_document_key(
                        public.egisz_xml_text(er.msgtext, 'localUid'),
                        public.egisz_xml_text(er.msgtext, 'DOCUMENTID'),
                        public.egisz_xml_text(er.msgtext, 'emdrId')
                    ),
                    public.egisz_clean_text_value(public.egisz_xml_text(er.msgtext, 'localUid'))
                FROM public.exchangelog_raw er
                WHERE r.relates_to_id IS NOT NULL
                  AND public.egisz_normalize_message_id(
                        COALESCE(public.egisz_xml_text(er.msgtext, 'messageId'), er.msgid)
                      ) = r.relates_to_id
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
        LEFT JOIN source_documents src_doc
          ON src_doc.document_key = COALESCE(
                r.document_key_xml,
                exch_ref.document_key,
                emdr_ref.document_key
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
            public.egisz_build_errors_json(e.final_status, e.error_code, e.event_message, e.msgtext) AS built_errors_json
        FROM enriched e
    ),
    with_bi_fields AS (
        SELECT
            e.*,
            regexp_split_to_array(public.egisz_clean_text_value(e.raw_patient_name), '\s+') AS patient_parts,
            regexp_replace(COALESCE(e.raw_snils, ''), '\D', '', 'g') AS snils_digits,
            public.egisz_clean_text_value(e.raw_doctor_name) AS doctor_name_clean
        FROM with_errors e
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
            WHEN e.final_status = 'error'   THEN public.egisz_error_classify(e.built_errors_json)
            ELSE NULL  -- success/pending/unknown: видимость через status, error_type не заполняется
        END,
        public.egisz_error_interpretation_row(e.built_errors_json),
        public.egisz_error_messages_row(e.built_errors_json),
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
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
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
        document_key, local_uid, document_id, emdr_id, semd_code,
        status, status_category, message_id, relates_to_id,
        callback_log_id, source_logid, document_created_at, registered_at,
        last_callback_at, last_status, jid, error_type, error_summary, error_text,
        patient_hash, doctor_hash, updated_at
    )
    SELECT DISTINCT ON (f.document_key)
        f.document_key,
        public.egisz_clean_text_value(f.local_uid_semd),
        NULL::text,
        public.egisz_clean_text_value(f.emdr_id),
        public.egisz_normalize_semd_code(f.semd_code),
        CASE
            WHEN f.status = 'success' THEN 'success'
            WHEN f.status = 'error' AND f.error_type = 'Сетевая ошибка' THEN 'network_error'
            WHEN f.status = 'error' THEN 'registration_error'
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
        document_id = COALESCE(EXCLUDED.document_id, public.fact_egisz_documents.document_id),
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

    RETURN affected;
END;
$$;
