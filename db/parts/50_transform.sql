-- ============================================================================
-- 50_transform.sql — egisz_transform_raw_to_facts
-- Source: db/dwh_init.sql, lines [1055..1254).
-- Loaded by db/dwh_init.sql via \i db/parts/50_transform.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

DROP FUNCTION IF EXISTS public.egisz_transform_raw_to_facts(bigint, bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(
    from_logid bigint,
    to_logid bigint,
    from_egmid bigint DEFAULT 0,
    to_egmid bigint DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
    inserted_rows integer := 0;
    message_updates integer := 0;
BEGIN
    WITH current_document_ids AS (
        SELECT DISTINCT document_id_norm
        FROM public.fact_egisz_messages
        WHERE egmid > from_egmid
          AND egmid <= to_egmid
          AND document_id_norm IS NOT NULL
    ),
    document_source_rows AS (
        SELECT sr.*
        FROM public.exchangelog_raw sr
        WHERE COALESCE(public.egisz_xml_text(sr.msgtext, 'action'), '') = 'getDocumentFile'
          AND (
              (sr.logid > from_logid AND sr.logid <= to_logid)
              OR lower(NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '')) IN (SELECT document_id_norm FROM current_document_ids)
              OR lower(NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')), '')) IN (SELECT document_id_norm FROM current_document_ids)
          )
    )
    INSERT INTO public.fact_egisz_documents (document_key, local_uid, document_id, semd_code, source_logid, updated_at)
    SELECT DISTINCT ON (document_key)
        document_key,
        local_uid,
        document_id,
        semd_code,
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
            sr.logid
        FROM document_source_rows sr
        WHERE NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL
          AND public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) IS NOT NULL

        UNION ALL

        SELECT
            public.egisz_document_key(
                public.egisz_xml_text(sr.msgtext, 'localUid'),
                public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')
            ) AS document_key,
            public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'localUid')) AS local_uid,
            public.egisz_clean_text_value(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')) AS document_id,
            public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) AS semd_code,
            sr.logid
        FROM document_source_rows sr
        WHERE NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'DOCUMENTID')), '') IS NOT NULL
          AND public.egisz_normalize_semd_code(public.egisz_xml_text(sr.msgtext, 'KIND')) IS NOT NULL
    ) src
    WHERE document_key IS NOT NULL
    ORDER BY document_key, logid DESC
    ON CONFLICT (document_key) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
        document_id = COALESCE(EXCLUDED.document_id, public.fact_egisz_documents.document_id),
        semd_code = EXCLUDED.semd_code,
        source_logid = GREATEST(public.fact_egisz_documents.source_logid, EXCLUDED.source_logid),
        updated_at = now()
    WHERE public.fact_egisz_documents.source_logid IS NULL
       OR public.fact_egisz_documents.source_logid <= EXCLUDED.source_logid;

    INSERT INTO public.fact_egisz_documents (document_key, document_id, first_sent_at, jid, updated_at)
    SELECT DISTINCT ON (m.document_key)
        m.document_key,
        public.egisz_clean_text_value(m.document_id),
        m.created_at,
        m.reply_to_jid,
        now()
    FROM public.fact_egisz_messages m
    WHERE m.egmid > from_egmid
      AND m.egmid <= to_egmid
      AND m.document_key IS NOT NULL
    ORDER BY m.document_key, m.created_at ASC NULLS LAST, m.egmid ASC
    ON CONFLICT (document_key) DO UPDATE SET
        document_id = COALESCE(public.fact_egisz_documents.document_id, EXCLUDED.document_id),
        first_sent_at = LEAST(
            COALESCE(public.fact_egisz_documents.first_sent_at, EXCLUDED.first_sent_at),
            COALESCE(EXCLUDED.first_sent_at, public.fact_egisz_documents.first_sent_at)
        ),
        jid = COALESCE(public.fact_egisz_documents.jid, EXCLUDED.jid),
        updated_at = now();

    INSERT INTO public.fact_egisz_channel_errors (
        id, created_at, error_code, message, error_top_type, error_global_subcategory,
        error_group_label_ru, exchangelog_log_id, journal_msgid, egisz_messages_egmid,
        relates_to_hint, local_uid_hint, emdr_id_hint, document_key, relates_to_id,
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
        m.egmid AS egisz_messages_egmid,
        COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext, x.relates_to_message_logtext) AS relates_to_hint,
        COALESCE(x.local_uid_msgtext, x.document_id_msgtext, m.document_id) AS local_uid_hint,
        x.emdr_id_msgtext AS emdr_id_hint,
        COALESCE(
            public.egisz_document_key(x.local_uid_msgtext, x.document_id_msgtext, x.emdr_id_msgtext),
            public.egisz_document_key(m.document_id, m.document_id)
        ) AS document_key,
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
    LEFT JOIN LATERAL (
        SELECT em.*
        FROM public.fact_egisz_messages em
        WHERE em.document_id_norm IN (
                lower(NULLIF(btrim(x.local_uid_msgtext), '')),
                lower(NULLIF(btrim(x.document_id_msgtext), ''))
              )
           OR em.msgid_norm = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext))
           OR em.msgid_norm = public.egisz_normalize_message_id(r.msgid)
        ORDER BY
            CASE
                WHEN em.document_id_norm IN (
                    lower(NULLIF(btrim(x.local_uid_msgtext), '')),
                    lower(NULLIF(btrim(x.document_id_msgtext), ''))
                ) THEN 0
                WHEN em.msgid_norm = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext)) THEN 1
                ELSE 2
            END,
            em.egmid DESC
        LIMIT 1
    ) m ON TRUE
    WHERE r.logid > from_logid
      AND r.logid <= to_logid
      AND (
          r.logstate = 3
          OR COALESCE(r.msgtext, '') ILIKE '%error%'
          OR COALESCE(r.logtext, '') ILIKE '%error%'
          OR COALESCE(r.logtext, '') ILIKE '%ошиб%'
      )
      AND COALESCE(
          public.egisz_document_key(x.local_uid_msgtext, x.document_id_msgtext, x.emdr_id_msgtext),
          public.egisz_document_key(m.document_id, m.document_id)
      ) IS NOT NULL
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        error_code = EXCLUDED.error_code,
        message = EXCLUDED.message,
        error_top_type = EXCLUDED.error_top_type,
        error_global_subcategory = EXCLUDED.error_global_subcategory,
        error_group_label_ru = EXCLUDED.error_group_label_ru,
        exchangelog_log_id = EXCLUDED.exchangelog_log_id,
        journal_msgid = EXCLUDED.journal_msgid,
        egisz_messages_egmid = EXCLUDED.egisz_messages_egmid,
        relates_to_hint = EXCLUDED.relates_to_hint,
        local_uid_hint = EXCLUDED.local_uid_hint,
        emdr_id_hint = EXCLUDED.emdr_id_hint,
        document_key = EXCLUDED.document_key,
        relates_to_id = EXCLUDED.relates_to_id,
        updated_at = now();

    WITH current_messages AS (
        SELECT DISTINCT
            em.msgid_norm,
            em.document_id_norm
        FROM public.fact_egisz_messages em
        WHERE em.egmid > from_egmid
          AND em.egmid <= to_egmid
    ),
    current_message_ids AS (
        SELECT msgid_norm
        FROM current_messages
        WHERE msgid_norm IS NOT NULL
    ),
    current_document_ids AS (
        SELECT document_id_norm
        FROM current_messages
        WHERE document_id_norm IS NOT NULL
    ),
    candidate_log_ids AS (
        -- LOG-id window: rows in the freshly extracted EXCHANGELOG batch
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > from_logid
          AND r.logid <= to_logid

        UNION

        -- EGMID window: re-process EXCHANGELOG rows whose linked EGISZ_MESSAGES row
        -- arrived in the current batch (late callback to an older request).
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE public.egisz_normalize_message_id(r.msgid) IN (SELECT msgid_norm FROM current_message_ids)

        UNION

        SELECT r.logid
        FROM exchangelog_raw r
        WHERE public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'messageId')) IN (SELECT msgid_norm FROM current_message_ids)

        UNION

        SELECT r.logid
        FROM exchangelog_raw r
        WHERE public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesToMessage')) IN (SELECT msgid_norm FROM current_message_ids)

        UNION

        SELECT r.logid
        FROM exchangelog_raw r
        WHERE public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesTo')) IN (SELECT msgid_norm FROM current_message_ids)

        UNION

        SELECT r.logid
        FROM exchangelog_raw r
        WHERE lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '')) IN (SELECT document_id_norm FROM current_document_ids)

        UNION

        SELECT r.logid
        FROM exchangelog_raw r
        WHERE lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')), '')) IN (SELECT document_id_norm FROM current_document_ids)
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
            COALESCE(m.created_at, r.createdate) AS logdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            r.message_id,
            r.relates_to_id,
            COALESCE(r.document_key_xml, public.egisz_document_key(m.document_id, m.document_id)) AS document_key,
            COALESCE(r.local_uid_xml, r.document_id_xml, public.egisz_clean_text_value(m.document_id)) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.egisz_normalize_semd_code(r.kind_xml) AS semd_code,
            NULL::text AS semd_name,
            r.error_code,
            r.xml_message,
            r.raw_status,
            r.jid_from_payload,
            r.creation_date,
            r.raw_patient_name,
            r.raw_snils,
            r.raw_doctor_name,
            m.egmid,
            m.license_jid AS message_jid,
            src_doc.semd_code AS source_document_semd_code
        FROM raw_parsed r
        LEFT JOIN LATERAL (
            SELECT candidate.*
            FROM (
                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, 0 AS priority
                FROM public.fact_egisz_messages em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = em.reply_to_host
                WHERE em.document_id_norm IN (
                    lower(NULLIF(btrim(r.local_uid_xml), '')),
                    lower(NULLIF(btrim(r.document_id_xml), ''))
                )

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, 1 AS priority
                FROM public.fact_egisz_messages em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = em.reply_to_host
                WHERE em.msgid_norm = r.relates_to_id

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, 2 AS priority
                FROM public.fact_egisz_messages em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = em.reply_to_host
                WHERE em.msgid_norm = r.message_id
            ) candidate
            ORDER BY candidate.priority, candidate.egmid DESC
            LIMIT 1
        ) m ON TRUE
        LEFT JOIN source_documents src_doc
          ON src_doc.document_key = COALESCE(r.document_key_xml, public.egisz_document_key(m.document_id, m.document_id))
    ),
    enriched AS (
        SELECT
            p.*,
            COALESCE(p.message_jid, p.jid_from_payload) AS resolved_jid,
            COALESCE(
                p.semd_code,
                p.source_document_semd_code
            ) AS resolved_semd_code,
            public.egisz_classify_async_status(p.logstate, p.raw_status, p.msgtext, p.logtext) AS final_status,
            CASE
                WHEN p.logstate = 3 THEN 'Сетевая ошибка: ' || COALESCE(NULLIF(p.logtext, ''), 'нет деталей')
                ELSE p.xml_message
            END AS final_error_message
        FROM parsed p
    ),
    with_errors AS (
        SELECT
            e.*,
            public.egisz_build_errors_json(e.final_status, e.error_code, e.final_error_message, e.msgtext) AS built_errors_json
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
        doc_number, org_oid, status, error_message, callback_url, egmid, jid, semd_code,
        semd_name, error_code, creation_date, processed_at,
        error_type, error_summary, error_json_text,
        patient_name_masked, snils_masked, doctor_name, patient_hash, doctor_hash
    )
    SELECT
        e.logid, e.document_key, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.final_error_message, e.logtext, e.egmid,
        e.resolved_jid, e.resolved_semd_code, e.semd_name, e.error_code,
        e.creation_date, now(),
        CASE
            WHEN e.final_status = 'error' AND e.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
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
        error_message = EXCLUDED.error_message,
        callback_url = EXCLUDED.callback_url,
        egmid = EXCLUDED.egmid,
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

    WITH current_messages AS (
        SELECT *
        FROM public.fact_egisz_messages em
        WHERE em.egmid > from_egmid
          AND em.egmid <= to_egmid
    ),
    message_matches AS (
        SELECT DISTINCT ON (f.exchangelog_log_id)
            f.exchangelog_log_id,
            m.egmid,
            COALESCE(f.document_key, m.document_key) AS document_key,
            COALESCE(f.local_uid_semd, m.document_id) AS local_uid_semd,
            COALESCE(f.jid, l.jid, m.reply_to_jid) AS jid,
            d.semd_code AS source_document_semd_code
        FROM public.fact_egisz_transactions f
        JOIN current_messages m
          ON m.document_key IN (
                f.document_key,
                public.egisz_document_key(f.local_uid_semd, f.doc_number, f.emdr_id)
             )
          OR m.msgid_norm = public.egisz_normalize_message_id(f.relates_to_id)
          OR m.msgid_norm = public.egisz_normalize_message_id(f.message_id)
        LEFT JOIN public.dim_licenses l
          ON m.reply_to_host IS NOT NULL
         AND public.egisz_clean_host(l.mo_domen) = m.reply_to_host
        LEFT JOIN public.fact_egisz_documents d
          ON d.document_key = m.document_key
        ORDER BY
            f.exchangelog_log_id,
            CASE
                WHEN m.document_key = f.document_key THEN 0
                WHEN m.document_key = public.egisz_document_key(f.local_uid_semd, f.doc_number, f.emdr_id) THEN 1
                WHEN m.msgid_norm = public.egisz_normalize_message_id(f.relates_to_id) THEN 1
                ELSE 2
            END,
            m.egmid DESC
    )
    UPDATE public.fact_egisz_transactions f
    SET egmid = m.egmid,
        document_key = m.document_key,
        local_uid_semd = m.local_uid_semd,
        jid = m.jid,
        semd_code = COALESCE(f.semd_code, m.source_document_semd_code),
        processed_at = now()
    FROM message_matches m
    WHERE m.exchangelog_log_id = f.exchangelog_log_id
      AND (
          f.egmid IS DISTINCT FROM m.egmid
          OR f.document_key IS DISTINCT FROM m.document_key
          OR f.local_uid_semd IS DISTINCT FROM m.local_uid_semd
          OR f.jid IS DISTINCT FROM m.jid
          OR (f.semd_code IS NULL AND m.source_document_semd_code IS NOT NULL)
      );
    GET DIAGNOSTICS message_updates = ROW_COUNT;
    affected := affected + message_updates;

    INSERT INTO public.fact_egisz_documents (
        document_key, local_uid, document_id, emdr_id, semd_code,
        source_logid, last_callback_at, last_status, last_egmid, jid, updated_at
    )
    SELECT DISTINCT ON (f.document_key)
        f.document_key,
        public.egisz_clean_text_value(f.local_uid_semd),
        public.egisz_clean_text_value(f.doc_number),
        public.egisz_clean_text_value(f.emdr_id),
        public.egisz_normalize_semd_code(f.semd_code),
        f.exchangelog_log_id,
        f.log_date,
        f.status,
        f.egmid,
        f.jid,
        now()
    FROM public.fact_egisz_transactions f
    WHERE (
          (f.exchangelog_log_id > from_logid AND f.exchangelog_log_id <= to_logid)
          OR (f.egmid > from_egmid AND f.egmid <= to_egmid)
      )
      AND f.document_key IS NOT NULL
    ORDER BY f.document_key, f.log_date DESC NULLS LAST, f.exchangelog_log_id DESC
    ON CONFLICT (document_key) DO UPDATE SET
        local_uid = COALESCE(EXCLUDED.local_uid, public.fact_egisz_documents.local_uid),
        document_id = COALESCE(EXCLUDED.document_id, public.fact_egisz_documents.document_id),
        emdr_id = COALESCE(EXCLUDED.emdr_id, public.fact_egisz_documents.emdr_id),
        semd_code = COALESCE(EXCLUDED.semd_code, public.fact_egisz_documents.semd_code),
        source_logid = GREATEST(COALESCE(public.fact_egisz_documents.source_logid, 0), COALESCE(EXCLUDED.source_logid, 0)),
        last_callback_at = GREATEST(COALESCE(public.fact_egisz_documents.last_callback_at, '-infinity'::timestamptz), COALESCE(EXCLUDED.last_callback_at, '-infinity'::timestamptz)),
        last_status = COALESCE(EXCLUDED.last_status, public.fact_egisz_documents.last_status),
        last_egmid = COALESCE(EXCLUDED.last_egmid, public.fact_egisz_documents.last_egmid),
        jid = COALESCE(EXCLUDED.jid, public.fact_egisz_documents.jid),
        updated_at = now();

    RETURN affected;
END;
$$;
