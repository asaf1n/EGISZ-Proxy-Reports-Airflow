-- ============================================================================
-- 50_transform.sql — egisz_transform_raw_to_facts
-- Source: db/dwh_init.sql, lines [1055..1254).
-- Loaded by db/dwh_init.sql via \i db/parts/50_transform.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(
    min_log_id bigint,
    max_log_id bigint,
    min_egmid bigint DEFAULT 0,
    max_egmid bigint DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
BEGIN
    WITH candidate_log_ids AS (
        -- LOG-id window: rows in the freshly extracted EXCHANGELOG batch
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > min_log_id
          AND r.logid <= max_log_id

        UNION

        -- EGMID window: re-process EXCHANGELOG rows whose linked EGISZ_MESSAGES row
        -- arrived in the current batch (late callback to an older request).
        SELECT DISTINCT r.logid
        FROM exchangelog_raw r
        JOIN egisz_messages_raw em
          ON em.egmid > min_egmid
         AND em.egmid <= max_egmid
         AND (
              public.egisz_normalize_message_id(em.msgid) IN (
                  public.egisz_normalize_message_id(r.msgid),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'messageId')),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesToMessage')),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesTo'))
              )
              OR lower(NULLIF(btrim(em.document_id), '')) IN (
                  lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '')),
                  lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')), ''))
              )
         )
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
            public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_xml,
            public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_xml,
            public.egisz_xml_text(r.msgtext, 'kind') AS kind_xml,
            public.egisz_xml_text(r.msgtext, 'KIND') AS kind_upper_xml,
            public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id,
            public.egisz_xml_text(r.msgtext, 'documentNumber') AS doc_number,
            COALESCE(public.egisz_xml_text(r.msgtext, 'organization'), public.egisz_xml_text(r.msgtext, 'organizationOid')) AS org_oid,
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'), public.egisz_xml_text(r.msgtext, 'name'), public.egisz_xml_text(r.msgtext, 'documentName')) AS semd_name,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorCode'), public.egisz_xml_text(r.msgtext, 'code')) AS error_code,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorMessage'), public.egisz_xml_text(r.msgtext, 'message'), public.egisz_xml_text(r.msgtext, 'faultstring')) AS xml_message,
            lower(COALESCE(public.egisz_xml_text(r.msgtext, 'status'), '')) AS raw_status,
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload,
            public.safe_cast_timestamptz(COALESCE(public.egisz_xml_text(r.msgtext, 'creationDateTime'), public.egisz_xml_text(r.msgtext, 'creationDate'))) AS creation_date
        FROM exchangelog_raw r
        JOIN candidate_log_ids c ON c.logid = r.logid
        WHERE COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
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
            COALESCE(r.local_uid_xml, r.document_id_xml, m.document_id) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.egisz_normalize_semd_code(COALESCE(r.kind_xml, r.kind_upper_xml)) AS semd_code,
            public.egisz_clean_text_value(r.semd_name) AS semd_name,
            r.error_code,
            r.xml_message,
            r.raw_status,
            r.jid_from_payload,
            r.creation_date,
            m.egmid,
            m.license_jid AS message_jid,
            public.egisz_normalize_semd_code(m.license_kind) AS message_kind
        FROM raw_parsed r
        LEFT JOIN LATERAL (
            SELECT candidate.*
            FROM (
                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 0 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE lower(NULLIF(btrim(em.document_id), '')) IN (
                    lower(NULLIF(btrim(r.local_uid_xml), '')),
                    lower(NULLIF(btrim(r.document_id_xml), '')),
                    lower(NULLIF(btrim(r.emdr_id), ''))
                )

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 1 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE public.egisz_normalize_message_id(em.msgid) = r.relates_to_id

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 2 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE public.egisz_normalize_message_id(em.msgid) = r.message_id
            ) candidate
            ORDER BY candidate.priority, candidate.egmid DESC
            LIMIT 1
        ) m ON TRUE
    ),
    enriched AS (
        SELECT
            p.*,
            COALESCE(p.message_jid, p.jid_from_payload) AS resolved_jid,
            COALESCE(p.semd_code, p.message_kind) AS resolved_semd_code,
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
    )
    INSERT INTO fact_egisz_transactions (
        exchangelog_log_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, error_message, callback_url, egmid, jid, semd_code,
        semd_name, error_code, creation_date, processed_at,
        error_type, error_summary, error_json_text
    )
    SELECT
        e.logid, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.final_error_message, e.logtext, e.egmid,
        e.resolved_jid, e.resolved_semd_code, e.semd_name, e.error_code,
        e.creation_date, now(),
        CASE
            WHEN e.final_status = 'error' AND e.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error'   THEN public.egisz_error_classify(e.built_errors_json)
            WHEN e.final_status = 'success' THEN 'Успешно'
            ELSE NULL  -- pending/unknown: видимость через status, error_type не заполняется
        END,
        public.egisz_error_interpretation_row(e.built_errors_json),
        public.egisz_error_messages_row(e.built_errors_json)
    FROM with_errors e
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
        log_date = EXCLUDED.log_date,
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
        error_json_text = EXCLUDED.error_json_text;
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

