-- ============================================================================
-- 70_views_core.sql — document_attributes (1:1 к documents)
-- Loaded by db/dwh_init.sql via \i db/parts/70_views_core.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.document_attributes (
    dwh_id text PRIMARY KEY,
    clinic_oid_xml text,
    clinic_oid_jpersons text,
    clinic_oid_license text,
    clinic_host text,
    clinic_jid_resolve_method text,
    message_endpoint text,
    clinic_jid_mismatch boolean,
    patient_name_masked text,
    snils_masked text,
    doctor_name text,
    patient_hash text,
    doctor_hash text,
    request_msgid text,
    updated_at timestamptz DEFAULT now()
);

-- request_msgid — MSGID исходящего запроса (getDocumentFile), нормализованный source MSGID строки
-- request_logid. На грейне документа его нет (documents.result_msgid — это MSGID ответа РЭМД), а
-- для «Отправлено» нужен именно MSGID запроса: пара request_msgid ↔ relates_to_msgid (relatesTo
-- ответа) — штатный ключ корреляции запрос↔ответ (README §«Парсинг», офиц. request_id/response_to_request_id).
ALTER TABLE public.document_attributes ADD COLUMN IF NOT EXISTS request_msgid text;

CREATE INDEX IF NOT EXISTS idx_document_attributes_updated_at
    ON public.document_attributes (updated_at);

-- Пересборка атрибутов документа из documents + справочников + последнего callback.
CREATE OR REPLACE FUNCTION public.reconcile_document_attributes(p_dwh_ids text[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    refreshed bigint := 0;
BEGIN
    IF p_dwh_ids IS NULL THEN
        SELECT COALESCE(array_agg(d.dwh_id), ARRAY[]::text[])
        INTO p_dwh_ids
        FROM public.documents d
        WHERE d.dwh_id IS NOT NULL;
    END IF;

    IF COALESCE(cardinality(p_dwh_ids), 0) = 0 THEN
        RETURN 0;
    END IF;

    INSERT INTO public.document_attributes (
        dwh_id,
        clinic_oid_xml,
        clinic_oid_jpersons,
        clinic_oid_license,
        clinic_host,
        clinic_jid_resolve_method,
        message_endpoint,
        clinic_jid_mismatch,
        patient_name_masked,
        snils_masked,
        doctor_name,
        patient_hash,
        doctor_hash,
        request_msgid,
        updated_at
    )
    SELECT
        d.dwh_id,
        public.clean_text_value(d.org_oid) AS clinic_oid_xml,
        o.fir_oid AS clinic_oid_jpersons,
        l.mo_uid AS clinic_oid_license,
        public.clean_host(l.mo_domen) AS clinic_host,
        d.jid_resolve_method AS clinic_jid_resolve_method,
        ep.endpoint AS message_endpoint,
        public.document_source_mismatch(
            d.jid_resolve_method,
            d.org_oid,
            o.fir_oid,
            l.mo_uid
        ) AS clinic_jid_mismatch,
        tx.patient_name_masked,
        tx.snils_masked,
        tx.doctor_name,
        COALESCE(tx.patient_hash, d.patient_hash) AS patient_hash,
        COALESCE(tx.doctor_hash, d.doctor_hash) AS doctor_hash,
        req.request_msgid,
        now() AS updated_at
    FROM public.documents d
    LEFT JOIN public.dim_organizations o ON o.jid = d.jid
    LEFT JOIN LATERAL (
        SELECT dl.*
        FROM public.dim_licenses dl
        WHERE d.jid IS NOT NULL AND dl.jid = d.jid
        ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC
        LIMIT 1
    ) l ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            t.patient_name_masked,
            t.snils_masked,
            t.doctor_name,
            t.patient_hash,
            t.doctor_hash
        FROM public.transactions t
        WHERE t.dwh_id = d.dwh_id
        ORDER BY t.log_date DESC NULLS LAST, t.logid DESC
        LIMIT 1
    ) tx ON TRUE
    LEFT JOIN LATERAL (
        SELECT public.extract_gost_endpoint(COALESCE(tx.xml_message, '')) AS endpoint
        FROM public.transactions tx
        WHERE tx.logid = COALESCE(d.result_logid, d.request_logid)
        LIMIT 1
    ) ep ON TRUE
    LEFT JOIN LATERAL (
        SELECT t.source_message_id_norm AS request_msgid
        FROM public.transactions t
        WHERE t.logid = d.request_logid
          AND t.source_message_id_norm IS NOT NULL
        LIMIT 1
    ) req ON TRUE
    WHERE d.dwh_id = ANY (p_dwh_ids)
    ON CONFLICT (dwh_id) DO UPDATE SET
        clinic_oid_xml = EXCLUDED.clinic_oid_xml,
        clinic_oid_jpersons = EXCLUDED.clinic_oid_jpersons,
        clinic_oid_license = EXCLUDED.clinic_oid_license,
        clinic_host = EXCLUDED.clinic_host,
        clinic_jid_resolve_method = EXCLUDED.clinic_jid_resolve_method,
        message_endpoint = EXCLUDED.message_endpoint,
        clinic_jid_mismatch = EXCLUDED.clinic_jid_mismatch,
        patient_name_masked = EXCLUDED.patient_name_masked,
        snils_masked = EXCLUDED.snils_masked,
        doctor_name = EXCLUDED.doctor_name,
        patient_hash = EXCLUDED.patient_hash,
        doctor_hash = EXCLUDED.doctor_hash,
        request_msgid = EXCLUDED.request_msgid,
        updated_at = now()
    -- Change-guard: переписываем строку (и двигаем updated_at) только при реальном
    -- расхождении. Без него полный reconcile (в т.ч. на каждом dwh_init) переписывал
    -- весь архив и менял updated_at — повторный прогон не был no-op (CLAUDE.md §3).
    WHERE
        public.document_attributes.clinic_oid_xml IS DISTINCT FROM EXCLUDED.clinic_oid_xml
     OR public.document_attributes.clinic_oid_jpersons IS DISTINCT FROM EXCLUDED.clinic_oid_jpersons
     OR public.document_attributes.clinic_oid_license IS DISTINCT FROM EXCLUDED.clinic_oid_license
     OR public.document_attributes.clinic_host IS DISTINCT FROM EXCLUDED.clinic_host
     OR public.document_attributes.clinic_jid_resolve_method IS DISTINCT FROM EXCLUDED.clinic_jid_resolve_method
     OR public.document_attributes.message_endpoint IS DISTINCT FROM EXCLUDED.message_endpoint
     OR public.document_attributes.clinic_jid_mismatch IS DISTINCT FROM EXCLUDED.clinic_jid_mismatch
     OR public.document_attributes.patient_name_masked IS DISTINCT FROM EXCLUDED.patient_name_masked
     OR public.document_attributes.snils_masked IS DISTINCT FROM EXCLUDED.snils_masked
     OR public.document_attributes.doctor_name IS DISTINCT FROM EXCLUDED.doctor_name
     OR public.document_attributes.patient_hash IS DISTINCT FROM EXCLUDED.patient_hash
     OR public.document_attributes.doctor_hash IS DISTINCT FROM EXCLUDED.doctor_hash
     OR public.document_attributes.request_msgid IS DISTINCT FROM EXCLUDED.request_msgid;

    GET DIAGNOSTICS refreshed = ROW_COUNT;
    RETURN refreshed;
END;
$$;

CREATE OR REPLACE FUNCTION public.reconcile_document_attributes_ui()
RETURNS bigint
LANGUAGE sql
AS $$
    SELECT public.reconcile_document_attributes(NULL::text[]);
$$;
