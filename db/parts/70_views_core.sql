-- ============================================================================
-- 70_views_core.sql — document-grain MVs + compatibility analytics view
-- Loaded by db/dwh_init.sql via \i db/parts/70_views_core.sql.
-- ============================================================================

CREATE MATERIALIZED VIEW public.v_egisz_documents_enriched_ui AS
SELECT
    d.document_key AS "Документ (ключ учёта)",
    d.callback_log_id::text AS "LOGID журнала EXCHANGELOG",
    d.message_id AS "MSGID обмена",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at) AS "Обработано IPS",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)::date AS "День",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)::date AS "День (тренд)",
    CASE
        WHEN d.status = 'success' THEN 'success'
        WHEN d.status IN ('registration_error', 'network_error') THEN 'error'
        ELSE 'waiting'
    END AS "Статус",
    CASE
        WHEN d.status = 'success' THEN 'Успешный ответ'
        WHEN d.status = 'network_error' THEN 'Ошибка связи'
        WHEN d.status = 'registration_error' THEN 'Ошибка регистрации'
        ELSE 'В обработке'
    END AS "Статус (отчёт)",
    CASE
        WHEN d.status = 'network_error' THEN 'Сетевая ошибка'
        WHEN d.status = 'registration_error' THEN d.error_type
        ELSE NULL
    END AS "Тип ошибки",
    d.error_summary AS "Сводка ошибки",
    public.egisz_semd_type_report_label(d.semd_code, NULL) AS "Тип СЭМД (код · НСИ)",
    public.egisz_normalize_semd_code(d.semd_code) AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_normalize_semd_code(d.semd_code) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    d.jid::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || d.jid::text) AS "Наименование клиники",
    d.jid::text AS "JID из журнала (gost, число)",
    o.name AS "Медицинская организация",
    NULL::text AS "OID организации",
    l.mo_uid AS "OID клиники",
    public.egisz_clean_host(l.mo_domen) AS "Хост клиники (VPN ГОСТ)",
    o.inn AS "ИНН клиники",
    l.mo_domen AS "Токен gost (нецифр., для отображения)",
    l.jid::text AS "JID (EGISZ_LICENSES)",
    'нет'::text AS "Расхождение источников JID",
    d.document_created_at AS "Создание СЭМД",
    d.jid::text AS "JID из gost в REPLYTO",
    public.egisz_clean_host(l.mo_domen) AS "Токен gost (REPLYTO)",
    public.egisz_clean_text_value(d.local_uid) AS "localUid СЭМД",
    public.egisz_clean_text_value(d.local_uid) AS "Идентификатор документа (localUid)",
    public.egisz_clean_text_value(d.relates_to_id) AS "Связанное сообщение",
    lower(public.egisz_clean_text_value(d.relates_to_id)) AS "Связанное сообщение (канон)",
    lower(public.egisz_clean_text_value(d.local_uid)) AS "localUid СЭМД (канон)",
    d.emdr_id AS "Рег. номер РЭМД (emdrid)",
    d.emdr_id AS "Регистрационный номер РЭМД",
    d.document_id AS "DOCUMENTID",
    d.error_text AS "Исходный текст ошибки",
    NULL::text AS patient_name_masked,
    NULL::text AS snils_masked,
    NULL::text AS doctor_name,
    d.patient_hash,
    d.doctor_hash,
    COALESCE(d.callback_log_id, d.source_logid) AS transaction_id,
    d.jid AS clinic_id,
    public.egisz_normalize_semd_code(d.semd_code) AS service_id,
    d.status AS document_status,
    d.status_category,
    d.sent_at,
    d.registered_at
FROM public.fact_egisz_documents d
LEFT JOIN LATERAL (
    SELECT dl.*
    FROM public.dim_licenses dl
    WHERE (d.jid IS NOT NULL AND dl.jid = d.jid)
    ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM public.dim_semd_types dst
    WHERE dst.oid = public.egisz_normalize_semd_code(d.semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN public.dim_organizations o ON d.jid = o.jid
WHERE d.document_key IS NOT NULL
WITH NO DATA;

CREATE UNIQUE INDEX ON public.v_egisz_documents_enriched_ui ("Документ (ключ учёта)");
CREATE INDEX ON public.v_egisz_documents_enriched_ui ("День");
CREATE INDEX ON public.v_egisz_documents_enriched_ui ("JID клиники");
CREATE INDEX ON public.v_egisz_documents_enriched_ui ("Статус");
CREATE INDEX ON public.v_egisz_documents_enriched_ui (lower(NULLIF(btrim("localUid СЭМД"), '')));
CREATE INDEX ON public.v_egisz_documents_enriched_ui (lower(NULLIF(btrim("Рег. номер РЭМД (emdrid)"), '')));
CREATE INDEX ON public.v_egisz_documents_enriched_ui (lower(NULLIF(btrim("Связанное сообщение"), '')));

CREATE MATERIALIZED VIEW public.v_egisz_documents_daily_ui AS
SELECT
    md5(concat_ws('|', COALESCE(day::text, ''), COALESCE(jid, ''), COALESCE(semd_code, ''), COALESCE(status, ''))) AS aggregate_key,
    day,
    jid,
    semd_code,
    status,
    documents_count
FROM (
    SELECT
        "День (тренд)" AS day,
        NULLIF("JID клиники", '') AS jid,
        NULLIF("Код СЭМД", '') AS semd_code,
        "Статус" AS status,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS documents_count
    FROM public.v_egisz_documents_enriched_ui
    GROUP BY 1, 2, 3, 4
) grouped
WITH NO DATA;

CREATE UNIQUE INDEX ON public.v_egisz_documents_daily_ui (aggregate_key);
CREATE INDEX ON public.v_egisz_documents_daily_ui (day);
CREATE INDEX ON public.v_egisz_documents_daily_ui (jid);
CREATE INDEX ON public.v_egisz_documents_daily_ui (semd_code);
CREATE INDEX ON public.v_egisz_documents_daily_ui (status);

CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui AS
SELECT
    "Обработано IPS",
    "День (тренд)",
    "LOGID журнала EXCHANGELOG",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Рег. номер РЭМД (emdrid)",
    "Связанное сообщение",
    "JID клиники",
    "Тип СЭМД (код · НСИ)",
    "Статус",
    CASE
        WHEN "Статус" = 'success' THEN 'Успешный ответ'
        WHEN "Статус" = 'error' THEN COALESCE(NULLIF("Исходный текст ошибки", ''), '(нет текста)')
        ELSE ''
    END AS "Исходный текст ошибки",
    CASE
        WHEN "Статус" = 'success' THEN 'Успешный ответ'
        WHEN "Статус" = 'error' THEN COALESCE(NULLIF("Сводка ошибки", ''), 'Неизвестная ошибка')
        ELSE ''
    END AS "Интерпретация ошибки",
    CASE
        WHEN "Статус" = 'success' THEN 'Успешный ответ'
        WHEN "Статус" = 'error' THEN "Тип ошибки"
        ELSE ''
    END AS "Тип ошибки",
    CASE WHEN "Статус" = 'error' THEN 1::bigint ELSE NULL::bigint END AS "Порядок ошибки"
FROM public.v_egisz_documents_enriched_ui
WHERE "Статус" IN ('success', 'error');
