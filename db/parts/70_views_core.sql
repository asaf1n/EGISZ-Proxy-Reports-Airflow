-- ============================================================================
-- 70_views_core.sql — document-grain MVs + compatibility analytics view
-- Loaded by db/dwh_init.sql via \i db/parts/70_views_core.sql.
-- ============================================================================

-- Источник обогащённой витрины: один SELECT, который переиспользуется и для полной
-- сборки таблицы, и для инкрементального обновления в egisz_transform_raw_to_facts.
-- Сама витрина v_egisz_documents_enriched_ui — persistent TABLE (не MATERIALIZED VIEW),
-- которая обновляется по затронутым document_key, а не полным REFRESH каждые 5 минут.
CREATE OR REPLACE VIEW public.v_egisz_documents_enriched_src AS
SELECT
    d.document_key AS "Документ (ключ учёта)",
    d.callback_log_id::text AS "LOGID журнала EXCHANGELOG",
    d.message_id AS "MSGID обмена",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at) AS "Обработано IPS",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)::date AS "День",
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)::date AS "День (тренд)",
    CASE
        WHEN d.status = 'success' THEN 'success'
        WHEN d.status IN ('async_error', 'network_error') THEN 'error'
        ELSE 'waiting'
    END AS "Статус",
    -- Единая нотификация статуса документа для всех карточек (4 значения).
    -- «Статус (код)» (см. ниже) — машинный код для фильтров/агрегаций.
    CASE
        WHEN d.status = 'success' THEN 'Успешно зарегистрирован'
        WHEN d.status = 'network_error' THEN 'Ошибка связи'
        WHEN d.status = 'async_error' THEN 'Ошибка асинхронного ответа РЭМД'
        ELSE 'В обработке'
    END AS "Статус (отчёт)",
    CASE
        WHEN d.status IN ('success', 'async_error', 'network_error', 'waiting') THEN d.status
        ELSE 'waiting'
    END AS "Статус (код)",
    CASE
        WHEN d.status = 'network_error' THEN 'Сетевая ошибка'
        WHEN d.status = 'async_error' THEN d.error_type
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
WHERE d.document_key IS NOT NULL;

-- Persistent-таблица витрины. Полностью наполняется в 90_..._finalize.sql при init,
-- далее точечно сопровождается egisz_transform_raw_to_facts по затронутым document_key.
-- CREATE TABLE AS идемпотентен в рамках полного init: 60_drop_dependents.sql дропает
-- объект до пересоздания (тот же контракт, что был у MATERIALIZED VIEW).
CREATE TABLE public.v_egisz_documents_enriched_ui AS
SELECT * FROM public.v_egisz_documents_enriched_src WITH NO DATA;

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
