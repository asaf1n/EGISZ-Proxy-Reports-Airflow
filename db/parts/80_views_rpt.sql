-- ============================================================================
-- 80_views_rpt.sql — reporting views for Metabase (rpt_*)
-- Loaded by db/dwh_init.sql via \i db/parts/80_views_rpt.sql.
-- ============================================================================

CREATE OR REPLACE VIEW public.rpt_document_versions AS
SELECT
    d.dwh_id,
    -- Дата обработки транспортом IPS (EXCHANGELOG.CREATEDATE): последнее доступное
    -- IPS-событие документа. XML CDA (document_created_at) сюда не входит — это отдельная
    -- сущность времени создания контента, см. semd_created_at и delivery_seconds.
    COALESCE(d.last_callback_at, d.registered_at, d.first_sent_at) AS ips_date,
    d.status,
    ds.label AS status_label,
    ds.sort_order AS status_sort,
    d.error_text,
    public.normalize_semd_code(d.semd_code) AS semd_code,
    st.name AS semd_name,
    CASE
        WHEN st.code IS NOT NULL AND st.name IS NOT NULL
            THEN st.code || ' · ' || st.name
        WHEN st.code IS NOT NULL
            THEN st.code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
        ELSE NULL
    END AS semd_label,
    public.clean_text_value(d.local_uid) AS semd_local_uid,
    d.document_created_at AS semd_created_at,
    d.emdr_id AS semd_emdr_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(BTRIM(d.jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(BTRIM(o.name), ''), '—') AS clinic_label,
    o.inn AS clinic_inn,
    COALESCE(
        NULLIF(btrim(a.clinic_oid_license), ''),
        NULLIF(btrim(d.org_oid), '')
    ) AS clinic_oid,
    a.clinic_host,
    a.clinic_jid_mismatch,
    -- Транспорт СЭМД (README §«Парсинг»): отправка = request_*, исход = result_*.
    -- relates_to_msgid (relatesToMessage ответа) = request_msgid у склеенных — ключ корреляции.
    public.clean_text_value(d.relates_to_msgid) AS relates_to_msgid,
    -- LOGID состояния: исход если есть, иначе LOGID отправки («Отправлено» несёт LOGID отправки).
    COALESCE(d.result_logid, d.request_logid)::text AS logid,
    d.request_logid::text AS request_logid,
    d.result_logid::text AS result_logid,
    a.request_msgid,
    d.result_msgid,
    CASE
        WHEN d.status = 'success'
         AND d.document_created_at IS NOT NULL
         AND COALESCE(d.last_callback_at, d.first_sent_at, d.document_created_at) >= d.document_created_at
        THEN ROUND(
            EXTRACT(
                EPOCH FROM (
                    COALESCE(d.last_callback_at, d.first_sent_at, d.document_created_at)
                    - d.document_created_at
                )
            )::numeric,
            0
        )
        ELSE NULL::numeric
    END AS delivery_seconds,
    a.patient_name_masked,
    a.snils_masked,
    a.doctor_name,
    a.patient_hash,
    a.doctor_hash,
    d.registered_at,
    d.first_sent_at,
    d.error_types,
    -- Слой версий (README §«Версии и идентичность документа»).
    d.document_group_id,
    COALESCE(d.is_current_version, true) AS is_current_version,
    d.semd_version_number,
    d.document_group_confidence,
    d.superseded_by_dwh_id,
    d.supersedes_dwh_id
FROM public.documents d
LEFT JOIN public.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN public.dim_document_status ds ON ds.code = d.status
LEFT JOIN public.dim_organizations o ON o.jid = d.jid
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM public.dim_semd_types dst
    WHERE dst.oid = public.normalize_semd_code(d.semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
WHERE NULLIF(btrim(d.dwh_id), '') IS NOT NULL;

COMMENT ON VIEW public.rpt_document_versions IS
'Все экземпляры/версии отправки СЭМД: одна строка на dwh_id (полный аудит, включая superseded).';

-- Основная витрина — ТЕКУЩИЕ версии (один логический документ = одна строка). Все попытки
-- (включая superseded) — rpt_document_versions.
CREATE OR REPLACE VIEW public.rpt_documents AS
SELECT * FROM public.rpt_document_versions
WHERE is_current_version;

COMMENT ON VIEW public.rpt_documents IS
'Документная витрина (текущие версии, is_current_version): одна строка на логический документ. Полный аудит версий — rpt_document_versions.';

CREATE OR REPLACE VIEW public.rpt_documents_waiting AS
SELECT
    d.first_sent_at,
    EXTRACT(EPOCH FROM (now() - d.first_sent_at)) / 3600.0 AS waiting_hours,
    ROUND(EXTRACT(EPOCH FROM (now() - d.first_sent_at)) / 86400.0, 1) AS waiting_days,
    CASE
        WHEN d.first_sent_at IS NULL THEN 'дата неизвестна'
        WHEN now() - d.first_sent_at > INTERVAL '30 days' THEN '>30 дней'
        WHEN now() - d.first_sent_at > INTERVAL '7 days' THEN '>7 дней'
        WHEN now() - d.first_sent_at > INTERVAL '3 days' THEN '>3 дней'
        ELSE 'до 3 дней'
    END AS wait_segment,
    r.semd_local_uid,
    r.semd_code,
    r.semd_name,
    r.semd_label,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.relates_to_msgid,
    r.request_msgid,
    r.result_msgid,
    r.clinic_host
FROM public.documents d
INNER JOIN public.rpt_documents r ON r.dwh_id = d.dwh_id
WHERE d.status = 'waiting';

CREATE OR REPLACE VIEW public.rpt_network_errors AS
SELECT
    r.ips_date,
    r.logid,
    r.result_msgid,
    r.request_msgid,
    r.dwh_id,
    r.semd_local_uid,
    r.relates_to_msgid,
    r.clinic_host,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.semd_code,
    r.semd_name,
    r.semd_label,
    public.network_error_type(r.error_text) AS network_error_type,
    r.error_text,
    r.error_types,
    r.semd_emdr_id,
    da.contour
FROM public.rpt_documents r
-- Контур зафиксирован на грейне документа (document_attributes): отчётный слой
-- не читает message-грейн напрямую (контракт «rpt только поверх documents/dims»).
LEFT JOIN public.document_attributes da ON da.dwh_id = r.dwh_id
WHERE r.status = 'network_error';

COMMENT ON VIEW public.rpt_network_errors IS
'Ошибки связи proxy_egisz: document-grain (status=network_error). contour — контур обмена (РЭМД/ИЭМК, exchange_contour).';

-- МАТЕРИАЛИЗОВАННОЕ представление: грейн «тип×документ»; все карточки вкладки
-- «Анализ ошибок» читают его. Обновляется в конце transform (extract/reconcile DAG) →
-- свежесть та же, что у фактов. См. refresh_error_breakdown().
-- Построение в два дешёвых шага:
--   1) atom_types — дедуп (dwh_id, тип) на УЗКИХ данных прямо из documents
--      (один LEFT JOIN к dim_error_type_group);
--   2) JOIN rpt_documents 1:1 по dwh_id — display-колонки добавляются ПОСЛЕ дедупа.
-- CASE страхует атомы вне словаря (сводятся в «Неизвестная ошибка», кроме «Код: X»).
CREATE MATERIALIZED VIEW public.rpt_error_breakdown AS
WITH atom_types AS (
    SELECT DISTINCT
        doc.dwh_id,
        CASE
            WHEN g.error_type IS NOT NULL THEN n.norm
            WHEN n.norm LIKE 'Код: %' THEN n.norm
            ELSE 'Неизвестная ошибка'
        END AS error_type,
        COALESCE(g.error_category, 'Прочие') AS error_category,
        -- 'Код: %' и атомы вне словаря — зона/повторяемость неизвестны.
        COALESCE(g.responsibility, 'смешанная') AS responsibility,
        COALESCE(g.is_retryable, false) AS is_retryable
    FROM public.documents doc
    CROSS JOIN LATERAL unnest(
        -- error_types гарантированно непустой ниже по WHERE, поэтому фолбэк не нужен.
        string_to_array(btrim(doc.error_types), ' · ')
    ) AS atom
    CROSS JOIN LATERAL (SELECT NULLIF(btrim(atom), '') AS norm) n
    LEFT JOIN public.dim_error_type_group g ON g.error_type = n.norm
    WHERE doc.status IN ('async_error', 'network_error')
      AND doc.error_types IS NOT NULL
      AND btrim(doc.error_types) <> ''
      AND n.norm IS NOT NULL
)
SELECT
    r.ips_date,
    a.dwh_id,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.semd_code,
    r.semd_label,
    a.error_type,
    a.error_category,
    a.responsibility,
    a.is_retryable
FROM atom_types a
INNER JOIN public.rpt_documents r ON r.dwh_id = a.dwh_id
WITH DATA;

-- UNIQUE индекс нужен для REFRESH ... CONCURRENTLY; грейн = (dwh_id, error_type).
CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_error_breakdown
    ON public.rpt_error_breakdown (dwh_id, error_type);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_ips_date ON public.rpt_error_breakdown (ips_date);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_error_type ON public.rpt_error_breakdown (error_type);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_error_category ON public.rpt_error_breakdown (error_category);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_clinic_jid ON public.rpt_error_breakdown (clinic_jid);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_semd_code ON public.rpt_error_breakdown (semd_code);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_responsibility ON public.rpt_error_breakdown (responsibility);

COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown IS
'Разбивка ошибок (matview): один ряд = один канонический тип на документ (split documents.error_types по '' · ''). Обновляется refresh_error_breakdown() после transform.';

CREATE OR REPLACE VIEW public.rpt_document_lineage AS
SELECT
    d.dwh_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    a.clinic_oid_xml,
    a.clinic_oid_jpersons,
    a.clinic_oid_license,
    a.clinic_host,
    a.clinic_jid_resolve_method,
    a.message_endpoint,
    a.clinic_jid_mismatch,
    d.org_oid AS document_org_oid,
    d.jid_resolve_method AS document_jid_resolve_method
FROM public.documents d
LEFT JOIN public.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN public.dim_organizations o ON o.jid = d.jid
WHERE d.dwh_id IS NOT NULL;

COMMENT ON VIEW public.rpt_document_lineage IS
'Lineage документа: атомы идентификаторов клиники из XML, лицензий и журнала.';

-- Доступные клинике типы СЭМД: одна запись EGISZ_LICENSES на пару JID+KIND.
-- Маркер актуальности — MAX(modifydate) по записям пары; дата начала использования —
-- MIN(bdate) (в источнике пока не заполняется, колонка экспонируется на будущее).
-- clinic_label собирается идентично rpt_documents, чтобы общий дашборд-фильтр
-- «Клиника» привязывался одним значением к обеим витринам.
CREATE OR REPLACE VIEW public.rpt_clinic_semd_licenses AS
SELECT
    l.jid AS clinic_jid,
    COALESCE(NULLIF(BTRIM(l.jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(BTRIM(o.name), ''), '—') AS clinic_label,
    o.name AS clinic_name,
    l.kind AS semd_code,
    st.name AS semd_name,
    CASE
        WHEN st.name IS NOT NULL THEN l.kind || ' · ' || st.name
        ELSE l.kind || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
    END AS semd_label,
    l.license_modified_at,
    l.license_started_at
FROM (
    SELECT
        jid,
        kind,
        MAX(modifydate) AS license_modified_at,
        MIN(bdate) AS license_started_at
    FROM public.dim_licenses
    WHERE jid IS NOT NULL
      AND NULLIF(btrim(kind), '') IS NOT NULL
    GROUP BY jid, kind
) l
LEFT JOIN public.dim_organizations o ON o.jid = l.jid
LEFT JOIN public.dim_semd_types st ON st.code = l.kind;

COMMENT ON VIEW public.rpt_clinic_semd_licenses IS
'Доступные клинике типы СЭМД: грейн (clinic_jid, semd_code = EGISZ_LICENSES.KIND); наименование — dim_semd_types, актуальность — MAX(modifydate), начало использования — MIN(bdate).';
