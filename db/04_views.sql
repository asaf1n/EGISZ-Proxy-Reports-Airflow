-- ============================================================================
-- 04_views.sql — drop dependents, document attributes, rpt_* layer, health, finalize
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: drop_dependents
-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views/marts before re-creating them.
-- CREATE OR REPLACE VIEW не меняет состав колонок, поэтому rpt-слой дропается
-- целиком и собирается заново в 80–90.
-- ============================================================================

DROP VIEW IF EXISTS public.rpt_health_by_clinic CASCADE;
DROP VIEW IF EXISTS public.rpt_health_signals CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.rpt_health_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_network_errors CASCADE;
-- Недельный (85_views_weekly) и месячный (86_views_monthly) слои зависят от
-- rpt_documents/rpt_error_breakdown —
-- дропаем до них. Эти объекты никогда не существовали как plain VIEW, relkind-обход
-- (как у rpt_error_breakdown ниже) не нужен.
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_monthly CASCADE;
-- rpt_error_breakdown — MATERIALIZED VIEW. DROP VIEW IF EXISTS / DROP MATERIALIZED VIEW
-- IF EXISTS подавляют только отсутствие объекта, но НЕ несовпадение типа (VIEW vs MATVIEW),
-- поэтому дропаем по фактическому relkind.
DO $$
DECLARE
    kind "char";
BEGIN
    SELECT c.relkind INTO kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'rpt_error_breakdown';

    IF kind = 'm' THEN
        DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE;
    ELSIF kind IS NOT NULL THEN
        DROP VIEW public.rpt_error_breakdown CASCADE;
    END IF;
END $$;
DROP VIEW IF EXISTS public.rpt_documents CASCADE;
DROP VIEW IF EXISTS public.rpt_document_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_sent CASCADE;
-- Предшественник rpt_documents_sent: имя отражало наблюдателя («ждёт»), а не состояние
-- документа. Дроп нужен, пока в развёртываниях остаётся представление под старым именем.
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;

-- Классификация даёт документу два поля: error_types (канонические типы,
-- error_classify) и error_text (исходные <message>, error_messages_row).
-- Прочие интерпретаторы и канонизация на чтении вне контракта; их дроп и снятие
-- error_summary идут ПОСЛЕ дропа витрин выше — rpt-слой на них ссылается.
DROP FUNCTION IF EXISTS public.error_interpretation_schematron_chunk(text);
DROP FUNCTION IF EXISTS public.error_interpretation_item(text, text);
DROP FUNCTION IF EXISTS public.error_interpretation_row(jsonb);
DROP FUNCTION IF EXISTS public.error_atom_normalize(text);
DROP FUNCTION IF EXISTS public.canonical_error_atom(text);
DROP FUNCTION IF EXISTS public.canonical_error_list(text);
DROP FUNCTION IF EXISTS public.error_category(text);
ALTER TABLE public.documents DROP COLUMN IF EXISTS error_summary;
ALTER TABLE public.transactions DROP COLUMN IF EXISTS error_summary;
-- Мёртвый предшественник dim_error_rules: жил только в старых развёртываниях,
-- ни одна функция его не читает.
DROP TABLE IF EXISTS public.error_interpretation_rules;

-- ---------------------------------------------------------------- section: document_attributes
-- ============================================================================
-- 70_views_core.sql — document_attributes (1:1 к documents)
-- Loaded by db/dwh_init.sql via \i db/04_views.sql.
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

-- request_msgid — MSGID строки getDocumentFile (request_logid), нормализованный source MSGID.
-- На грейне документа его нет: documents.result_msgid — это MSGID ответа ЕГИСЗ, а для
-- «Отправлено» нужен идентификатор запроса файла.
ALTER TABLE public.document_attributes ADD COLUMN IF NOT EXISTS request_msgid text;

-- egisz_subsystem — подсистема ЕГИСЗ документа (РЭМД/ИЭМК, transactions.egisz_subsystem).
-- Отчётный слой (rpt_network_errors) не читает message-грейн transactions напрямую,
-- поэтому подсистема фиксируется здесь: последнее непустое значение по строкам документа.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'document_attributes'
                 AND column_name = 'contour')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = 'document_attributes'
                         AND column_name = 'egisz_subsystem') THEN
        ALTER TABLE public.document_attributes RENAME COLUMN contour TO egisz_subsystem;
    END IF;
END $$;
ALTER TABLE public.document_attributes ADD COLUMN IF NOT EXISTS egisz_subsystem text;
ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS contour;

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
        egisz_subsystem,
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
        sub.egisz_subsystem,
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
    LEFT JOIN LATERAL (
        SELECT t.egisz_subsystem
        FROM public.transactions t
        WHERE t.dwh_id = d.dwh_id
          AND t.egisz_subsystem IS NOT NULL
        ORDER BY t.log_date DESC NULLS LAST, t.logid DESC
        LIMIT 1
    ) sub ON TRUE
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
        egisz_subsystem = EXCLUDED.egisz_subsystem,
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
     OR public.document_attributes.request_msgid IS DISTINCT FROM EXCLUDED.request_msgid
     OR public.document_attributes.egisz_subsystem IS DISTINCT FROM EXCLUDED.egisz_subsystem;

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

-- error_text на грейне документа принадлежит последнему вердикту и проставляется
-- в transform вместе со статусом. Отдельная сверка по архиву здесь не нужна и вредна:
-- выбирая последнее сообщение С текстом, она возвращала текст отказа на документы,
-- которые прошли со второй попытки.
DROP FUNCTION IF EXISTS public.repair_document_error_text();

-- ---------------------------------------------------------------- section: rpt_documents
-- ============================================================================
-- 80_views_rpt.sql — reporting views for Metabase (rpt_*)
-- Loaded by db/dwh_init.sql via \i db/04_views.sql.
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
    -- Состояние отправки: нефинальный статус раскрывается ступенью возраста обработки.
    -- «В обработке» участвует в общих срезах наравне с исходами, «Без ответа» — только
    -- на вкладке отправленных (README §«Учёт отправленных»).
    ps.code AS pending_segment,
    ps.label AS pending_segment_label,
    ps.sort_order AS pending_segment_sort,
    ss.code AS sent_state,
    ss.label AS sent_state_label,
    CASE WHEN ds.is_final THEN d.status ELSE ss.code END AS status_detail,
    CASE WHEN ds.is_final THEN ds.label ELSE ss.label END AS status_detail_label,
    CASE
        WHEN ds.is_final THEN ds.sort_order
        ELSE ds.sort_order + ss.sort_order - 1
    END AS status_detail_sort,
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
    -- Время отклика ЕГИСЗ: от запроса файла (шаг 6 схемы регистрации) до вердикта.
    -- Считается по журналу, а не по дате создания CDA: document_created_at приходит далеко
    -- не во всех отправках, и метрика на его основе покрывала доли процента корпуса.
    CASE
        WHEN d.first_sent_at IS NOT NULL
         AND d.last_callback_at IS NOT NULL
         AND d.last_callback_at >= d.first_sent_at
        THEN ROUND(EXTRACT(EPOCH FROM (d.last_callback_at - d.first_sent_at))::numeric, 0)
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
    -- Число подач документа в ЕГИСЗ по реестру шлюза: повторная подача не меняет localUid,
    -- поэтому счётчик показывает, сколько раз документ отправлялся до текущего исхода.
    COALESCE(d.attempt_count, 1) AS attempt_count,
    (COALESCE(d.attempt_count, 1) > 1) AS is_resubmitted,
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
-- Ступень — первая по sort_order, чья граница покрывает возраст обработки; терминальная
-- (max_age_minutes IS NULL) замыкает лестницу и ловит в том числе отправки без first_sent_at:
-- без известного момента запроса файла документ не может считаться находящимся в обработке.
LEFT JOIN LATERAL (
    SELECT s.code, s.label, s.sort_order, s.is_no_response
    FROM public.dim_pending_segments s
    WHERE NOT ds.is_final
      AND (
          s.max_age_minutes IS NULL
          OR (
              d.first_sent_at IS NOT NULL
              AND EXTRACT(EPOCH FROM (now() - d.first_sent_at)) / 60.0 <= s.max_age_minutes
          )
      )
    ORDER BY s.sort_order
    LIMIT 1
) ps ON TRUE
LEFT JOIN public.dim_sent_state ss
    ON ss.code = CASE
        WHEN ps.code IS NULL THEN NULL          -- финальный статус: состояния отправки нет
        WHEN ps.is_no_response THEN 'no_response'
        ELSE 'pending'
    END
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

CREATE OR REPLACE VIEW public.rpt_documents_sent AS
SELECT
    r.dwh_id,
    r.first_sent_at,
    EXTRACT(EPOCH FROM (now() - r.first_sent_at)) / 3600.0 AS pending_hours,
    ROUND(EXTRACT(EPOCH FROM (now() - r.first_sent_at)) / 86400.0, 1) AS pending_days,
    r.pending_segment,
    r.pending_segment_label,
    r.pending_segment_sort,
    r.sent_state,
    r.sent_state_label,
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
    r.clinic_host,
    r.attempt_count,
    r.is_resubmitted
FROM public.rpt_documents r
WHERE r.sent_state IS NOT NULL;

COMMENT ON VIEW public.rpt_documents_sent IS
'Отправленные документы без вердикта ЕГИСЗ: ступень возраста обработки (dim_pending_segments) и состояние отправки («В обработке» / «Без ответа»).';

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
    da.egisz_subsystem
FROM public.rpt_documents r
-- Подсистема зафиксирована на грейне документа (document_attributes): отчётный слой
-- не читает message-грейн напрямую (контракт «rpt только поверх documents/dims»).
LEFT JOIN public.document_attributes da ON da.dwh_id = r.dwh_id
WHERE r.status = 'network_error';

COMMENT ON VIEW public.rpt_network_errors IS
'Ошибки связи proxy_egisz: document-grain (status=network_error). egisz_subsystem — подсистема ЕГИСЗ (РЭМД/ИЭМК).';

-- МАТЕРИАЛИЗОВАННОЕ представление: грейн «тип×документ»; все карточки вкладки
-- «Анализ ошибок» читают его. Обновляется в конце transform (extract/reconcile DAG) →
-- свежесть та же, что у фактов. См. refresh_error_breakdown().
-- Построение в два дешёвых шага:
--   1) atom_types — дедуп (dwh_id, тип) на УЗКИХ данных прямо из documents
--      (один LEFT JOIN к dim_error_type_group);
--   2) JOIN rpt_documents 1:1 по dwh_id — display-колонки добавляются ПОСЛЕ дедупа.
-- Атом вне словаря — это формулировка отказа, не покрытая правилом: пропускаем её
-- как есть в категорию «Прочие». Подмена такой строки заглушкой скрывала бы от
-- аналитика ровно то, ради чего он открывает разбор.
CREATE MATERIALIZED VIEW public.rpt_error_breakdown AS
WITH atom_types AS (
    SELECT DISTINCT
        doc.dwh_id,
        n.norm AS error_type,
        COALESCE(g.error_category, 'Прочие') AS error_category,
        -- Для непокрытых формулировок зона ответственности и повторяемость неизвестны.
        COALESCE(g.responsibility, 'смешанная') AS responsibility,
        COALESCE(g.is_retryable, false) AS is_retryable,
        g.nsi_error_code,
        c.nsi_error_description
    FROM public.documents doc
    CROSS JOIN LATERAL unnest(
        -- error_types гарантированно непустой ниже по WHERE, поэтому фолбэк не нужен.
        string_to_array(btrim(doc.error_types), ' · ')
    ) AS atom
    CROSS JOIN LATERAL (SELECT NULLIF(btrim(atom), '') AS norm) n
    LEFT JOIN public.dim_error_type_group g ON g.error_type = n.norm
    LEFT JOIN public.dim_nsi_error_code c ON c.nsi_error_code = g.nsi_error_code
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
    a.is_retryable,
    a.nsi_error_code,
    a.nsi_error_description
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
CREATE INDEX IF NOT EXISTS idx_rpt_eb_nsi_error_code ON public.rpt_error_breakdown (nsi_error_code);

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

-- ---------------------------------------------------------------- section: weekly
-- ============================================================================
-- 85_views_weekly.sql — недельный слой динамики для дашборда «Динамика по
-- неделям». Идемпотентность — как у rpt_error_breakdown: DROP в 60, CREATE
-- здесь, REFRESH + ANALYZE в 90.
--
-- Неделя = понедельник МСК. AT TIME ZONE 'Europe/Moscow' применяется ОДИН раз
-- и сознательно: date_trunc вычисляется в момент REFRESH, а init-прогон (90-я
-- часть) обновляет matview под ролью postgres, у которой timezone НЕ запинен
-- (пин стоит только на роли egisz, 00_bootstrap). Это не двойной сдвиг:
-- ips_date — timestamptz, сдвиг задаёт стену МСК до усечения.
-- ============================================================================

-- Недельная витрина документов: грейн (week_start, клиника). Хранятся только
-- счётчики — доли считаются потребителями как ratio-of-sums, что даёт
-- корректное взвешивание при агрегации недель/клиник. Инвариант:
-- docs_success + docs_error = docs_total (отправленные без вердикта вне корпуса).
-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label
-- NOT NULL по построению ('— · —' при пустом jid), и REFRESH CONCURRENTLY
-- требует уникальный btree без выражений.
CREATE MATERIALIZED VIEW public.rpt_documents_weekly AS
SELECT
    date_trunc('week', r.ips_date AT TIME ZONE 'Europe/Moscow')::date AS week_start,
    r.clinic_jid,
    MAX(r.clinic_name) AS clinic_name,
    r.clinic_label,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status <> 'sent')::bigint AS docs_total,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'success')::bigint AS docs_success,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status IN ('async_error', 'network_error'))::bigint AS docs_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'async_error')::bigint AS docs_async_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'network_error')::bigint AS docs_network_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'sent')::bigint AS docs_sent,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.sent_state = 'pending')::bigint AS docs_pending,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.sent_state = 'no_response')::bigint AS docs_no_response,
    (date_trunc('week', r.ips_date AT TIME ZONE 'Europe/Moscow')::date
        < date_trunc('week', now() AT TIME ZONE 'Europe/Moscow')::date) AS is_complete_week
FROM public.rpt_documents r
WHERE r.ips_date IS NOT NULL
GROUP BY 1, r.clinic_jid, r.clinic_label
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_documents_weekly
    ON public.rpt_documents_weekly (week_start, clinic_label);
CREATE INDEX IF NOT EXISTS idx_rpt_docs_weekly_week ON public.rpt_documents_weekly (week_start);
CREATE INDEX IF NOT EXISTS idx_rpt_docs_weekly_clinic_jid ON public.rpt_documents_weekly (clinic_jid);

COMMENT ON MATERIALIZED VIEW public.rpt_documents_weekly IS
'Недельная витрина документов: грейн (week_start = понедельник МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_success + docs_error = docs_total; docs_pending + docs_no_response = docs_sent. Обновляется refresh_report_marts() после transform.';

-- Недельная структура ошибок по категориям (уровень сообщений): документ с
-- несколькими категориями учитывается в каждой — сумма долей категорий может
-- превышать 100 % от числа документов; это контракт панели структуры.
CREATE MATERIALIZED VIEW public.rpt_error_breakdown_weekly AS
SELECT
    date_trunc('week', b.ips_date AT TIME ZONE 'Europe/Moscow')::date AS week_start,
    b.clinic_jid,
    MAX(b.clinic_name) AS clinic_name,
    b.clinic_label,
    b.error_category,
    COUNT(DISTINCT b.dwh_id)::bigint AS docs_with_category,
    (date_trunc('week', b.ips_date AT TIME ZONE 'Europe/Moscow')::date
        < date_trunc('week', now() AT TIME ZONE 'Europe/Moscow')::date) AS is_complete_week
FROM public.rpt_error_breakdown b
WHERE b.ips_date IS NOT NULL
GROUP BY 1, b.clinic_jid, b.clinic_label, b.error_category
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_error_breakdown_weekly
    ON public.rpt_error_breakdown_weekly (week_start, clinic_label, error_category);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_weekly_week ON public.rpt_error_breakdown_weekly (week_start);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_weekly_category ON public.rpt_error_breakdown_weekly (error_category);

COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown_weekly IS
'Недельная структура ошибок: грейн (week_start, клиника, error_category); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории. Обновляется refresh_report_marts() после rpt_error_breakdown.';

-- ---------------------------------------------------------------- section: monthly
-- ============================================================================
-- 86_views_monthly.sql — месячный слой динамики для вкладки «Динамика по
-- месяцам» управленческого дашборда. Идемпотентность — как у недельного слоя:
-- DROP в 60, CREATE здесь, REFRESH + ANALYZE в 90.
--
-- Месяц = первое число месяца МСК. AT TIME ZONE 'Europe/Moscow' применяется
-- ОДИН раз и сознательно: date_trunc вычисляется в момент REFRESH, а init-прогон
-- (90-я часть) обновляет matview под ролью postgres, у которой timezone НЕ
-- запинен (пин стоит только на роли egisz, 00_bootstrap). Это не двойной сдвиг:
-- ips_date — timestamptz, сдвиг задаёт стену МСК до усечения.
-- ============================================================================

-- Месячная витрина документов: грейн (month_start, клиника). Хранятся только
-- счётчики — доли считаются потребителями как ratio-of-sums, что даёт
-- корректное взвешивание при агрегации месяцев/клиник. Инвариант:
-- docs_success + docs_error = docs_total (отправленные без вердикта вне корпуса).
-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label
-- NOT NULL по построению ('— · —' при пустом jid), и REFRESH CONCURRENTLY
-- требует уникальный btree без выражений.
CREATE MATERIALIZED VIEW public.rpt_documents_monthly AS
SELECT
    date_trunc('month', r.ips_date AT TIME ZONE 'Europe/Moscow')::date AS month_start,
    r.clinic_jid,
    MAX(r.clinic_name) AS clinic_name,
    r.clinic_label,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status <> 'sent')::bigint AS docs_total,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'success')::bigint AS docs_success,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status IN ('async_error', 'network_error'))::bigint AS docs_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'async_error')::bigint AS docs_async_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'network_error')::bigint AS docs_network_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'sent')::bigint AS docs_sent,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.sent_state = 'pending')::bigint AS docs_pending,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.sent_state = 'no_response')::bigint AS docs_no_response,
    (date_trunc('month', r.ips_date AT TIME ZONE 'Europe/Moscow')::date
        < date_trunc('month', now() AT TIME ZONE 'Europe/Moscow')::date) AS is_complete_month
FROM public.rpt_documents r
WHERE r.ips_date IS NOT NULL
GROUP BY 1, r.clinic_jid, r.clinic_label
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_documents_monthly
    ON public.rpt_documents_monthly (month_start, clinic_label);
CREATE INDEX IF NOT EXISTS idx_rpt_docs_monthly_month ON public.rpt_documents_monthly (month_start);
CREATE INDEX IF NOT EXISTS idx_rpt_docs_monthly_clinic_jid ON public.rpt_documents_monthly (clinic_jid);

COMMENT ON MATERIALIZED VIEW public.rpt_documents_monthly IS
'Месячная витрина документов: грейн (month_start = первое число месяца МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_success + docs_error = docs_total; docs_pending + docs_no_response = docs_sent. Обновляется refresh_report_marts() после transform.';

-- Месячная структура ошибок по категориям (уровень сообщений): документ с
-- несколькими категориями учитывается в каждой — сумма долей категорий может
-- превышать 100 % от числа документов; это контракт панели структуры.
CREATE MATERIALIZED VIEW public.rpt_error_breakdown_monthly AS
SELECT
    date_trunc('month', b.ips_date AT TIME ZONE 'Europe/Moscow')::date AS month_start,
    b.clinic_jid,
    MAX(b.clinic_name) AS clinic_name,
    b.clinic_label,
    b.error_category,
    COUNT(DISTINCT b.dwh_id)::bigint AS docs_with_category,
    (date_trunc('month', b.ips_date AT TIME ZONE 'Europe/Moscow')::date
        < date_trunc('month', now() AT TIME ZONE 'Europe/Moscow')::date) AS is_complete_month
FROM public.rpt_error_breakdown b
WHERE b.ips_date IS NOT NULL
GROUP BY 1, b.clinic_jid, b.clinic_label, b.error_category
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_error_breakdown_monthly
    ON public.rpt_error_breakdown_monthly (month_start, clinic_label, error_category);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_monthly_month ON public.rpt_error_breakdown_monthly (month_start);
CREATE INDEX IF NOT EXISTS idx_rpt_eb_monthly_category ON public.rpt_error_breakdown_monthly (error_category);

COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown_monthly IS
'Месячная структура ошибок: грейн (month_start, клиника, error_category); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории. Обновляется refresh_report_marts() после rpt_error_breakdown.';

-- ---------------------------------------------------------------- section: health
-- ============================================================================
-- 90_views_health_and_finalize.sql — v_health_* views, final GRANT verification,
-- refresh, and ANALYZE.
-- ============================================================================

-- Business backfill lives only in public.transform_raw_to_facts().

CREATE OR REPLACE VIEW public.rpt_health_by_clinic AS
WITH anchor AS (
    SELECT COALESCE(MAX(COALESCE(last_callback_at, first_sent_at, document_created_at)), now()) AS ref_ts
    FROM public.documents
),
fact_24h AS (
    SELECT
        d.jid::text AS clinic_jid,
        MAX(COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || d.jid::text)) AS clinic_name,
        COUNT(DISTINCT d.dwh_id)::bigint AS docs_cnt,
        COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status IN ('async_error', 'network_error'))::bigint AS err_cnt
    FROM public.documents d
    CROSS JOIN anchor
    LEFT JOIN public.dim_organizations o ON o.jid = d.jid
    WHERE COALESCE(d.last_callback_at, d.first_sent_at, d.document_created_at) >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY d.jid
),
sent_without_verdict AS (
    SELECT jid::text AS clinic_jid, COUNT(DISTINCT dwh_id)::bigint AS sent_cnt
    FROM public.documents
    WHERE status = 'sent'
    GROUP BY jid
)
SELECT
    f.clinic_jid AS "JID Клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f.clinic_jid) AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.sent_cnt, 0)::bigint AS "Отправлено без вердикта (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.sent_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.sent_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN sent_without_verdict q ON q.clinic_jid = f.clinic_jid;

CREATE OR REPLACE VIEW public.rpt_health_proxy_db AS
SELECT
    (SELECT COUNT(*) FROM public.documents)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent')::bigint AS "Отправлено без вердикта",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours')::bigint AS "Без вердикта > 24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '24 hours' AND first_sent_at < now() - INTERVAL '1 hour')::bigint AS "Без вердикта 1-24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '1 hour')::bigint AS "Без вердикта < 1ч",
    (SELECT MAX(first_sent_at) FROM public.documents) AS "DWH max Sent",
    (SELECT updated_at FROM elt_state WHERE pipeline = 'egisz') AS "Последний апдейт курсора",
    (SELECT last_logid FROM elt_state WHERE pipeline = 'egisz') AS "elt_state.last_logid",
    (SELECT MAX(COALESCE(result_logid, request_logid)) FROM public.documents) AS "DWH max LOGID fact",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.rpt_health_signals AS
WITH anchor AS (
    SELECT MAX(COALESCE(last_callback_at, first_sent_at, document_created_at)) AS last_fact_ts
    FROM public.documents
),
-- Доля вердиктов ЕГИСЗ, которые не удалось связать с документом, за 24ч.
-- Вердикт несёт relatesToMessage; документ находится по реестру подач. Рост доли означает,
-- что реестр отстал от журнала или подача в него не попала, — исход отправки при этом
-- теряется, а документ остаётся в статусе «Отправлено».
unlinked_24h AS (
    SELECT ROUND(
        100.0 * COUNT(*) FILTER (WHERE link_method = 'unlinked')
        / NULLIF(COUNT(*), 0),
        1
    ) AS pct
    FROM public.transactions
    WHERE log_date >= now() - INTERVAL '24 hours'
      AND xml_relates_to_id IS NOT NULL
),
-- Граница перехода в состояние «Без ответа» берётся из лестницы ступеней, а не задаётся
-- здесь повторно: ужесточение порога делается UPDATE по dim_pending_segments.
no_response_after AS (
    SELECT now() - make_interval(
        mins => (SELECT MAX(max_age_minutes) FROM public.dim_pending_segments WHERE NOT is_no_response)
    ) AS ts
),
-- Отказы, чью формулировку ни одно правило не распознало: они видны в разборе текстом,
-- и каждая такая строка — кандидат на новое правило либо на код, отсутствующий в ФНСИ.
uncovered_types AS (
    SELECT DISTINCT b.dwh_id
    FROM public.rpt_error_breakdown b
    LEFT JOIN public.dim_error_type_group g ON g.error_type = b.error_type
    WHERE g.error_type IS NULL
)
SELECT * FROM (
    VALUES
        ('parsed_documents', 'Разложенные документы proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM public.documents), 'документов', 'documents', 'Контроль поступления СЭМД в DWH'),
        ('sent_24h', 'Отправлено без вердикта > 24ч', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours'), 'документов', 'documents.status=sent', 'Проверить клиники без вердикта ЕГИСЗ и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'network_error'), 'документов', 'documents.status=network_error', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки асинхронного ответа РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM public.documents WHERE status = 'async_error'), 'документов', 'documents.status=async_error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('no_response_backlog',
         'Документы без ответа',
         CASE
             WHEN (SELECT COUNT(*) FROM public.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts) >= 50 THEN 'red'
             WHEN (SELECT COUNT(*) FROM public.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts) >= 20 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM public.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts),
         'документов',
         'rpt_documents_sent (состояние отправки)',
         'Проверить транспорт клиник на вкладке «Отправленные»: вердикт по этим документам уже не ожидается'),
        ('uncovered_error_types',
         'Отказы без правила классификации',
         CASE
             WHEN (SELECT COUNT(*) FROM uncovered_types) >= 1000 THEN 'red'
             WHEN (SELECT COUNT(*) FROM uncovered_types) >= 100 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM uncovered_types),
         'документов',
         'rpt_error_breakdown вне dim_error_type_group',
         'Разобрать формулировки на вкладке «Анализ ошибок» и завести правило в dim_error_rules'),
        ('unlinked_verdicts',
         'Доля несвязанных вердиктов ЕГИСЗ',
         CASE
             WHEN COALESCE((SELECT pct FROM unlinked_24h), 0) >= 5 THEN 'red'
             WHEN COALESCE((SELECT pct FROM unlinked_24h), 0) >= 1 THEN 'yellow'
             ELSE 'green'
         END,
         COALESCE((SELECT pct FROM unlinked_24h), 0)::numeric,
         '% (за 24ч)',
         'transactions.link_method=unlinked за 24ч',
         'Проверить наполнение dim_message_document: подача документа не попала в реестр шлюза'),
        ('data_freshness',
         'Свежесть данных (последний факт)',
         CASE
             WHEN (SELECT last_fact_ts FROM anchor) IS NULL THEN 'red'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '1 hour'  THEN 'green'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '24 hours' THEN 'yellow'
             ELSE 'red'
         END,
         ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT last_fact_ts FROM anchor), now()))) / 60.0, 1)::numeric,
         'минут с последнего факта',
         'documents.last_callback_at/first_sent_at',
         'Проверить ELT-цикл, Airflow scheduler и доступ к Firebird')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");

-- Наблюдаемость слоя версий (README §«Версии и идентичность документа»).
-- «Макс. размер группы» — детектор перемола: группа по (jid+тип+documentNumber) не должна
-- схлопывать РАЗНЫЕ документы (страховка c_cap=50 в recompute_document_versions; max по
-- базе = 7). «Коллизии localUid» — один dwh_id с разным типом СЭМД в transactions: признак
-- переиспользования localUid под другой документ.
CREATE OR REPLACE VIEW public.rpt_health_versions AS
WITH grp AS (
    SELECT document_group_id, count(*) AS versions
    FROM public.documents
    WHERE document_group_id IS NOT NULL
    GROUP BY document_group_id
)
SELECT
    (SELECT count(*) FROM public.documents)::bigint AS "Экземпляров всего",
    (SELECT count(*) FROM public.documents WHERE is_current_version)::bigint AS "Уникальных документов (текущих)",
    (SELECT count(*) FROM public.documents WHERE is_current_version IS FALSE)::bigint AS "Superseded версий",
    (SELECT count(*) FROM grp WHERE versions > 1)::bigint AS "Групп с >1 версией",
    (SELECT count(*) FROM public.documents WHERE document_group_confidence = 'doc_number')::bigint AS "Экземпляров в группах по documentNumber",
    (SELECT COALESCE(max(versions), 0) FROM grp)::bigint AS "Макс. размер группы (детектор перемола)",
    (SELECT count(*) FROM (
        SELECT dwh_id
        FROM public.transactions
        WHERE dwh_id IS NOT NULL
          AND log_date >= now() - INTERVAL '30 days'
        GROUP BY dwh_id
        HAVING count(DISTINCT NULLIF(btrim(semd_code), '')) > 1
     ) c)::bigint AS "Коллизии localUid (30д)";

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    LOOP
        IF r.relkind IN ('r', 'p') THEN
            EXECUTE format('ALTER TABLE public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'v' THEN
            EXECUTE format('ALTER VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'S' THEN
            EXECUTE format('ALTER SEQUENCE public.%I OWNER TO egisz', r.relname);
        END IF;
    END LOOP;

    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO egisz', r.sig);
    END LOOP;
END;
$$;

DO $$
DECLARE
    can_create boolean;
    can_usage  boolean;
BEGIN
    SELECT
        has_schema_privilege('egisz', 'public', 'CREATE'),
        has_schema_privilege('egisz', 'public', 'USAGE')
    INTO can_create, can_usage;

    IF NOT (can_create AND can_usage) THEN
        RAISE EXCEPTION 'egisz is still missing public schema privileges';
    END IF;
END;
$$;

-- Первичное наполнение отчётного слоя: выполняется, только когда в схеме уже есть
-- документы, а производные слои ещё пусты (развёртывание на существующий архив).
-- Сопровождение архива — пересчёт атрибутов, слоя версий и текстов ошибок — ведёт
-- суточный DAG обслуживания, обновление витрин — DAG витрин. Полные проходы в теле
-- наката пересекались по блокировкам с пятиминутным приёмом и давали взаимоблокировки.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.documents)
       AND NOT EXISTS (SELECT 1 FROM public.document_attributes) THEN
        PERFORM public.reconcile_document_attributes(NULL::text[]);
        PERFORM public.recompute_document_versions(NULL::text[]);
        -- Матпредставления созданы в 80/85/86 с данными; пересобираем после сборки
        -- атрибутов, чтобы отображаемые колонки (клиника, СЭМД) были финальными.
        -- Порядок обязателен: разбивка ошибок раньше периодических витрин.
        REFRESH MATERIALIZED VIEW public.rpt_error_breakdown;
        REFRESH MATERIALIZED VIEW public.rpt_documents_weekly;
        REFRESH MATERIALIZED VIEW public.rpt_error_breakdown_weekly;
        REFRESH MATERIALIZED VIEW public.rpt_documents_monthly;
        REFRESH MATERIALIZED VIEW public.rpt_error_breakdown_monthly;
    END IF;
END
$$;

ANALYZE public.exchangelog_raw;
ANALYZE public.documents;
ANALYZE public.transactions;
ANALYZE public.document_attributes;
ANALYZE public.rpt_error_breakdown;
ANALYZE public.rpt_documents_weekly;
ANALYZE public.rpt_error_breakdown_weekly;
ANALYZE public.rpt_documents_monthly;
ANALYZE public.rpt_error_breakdown_monthly;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
