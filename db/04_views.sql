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
DROP VIEW IF EXISTS public.rpt_health_message_registry_no_document CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.rpt_health_sync CASCADE;
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
DROP VIEW IF EXISTS public.rpt_document_file_request CASCADE;
-- Снятое имя представления отправленных документов.
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;
DROP VIEW IF EXISTS public.rpt_clinic_nsi_mapping CASCADE;
-- Снятое имя витрины типов СЭМД по лицензиям.
DROP VIEW IF EXISTS public.rpt_clinic_semd_licenses CASCADE;
DROP VIEW IF EXISTS public.rpt_clinic_semd_activity CASCADE;

-- Классификация даёт документу два поля: error_types (канонические типы,
-- error_classify) и error_text (исходные <message>, error_messages_row).
-- Объекты вне текущего контракта классификации.
DROP FUNCTION IF EXISTS public.error_interpretation_schematron_chunk(text);
DROP FUNCTION IF EXISTS public.error_interpretation_item(text, text);
DROP FUNCTION IF EXISTS public.error_interpretation_row(jsonb);
DROP FUNCTION IF EXISTS public.error_atom_normalize(text);
DROP FUNCTION IF EXISTS public.canonical_error_atom(text);
DROP FUNCTION IF EXISTS public.canonical_error_list(text);
DROP FUNCTION IF EXISTS public.error_category(text);
ALTER TABLE public.documents DROP COLUMN IF EXISTS error_summary;
ALTER TABLE public.transactions DROP COLUMN IF EXISTS error_summary;
-- Снятая таблица правил классификации.
DROP TABLE IF EXISTS public.error_interpretation_rules;

-- ---------------------------------------------------------------- section: document_attributes
-- ============================================================================
-- 70_views_core.sql — document_attributes (1:1 к documents)
-- Loaded by db/dwh_init.sql via \i db/04_views.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.document_attributes (
    dwh_id text PRIMARY KEY,
    clinic_oid_xml text,
    clinic_host text,
    clinic_jid_resolve_method text,
    patient_name_masked text,
    snils_masked text,
    doctor_name text,
    patient_hash text,
    doctor_hash text,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS request_msgid;

-- egisz_subsystem — подсистема ЕГИСЗ документа.
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

-- Реквизиты, снятые вместе с отказом от EGISZ_LICENSES в резолве:
--   clinic_oid_jpersons — JPERSONS.FIR_OID источником не заполняется;
--   clinic_oid_license  — OID берётся из содержания обмена (documents.org_oid);
--   message_endpoint    — адрес искали в теле ответа, где его нет (0 заполненных строк);
--   clinic_jid_mismatch — считается в rpt_document_versions поверх dim_clinic_oid.
ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS clinic_oid_jpersons;
ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS clinic_oid_license;
ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS message_endpoint;
ALTER TABLE public.document_attributes DROP COLUMN IF EXISTS clinic_jid_mismatch;

CREATE INDEX IF NOT EXISTS idx_document_attributes_updated_at
    ON public.document_attributes (updated_at);

-- Пересборка атрибутов документа из documents + справочников + последнего callback.
CREATE OR REPLACE FUNCTION public.recompute_document_attributes(p_dwh_ids text[] DEFAULT NULL)
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
        clinic_host,
        clinic_jid_resolve_method,
        patient_name_masked,
        snils_masked,
        doctor_name,
        patient_hash,
        doctor_hash,
        egisz_subsystem,
        updated_at
    )
    SELECT
        d.dwh_id,
        public.clean_text_value(d.org_oid) AS clinic_oid_xml,
        public.clean_host(COALESCE(attrs.clinic_host, ep.endpoint, reg.reply_to)) AS clinic_host,
        d.jid_resolve_method AS clinic_jid_resolve_method,
        tx.patient_name_masked,
        tx.snils_masked,
        tx.doctor_name,
        COALESCE(tx.patient_hash, d.patient_hash) AS patient_hash,
        COALESCE(tx.doctor_hash, d.doctor_hash) AS doctor_hash,
        sub.egisz_subsystem,
        now() AS updated_at
    FROM public.documents d
    LEFT JOIN public.document_attributes attrs ON attrs.dwh_id = d.dwh_id
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
    -- Источники host: сохранённый атрибут, текст транзакции, REPLY_TO реестра.
    LEFT JOIN LATERAL (
        SELECT public.extract_gost_endpoint(
            COALESCE(t.message, '')
        ) AS endpoint
        FROM public.transactions t
        WHERE t.logid = d.request_logid
        LIMIT 1
    ) ep ON TRUE
    LEFT JOIN LATERAL (
        SELECT m.reply_to
        FROM public.dim_message_document m
        WHERE m.document_uid = d.dwh_id
        ORDER BY m.source_egmid DESC
        LIMIT 1
    ) reg ON TRUE
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
        clinic_host = EXCLUDED.clinic_host,
        clinic_jid_resolve_method = EXCLUDED.clinic_jid_resolve_method,
        patient_name_masked = EXCLUDED.patient_name_masked,
        snils_masked = EXCLUDED.snils_masked,
        doctor_name = EXCLUDED.doctor_name,
        patient_hash = EXCLUDED.patient_hash,
        doctor_hash = EXCLUDED.doctor_hash,
        egisz_subsystem = EXCLUDED.egisz_subsystem,
        updated_at = now()
    -- Change-guard: переписываем строку (и двигаем updated_at) только при реальном
    -- расхождении. Без него полный reconcile (в т.ч. на каждом dwh_init) переписывал
    -- весь архив и менял updated_at — повторный прогон не был no-op (CLAUDE.md §3).
    WHERE
        public.document_attributes.clinic_oid_xml IS DISTINCT FROM EXCLUDED.clinic_oid_xml
     OR public.document_attributes.clinic_host IS DISTINCT FROM EXCLUDED.clinic_host
     OR public.document_attributes.clinic_jid_resolve_method IS DISTINCT FROM EXCLUDED.clinic_jid_resolve_method
     OR public.document_attributes.patient_name_masked IS DISTINCT FROM EXCLUDED.patient_name_masked
     OR public.document_attributes.snils_masked IS DISTINCT FROM EXCLUDED.snils_masked
     OR public.document_attributes.doctor_name IS DISTINCT FROM EXCLUDED.doctor_name
     OR public.document_attributes.patient_hash IS DISTINCT FROM EXCLUDED.patient_hash
     OR public.document_attributes.doctor_hash IS DISTINCT FROM EXCLUDED.doctor_hash
     OR public.document_attributes.egisz_subsystem IS DISTINCT FROM EXCLUDED.egisz_subsystem;

    GET DIAGNOSTICS refreshed = ROW_COUNT;
    RETURN refreshed;
END;
$$;

CREATE OR REPLACE FUNCTION public.recompute_document_jids(p_dwh_ids text[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    affected_dwh_ids text[] := ARRAY[]::text[];
    refreshed bigint := 0;
BEGIN
    WITH target_documents AS (
        SELECT
            d.dwh_id,
            d.jid,
            d.jid_resolve_method,
            d.org_oid,
            COALESCE(attrs.clinic_host, '') || ' ' || COALESCE(ep.endpoint, '') || ' ' || COALESCE(reg.reply_to, '') AS endpoint_text
        FROM public.documents d
        LEFT JOIN public.document_attributes attrs ON attrs.dwh_id = d.dwh_id
        LEFT JOIN LATERAL (
            SELECT public.extract_gost_endpoint(
                COALESCE(t.message, '')
            ) AS endpoint
            FROM public.transactions t
            WHERE t.logid = d.request_logid
            LIMIT 1
        ) ep ON TRUE
        LEFT JOIN LATERAL (
            SELECT m.reply_to
            FROM public.dim_message_document m
            WHERE m.document_uid = d.dwh_id
            ORDER BY m.source_egmid DESC
            LIMIT 1
        ) reg ON TRUE
        WHERE d.dwh_id IS NOT NULL
          AND (p_dwh_ids IS NULL OR d.dwh_id = ANY (p_dwh_ids))
    ),
    resolved AS (
        SELECT
            d.dwh_id,
            r.jid,
            r.resolve_method
        FROM target_documents d
        JOIN LATERAL public.resolve_document_jid(d.org_oid, d.endpoint_text) r ON TRUE
    ),
    updated AS (
        UPDATE public.documents d
        SET
            jid = r.jid,
            jid_resolve_method = r.resolve_method,
            updated_at = now()
        FROM resolved r
        WHERE d.dwh_id = r.dwh_id
          AND (
              d.jid IS DISTINCT FROM r.jid
           OR d.jid_resolve_method IS DISTINCT FROM r.resolve_method
          )
        RETURNING d.dwh_id
    )
    SELECT COALESCE(array_agg(dwh_id), ARRAY[]::text[])
    INTO affected_dwh_ids
    FROM updated;

    refreshed := COALESCE(cardinality(affected_dwh_ids), 0);
    IF refreshed > 0 THEN
        PERFORM public.recompute_document_attributes(affected_dwh_ids);
    END IF;

    RETURN refreshed;
END;
$$;

-- Параметр по умолчанию покрывает полный проход.
DROP FUNCTION IF EXISTS public.reconcile_document_attributes_ui();
DROP FUNCTION IF EXISTS public.reconcile_document_attributes(text[]);

-- error_text на грейне документа принадлежит последнему ответу и проставляется
-- в transform вместе со статусом.
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
    public.clean_text_value(d.org_oid) AS clinic_oid,
    a.clinic_host,
    -- OID из обмена не найден в реестре медорганизаций: клиника определена по адресу или
    -- пришла с OID, которого нет ни в одной лицензии. Признак живой — реестр меняется
    -- справочником, а не фактами документа.
    (NULLIF(btrim(d.org_oid), '') IS NOT NULL AND oid_ref.jid IS NULL) AS clinic_oid_unknown,
    -- msgid — собственный MSGID текущего события документа; relates_to_msgid —
    -- корреляционный MSGID из XML relatesToMessage/relatesTo.
    public.clean_text_value(d.msgid) AS msgid,
    public.clean_text_value(d.relates_to_msgid) AS relates_to_msgid,
    -- LOGID состояния: исход если есть, иначе LOGID отправки («Отправлено» несёт LOGID отправки).
    COALESCE(d.result_logid, d.request_logid)::text AS logid,
    d.request_logid::text AS request_logid,
    d.result_logid::text AS result_logid,
    -- Время отклика ЕГИСЗ: от запроса файла (шаг 6 схемы регистрации) до ответа.
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
-- Реестр OID — маленькое соединение (тысячи строк): коррелированный NOT EXISTS на грейне
-- документа пересобирал бы представление реестра на каждую строку.
LEFT JOIN public.dim_clinic_oid oid_ref ON oid_ref.oid = btrim(public.clean_text_value(d.org_oid))
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
    r.msgid,
    r.relates_to_msgid,
    r.clinic_host,
    r.attempt_count,
    r.is_resubmitted
FROM public.rpt_documents r
WHERE r.sent_state IS NOT NULL;

COMMENT ON VIEW public.rpt_documents_sent IS
'Отправленные документы без ответа ЕГИСЗ: ступень возраста обработки (dim_pending_segments) и состояние отправки («В обработке» / «Без ответа»).';

-- ---------------------------------------------------------------- section: document_file_request

CREATE OR REPLACE VIEW public.rpt_document_file_request AS
SELECT
    tx.log_date AS request_at,
    tx.logid::text AS request_logid,
    tx.msgid,
    public.clean_text_value(tx.xml_local_uid) AS semd_local_uid,
    public.clean_text_value(tx.xml_dwh_id) AS dwh_id,
    public.clean_text_value(tx.xml_emdr_id) AS emdr_id,
    public.normalize_semd_code(tx.xml_semd_code) AS semd_code,
    st.name AS semd_name,
    COALESCE(NULLIF(btrim(st.name), ''), public.normalize_semd_code(tx.xml_semd_code), '(неизвестно)') AS semd_label,
    tx.jid::text AS clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(btrim(o.name), ''), 'Клиника JID: ' || tx.jid::text, '(неизвестно)') AS clinic_label,
    public.clean_text_value(tx.xml_org_oid) AS clinic_oid,
    tx.egisz_subsystem,
    tx.link_method,
    next_tx.logid::text AS next_logid,
    next_tx.source_action AS next_action,
    next_tx.egisz_subsystem AS next_egisz_subsystem,
    next_tx.link_method AS next_link_method,
    existing_doc.dwh_id AS existing_document_dwh_id,
    existing_doc.status AS existing_document_status,
    existing_doc.registered_at AS existing_document_registered_at
FROM public.transactions tx
LEFT JOIN public.transactions next_tx ON next_tx.logid = tx.logid + 1
LEFT JOIN public.dim_organizations o ON o.jid = tx.jid
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM public.dim_semd_types dst
    WHERE dst.oid = public.normalize_semd_code(tx.xml_semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT d.dwh_id, d.status, d.registered_at
    FROM public.documents d
    WHERE lower(NULLIF(btrim(d.emdr_id), '')) = lower(NULLIF(btrim(tx.xml_emdr_id), ''))
    ORDER BY d.registered_at DESC NULLS LAST, d.last_callback_at DESC NULLS LAST, d.result_logid DESC NULLS LAST
    LIMIT 1
) existing_doc ON TRUE
WHERE tx.source_action = 'getDocumentFile'
  AND NULLIF(btrim(tx.xml_emdr_id), '') IS NOT NULL;

COMMENT ON VIEW public.rpt_document_file_request IS
'История запросов файлов уже зарегистрированных ЭМД: getDocumentFile с emdrId. Это не подача документа и не источник состояния documents.status=sent.';

-- ---------------------------------------------------------------- section: transport

CREATE OR REPLACE VIEW public.rpt_network_errors AS
SELECT
    r.ips_date,
    r.logid,
    r.msgid,
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
-- как есть в категорию «Прочие».
CREATE MATERIALIZED VIEW public.rpt_error_breakdown AS
WITH atom_types AS (
    SELECT DISTINCT
        doc.dwh_id,
        n.norm AS error_type,
        COALESCE(g.error_category, 'Прочие') AS error_category,
        -- Для непокрытых формулировок зона ответственности и повторяемость неизвестны.
        COALESCE(g.responsibility, 'смешанная') AS responsibility,
        COALESCE(g.is_retryable, false) AS is_retryable,
        -- Мнемоника, под которой пришёл отказ: своя, если тип рождён кодовым правилом,
        -- иначе зонтичная (тип распознан текстом внутри VALIDATION_ERROR/RUNTIME_ERROR).
        COALESCE(g.nsi_error_code, g.parent_nsi_error_code) AS nsi_error_code,
        c.nsi_error_description,
        -- Код отказа в своём пространстве имён: у контуров ИЭМК и шлюза мнемоники ФНСИ 305
        -- нет по существу, и без этой пары колонка кода оставалась бы у них пустой.
        g.error_code,
        g.code_namespace
    FROM public.documents doc
    CROSS JOIN LATERAL unnest(
        -- error_types гарантированно непустой ниже по WHERE, поэтому фолбэк не нужен.
        string_to_array(btrim(doc.error_types), ' · ')
    ) AS atom
    CROSS JOIN LATERAL (SELECT NULLIF(btrim(atom), '') AS norm) n
    LEFT JOIN public.dim_error_type_group g ON g.error_type = n.norm
    LEFT JOIN public.dim_nsi_error_code c
      ON c.nsi_error_code = COALESCE(g.nsi_error_code, g.parent_nsi_error_code)
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
    a.nsi_error_description,
    a.error_code,
    a.code_namespace
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
CREATE INDEX IF NOT EXISTS idx_rpt_eb_error_code ON public.rpt_error_breakdown (error_code);

COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown IS
'Разбивка ошибок (matview): один ряд = один канонический тип на документ (split documents.error_types по '' · ''). Обновляется refresh_error_breakdown() после transform.';

CREATE OR REPLACE VIEW public.rpt_document_lineage AS
SELECT
    d.dwh_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    a.clinic_oid_xml,
    a.clinic_host,
    a.clinic_jid_resolve_method,
    r.jid AS clinic_jid_by_oid,
    d.org_oid AS document_org_oid,
    d.jid_resolve_method AS document_jid_resolve_method
FROM public.documents d
LEFT JOIN public.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN public.dim_organizations o ON o.jid = d.jid
LEFT JOIN public.dim_clinic_oid r ON r.oid = btrim(public.clean_text_value(d.org_oid))
WHERE d.dwh_id IS NOT NULL;

COMMENT ON VIEW public.rpt_document_lineage IS
'Lineage документа: OID и адрес обмена из журнала рядом с ЮЛ, к которому их относит реестр OID.';

CREATE OR REPLACE VIEW public.rpt_clinic_nsi_mapping AS
SELECT
    o.jid,
    o.name AS cash_name,
    COALESCE(NULLIF(btrim(o.nsi_name), ''), NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), '')) AS nsi_name,
    o.inn,
    public.clean_text_value(o.fir_oid) AS oid,
    (NULLIF(btrim(o.fir_oid), '') IS NOT NULL) AS is_mapped
FROM public.dim_organizations o
LEFT JOIN public.dim_nsi_organization n ON n.oid = public.clean_text_value(o.fir_oid)
WHERE o.jid IS NOT NULL;

COMMENT ON VIEW public.rpt_clinic_nsi_mapping IS
'Аудит сопоставления клиник CASH/JPERSONS с НСИ 1461: JID, наименование CASH, наименование НСИ, ИНН, OID и признак сопоставления.';

-- Типы СЭМД, которые клиника фактически отправляет: грейн (clinic_jid, semd_code)
-- по документам.
-- clinic_label собирается идентично rpt_documents, чтобы общий дашборд-фильтр «Клиника»
-- привязывался одним значением к обеим витринам.
CREATE OR REPLACE VIEW public.rpt_clinic_semd_activity AS
SELECT
    f.clinic_jid,
    COALESCE(NULLIF(BTRIM(f.clinic_jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(BTRIM(o.name), ''), '—') AS clinic_label,
    o.name AS clinic_name,
    f.semd_code,
    st.name AS semd_name,
    CASE
        WHEN st.name IS NOT NULL THEN f.semd_code || ' · ' || st.name
        ELSE f.semd_code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
    END AS semd_label,
    f.last_sent_at,
    f.last_registered_at,
    f.documents_total
FROM (
    -- Счётчик на грейне логического документа (rpt_documents = текущие версии), иначе
    -- повторная подача того же документа считалась бы как ещё один документ клиники.
    SELECT
        r.clinic_jid,
        r.semd_code,
        MAX(r.first_sent_at) AS last_sent_at,
        MAX(r.registered_at) AS last_registered_at,
        count(*) AS documents_total
    FROM public.rpt_documents r
    WHERE r.clinic_jid IS NOT NULL
      AND r.semd_code IS NOT NULL
    GROUP BY 1, 2
) f
LEFT JOIN public.dim_organizations o ON o.jid = f.clinic_jid
LEFT JOIN public.dim_semd_types st ON st.code = f.semd_code;

COMMENT ON VIEW public.rpt_clinic_semd_activity IS
'Типы СЭМД в обмене клиники: грейн (clinic_jid, semd_code) по фактам документов; последняя отправка, последняя регистрация и число документов.';

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
-- docs_success + docs_error = docs_total (отправленные без ответа вне корпуса).
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
-- docs_success + docs_error = docs_total (отправленные без ответа вне корпуса).
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
sent_without_response AS (
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
    COALESCE(q.sent_cnt, 0)::bigint AS "Отправлено без ответа (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.sent_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.sent_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN sent_without_response q ON q.clinic_jid = f.clinic_jid;

-- Сопоставление отметок конвейера с фактами DWH: докуда журнал выгружен, докуда разобран
-- и куда дошли документы. Сам шлюз здесь не проверяется.
CREATE OR REPLACE VIEW public.rpt_health_sync AS
SELECT
    (SELECT COUNT(*) FROM public.documents)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent')::bigint AS "Отправлено без ответа",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours')::bigint AS "Без ответа > 24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '24 hours' AND first_sent_at < now() - INTERVAL '1 hour')::bigint AS "Без ответа 1-24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '1 hour')::bigint AS "Без ответа < 1ч",
    (SELECT MAX(first_sent_at) FROM public.documents) AS "DWH max Sent",
    (SELECT updated_at FROM etl_state WHERE pipeline = 'egisz') AS "Последний апдейт курсора",
    -- Хвост exchangelog_raw отдельной колонкой не выводится: он выводим из самой таблицы
    -- и в установившемся режиме повторяет позицию разбора.
    (SELECT extract_logid_cursor FROM etl_state WHERE pipeline = 'egisz') AS "etl_state.extract_logid_cursor",
    (SELECT transform_logid_cursor FROM etl_state WHERE pipeline = 'egisz') AS "etl_state.transform_logid_cursor",
    (SELECT MAX(COALESCE(result_logid, request_logid)) FROM public.documents) AS "DWH max LOGID fact",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.rpt_health_message_registry_no_document AS
SELECT
    tx.log_date AS "Дата события",
    tx.logid::text AS "LOGID",
    tx.source_action AS "Метод",
    tx.status AS "Статус ответа",
    tx.link_method AS "Метод связки",
    tx.egisz_subsystem AS "Подсистема ЕГИСЗ",
    tx.msgid AS "MSGID сообщения",
    tx.relates_to_msgid AS "relatesTo MSGID",
    reg.source_egmid::text AS "EGMID EGISZ_MESSAGES",
    reg.created_at AS "Дата EGISZ_MESSAGES",
    tx.dwh_id AS "dwh_id",
    tx.local_uid_semd AS "localUid",
    tx.emdr_id AS "emdrId",
    tx.semd_code AS "Код СЭМД",
    tx.jid::text AS "JID Клиники",
    COALESCE(NULLIF(btrim(o.name), ''), 'Клиника JID: ' || tx.jid::text, '(неизвестно)') AS "Клиника",
    LEFT(COALESCE(tx.message, ''), 240) AS "Сообщение",
    reg.reply_to AS "replyTo EGISZ_MESSAGES"
FROM public.transactions tx
JOIN LATERAL (
    SELECT m.source_egmid, m.created_at, m.reply_to
    FROM public.dim_message_document m
    WHERE tx.relates_to_msgid IS NOT NULL
      AND m.msgid = public.message_registry_key(tx.relates_to_msgid)
      AND m.document_uid IS NULL
    ORDER BY m.source_egmid DESC NULLS LAST
    LIMIT 1
) reg ON TRUE
LEFT JOIN public.dim_organizations o ON o.jid = tx.jid
WHERE tx.relates_to_msgid IS NOT NULL
  AND tx.egisz_subsystem IS DISTINCT FROM 'ИЭМК';

COMMENT ON VIEW public.rpt_health_message_registry_no_document IS
'Health-детализация РЭМД: relatesToMessage найден в EGISZ_MESSAGES, DOCUMENTID пустой. ИЭМК не использует DOCUMENTID.';

CREATE OR REPLACE VIEW public.rpt_health_signals AS
WITH anchor AS (
    SELECT MAX(COALESCE(last_callback_at, first_sent_at, document_created_at)) AS last_fact_ts
    FROM public.documents
),
-- Доля ответов ЕГИСЗ, которые не удалось связать с документом, за последний час приёма.
-- Ответ несёт relatesToMessage; документ находится по реестру подач. Рост доли означает,
-- что реестр отстал от журнала или подача в него не попала, — исход отправки при этом
-- теряется, а документ остаётся в статусе «Отправлено».
--
-- Доля считается по последним ответам журнала — скользящей выборкой по LOGID.
-- Выборка не пустеет, пока есть хотя бы один размеченный
-- ответ, не зависит от темпа приёма и не тянет дату разбора в отчётность.
unlinked_recent AS (
    SELECT ROUND(
        100.0 * COUNT(*) FILTER (WHERE link_method = 'unlinked')
        / NULLIF(COUNT(*), 0),
        1
    ) AS pct
    FROM (
        SELECT link_method
        FROM public.transactions
        WHERE link_method IS NOT NULL
          AND relates_to_msgid IS NOT NULL
        ORDER BY logid DESC
        LIMIT 500
    ) recent
),
registry_no_document_recent AS (
    SELECT COUNT(*)::numeric AS cnt
    FROM (
        SELECT "LOGID"
        FROM public.rpt_health_message_registry_no_document
        ORDER BY "LOGID"::bigint DESC
        LIMIT 500
    ) recent
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
),
-- Полнота выгрузки журнала. Шлюз нумерует EXCHANGELOG непрерывно, поэтому число
-- пропущенных LOGID между краями загруженного диапазона — прямая мера потерь. Это тот же
-- инвариант, по которому продвигается etl_state.extract_logid_cursor: отметка идёт только
-- по непрерывному участку, значит ненулевое значение означает и недостачу строк, и
-- остановку разбора на этом месте.
journal_gaps AS (
    SELECT CASE
        WHEN MAX(r.logid) IS NULL THEN 0
        ELSE MAX(r.logid) - COALESCE(MAX(s.transform_logid_cursor), 0) - COUNT(*)
    END AS missing
    FROM public.etl_state s
    LEFT JOIN public.exchangelog_raw r
      ON r.logid > COALESCE(s.transform_logid_cursor, 0)
    WHERE s.pipeline = 'egisz'
),
-- Стык месячной сетки партиций: нижняя граница каждой партиции обязана совпадать с верхней
-- границей предыдущей. Расхождение означает смену якоря сетки — зазор, в который строка не
-- вставится вовсе, либо перекрытие, которое ломает создание следующего месяца. Проверка по
-- имени партиции этого не видит, поэтому сигнал считается по фактическим границам каталога.
partition_grid AS (
    SELECT COUNT(*) AS breaks
    FROM (
        SELECT lag(upper_bound) OVER (PARTITION BY parent_name ORDER BY lower_bound) AS prev_upper,
               lower_bound
        FROM (
            SELECT parent.relname AS parent_name,
                   (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'FROM \(''([^'']+)''\)'))[1]::timestamptz AS lower_bound,
                   (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'TO \(''([^'']+)''\)'))[1]::timestamptz AS upper_bound
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            JOIN pg_partitioned_table p ON p.partrelid = parent.oid
            WHERE n.nspname = 'public'
              AND p.partstrat = 'r'
              AND p.partnatts = 1
              AND pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
        ) bounds
    ) ordered
    WHERE prev_upper IS NOT NULL AND prev_upper <> lower_bound
),
-- Клиники, документы которых есть, а записи в справочнике организаций нет: подпись
-- вырождается в «<jid> · —» во всех срезах. Наполнение справочника — задача выгрузки
-- (JPERSONS выгружается целиком), поэтому в отчётном слое это только сигнал.
unknown_clinics AS (
    SELECT d.jid, COUNT(*) AS docs
    FROM public.documents d
    LEFT JOIN public.dim_organizations o ON o.jid = d.jid
    WHERE d.jid IS NOT NULL AND o.jid IS NULL
    GROUP BY d.jid
)
SELECT * FROM (
    VALUES
        ('parsed_documents', 'Разложенные документы proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM public.documents), 'документов', 'documents', 'Контроль поступления СЭМД в DWH'),
        ('sent_24h', 'Отправлено без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours'), 'документов', 'documents.status=sent', 'Проверить клиники без ответа ЕГИСЗ и транспортный канал'),
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
         'Проверить транспорт клиник на вкладке «Отправленные»: ответ по этим документам уже не ожидается'),
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
        ('unknown_clinics',
         'JID без записи в справочнике организаций',
         CASE
             WHEN (SELECT COUNT(*) FROM unknown_clinics) >= 10 THEN 'red'
             WHEN (SELECT COUNT(*) FROM unknown_clinics) >= 1 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM unknown_clinics),
         'клиник',
         'documents.jid вне dim_organizations',
         'Проверить наличие организации в JPERSONS источника: подпись клиники выводится как «<jid> · —»'),
        ('journal_continuity',
         'Пропуски в выгрузке журнала',
         CASE
             WHEN (SELECT missing FROM journal_gaps) >= 100 THEN 'red'
             WHEN (SELECT missing FROM journal_gaps) >= 1 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT missing FROM journal_gaps)::numeric,
         'LOGID',
         'разрывы LOGID в необработанном хвосте exchangelog_raw',
         'Проверить Firebird и consistency_check. Разобранный raw можно архивировать'),
        ('partition_grid',
         'Разрывы в сетке партиций',
         CASE WHEN (SELECT breaks FROM partition_grid) >= 1 THEN 'red' ELSE 'green' END,
         (SELECT breaks FROM partition_grid)::numeric,
         'стыков',
         'границы партиций pg_inherits: нижняя граница ≠ верхней границе предыдущей',
         'Сетка потеряла общий якорь: партиции созданы при разных часовых поясах сессии. Строка в зазор не вставится, следующий месяц не создастся'),
        ('unlinked_responses',
         'Доля несвязанных ответов ЕГИСЗ',
         CASE
             WHEN COALESCE((SELECT pct FROM unlinked_recent), 0) >= 5 THEN 'red'
             WHEN COALESCE((SELECT pct FROM unlinked_recent), 0) >= 1 THEN 'yellow'
             ELSE 'green'
         END,
         COALESCE((SELECT pct FROM unlinked_recent), 0)::numeric,
         '% последних ответов',
         'transactions.link_method=unlinked, последние 500 по LOGID',
        'Проверить dim_message_document: в реестре нет строки по msgid из relatesToMessage'),
        ('message_registry_no_document',
         'РЭМД: EGISZ_MESSAGES без DOCUMENTID',
         CASE
             WHEN COALESCE((SELECT cnt FROM registry_no_document_recent), 0) >= 100 THEN 'red'
             WHEN COALESCE((SELECT cnt FROM registry_no_document_recent), 0) >= 10 THEN 'yellow'
             ELSE 'green'
        END,
         COALESCE((SELECT cnt FROM registry_no_document_recent), 0)::numeric,
         'ответов в последних 500',
         'РЭМД: relatesToMessage найден в EGISZ_MESSAGES, DOCUMENTID пустой',
         'Проверить заполнение DOCUMENTID в EGISZ_MESSAGES РЭМД'),
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

-- Смена владельца требует ACCESS EXCLUSIVE. Объект, занятый работающим конвейером
-- (etl_state правится каждым запуском ETL), уводил весь накат в ошибку по lock_timeout —
-- при том что смена владельца ему, как правило, и не нужна. Поэтому владелец меняется
-- только там, где он не egisz, а занятый объект пропускается с предупреждением: схема
-- идемпотентна, следующий прогон доберёт его.
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
          AND pg_get_userbyid(c.relowner) <> 'egisz'
    LOOP
        BEGIN
            IF r.relkind IN ('r', 'p') THEN
                EXECUTE format('ALTER TABLE public.%I OWNER TO egisz', r.relname);
            ELSIF r.relkind = 'v' THEN
                EXECUTE format('ALTER VIEW public.%I OWNER TO egisz', r.relname);
            ELSIF r.relkind = 'm' THEN
                EXECUTE format('ALTER MATERIALIZED VIEW public.%I OWNER TO egisz', r.relname);
            ELSIF r.relkind = 'S' THEN
                EXECUTE format('ALTER SEQUENCE public.%I OWNER TO egisz', r.relname);
            END IF;
        EXCEPTION WHEN lock_not_available OR query_canceled THEN
            RAISE WARNING 'owner change skipped for %: object is busy', r.relname;
        END;
    END LOOP;

    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND pg_get_userbyid(p.proowner) <> 'egisz'
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
-- документы, а атрибуты ещё пусты (развёртывание на существующий архив).
-- Сопровождение архива — пересчёт атрибутов, слоя версий и текстов ошибок — ведёт
-- суточный DAG обслуживания, обновление витрин — DAG витрин. Полные проходы в теле
-- наката пересекались по блокировкам с пятиминутным приёмом и давали взаимоблокировки.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.documents)
       AND NOT EXISTS (SELECT 1 FROM public.document_attributes) THEN
        PERFORM public.recompute_document_attributes(NULL::text[]);
        PERFORM public.recompute_document_versions(NULL::text[]);
        -- Матпредставления созданы в 80/85/86 с данными; пересобираем после сборки
        -- атрибутов, чтобы отображаемые колонки (клиника, СЭМД) были финальными.
        -- Порядок обязателен: разбивка ошибок перед периодическими витринами.
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
