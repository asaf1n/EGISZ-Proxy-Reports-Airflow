-- ============================================================================
-- 04_views.sql — drop dependents, document attributes, serving layer, health, finalize
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: drop_dependents
-- ============================================================================
-- Отчётный слой пересобирается целиком: CREATE OR REPLACE VIEW не меняет состав колонок,
-- поэтому представления и материализованные представления удаляются и создаются заново.
-- ============================================================================

DROP VIEW IF EXISTS mart_egisz_admin.health_by_clinic CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.health_signals CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.health_message_registry_no_document CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.health_sync CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.health_versions CASCADE;
DROP VIEW IF EXISTS serving_egisz.network_errors CASCADE;
DROP VIEW IF EXISTS stg_egisz.message_errors CASCADE;
-- Недельный и месячный слои читают documents_current и текущие ошибки документа —
-- удаляются до них.
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stg_egisz.document_errors_current CASCADE;
DROP VIEW IF EXISTS serving_egisz.documents_current CASCADE;
DROP VIEW IF EXISTS serving_egisz.document_versions CASCADE;
DROP VIEW IF EXISTS serving_egisz.documents_sent CASCADE;
DROP VIEW IF EXISTS serving_egisz.document_file_requests CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.document_lineage CASCADE;
DROP VIEW IF EXISTS serving_egisz.clinic_nsi_mapping CASCADE;
DROP VIEW IF EXISTS serving_egisz.clinic_semd_activity CASCADE;
DROP VIEW IF EXISTS serving_egisz.semd_dictionaries CASCADE;
DROP VIEW IF EXISTS serving_egisz.semd_guides CASCADE;

-- ---------------------------------------------------------------- section: document_attributes
-- ============================================================================
-- document_attributes (1:1 к documents)
-- Loaded by db/dwh_init.sql via \i db/04_views.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart_egisz.document_attributes (
    dwh_id text PRIMARY KEY,
    clinic_oid_xml text,
    clinic_host text,
    clinic_jid_resolve_method text,
    patient_name_masked text,
    snils_masked text,
    doctor_name text,
    patient_hash text,
    doctor_hash text,
    updated_at timestamptz DEFAULT now(),
    egisz_subsystem text
);

CREATE INDEX IF NOT EXISTS idx_document_attributes_updated_at
    ON mart_egisz.document_attributes (updated_at);

-- Пересборка атрибутов документа из documents + справочников + последнего callback.
CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_attributes(p_dwh_ids text[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    refreshed bigint := 0;
BEGIN
    IF p_dwh_ids IS NULL THEN
        SELECT COALESCE(array_agg(d.dwh_id), ARRAY[]::text[])
        INTO p_dwh_ids
        FROM mart_egisz.documents d
        WHERE d.dwh_id IS NOT NULL;
    END IF;

    IF COALESCE(cardinality(p_dwh_ids), 0) = 0 THEN
        RETURN 0;
    END IF;

    INSERT INTO mart_egisz.document_attributes (
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
        stg_egisz.clean_text_value(d.org_oid) AS clinic_oid_xml,
        stg_egisz.clean_host(COALESCE(attrs.clinic_host, ep.endpoint, reg.reply_to)) AS clinic_host,
        d.jid_resolve_method AS clinic_jid_resolve_method,
        tx.patient_name_masked,
        tx.snils_masked,
        tx.doctor_name,
        COALESCE(tx.patient_hash, d.patient_hash) AS patient_hash,
        COALESCE(tx.doctor_hash, d.doctor_hash) AS doctor_hash,
        sub.egisz_subsystem,
        now() AS updated_at
    FROM mart_egisz.documents d
    LEFT JOIN mart_egisz.document_attributes attrs ON attrs.dwh_id = d.dwh_id
    LEFT JOIN LATERAL (
        SELECT
            t.patient_name_masked,
            t.snils_masked,
            t.doctor_name,
            t.patient_hash,
            t.doctor_hash
        FROM stg_egisz.exchange_messages t
        WHERE t.dwh_id = d.dwh_id
        ORDER BY t.log_date DESC NULLS LAST, t.logid DESC
        LIMIT 1
    ) tx ON TRUE
    -- Источники host: сохранённый атрибут, текст транзакции, REPLY_TO реестра.
    LEFT JOIN LATERAL (
        SELECT stg_egisz.extract_gost_endpoint(
            COALESCE(t.message, '')
        ) AS endpoint
        FROM stg_egisz.exchange_messages t
        WHERE t.logid = d.request_logid
        LIMIT 1
    ) ep ON TRUE
    LEFT JOIN LATERAL (
        SELECT m.reply_to
        FROM stg_egisz.message_registry m
        WHERE m.document_uid = d.dwh_id
        ORDER BY m.egmid DESC
        LIMIT 1
    ) reg ON TRUE
    LEFT JOIN LATERAL (
        SELECT t.egisz_subsystem
        FROM stg_egisz.exchange_messages t
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
        mart_egisz.document_attributes.clinic_oid_xml IS DISTINCT FROM EXCLUDED.clinic_oid_xml
     OR mart_egisz.document_attributes.clinic_host IS DISTINCT FROM EXCLUDED.clinic_host
     OR mart_egisz.document_attributes.clinic_jid_resolve_method IS DISTINCT FROM EXCLUDED.clinic_jid_resolve_method
     OR mart_egisz.document_attributes.patient_name_masked IS DISTINCT FROM EXCLUDED.patient_name_masked
     OR mart_egisz.document_attributes.snils_masked IS DISTINCT FROM EXCLUDED.snils_masked
     OR mart_egisz.document_attributes.doctor_name IS DISTINCT FROM EXCLUDED.doctor_name
     OR mart_egisz.document_attributes.patient_hash IS DISTINCT FROM EXCLUDED.patient_hash
     OR mart_egisz.document_attributes.doctor_hash IS DISTINCT FROM EXCLUDED.doctor_hash
     OR mart_egisz.document_attributes.egisz_subsystem IS DISTINCT FROM EXCLUDED.egisz_subsystem;

    GET DIAGNOSTICS refreshed = ROW_COUNT;
    RETURN refreshed;
END;
$$;

CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_jids(p_dwh_ids text[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
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
        FROM mart_egisz.documents d
        LEFT JOIN mart_egisz.document_attributes attrs ON attrs.dwh_id = d.dwh_id
        LEFT JOIN LATERAL (
            SELECT stg_egisz.extract_gost_endpoint(
                COALESCE(t.message, '')
            ) AS endpoint
            FROM stg_egisz.exchange_messages t
            WHERE t.logid = d.request_logid
            LIMIT 1
        ) ep ON TRUE
        LEFT JOIN LATERAL (
            SELECT m.reply_to
            FROM stg_egisz.message_registry m
            WHERE m.document_uid = d.dwh_id
            ORDER BY m.egmid DESC
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
        JOIN LATERAL mart_egisz.resolve_document_jid(d.org_oid, d.endpoint_text) r ON TRUE
    ),
    updated AS (
        UPDATE mart_egisz.documents d
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
        PERFORM mart_egisz.recompute_document_attributes(affected_dwh_ids);
    END IF;

    RETURN refreshed;
END;
$$;

-- ---------------------------------------------------------------- section: documents_current
-- ============================================================================
-- serving_egisz — представления и агрегаты для потребителей
-- Loaded by db/dwh_init.sql via \i db/04_views.sql.
-- ============================================================================

CREATE OR REPLACE VIEW serving_egisz.document_versions AS
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
    -- на вкладке отправленных.
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
    stg_egisz.normalize_semd_code(d.semd_code) AS semd_code,
    st.name AS semd_name,
    CASE
        WHEN st.code IS NOT NULL AND st.name IS NOT NULL
            THEN st.code || ' · ' || st.name
        WHEN st.code IS NOT NULL
            THEN st.code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
        ELSE NULL
    END AS semd_label,
    stg_egisz.clean_text_value(d.local_uid) AS semd_local_uid,
    d.document_created_at AS semd_created_at,
    d.emdr_id AS semd_emdr_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(BTRIM(d.jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(BTRIM(o.name), ''), '—') AS clinic_label,
    o.inn AS clinic_inn,
    stg_egisz.clean_text_value(d.org_oid) AS clinic_oid,
    a.clinic_host,
    -- OID из обмена не найден в реестре медорганизаций: клиника определена по адресу или
    -- пришла с OID, которого нет ни в одной лицензии. Признак живой — реестр меняется
    -- справочником, а не данными документа.
    (NULLIF(btrim(d.org_oid), '') IS NOT NULL AND oid_ref.jid IS NULL) AS clinic_oid_unknown,
    -- msgid — собственный MSGID текущего события документа; relates_to_msgid —
    -- корреляционный MSGID из XML relatesToMessage/relatesTo.
    stg_egisz.clean_text_value(d.msgid) AS msgid,
    stg_egisz.clean_text_value(d.relates_to_msgid) AS relates_to_msgid,
    -- LOGID состояния: исход если есть, иначе LOGID отправки («Отправлено» несёт LOGID отправки).
    COALESCE(d.result_logid, d.request_logid)::text AS logid,
    d.request_logid::text AS request_logid,
    d.result_logid::text AS result_logid,
    -- Время отклика ЕГИСЗ: от запроса файла (шаг 6 схемы регистрации) до ПОСЛЕДНЕГО ответа.
    -- Считается по журналу, а не по дате создания CDA: document_created_at приходит далеко
    -- не во всех отправках, и метрика на его основе покрывала доли процента набора
    -- документов. Выход из очереди обработки определяет не эта величина, а
    -- first_callback_at — отметка первого ответа.
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
    d.first_callback_at,
    -- Число подач документа в ЕГИСЗ по реестру шлюза: повторная подача не меняет localUid,
    -- поэтому счётчик показывает, сколько раз документ отправлялся до текущего исхода.
    COALESCE(d.attempt_count, 1) AS attempt_count,
    (COALESCE(d.attempt_count, 1) > 1) AS is_resubmitted,
    -- Слой версий.
    d.document_group_id,
    COALESCE(d.is_current_version, true) AS is_current_version,
    d.semd_version_number,
    d.document_group_confidence,
    d.superseded_by_dwh_id,
    d.supersedes_dwh_id
FROM mart_egisz.documents d
LEFT JOIN mart_egisz.document_attributes a ON a.dwh_id = d.dwh_id
-- Реестр OID — маленькое соединение (тысячи строк): коррелированный NOT EXISTS на грейне
-- документа пересобирал бы представление реестра на каждую строку.
LEFT JOIN mart_egisz.dim_clinic_oid oid_ref ON oid_ref.oid = btrim(stg_egisz.clean_text_value(d.org_oid))
LEFT JOIN mart_egisz.dim_document_status ds ON ds.code = d.status
-- Ступень подбирает pending_segment_code_at от якоря представления — текущего момента.
-- CASE оставляет вызов только нефинальным статусам: у документа с исходом ожидания нет.
LEFT JOIN mart_egisz.dim_pending_segments ps
    ON ps.code = CASE
        WHEN ds.is_final THEN NULL
        ELSE serving_egisz.pending_segment_code_at(d.first_sent_at, now())
    END
LEFT JOIN mart_egisz.dim_sent_state ss
    ON ss.code = CASE
        WHEN ps.code IS NULL THEN NULL          -- финальный статус: состояния отправки нет
        WHEN ps.is_no_response THEN 'no_response'
        ELSE 'pending'
    END
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = d.jid
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM mart_egisz.dim_semd_types dst
    WHERE dst.oid = stg_egisz.normalize_semd_code(d.semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
WHERE NULLIF(btrim(d.dwh_id), '') IS NOT NULL;

COMMENT ON VIEW serving_egisz.document_versions IS
'Все экземпляры/версии отправки СЭМД: одна строка на dwh_id (полный аудит, включая superseded).';

-- Основная витрина — ТЕКУЩИЕ версии (один логический документ = одна строка). Все попытки
-- (включая superseded) — document_versions.
CREATE OR REPLACE VIEW serving_egisz.documents_current AS
SELECT * FROM serving_egisz.document_versions
WHERE is_current_version;

COMMENT ON VIEW serving_egisz.documents_current IS
'Документная витрина (текущие версии, is_current_version): одна строка на логический документ. Полный аудит версий — document_versions.';

-- Якорь объявлен один раз и обслуживает и ступень, и возраст: разные якоря у соседних
-- колонок давали бы «до 5 минут» рядом с ненулевым числом суток.
CREATE OR REPLACE VIEW serving_egisz.documents_sent AS
SELECT
    r.dwh_id,
    r.first_sent_at,
    EXTRACT(EPOCH FROM (anchor.ts - r.first_sent_at)) / 3600.0 AS pending_hours,
    ROUND(EXTRACT(EPOCH FROM (anchor.ts - r.first_sent_at)) / 86400.0, 1) AS pending_days,
    seg.code AS pending_segment,
    seg.label AS pending_segment_label,
    seg.sort_order AS pending_segment_sort,
    st.code AS sent_state,
    st.label AS sent_state_label,
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
FROM serving_egisz.documents_current r
CROSS JOIN LATERAL (SELECT now() AS ts) anchor
LEFT JOIN mart_egisz.dim_pending_segments seg
    ON seg.code = serving_egisz.pending_segment_code_at(r.first_sent_at, anchor.ts)
LEFT JOIN mart_egisz.dim_sent_state st
    ON st.code = CASE WHEN seg.is_no_response THEN 'no_response' ELSE 'pending' END
WHERE r.sent_state IS NOT NULL;

COMMENT ON VIEW serving_egisz.documents_sent IS
'Отправленные документы без ответа ЕГИСЗ: ступень возраста обработки (dim_pending_segments), возраст и состояние отправки («В обработке» / «Без ответа») на текущий момент. Срез на прошлый момент строится теми же функциями от своего якоря (is_pending_at, pending_segment_code_at).';

-- ---------------------------------------------------------------- section: document_file_request

CREATE OR REPLACE VIEW serving_egisz.document_file_requests AS
SELECT
    tx.log_date AS request_at,
    tx.logid::text AS request_logid,
    tx.msgid,
    stg_egisz.clean_text_value(tx.xml_local_uid) AS semd_local_uid,
    stg_egisz.clean_text_value(tx.xml_dwh_id) AS dwh_id,
    stg_egisz.clean_text_value(tx.xml_emdr_id) AS emdr_id,
    stg_egisz.normalize_semd_code(tx.xml_semd_code) AS semd_code,
    st.name AS semd_name,
    COALESCE(NULLIF(btrim(st.name), ''), stg_egisz.normalize_semd_code(tx.xml_semd_code), '(неизвестно)') AS semd_label,
    tx.jid::text AS clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(btrim(o.name), ''), 'Клиника JID: ' || tx.jid::text, '(неизвестно)') AS clinic_label,
    stg_egisz.clean_text_value(tx.xml_org_oid) AS clinic_oid,
    tx.egisz_subsystem,
    tx.link_method,
    next_tx.logid::text AS next_logid,
    next_tx.source_action AS next_action,
    next_tx.egisz_subsystem AS next_egisz_subsystem,
    next_tx.link_method AS next_link_method,
    existing_doc.dwh_id AS existing_document_dwh_id,
    existing_doc.status AS existing_document_status,
    existing_doc.registered_at AS existing_document_registered_at
FROM stg_egisz.exchange_messages tx
LEFT JOIN stg_egisz.exchange_messages next_tx ON next_tx.logid = tx.logid + 1
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = tx.jid
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM mart_egisz.dim_semd_types dst
    WHERE dst.oid = stg_egisz.normalize_semd_code(tx.xml_semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT d.dwh_id, d.status, d.registered_at
    FROM mart_egisz.documents d
    WHERE lower(NULLIF(btrim(d.emdr_id), '')) = lower(NULLIF(btrim(tx.xml_emdr_id), ''))
    ORDER BY d.registered_at DESC NULLS LAST, d.last_callback_at DESC NULLS LAST, d.result_logid DESC NULLS LAST
    LIMIT 1
) existing_doc ON TRUE
WHERE tx.source_action = 'getDocumentFile'
  AND NULLIF(btrim(tx.xml_emdr_id), '') IS NOT NULL;

COMMENT ON VIEW serving_egisz.document_file_requests IS
'История запросов файлов уже зарегистрированных ЭМД: getDocumentFile с emdrId. Это не подача документа и не источник состояния documents.status=sent.';

-- ---------------------------------------------------------------- section: transport

-- Элементы ошибки хранятся в разобранных сообщениях stg_egisz.exchange_messages: отчётный слой читает
-- их только в этом разделе.

-- Ошибки текущего состояния документа: элементы последнего асинхронного ответа и ошибки
-- связи после него; у документа без асинхронного ответа — все его ошибки связи. Время
-- последнего ответа берётся из тех же разобранных сообщений. Строка — одна ошибка
-- документа; error_no нумерует ошибки документа по порядку сообщений.
CREATE MATERIALIZED VIEW stg_egisz.document_errors_current AS
WITH last_response AS (
    SELECT t.dwh_id, max(t.log_date) AS responded_at
    FROM stg_egisz.exchange_messages t
    WHERE t.dwh_id IS NOT NULL
      AND t.status IN ('success', 'error')
    GROUP BY t.dwh_id
)
SELECT
    tx.dwh_id,
    row_number() OVER (PARTITION BY tx.dwh_id ORDER BY tx.log_date, tx.logid, e.item_no)::integer AS error_no,
    tx.log_date AS message_at,
    tx.egisz_subsystem,
    tx.source_action,
    e.error_kind,
    e.error_code,
    e.error_text COLLATE "und-x-icu" AS error_text,
    e.error_type,
    e.nsi_dictionary_oid
FROM stg_egisz.exchange_messages tx
LEFT JOIN last_response lr ON lr.dwh_id = tx.dwh_id
CROSS JOIN LATERAL jsonb_to_recordset(tx.error_details)
    AS e(item_no integer, error_kind text, error_code text, error_text text, error_type text, nsi_dictionary_oid text)
WHERE tx.dwh_id IS NOT NULL
  AND tx.error_details IS NOT NULL
  AND tx.log_date >= COALESCE(lr.responded_at, '-infinity'::timestamptz)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors_current
    ON stg_egisz.document_errors_current (dwh_id, error_no);
CREATE INDEX IF NOT EXISTS idx_document_errors_current_type
    ON stg_egisz.document_errors_current (error_type);

COMMENT ON MATERIALIZED VIEW stg_egisz.document_errors_current IS
'Ошибки текущего состояния документа: элементы последнего асинхронного ответа и ошибки связи после него. Строка — одна ошибка документа, ключ (dwh_id, error_no) общий с serving_egisz.document_errors. error_text — исходный текст: в опубликованный слой он не выносится, дашборды до решения о доступе читают его отсюда. Обновляется refresh_report_marts() после transform.';

-- Элементы ошибки всех разобранных сообщений по времени сообщения, в том числе без связи с
-- документом. Исходный текст остаётся в слое разбора (стандарт хранилища, §2): дашборды до
-- решения о доступе читают его отсюда. Коллация текстов задана явно: база развёрнута с
-- lc_ctype = C, где ILIKE складывает регистр только для латиницы, и отбор «содержит» по
-- кириллице молча терял строки; корневая — потому что текст смешанный.
CREATE VIEW stg_egisz.message_errors AS
SELECT
    tx.log_date AS message_at,
    tx.logid,
    tx.msgid,
    tx.dwh_id,
    tx.jid AS clinic_jid,
    stg_egisz.normalize_semd_code(tx.semd_code) AS semd_code,
    tx.egisz_subsystem,
    tx.source_action,
    e.item_no,
    e.error_kind,
    e.error_code,
    e.error_text COLLATE "und-x-icu" AS error_text,
    e.error_type COLLATE "und-x-icu" AS error_type,
    e.nsi_dictionary_oid
FROM stg_egisz.exchange_messages tx
CROSS JOIN LATERAL jsonb_to_recordset(tx.error_details)
    AS e(item_no integer, error_kind text, error_code text, error_text text, error_type text, nsi_dictionary_oid text)
WHERE tx.error_details IS NOT NULL;

COMMENT ON VIEW stg_egisz.message_errors IS
'Элементы ошибки разобранных сообщений по времени сообщения. Строка — один элемент; dwh_id пуст у сообщения без связи с документом. error_text — исходный текст: публикуется в дашборды отсюда по исключению из правил стандарта до решения о доступе.';

-- Опубликованные ошибки текущего состояния документа: тип, вид, категория, код и атрибуты
-- справочников вместе с реквизитами документа. Исходный текст остаётся в слое разбора
-- (stg_egisz.document_errors_current, тот же ключ). Материализовано: анализ ошибок читает
-- его на каждом фильтре. Коллация типа задана явно: база развёрнута с lc_ctype = C, где
-- ILIKE складывает регистр только для латиницы, и отбор «содержит» по кириллице молча
-- терял строки. Корневая коллация выбрана вместо русской: текст смешанный.
CREATE MATERIALIZED VIEW serving_egisz.document_errors AS
SELECT
    r.ips_date,
    c.dwh_id,
    c.error_no,
    r.status,
    r.status_label,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.semd_code,
    r.semd_label,
    c.message_at,
    c.egisz_subsystem,
    c.source_action,
    c.error_kind,
    t.error_category,
    c.error_type COLLATE "und-x-icu" AS error_type,
    c.error_code,
    n.nsi_error_code,
    n.nsi_error_description,
    c.nsi_dictionary_oid,
    nd.name AS nsi_dictionary_name,
    t.responsibility,
    t.is_retryable
FROM stg_egisz.document_errors_current c
JOIN serving_egisz.documents_current r ON r.dwh_id = c.dwh_id
LEFT JOIN mart_egisz.dim_error_type t ON t.error_type = c.error_type
LEFT JOIN mart_egisz.dim_nsi_error_code_alias a ON a.alias = upper(btrim(c.error_code))
LEFT JOIN mart_egisz.dim_nsi_error_code n
  ON n.nsi_error_code = COALESCE(a.nsi_error_code, upper(btrim(c.error_code)))
LEFT JOIN mart_egisz.dim_nsi_dictionary nd ON nd.oid = c.nsi_dictionary_oid
WITH DATA;

-- Уникальный индекс нужен для REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors
    ON serving_egisz.document_errors (dwh_id, error_no);
CREATE INDEX IF NOT EXISTS idx_document_errors_ips_date ON serving_egisz.document_errors (ips_date);
CREATE INDEX IF NOT EXISTS idx_document_errors_type ON serving_egisz.document_errors (error_type);
CREATE INDEX IF NOT EXISTS idx_document_errors_category ON serving_egisz.document_errors (error_category);
CREATE INDEX IF NOT EXISTS idx_document_errors_kind ON serving_egisz.document_errors (error_kind);
CREATE INDEX IF NOT EXISTS idx_document_errors_clinic_jid ON serving_egisz.document_errors (clinic_jid);
CREATE INDEX IF NOT EXISTS idx_document_errors_semd_code ON serving_egisz.document_errors (semd_code);
CREATE INDEX IF NOT EXISTS idx_document_errors_responsibility ON serving_egisz.document_errors (responsibility);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors IS
'Ошибки текущего состояния документа (текущие версии). Строка — одна ошибка документа: error_type — тип с замаскированными значениями; вид, категория, код и атрибуты справочников. Исходный текст — в stg_egisz.document_errors_current по тому же ключу (dwh_id, error_no). Статус документа — отдельная колонка: элементы ошибки в подтверждении регистрации статус не меняют.';

-- Ошибки связи за период: шлюз не доставил сообщение. Строка — одна ошибка связи, в том
-- числе в сообщениях без связи с документом. Исходный текст — в stg_egisz.message_errors.
CREATE VIEW serving_egisz.network_errors AS
SELECT
    m.message_at,
    m.logid,
    m.msgid,
    m.dwh_id,
    m.clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(btrim(m.clinic_jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(btrim(o.name), ''), '—') AS clinic_label,
    m.semd_code,
    -- Подпись СЭМД та же, что в documents_current: фильтр «Код СЭМД» дашборда передаёт её.
    CASE
        WHEN st.code IS NOT NULL AND st.name IS NOT NULL
            THEN st.code || ' · ' || st.name
        WHEN st.code IS NOT NULL
            THEN st.code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
    END AS semd_label,
    m.egisz_subsystem,
    m.source_action,
    m.error_type,
    m.error_code,
    t.responsibility,
    t.is_retryable
FROM stg_egisz.message_errors m
LEFT JOIN mart_egisz.dim_error_type t ON t.error_type = m.error_type
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = m.clinic_jid
LEFT JOIN LATERAL (
    SELECT dst.code, dst.name
    FROM mart_egisz.dim_semd_types dst
    WHERE dst.oid = m.semd_code
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
WHERE m.error_kind = 'Ошибка связи';

COMMENT ON VIEW serving_egisz.network_errors IS
'Ошибки связи по времени сообщения: шлюз не доставил сообщение (LOGSTATE = 3). Строка — одна ошибка связи; dwh_id пуст у сообщения без связи с документом. Исходный текст — в stg_egisz.message_errors по сообщению (logid, message_at): ошибка связи у сообщения одна.';

CREATE OR REPLACE VIEW mart_egisz_admin.document_lineage AS
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
FROM mart_egisz.documents d
LEFT JOIN mart_egisz.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = d.jid
LEFT JOIN mart_egisz.dim_clinic_oid r ON r.oid = btrim(stg_egisz.clean_text_value(d.org_oid))
WHERE d.dwh_id IS NOT NULL;

COMMENT ON VIEW mart_egisz_admin.document_lineage IS
'Lineage документа: OID и адрес обмена из журнала рядом с ЮЛ, к которому их относит реестр OID.';

CREATE OR REPLACE VIEW serving_egisz.clinic_nsi_mapping AS
SELECT
    o.jid,
    o.name AS cash_name,
    COALESCE(NULLIF(btrim(o.nsi_name), ''), NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), '')) AS nsi_name,
    o.inn,
    stg_egisz.clean_text_value(o.fir_oid) AS oid,
    (NULLIF(btrim(o.fir_oid), '') IS NOT NULL) AS is_mapped,
    doc.last_success_registered_at
FROM mart_egisz.dim_organizations o
LEFT JOIN mart_egisz.dim_nsi_organization n ON n.oid = stg_egisz.clean_text_value(o.fir_oid)
LEFT JOIN LATERAL (
    SELECT MAX(r.registered_at) AS last_success_registered_at
    FROM serving_egisz.documents_current r
    WHERE r.clinic_jid = o.jid AND r.status = 'success'
) doc ON true
WHERE o.jid IS NOT NULL;

COMMENT ON VIEW serving_egisz.clinic_nsi_mapping IS
'Аудит сопоставления клиник CASH/JPERSONS с НСИ 1461: JID, наименование CASH, наименование НСИ, ИНН, OID, признак сопоставления и дата последней успешной регистрации ЭМД.';

-- Типы СЭМД, которые клиника фактически отправляет: грейн (clinic_jid, semd_code)
-- по документам.
-- clinic_label собирается идентично documents_current, чтобы общий дашборд-фильтр «Клиника»
-- привязывался одним значением к обеим витринам.
CREATE OR REPLACE VIEW serving_egisz.clinic_semd_activity AS
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
    -- Счётчик на грейне логического документа (documents_current = текущие версии), иначе
    -- повторная подача того же документа считалась бы как ещё один документ клиники.
    SELECT
        r.clinic_jid,
        r.semd_code,
        MAX(r.first_sent_at) AS last_sent_at,
        MAX(r.registered_at) AS last_registered_at,
        count(*) AS documents_total
    FROM serving_egisz.documents_current r
    WHERE r.clinic_jid IS NOT NULL
      AND r.semd_code IS NOT NULL
    GROUP BY 1, 2
) f
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = f.clinic_jid
LEFT JOIN mart_egisz.dim_semd_types st ON st.code = f.semd_code;

COMMENT ON VIEW serving_egisz.clinic_semd_activity IS
'Типы СЭМД в обмене клиники: грейн (clinic_jid, semd_code) по документам; последняя отправка, последняя регистрация и число документов.';

-- ---------------------------------------------------------------- section: semd_guides
-- ============================================================================
-- Требования руководств по реализации: какие справочники НСИ обязан использовать
-- документ данного вида. Источник — НСИ 638 и 805, якорь — dim_semd_types.
-- ============================================================================

-- Одна строка на вид медицинской документации, включая виды без руководства. Иначе вид
-- формата PDF/A, которому руководство не положено, и вид, чьё руководство не заведено
-- в реестре, одинаково пропадали бы из выборки; различает их guide_match.
CREATE OR REPLACE VIEW serving_egisz.semd_guides AS
SELECT
    st.code AS semd_code,
    st.name AS semd_name,
    st.code || ' · ' || COALESCE(NULLIF(btrim(st.name), ''), '—') AS semd_label,
    st.type_code AS semd_type_code,
    st.level AS semd_level,
    (st.level = '3') AS semd_is_cda,
    st.start_date AS semd_start_date,
    st.end_date AS semd_end_date,
    (st.end_date IS NULL) AS semd_is_active,
    g.oid AS guide_oid,
    g.full_name AS guide_name,
    g.release_number AS guide_release,
    g.git_link AS guide_git_link,
    g.git_pub_date AS guide_git_pub_date,
    st.implementation_guide AS guide_portal_url,
    CASE
        WHEN NULLIF(btrim(st.ig_oid), '') IS NULL THEN 'руководство не предусмотрено'
        WHEN g.oid IS NULL THEN 'руководство отсутствует в реестре'
        WHEN r.is_alias THEN 'сопоставлено по синониму OID'
        ELSE 'сопоставлено'
    END AS guide_match,
    d.dictionaries_total
FROM mart_egisz.dim_semd_types st
LEFT JOIN mart_egisz.dim_semd_guide_oid r ON r.published_oid = NULLIF(btrim(st.ig_oid), '')
LEFT JOIN mart_egisz.dim_nsi_semd_guide g ON g.oid = r.guide_oid
LEFT JOIN LATERAL (
    SELECT count(*) AS dictionaries_total
    FROM mart_egisz.dim_nsi_semd_guide_dictionary gd
    WHERE gd.guide_oid = g.oid
) d ON TRUE;

COMMENT ON VIEW serving_egisz.semd_guides IS
'Виды медицинской документации и их руководства по реализации: грейн semd_code, признак сопоставления и число предписанных справочников НСИ.';

-- Витрина требований: грейн (вид документации, справочник). Строится поверх semd_guides,
-- чтобы правила сопоставления с руководством были определены в одном месте. Один набор
-- справочников может относиться к нескольким видам: руководство обслуживает не обязательно
-- один вид документации.
CREATE OR REPLACE VIEW serving_egisz.semd_dictionaries AS
SELECT
    s.semd_code,
    s.semd_name,
    s.semd_label,
    s.semd_type_code,
    s.semd_level,
    s.semd_is_cda,
    s.semd_start_date,
    s.semd_end_date,
    s.semd_is_active,
    s.guide_oid,
    s.guide_name,
    s.guide_release,
    s.guide_git_link,
    s.guide_portal_url,
    s.guide_match,
    s.dictionaries_total,
    gd.dict_oid,
    gd.dict_name,
    gd.dict_oid || ' · ' || COALESCE(NULLIF(btrim(gd.dict_name), ''), '—') AS dict_label,
    gd.dict_version,
    (gd.dict_version = '*') AS dict_any_version,
    gd.dict_ids_systemname AS dict_id_field
FROM serving_egisz.semd_guides s
JOIN mart_egisz.dim_nsi_semd_guide_dictionary gd ON gd.guide_oid = s.guide_oid;

COMMENT ON VIEW serving_egisz.semd_dictionaries IS
'Справочники НСИ, предписанные руководством по реализации для вида медицинской документации: грейн (semd_code, dict_oid).';

-- ---------------------------------------------------------------- section: weekly
-- ============================================================================
-- 85_views_weekly.sql — недельный слой динамики для дашборда «Динамика по
-- неделям». Идемпотентность — как у всего отчётного слоя: DROP в начале модуля,
-- CREATE здесь, REFRESH + ANALYZE в конце.
--
-- Неделя = понедельник по отчётному календарю. Пояс берётся у report_timezone()
-- и применяется ОДИН раз: ips_date — timestamptz, сдвиг задаёт стену календаря до
-- усечения, второй сдвиг дал бы двойной перенос.
--
-- Пояс намеренно не читается из сессии: date_trunc вычисляется в момент REFRESH, и
-- обновление из сессии с другим поясом переписало бы границы уже закрытых недель.
-- ============================================================================

-- Недельная витрина документов: грейн (week_start, клиника). Хранятся только
-- счётчики — доли считаются потребителями как ratio-of-sums, что даёт
-- корректное взвешивание при агрегации недель/клиник. Инвариант:
-- docs_success + docs_error = docs_total (отправленные без ответа вне корпуса).
-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label
-- NOT NULL по построению ('— · —' при пустом jid), и REFRESH CONCURRENTLY
-- требует уникальный btree без выражений.
-- Состояния отправки считаются на конец своей недели, а не на момент обновления витрины:
-- иначе строки давно закрытой недели меняли бы значения при каждом refresh_report_marts().
-- Открытая справа неделя берёт якорем текущий момент.
CREATE MATERIALIZED VIEW serving_egisz.documents_weekly AS
SELECT
    d.week_start,
    d.clinic_jid,
    MAX(d.clinic_name) AS clinic_name,
    d.clinic_label,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status <> 'sent')::bigint AS docs_total,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'success')::bigint AS docs_success,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'async_error')::bigint AS docs_error,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.has_network_error)::bigint AS docs_network_error,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent')::bigint AS docs_sent,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent' AND NOT seg.is_no_response)::bigint AS docs_pending,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent' AND seg.is_no_response)::bigint AS docs_no_response,
    (d.week_start < date_trunc('week', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_week
FROM (
    SELECT
        r.dwh_id,
        r.clinic_jid,
        r.clinic_name,
        r.clinic_label,
        r.status,
        r.first_sent_at,
        -- Ошибка связи хранится рядом со статусом: документ учитывается по ней, если она
        -- есть в текущем состоянии документа.
        EXISTS (
            SELECT 1 FROM stg_egisz.document_errors_current c
            WHERE c.dwh_id = r.dwh_id AND c.error_kind = 'Ошибка связи'
        ) AS has_network_error,
        date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS week_start
    FROM serving_egisz.documents_current r
    WHERE r.ips_date IS NOT NULL
) d
CROSS JOIN LATERAL (
    SELECT LEAST(
        ((d.week_start + 7)::timestamp AT TIME ZONE serving_egisz.report_timezone()),
        now()
    ) AS ts
) anchor
LEFT JOIN mart_egisz.dim_pending_segments seg
    ON seg.code = CASE
        WHEN d.status = 'sent' THEN serving_egisz.pending_segment_code_at(d.first_sent_at, anchor.ts)
    END
GROUP BY d.week_start, d.clinic_jid, d.clinic_label
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_weekly
    ON serving_egisz.documents_weekly (week_start, clinic_label);
CREATE INDEX IF NOT EXISTS idx_documents_weekly_week ON serving_egisz.documents_weekly (week_start);
CREATE INDEX IF NOT EXISTS idx_documents_weekly_clinic_jid ON serving_egisz.documents_weekly (clinic_jid);

COMMENT ON MATERIALIZED VIEW serving_egisz.documents_weekly IS
'Недельная витрина документов: грейн (week_start = понедельник МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_success + docs_error = docs_total; docs_network_error — документы с ошибкой связи в текущем состоянии; docs_pending + docs_no_response = docs_sent. Состояния отправки считаются на конец своей недели (МСК), для открытой недели — на текущий момент: строки закрытых недель не меняются между обновлениями. Обновляется refresh_report_marts() после transform.';

-- Недельная структура ошибок текущего состояния по виду и категории: документ с
-- несколькими категориями учитывается в каждой — сумма долей категорий может превышать
-- 100 % от числа документов; это контракт панели структуры. Учитываются отказы
-- (статус «Ошибка асинхронного ответа РЭМД») и ошибки связи: элементы ошибки в
-- подтверждении регистрации статус не меняют и в структуру отказов не входят.
CREATE MATERIALIZED VIEW serving_egisz.document_errors_weekly AS
SELECT
    date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS week_start,
    r.clinic_jid,
    MAX(r.clinic_name) AS clinic_name,
    r.clinic_label,
    c.error_kind,
    t.error_category,
    COUNT(DISTINCT c.dwh_id)::bigint AS docs_with_category,
    (date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
        < date_trunc('week', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_week
FROM stg_egisz.document_errors_current c
JOIN serving_egisz.documents_current r ON r.dwh_id = c.dwh_id
LEFT JOIN mart_egisz.dim_error_type t ON t.error_type = c.error_type
WHERE r.ips_date IS NOT NULL
  AND (r.status = 'async_error' OR c.error_kind = 'Ошибка связи')
GROUP BY 1, r.clinic_jid, r.clinic_label, c.error_kind, t.error_category
WITH DATA;

-- У вида «Ошибка связи» категория пуста: ключ сравнивает пустые значения как равные.
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors_weekly
    ON serving_egisz.document_errors_weekly (week_start, clinic_label, error_kind, error_category) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_document_errors_weekly_week
    ON serving_egisz.document_errors_weekly (week_start);
CREATE INDEX IF NOT EXISTS idx_document_errors_weekly_category
    ON serving_egisz.document_errors_weekly (error_category);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_weekly IS
'Недельная структура ошибок: грейн (week_start, клиника, вид, категория); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории. Обновляется refresh_report_marts() после текущих ошибок документа.';

-- ---------------------------------------------------------------- section: monthly
-- ============================================================================
-- 86_views_monthly.sql — месячный слой динамики для вкладки «Динамика по
-- месяцам» управленческого дашборда. Идемпотентность — как у недельного слоя:
-- DROP в 60, CREATE здесь, REFRESH + ANALYZE в 90.
--
-- Месяц = первое число месяца по отчётному календарю. Пояс берётся у report_timezone()
-- и применяется ОДИН раз — как в недельном слое.
-- ============================================================================

-- Месячная витрина документов: грейн (month_start, клиника). Хранятся только
-- счётчики — доли считаются потребителями как ratio-of-sums, что даёт
-- корректное взвешивание при агрегации месяцев/клиник. Инвариант:
-- docs_success + docs_error = docs_total (отправленные без ответа вне корпуса).
-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label
-- NOT NULL по построению ('— · —' при пустом jid), и REFRESH CONCURRENTLY
-- требует уникальный btree без выражений.
-- Якорь состояний отправки — конец своего месяца (для открытого справа месяца текущий
-- момент), см. недельный слой.
CREATE MATERIALIZED VIEW serving_egisz.documents_monthly AS
SELECT
    d.month_start,
    d.clinic_jid,
    MAX(d.clinic_name) AS clinic_name,
    d.clinic_label,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status <> 'sent')::bigint AS docs_total,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'success')::bigint AS docs_success,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'async_error')::bigint AS docs_error,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.has_network_error)::bigint AS docs_network_error,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent')::bigint AS docs_sent,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent' AND NOT seg.is_no_response)::bigint AS docs_pending,
    COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'sent' AND seg.is_no_response)::bigint AS docs_no_response,
    (d.month_start < date_trunc('month', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_month
FROM (
    SELECT
        r.dwh_id,
        r.clinic_jid,
        r.clinic_name,
        r.clinic_label,
        r.status,
        r.first_sent_at,
        -- Ошибка связи хранится рядом со статусом: документ учитывается по ней, если она
        -- есть в текущем состоянии документа.
        EXISTS (
            SELECT 1 FROM stg_egisz.document_errors_current c
            WHERE c.dwh_id = r.dwh_id AND c.error_kind = 'Ошибка связи'
        ) AS has_network_error,
        date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS month_start
    FROM serving_egisz.documents_current r
    WHERE r.ips_date IS NOT NULL
) d
CROSS JOIN LATERAL (
    SELECT LEAST(
        ((d.month_start + INTERVAL '1 month')::timestamp AT TIME ZONE serving_egisz.report_timezone()),
        now()
    ) AS ts
) anchor
LEFT JOIN mart_egisz.dim_pending_segments seg
    ON seg.code = CASE
        WHEN d.status = 'sent' THEN serving_egisz.pending_segment_code_at(d.first_sent_at, anchor.ts)
    END
GROUP BY d.month_start, d.clinic_jid, d.clinic_label
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_monthly
    ON serving_egisz.documents_monthly (month_start, clinic_label);
CREATE INDEX IF NOT EXISTS idx_documents_monthly_month ON serving_egisz.documents_monthly (month_start);
CREATE INDEX IF NOT EXISTS idx_documents_monthly_clinic_jid ON serving_egisz.documents_monthly (clinic_jid);

COMMENT ON MATERIALIZED VIEW serving_egisz.documents_monthly IS
'Месячная витрина документов: грейн (month_start = первое число месяца МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_success + docs_error = docs_total; docs_network_error — документы с ошибкой связи в текущем состоянии; docs_pending + docs_no_response = docs_sent. Состояния отправки считаются на конец своего месяца (МСК), для открытого месяца — на текущий момент: строки закрытых месяцев не меняются между обновлениями. Обновляется refresh_report_marts() после transform.';

-- Месячная структура ошибок текущего состояния по виду и категории: документ с
-- несколькими категориями учитывается в каждой — сумма долей категорий может превышать
-- 100 % от числа документов; это контракт панели структуры. Учитываются отказы
-- (статус «Ошибка асинхронного ответа РЭМД») и ошибки связи: элементы ошибки в
-- подтверждении регистрации статус не меняют и в структуру отказов не входят.
CREATE MATERIALIZED VIEW serving_egisz.document_errors_monthly AS
SELECT
    date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS month_start,
    r.clinic_jid,
    MAX(r.clinic_name) AS clinic_name,
    r.clinic_label,
    c.error_kind,
    t.error_category,
    COUNT(DISTINCT c.dwh_id)::bigint AS docs_with_category,
    (date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
        < date_trunc('month', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_month
FROM stg_egisz.document_errors_current c
JOIN serving_egisz.documents_current r ON r.dwh_id = c.dwh_id
LEFT JOIN mart_egisz.dim_error_type t ON t.error_type = c.error_type
WHERE r.ips_date IS NOT NULL
  AND (r.status = 'async_error' OR c.error_kind = 'Ошибка связи')
GROUP BY 1, r.clinic_jid, r.clinic_label, c.error_kind, t.error_category
WITH DATA;

-- У вида «Ошибка связи» категория пуста: ключ сравнивает пустые значения как равные.
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors_monthly
    ON serving_egisz.document_errors_monthly (month_start, clinic_label, error_kind, error_category) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_document_errors_monthly_month
    ON serving_egisz.document_errors_monthly (month_start);
CREATE INDEX IF NOT EXISTS idx_document_errors_monthly_category
    ON serving_egisz.document_errors_monthly (error_category);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_monthly IS
'Месячная структура ошибок: грейн (month_start, клиника, вид, категория); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории. Обновляется refresh_report_marts() после текущих ошибок документа.';

-- Обновление материализованных витрин — единственное определение их состава и порядка:
-- функцию вызывают DAG-и и сценарий применения схемы. Порядок обязателен: ошибки
-- документа, недельный и месячный слои читают текущие ошибки документа. CONCURRENTLY не
-- блокирует чтение дашбордов, но требует наполненного представления — ненаполненное
-- обновляется обычным способом. Статистика собирается сразу после обновления.
CREATE OR REPLACE FUNCTION serving_egisz.refresh_report_marts(p_concurrently boolean DEFAULT true)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    mart regclass;
BEGIN
    FOREACH mart IN ARRAY ARRAY[
        'stg_egisz.document_errors_current',
        'serving_egisz.document_errors',
        'serving_egisz.documents_weekly',
        'serving_egisz.document_errors_weekly',
        'serving_egisz.documents_monthly',
        'serving_egisz.document_errors_monthly'
    ]::regclass[]
    LOOP
        IF p_concurrently AND (SELECT c.relispopulated FROM pg_class c WHERE c.oid = mart) THEN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %s', mart);
        ELSE
            EXECUTE format('REFRESH MATERIALIZED VIEW %s', mart);
        END IF;
        EXECUTE format('ANALYZE %s', mart);
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------- section: health
-- ============================================================================
-- mart_egisz_admin — эксплуатационные представления; владелец объектов, первичное
-- наполнение и ANALYZE.
-- ============================================================================

-- Business backfill lives only in mart_egisz.transform_raw_to_facts().

CREATE OR REPLACE VIEW mart_egisz_admin.health_by_clinic AS
WITH anchor AS (
    SELECT COALESCE(MAX(COALESCE(last_callback_at, first_sent_at, document_created_at)), now()) AS ref_ts
    FROM mart_egisz.documents
),
fact_24h AS (
    SELECT
        d.jid::text AS clinic_jid,
        MAX(COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || d.jid::text)) AS clinic_name,
        COUNT(DISTINCT d.dwh_id)::bigint AS docs_cnt,
        COUNT(DISTINCT d.dwh_id) FILTER (WHERE d.status = 'async_error')::bigint AS err_cnt
    FROM mart_egisz.documents d
    CROSS JOIN anchor
    LEFT JOIN mart_egisz.dim_organizations o ON o.jid = d.jid
    WHERE COALESCE(d.last_callback_at, d.first_sent_at, d.document_created_at) >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY d.jid
),
sent_without_response AS (
    SELECT jid::text AS clinic_jid, COUNT(DISTINCT dwh_id)::bigint AS sent_cnt
    FROM mart_egisz.documents
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

-- Сопоставление отметок конвейера с данными DWH: докуда журнал выгружен, докуда разобран
-- и куда дошли документы. Сам шлюз здесь не проверяется.
CREATE OR REPLACE VIEW mart_egisz_admin.health_sync AS
SELECT
    (SELECT COUNT(*) FROM mart_egisz.documents)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(DISTINCT dwh_id) FROM mart_egisz.documents WHERE status = 'sent')::bigint AS "Отправлено без ответа",
    (SELECT COUNT(DISTINCT dwh_id) FROM mart_egisz.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours')::bigint AS "Без ответа > 24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM mart_egisz.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '24 hours' AND first_sent_at < now() - INTERVAL '1 hour')::bigint AS "Без ответа 1-24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM mart_egisz.documents WHERE status = 'sent' AND first_sent_at >= now() - INTERVAL '1 hour')::bigint AS "Без ответа < 1ч",
    (SELECT MAX(first_sent_at) FROM mart_egisz.documents) AS "DWH max Sent",
    (SELECT updated_at FROM etl_meta.egisz_etl_state WHERE pipeline = 'egisz') AS "Последний апдейт курсора",
    -- Хвост журнала raw_egisz.exchangelog отдельной колонкой не выводится: он выводим из самой таблицы
    -- и в установившемся режиме повторяет позицию разбора.
    (SELECT extract_logid_cursor FROM etl_meta.egisz_etl_state WHERE pipeline = 'egisz') AS "egisz_etl_state.extract_logid_cursor",
    (SELECT transform_logid_cursor FROM etl_meta.egisz_etl_state WHERE pipeline = 'egisz') AS "egisz_etl_state.transform_logid_cursor",
    (SELECT MAX(COALESCE(result_logid, request_logid)) FROM mart_egisz.documents) AS "DWH max LOGID fact",
    (SELECT COUNT(DISTINCT dwh_id) FROM mart_egisz.documents)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW mart_egisz_admin.health_message_registry_no_document AS
SELECT
    tx.log_date AS "Дата события",
    tx.logid::text AS "LOGID",
    tx.source_action AS "Метод",
    tx.status AS "Статус ответа",
    tx.link_method AS "Метод связки",
    tx.egisz_subsystem AS "Подсистема ЕГИСЗ",
    tx.msgid AS "MSGID сообщения",
    tx.relates_to_msgid AS "relatesTo MSGID",
    reg.egmid::text AS "EGMID EGISZ_MESSAGES",
    reg.created_at AS "Дата EGISZ_MESSAGES",
    tx.dwh_id AS "dwh_id",
    tx.local_uid_semd AS "localUid",
    tx.emdr_id AS "emdrId",
    tx.semd_code AS "Код СЭМД",
    tx.jid::text AS "JID Клиники",
    COALESCE(NULLIF(btrim(o.name), ''), 'Клиника JID: ' || tx.jid::text, '(неизвестно)') AS "Клиника",
    LEFT(COALESCE(tx.message, ''), 240) AS "Сообщение",
    reg.reply_to AS "replyTo EGISZ_MESSAGES"
FROM stg_egisz.exchange_messages tx
JOIN LATERAL (
    SELECT m.egmid, m.created_at, m.reply_to
    FROM stg_egisz.message_registry m
    WHERE tx.relates_to_msgid IS NOT NULL
      AND m.msgid = stg_egisz.message_registry_key(tx.relates_to_msgid)
      AND m.document_uid IS NULL
    ORDER BY m.egmid DESC NULLS LAST
    LIMIT 1
) reg ON TRUE
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = tx.jid
WHERE tx.relates_to_msgid IS NOT NULL
  AND tx.egisz_subsystem IS DISTINCT FROM 'ИЭМК';

COMMENT ON VIEW mart_egisz_admin.health_message_registry_no_document IS
'Health-детализация РЭМД: relatesToMessage найден в EGISZ_MESSAGES, DOCUMENTID пустой. ИЭМК не использует DOCUMENTID.';

CREATE OR REPLACE VIEW mart_egisz_admin.health_signals AS
WITH anchor AS (
    SELECT MAX(COALESCE(last_callback_at, first_sent_at, document_created_at)) AS last_event_ts
    FROM mart_egisz.documents
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
        FROM stg_egisz.exchange_messages
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
        FROM mart_egisz_admin.health_message_registry_no_document
        ORDER BY "LOGID"::bigint DESC
        LIMIT 500
    ) recent
),
-- Граница перехода в состояние «Без ответа» берётся из лестницы ступеней, а не задаётся
-- здесь повторно: ужесточение порога делается UPDATE по dim_pending_segments.
no_response_after AS (
    SELECT now() - make_interval(
        mins => (SELECT MAX(max_age_minutes) FROM mart_egisz.dim_pending_segments WHERE NOT is_no_response)
    ) AS ts
),
-- Отказы, чью формулировку ни одно правило не распознало: они видны в разборе текстом,
-- и каждая такая строка — кандидат на новое правило либо на код, отсутствующий в ФНСИ.
uncovered_types AS (
    SELECT DISTINCT c.dwh_id
    FROM stg_egisz.document_errors_current c
    JOIN mart_egisz.dim_error_type t ON t.error_type = c.error_type
    WHERE c.error_kind = 'Ошибка асинхронного ответа'
      AND t.rule_code IS NULL
),
-- Элемент ошибки без строки в справочнике типов — сбой архитектуры: тип заводится при
-- разборе, и расхождение означает, что элементы не приведены к текущим правилам.
untyped_errors AS (
    SELECT COUNT(*) AS cnt
    FROM stg_egisz.document_errors_current c
    LEFT JOIN mart_egisz.dim_error_type t ON t.error_type = c.error_type
    WHERE t.error_type IS NULL
),
-- Асинхронный ответ, исход которого не распознан, не отбрасывается молча: он остаётся без
-- исхода и считается здесь за последние сутки приёма.
unrecognized_responses AS (
    SELECT COUNT(*) AS cnt
    FROM stg_egisz.exchange_messages t
    WHERE t.log_date >= now() - INTERVAL '1 day'
      AND t.source_action IN ('sendRegisterDocumentResult',
                              'urn:ihe:iti:2007:ProvideAndRegisterDocumentSet-bAsyncResponse')
      AND t.xml_parsed_at IS NOT NULL
      AND t.status IS NULL
),
-- Полнота выгрузки журнала. Шлюз нумерует EXCHANGELOG непрерывно, поэтому число
-- пропущенных LOGID между краями загруженного диапазона — прямая мера потерь. Это тот же
-- инвариант, по которому продвигается egisz_etl_state.extract_logid_cursor: отметка идёт только
-- по непрерывному участку, значит ненулевое значение означает и недостачу строк, и
-- остановку разбора на этом месте.
journal_gaps AS (
    SELECT CASE
        WHEN MAX(r.logid) IS NULL THEN 0
        ELSE MAX(r.logid) - COALESCE(MAX(s.transform_logid_cursor), 0) - COUNT(*)
    END AS missing
    FROM etl_meta.egisz_etl_state s
    LEFT JOIN raw_egisz.exchangelog r
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
            WHERE n.nspname IN ('raw_egisz', 'stg_egisz', 'mart_egisz', 'serving_egisz', 'mart_egisz_admin')
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
    FROM mart_egisz.documents d
    LEFT JOIN mart_egisz.dim_organizations o ON o.jid = d.jid
    WHERE d.jid IS NOT NULL AND o.jid IS NULL
    GROUP BY d.jid
)
SELECT * FROM (
    VALUES
        ('parsed_documents', 'Разложенные документы proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM mart_egisz.documents), 'документов', 'documents', 'Контроль поступления СЭМД в DWH'),
        ('sent_24h', 'Отправлено без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM mart_egisz.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours'), 'документов', 'documents.status=sent', 'Проверить клиники без ответа ЕГИСЗ и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM stg_egisz.document_errors_current WHERE error_kind = 'Ошибка связи'), 'документов', 'stg_egisz.document_errors_current, вид «Ошибка связи»', 'Разобрать типы ошибок связи в serving_egisz.network_errors'),
        ('error_rows', 'Ошибки асинхронного ответа РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM mart_egisz.documents WHERE status = 'async_error'), 'документов', 'documents.status=async_error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('no_response_backlog',
         'Документы без ответа',
         CASE
             WHEN (SELECT COUNT(*) FROM mart_egisz.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts) >= 50 THEN 'red'
             WHEN (SELECT COUNT(*) FROM mart_egisz.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts) >= 20 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM mart_egisz.documents d, no_response_after c WHERE d.status = 'sent' AND d.first_sent_at < c.ts),
         'документов',
         'documents_sent (состояние отправки)',
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
         'stg_egisz.document_errors_current: тип без правила классификации',
         'Разобрать формулировки на вкладке «Анализ ошибок» и завести правило в dim_error_rules'),
        ('untyped_errors',
         'Элементы ошибки без типа в справочнике',
         CASE WHEN (SELECT cnt FROM untyped_errors) >= 1 THEN 'red' ELSE 'green' END,
         (SELECT cnt FROM untyped_errors)::numeric,
         'элементов',
         'stg_egisz.document_errors_current вне mart_egisz.dim_error_type',
         'Выполнить пересчёт ошибок: элементы не приведены к текущим правилам'),
        ('unrecognized_async_responses',
         'Асинхронные ответы с нераспознанным исходом',
         CASE WHEN (SELECT cnt FROM unrecognized_responses) >= 1 THEN 'red' ELSE 'green' END,
         (SELECT cnt FROM unrecognized_responses)::numeric,
         'ответов за сутки',
         'stg_egisz.exchange_messages: асинхронный ответ без исхода',
         'Разобрать ответ и дополнить распознавание исхода в classify_async_status'),
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
         'разрывы LOGID в необработанном хвосте raw_egisz.exchangelog',
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
         'stg_egisz.exchange_messages.link_method=unlinked, последние 500 по LOGID',
        'Проверить реестр подач raw_egisz.egisz_messages: в нём нет строки по msgid из relatesToMessage'),
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
         'Свежесть данных (последнее событие документа)',
         CASE
             WHEN (SELECT last_event_ts FROM anchor) IS NULL THEN 'red'
             WHEN (SELECT last_event_ts FROM anchor) >= now() - INTERVAL '1 hour'  THEN 'green'
             WHEN (SELECT last_event_ts FROM anchor) >= now() - INTERVAL '24 hours' THEN 'yellow'
             ELSE 'red'
         END,
         ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT last_event_ts FROM anchor), now()))) / 60.0, 1)::numeric,
         'минут с последнего события документа',
         'documents.last_callback_at/first_sent_at',
         'Проверить ELT-цикл, Airflow scheduler и доступ к Firebird')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");

-- Наблюдаемость слоя версий.
-- «Макс. размер группы» — детектор перемола: группа по (jid+тип+documentNumber) не должна
-- схлопывать РАЗНЫЕ документы (страховка c_cap=50 в recompute_document_versions; max по
-- базе = 7). «Коллизии localUid» — один dwh_id с разным типом СЭМД в exchange_messages: признак
-- переиспользования localUid под другой документ.
CREATE OR REPLACE VIEW mart_egisz_admin.health_versions AS
WITH grp AS (
    SELECT document_group_id, count(*) AS versions
    FROM mart_egisz.documents
    WHERE document_group_id IS NOT NULL
    GROUP BY document_group_id
)
SELECT
    (SELECT count(*) FROM mart_egisz.documents)::bigint AS "Экземпляров всего",
    (SELECT count(*) FROM mart_egisz.documents WHERE is_current_version)::bigint AS "Уникальных документов (текущих)",
    (SELECT count(*) FROM mart_egisz.documents WHERE is_current_version IS FALSE)::bigint AS "Superseded версий",
    (SELECT count(*) FROM grp WHERE versions > 1)::bigint AS "Групп с >1 версией",
    (SELECT count(*) FROM mart_egisz.documents WHERE document_group_confidence = 'doc_number')::bigint AS "Экземпляров в группах по documentNumber",
    (SELECT COALESCE(max(versions), 0) FROM grp)::bigint AS "Макс. размер группы (детектор перемола)",
    (SELECT count(*) FROM (
        SELECT dwh_id
        FROM stg_egisz.exchange_messages
        WHERE dwh_id IS NOT NULL
          AND log_date >= now() - INTERVAL '30 days'
        GROUP BY dwh_id
        HAVING count(DISTINCT NULLIF(btrim(semd_code), '')) > 1
     ) c)::bigint AS "Коллизии localUid (30д)";

-- Смена владельца требует ACCESS EXCLUSIVE. Объект, занятый работающим конвейером
-- (egisz_etl_state правится каждым запуском ETL), уводил весь накат в ошибку по lock_timeout —
-- при том что смена владельца ему, как правило, и не нужна. Поэтому владелец меняется
-- только там, где он не egisz, а занятый объект пропускается с предупреждением: схема
-- идемпотентна, следующий прогон доберёт его. Обходятся схемы слоёв ЕГИСЗ и объекты
-- egisz_ общей схемы etl_meta: остальные её объекты принадлежат другим конвейерам.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT n.nspname, c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE (n.nspname IN ('raw_egisz', 'stg_egisz', 'mart_egisz', 'serving_egisz', 'mart_egisz_admin')
               OR (n.nspname = 'etl_meta' AND c.relname LIKE 'egisz\_%'))
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
          AND pg_get_userbyid(c.relowner) <> 'egisz'
    LOOP
        BEGIN
            IF r.relkind IN ('r', 'p') THEN
                EXECUTE format('ALTER TABLE %I.%I OWNER TO egisz', r.nspname, r.relname);
            ELSIF r.relkind = 'v' THEN
                EXECUTE format('ALTER VIEW %I.%I OWNER TO egisz', r.nspname, r.relname);
            ELSIF r.relkind = 'm' THEN
                EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO egisz', r.nspname, r.relname);
            ELSIF r.relkind = 'S' THEN
                EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO egisz', r.nspname, r.relname);
            END IF;
        EXCEPTION WHEN lock_not_available OR query_canceled THEN
            RAISE WARNING 'owner change skipped for %.%: object is busy', r.nspname, r.relname;
        END;
    END LOOP;

    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE (n.nspname IN ('raw_egisz', 'stg_egisz', 'mart_egisz', 'serving_egisz', 'mart_egisz_admin')
               OR (n.nspname = 'etl_meta' AND p.proname LIKE 'egisz\_%'))
          AND pg_get_userbyid(p.proowner) <> 'egisz'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO egisz', r.sig);
    END LOOP;
END;
$$;

-- Первичное наполнение отчётного слоя: выполняется, только когда в схеме уже есть
-- документы, а атрибуты ещё пусты (развёртывание на существующий архив).
-- Сопровождение архива — пересчёт атрибутов, слоя версий и текстов ошибок — ведёт
-- суточный DAG обслуживания, обновление витрин — задача refresh_marts DAG-а приёма. Полные проходы в теле
-- наката пересекались по блокировкам с пятиминутным приёмом и давали взаимоблокировки.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM mart_egisz.documents)
       AND NOT EXISTS (SELECT 1 FROM mart_egisz.document_attributes) THEN
        PERFORM mart_egisz.recompute_document_attributes(NULL::text[]);
        PERFORM mart_egisz.recompute_document_versions(NULL::text[]);
        -- Материализованные представления созданы выше с данными; пересобираем после
        -- сборки атрибутов, чтобы отображаемые колонки (клиника, СЭМД) были финальными.
        PERFORM serving_egisz.refresh_report_marts();
    END IF;
END
$$;

ANALYZE raw_egisz.exchangelog;
-- Статистика по выражениям индексов реестра: без неё поиск подачи документа уходит в обход
-- таблицы по EGMID.
ANALYZE raw_egisz.egisz_messages;
ANALYZE mart_egisz.documents;
ANALYZE stg_egisz.exchange_messages;
ANALYZE mart_egisz.document_attributes;
ANALYZE stg_egisz.document_errors_current;
ANALYZE serving_egisz.document_errors;
ANALYZE serving_egisz.documents_weekly;
ANALYZE serving_egisz.document_errors_weekly;
ANALYZE serving_egisz.documents_monthly;
ANALYZE serving_egisz.document_errors_monthly;

\echo 'DWH init complete: egisz owns all objects of the EGISZ layer schemas in dwh_egisz'
