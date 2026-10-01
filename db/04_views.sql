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
DROP VIEW IF EXISTS mart_egisz.exchangelog_errors CASCADE;
DROP VIEW IF EXISTS stg_egisz.network_errors CASCADE;
DROP VIEW IF EXISTS stg_egisz.remd_errors CASCADE;
DROP VIEW IF EXISTS stg_egisz.ihe_errors CASCADE;
-- Недельный и месячный слои читают document_versions и текущие ошибки документа —
-- удаляются до них.
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.pending_queue_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.semd_error_categories_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.registration_speed_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.clinic_activity_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.clinic_semd_types CASCADE;
DROP VIEW IF EXISTS serving_egisz.document_status_details CASCADE;
DROP VIEW IF EXISTS serving_egisz.pending_segments CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_error_types CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.document_errors CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart_egisz.document_errors CASCADE;
DROP VIEW IF EXISTS serving_egisz.documents_sent CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_no_response CASCADE;
DROP MATERIALIZED VIEW IF EXISTS serving_egisz.documents_current CASCADE;
DROP VIEW IF EXISTS serving_egisz.document_versions CASCADE;
DROP VIEW IF EXISTS serving_egisz.document_file_requests CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.document_lineage CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart_egisz_admin.document_quality CASCADE;
DROP VIEW IF EXISTS serving_egisz.clinic_nsi_mapping CASCADE;
DROP VIEW IF EXISTS serving_egisz.clinic_semd_activity CASCADE;
DROP VIEW IF EXISTS serving_egisz.semd_dictionaries CASCADE;
DROP VIEW IF EXISTS serving_egisz.semd_guides CASCADE;
DROP VIEW IF EXISTS serving_egisz.error_types CASCADE;
DROP VIEW IF EXISTS mart_egisz_admin.unrecognized_errors CASCADE;
DROP VIEW IF EXISTS serving_egisz.nsi_dictionary_versions CASCADE;

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
        egisz_subsystem,
        updated_at
    )
    SELECT
        d.dwh_id,
        stg_egisz.clean_text_value(d.org_oid) AS clinic_oid_xml,
        stg_egisz.clean_host(COALESCE(attrs.clinic_host, ep.endpoint, reg.reply_to)) AS clinic_host,
        d.jid_resolve_method AS clinic_jid_resolve_method,
        sub.egisz_subsystem,
        now() AS updated_at
    FROM mart_egisz.documents d
    LEFT JOIN mart_egisz.document_attributes attrs ON attrs.dwh_id = d.dwh_id
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
        egisz_subsystem = EXCLUDED.egisz_subsystem,
        updated_at = now()
    -- Change-guard: переписываем строку (и двигаем updated_at) только при реальном
    -- расхождении. Без него полный reconcile (в т.ч. на каждом dwh_init) переписывал
    -- весь архив и менял updated_at — повторный прогон не был no-op (CLAUDE.md §3).
    WHERE
        mart_egisz.document_attributes.clinic_oid_xml IS DISTINCT FROM EXCLUDED.clinic_oid_xml
     OR mart_egisz.document_attributes.clinic_host IS DISTINCT FROM EXCLUDED.clinic_host
     OR mart_egisz.document_attributes.clinic_jid_resolve_method IS DISTINCT FROM EXCLUDED.clinic_jid_resolve_method
     OR mart_egisz.document_attributes.egisz_subsystem IS DISTINCT FROM EXCLUDED.egisz_subsystem;

    GET DIAGNOSTICS refreshed = ROW_COUNT;
    RETURN refreshed;
END;
$$;

-- Пересчёт JID документов после изменения справочников. Проверяются только документы,
-- чей резолв мог измениться: с изменённым OID медорганизации, а также разрешённые не по
-- OID, у которых JID пуст или принадлежит изменённому ЮЛ либо ЮЛ с тем же хостом обмена.
-- Полный проход по архиву занимает минуты и не укладывается в пятиминутный цикл приёма.
CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_jids(p_oids text[], p_jids bigint[])
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    affected_oids text[];
    affected_jids bigint[];
    affected_dwh_ids text[] := ARRAY[]::text[];
    refreshed bigint := 0;
BEGIN
    SELECT COALESCE(array_agg(DISTINCT oid) FILTER (WHERE oid IS NOT NULL), ARRAY[]::text[])
    INTO affected_oids
    FROM unnest(COALESCE(p_oids, ARRAY[]::text[])) AS o (raw_oid)
    CROSS JOIN LATERAL (SELECT NULLIF(btrim(o.raw_oid), '') AS oid) n;

    -- Хост обмена изменённого ЮЛ (по лицензии или gost-<N>) мог разрешаться в другое ЮЛ:
    -- документы этого ЮЛ проверяются вместе с изменённым.
    SELECT COALESCE(array_agg(DISTINCT jid) FILTER (WHERE jid IS NOT NULL), ARRAY[]::bigint[])
    INTO affected_jids
    FROM (
        SELECT unnest(COALESCE(p_jids, ARRAY[]::bigint[]))
        UNION
        SELECT dl.jid
        FROM mart_egisz.dim_licenses dl
        WHERE stg_egisz.clean_host(dl.mo_domen) IN (
            SELECT stg_egisz.clean_host(l.mo_domen)
            FROM mart_egisz.dim_licenses l
            WHERE l.jid = ANY (p_jids)
               OR (regexp_match(COALESCE(l.mo_domen, ''), 'gost-([0-9]+)'))[1]::bigint = ANY (p_jids)
        )
    ) t (jid);

    IF cardinality(affected_oids) = 0 AND cardinality(affected_jids) = 0 THEN
        RETURN 0;
    END IF;

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
          AND (
              btrim(d.org_oid) = ANY (affected_oids)
           OR (
                  d.jid_resolve_method IS DISTINCT FROM 'mo_uid'
              AND (d.jid IS NULL OR d.jid = ANY (affected_jids))
              )
          )
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

-- ---------------------------------------------------------------- section: document_versions
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
        WHEN st.oid IS NOT NULL AND st.name IS NOT NULL
            THEN st.oid || ' · ' || st.name
        WHEN st.oid IS NOT NULL
            THEN st.oid || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
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
LEFT JOIN mart_egisz.dim_clinic_oids oid_ref ON oid_ref.oid = btrim(stg_egisz.clean_text_value(d.org_oid))
LEFT JOIN mart_egisz.dim_document_statuses ds ON ds.code = d.status
-- Ступень подбирает pending_segment_at от якоря представления — текущего момента.
-- Условие внутри LATERAL оставляет вызов только нефинальным статусам: у документа с исходом
-- ожидания нет. OFFSET 0 не даёт планировщику вынести условие в соединение — тогда ступень
-- подбиралась бы и для документов с исходом.
LEFT JOIN LATERAL (
    SELECT c.*
    FROM serving_egisz.pending_segment_at(d.first_sent_at, now()) c
    WHERE ds.is_final IS NOT TRUE
    OFFSET 0
) ps ON TRUE
LEFT JOIN mart_egisz.dim_sent_states ss
    ON ss.code = CASE
        WHEN ps.code IS NULL THEN NULL          -- финальный статус: состояния отправки нет
        WHEN ps.is_no_response THEN 'no_response'
        ELSE 'pending'
    END
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = d.jid
LEFT JOIN mart_egisz.dim_nsi_semd_types st ON st.oid = stg_egisz.normalize_semd_code(d.semd_code)
WHERE NULLIF(btrim(d.dwh_id), '') IS NOT NULL;

COMMENT ON VIEW serving_egisz.document_versions IS
'Все версии документа: одна строка на экземпляр (версию) отправки СЭМД, включая замещённые. Состояние документа к выдаче — serving_egisz.documents_current; это представление читают объекты, которым нужны все версии или состояние на прошлый момент (недели и месяцы, история очереди, активность клиник).';

-- Состояние документа к выдаче: текущая версия документа, кроме документов, ответ по которым
-- не получен за срок последней ступени лестницы ожидания (dim_pending_segments): ответ по ним
-- уже не придёт, и в отчётность они не входят, кроме списка documents_no_response. Ступень и
-- состояние отправки — на момент обновления витрины. Материализовано с индексами по фильтрам и
-- ключам поиска: дашборды отбирают документы по периоду, клинике, типу СЭМД, статусу и ключам.
CREATE MATERIALIZED VIEW serving_egisz.documents_current AS
SELECT
    v.dwh_id,
    v.ips_date,
    v.status,
    v.status_label,
    v.status_sort,
    v.pending_segment,
    v.pending_segment_label,
    v.pending_segment_sort,
    v.sent_state,
    v.sent_state_label,
    v.status_detail,
    v.status_detail_label,
    v.status_detail_sort,
    v.semd_code,
    v.semd_name,
    v.semd_label,
    v.semd_local_uid,
    v.semd_created_at,
    v.semd_emdr_id,
    v.clinic_jid,
    v.clinic_name,
    v.clinic_label,
    v.clinic_inn,
    v.clinic_oid,
    v.clinic_host,
    v.clinic_oid_unknown,
    v.msgid,
    v.relates_to_msgid,
    v.logid,
    v.request_logid,
    v.result_logid,
    v.delivery_seconds,
    v.registered_at,
    v.first_sent_at,
    v.first_callback_at,
    v.attempt_count,
    v.is_resubmitted,
    v.document_group_id,
    v.semd_version_number,
    v.document_group_confidence,
    v.supersedes_dwh_id
FROM serving_egisz.document_versions v
WHERE v.is_current_version
  AND v.sent_state IS DISTINCT FROM 'no_response'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_current
    ON serving_egisz.documents_current (dwh_id);
CREATE INDEX IF NOT EXISTS idx_documents_current_ips_date ON serving_egisz.documents_current (ips_date);
CREATE INDEX IF NOT EXISTS idx_documents_current_first_sent_at ON serving_egisz.documents_current (first_sent_at);
CREATE INDEX IF NOT EXISTS idx_documents_current_clinic_label ON serving_egisz.documents_current (clinic_label);
CREATE INDEX IF NOT EXISTS idx_documents_current_semd_label ON serving_egisz.documents_current (semd_label);
CREATE INDEX IF NOT EXISTS idx_documents_current_sent ON serving_egisz.documents_current (first_sent_at) WHERE status = 'sent';
CREATE INDEX IF NOT EXISTS idx_documents_current_local_uid ON serving_egisz.documents_current (semd_local_uid);
CREATE INDEX IF NOT EXISTS idx_documents_current_relates_to ON serving_egisz.documents_current (relates_to_msgid);
CREATE INDEX IF NOT EXISTS idx_documents_current_emdr_id ON serving_egisz.documents_current (semd_emdr_id);
CREATE INDEX IF NOT EXISTS idx_documents_current_logid ON serving_egisz.documents_current (logid);

COMMENT ON MATERIALIZED VIEW serving_egisz.documents_current IS
'Состояние документа к выдаче: строка — текущая версия логического документа (ключ dwh_id), столбцы — как в document_versions без признака текущей версии. Документы без ответа дольше последней ступени лестницы ожидания (dim_pending_segments) не входят — их список в serving_egisz.documents_no_response. Ступень и состояние отправки — на момент обновления. Индексы — по дате обработки, клинике, типу СЭМД и ключам поиска (localUid, relatesTo, рег. номер РЭМД, LOGID). Обновляется refresh_report_marts().';

-- Документы, ответ по которым не получен за срок последней ступени лестницы ожидания: ответ
-- уже не придёт. Отдельный список с ключами поиска; обновляется вместе с documents_current
-- одним вызовом refresh_report_marts(), поэтому документ входит ровно в один из объектов.
CREATE MATERIALIZED VIEW serving_egisz.documents_no_response AS
SELECT
    v.dwh_id,
    v.first_sent_at,
    v.semd_code,
    v.semd_name,
    v.semd_label,
    v.semd_local_uid,
    v.clinic_jid,
    v.clinic_name,
    v.clinic_label,
    v.clinic_host,
    v.msgid,
    v.relates_to_msgid,
    v.request_logid,
    v.attempt_count,
    v.is_resubmitted
FROM serving_egisz.document_versions v
WHERE v.is_current_version
  AND v.sent_state = 'no_response'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_no_response
    ON serving_egisz.documents_no_response (dwh_id);
CREATE INDEX IF NOT EXISTS idx_documents_no_response_first_sent_at ON serving_egisz.documents_no_response (first_sent_at);
CREATE INDEX IF NOT EXISTS idx_documents_no_response_local_uid ON serving_egisz.documents_no_response (semd_local_uid);
CREATE INDEX IF NOT EXISTS idx_documents_no_response_relates_to ON serving_egisz.documents_no_response (relates_to_msgid);
CREATE INDEX IF NOT EXISTS idx_documents_no_response_request_logid ON serving_egisz.documents_no_response (request_logid);

COMMENT ON MATERIALIZED VIEW serving_egisz.documents_no_response IS
'Документы без ответа ЕГИСЗ дольше последней ступени лестницы ожидания (dim_pending_segments) на момент обновления: строка — текущая версия документа (ключ dwh_id), отправка, тип СЭМД, клиника и ключи поиска (localUid, relatesTo, LOGID запроса). В состояние документа к выдаче serving_egisz.documents_current не входят. Обновляется refresh_report_marts() вместе с documents_current.';

-- Очередь обработки на текущий момент: документы состояния к выдаче без ответа. Возраст и
-- ступень считаются от текущего момента; документ, перешагнувший последнюю ступень после
-- обновления витрины, в очередь уже не входит.
CREATE OR REPLACE VIEW serving_egisz.documents_sent AS
SELECT
    r.dwh_id,
    r.first_sent_at,
    EXTRACT(EPOCH FROM (now() - r.first_sent_at)) / 3600.0 AS pending_hours,
    ROUND(EXTRACT(EPOCH FROM (now() - r.first_sent_at)) / 86400.0, 1) AS pending_days,
    seg.code AS pending_segment,
    seg.label AS pending_segment_label,
    seg.sort_order AS pending_segment_sort,
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
CROSS JOIN LATERAL serving_egisz.pending_segment_at(r.first_sent_at, now()) seg
WHERE r.status = 'sent'
  AND NOT seg.is_no_response;

COMMENT ON VIEW serving_egisz.documents_sent IS
'Очередь обработки: отправленные документы состояния к выдаче без ответа ЕГИСЗ, срок ожидания в пределах лестницы (dim_pending_segments). Ступень и возраст — на текущий момент. Срез на прошлый момент строится теми же функциями от своего якоря (is_pending_at, pending_segment_code_at).';

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
LEFT JOIN mart_egisz.dim_nsi_semd_types st ON st.oid = stg_egisz.normalize_semd_code(tx.xml_semd_code)
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

-- Ошибки хранятся в разобранных сообщениях stg_egisz.exchange_messages по источникам: ошибка
-- связи — столбцы network_error_*, элементы ответа РЭМД — remd_errors, ИЭМК — ihe_errors.
-- Отчётный слой читает их только в этом разделе. Коллация текстов задана явно: база
-- развёрнута с lc_ctype = C, где ILIKE складывает регистр только для латиницы, и отбор
-- «содержит» по кириллице молча терял строки; корневая — потому что текст смешанный.

CREATE VIEW stg_egisz.network_errors AS
SELECT
    tx.log_date AS message_at,
    tx.logid,
    tx.msgid,
    tx.dwh_id,
    tx.jid AS clinic_jid,
    stg_egisz.normalize_semd_code(tx.semd_code) AS semd_code,
    tx.egisz_subsystem,
    tx.source_action,
    tx.network_error_code AS error_code,
    tx.network_error_text COLLATE "und-x-icu" AS error_text,
    tx.network_error_type COLLATE "und-x-icu" AS error_type,
    tx.network_error_normalized_text COLLATE "und-x-icu" AS normalized_text
FROM stg_egisz.exchange_messages tx
WHERE tx.network_error_text IS NOT NULL;

COMMENT ON VIEW stg_egisz.network_errors IS
'Ошибки связи: шлюз не доставил сообщение (LOGSTATE = 3). Строка — сообщение с ошибкой связи, ключ (logid, message_at); dwh_id пуст у сообщения без связи с документом. error_text — исходный текст шлюза; normalized_text — нормализованный текст нераспознанной ошибки.';

CREATE VIEW stg_egisz.remd_errors AS
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
    e.section,
    e.code,
    e.message COLLATE "und-x-icu" AS message,
    e.error_type COLLATE "und-x-icu" AS error_type,
    e.nsi_dictionary_oid,
    e.normalized_text COLLATE "und-x-icu" AS normalized_text
FROM stg_egisz.exchange_messages tx
CROSS JOIN LATERAL jsonb_to_recordset(tx.remd_errors)
    AS e(item_no integer, section text, code text, message text, error_type text, nsi_dictionary_oid text,
         normalized_text text)
WHERE tx.remd_errors IS NOT NULL;

COMMENT ON VIEW stg_egisz.remd_errors IS
'Элементы ответа РЭМД: строка — <item>, ключ (logid, message_at, item_no). section — раздел ответа: errors либо registrationWarnings (предупреждения при успешной регистрации); code, message — исходные код и текст; normalized_text — нормализованный текст нераспознанного элемента.';

CREATE VIEW stg_egisz.ihe_errors AS
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
    e.error_code,
    e.code_context COLLATE "und-x-icu" AS code_context,
    e.severity,
    e.location,
    e.error_type COLLATE "und-x-icu" AS error_type,
    e.nsi_dictionary_oid,
    e.normalized_text COLLATE "und-x-icu" AS normalized_text
FROM stg_egisz.exchange_messages tx
CROSS JOIN LATERAL jsonb_to_recordset(tx.ihe_errors)
    AS e(item_no integer, error_code text, code_context text, severity text, location text,
         error_type text, nsi_dictionary_oid text, normalized_text text)
WHERE tx.ihe_errors IS NOT NULL;

COMMENT ON VIEW stg_egisz.ihe_errors IS
'Элементы ответа ИЭМК: строка — IHE RegistryError, ключ (logid, message_at, item_no). error_code — errorCode, code_context — codeContext (исходный текст), severity и location — атрибуты элемента; normalized_text — нормализованный текст нераспознанного элемента.';

-- Общая форма ошибок журнала обмена: вид, код, тип, признак предупреждения и исходный текст
-- из атрибутов своего источника. Ключ элемента — (logid, message_at, error_source, item_no).
-- Тексты источников объединяются здесь, выше stage.
CREATE VIEW mart_egisz.exchangelog_errors AS
SELECT
    n.message_at, n.logid, n.msgid, n.dwh_id, n.clinic_jid, n.semd_code, n.egisz_subsystem, n.source_action,
    'связь'::text AS error_source,
    0 AS item_no,
    'Ошибка связи'::text AS error_kind,
    n.error_code,
    n.error_type,
    NULL::text AS nsi_dictionary_oid,
    false AS is_warning,
    n.error_text,
    n.normalized_text
FROM stg_egisz.network_errors n
UNION ALL
SELECT
    r.message_at, r.logid, r.msgid, r.dwh_id, r.clinic_jid, r.semd_code, r.egisz_subsystem, r.source_action,
    'РЭМД', r.item_no, 'Ошибка асинхронного ответа', r.code, r.error_type, r.nsi_dictionary_oid,
    r.section IS NOT DISTINCT FROM 'registrationWarnings',
    r.message,
    r.normalized_text
FROM stg_egisz.remd_errors r
UNION ALL
SELECT
    h.message_at, h.logid, h.msgid, h.dwh_id, h.clinic_jid, h.semd_code, h.egisz_subsystem, h.source_action,
    'ИЭМК', h.item_no, 'Ошибка асинхронного ответа', h.error_code, h.error_type, h.nsi_dictionary_oid,
    COALESCE(h.severity ~* 'Warning$', false),
    h.code_context,
    h.normalized_text
FROM stg_egisz.ihe_errors h;

COMMENT ON VIEW mart_egisz.exchangelog_errors IS
'Ошибки разобранного журнала обмена (EXCHANGELOG) в общей форме: строка — ошибка связи либо элемент ответа РЭМД или ИЭМК, ключ (logid, message_at, error_source, item_no). is_warning — предупреждение: раздел registrationWarnings РЭМД либо severity Warning ИЭМК. error_text — исходный текст источника (LOGTEXT ошибки связи, message РЭМД, codeContext ИЭМК); при выдаче персональные данные скрывает mart_egisz.mask_personal_data. normalized_text — нормализованный текст нераспознанной ошибки (тип «Не распознано»), без персональных данных.';

-- Ошибки текущего состояния документа: элементы последнего асинхронного ответа и ошибки
-- связи после него; у документа без асинхронного ответа — все его ошибки связи. Время
-- последнего ответа берётся из тех же разобранных сообщений. error_no нумерует ошибки
-- документа по порядку сообщений. Исходный текст — в строке документа mart_egisz.documents.
CREATE MATERIALIZED VIEW mart_egisz.document_errors AS
WITH last_response AS (
    SELECT t.dwh_id, max(t.log_date) AS responded_at
    FROM stg_egisz.exchange_messages t
    WHERE t.dwh_id IS NOT NULL
      AND t.status IN ('success', 'error')
    GROUP BY t.dwh_id
)
SELECT
    m.dwh_id,
    row_number() OVER (PARTITION BY m.dwh_id ORDER BY m.message_at, m.logid, m.item_no, m.error_source)::integer AS error_no,
    m.message_at,
    m.logid,
    m.error_source,
    m.item_no,
    m.egisz_subsystem,
    m.source_action,
    m.error_kind,
    m.error_code,
    m.error_type,
    m.nsi_dictionary_oid,
    m.is_warning
FROM mart_egisz.exchangelog_errors m
LEFT JOIN last_response lr ON lr.dwh_id = m.dwh_id
WHERE m.dwh_id IS NOT NULL
  AND m.message_at >= COALESCE(lr.responded_at, '-infinity'::timestamptz)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_document_errors
    ON mart_egisz.document_errors (dwh_id, error_no);
CREATE INDEX IF NOT EXISTS idx_mart_document_errors_kind
    ON mart_egisz.document_errors (error_kind);

COMMENT ON MATERIALIZED VIEW mart_egisz.document_errors IS
'Ошибки текущего состояния документа в общей форме: элементы последнего асинхронного ответа и ошибки связи после него. Строка — одна ошибка документа, ключ (dwh_id, error_no); ключ источника (logid, message_at, error_source, item_no). Исходный текст ошибок текущего состояния — mart_egisz.documents.error_text. Обновляется refresh_report_marts() после transform.';

-- Опубликованные ошибки текущего состояния документа (состояние к выдаче): тип, вид,
-- категория, код и атрибуты справочников вместе с реквизитами документа. Материализовано:
-- анализ ошибок читает его на каждом фильтре.
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
    c.logid,
    c.error_source,
    c.item_no,
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
    t.is_retryable,
    c.is_warning,
    -- Корпус ошибок — отказы асинхронного ответа и ошибки связи. Предупреждения и
    -- элементы подтверждения регистрации статус не меняют и в корпус не входят. Тот же
    -- отбор у недельных и месячных агрегатов ошибок.
    (r.status = 'async_error' OR c.error_kind = 'Ошибка связи') AS is_error_corpus
FROM mart_egisz.document_errors c
JOIN serving_egisz.documents_current r ON r.dwh_id = c.dwh_id
LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
LEFT JOIN mart_egisz.dim_error_code_aliases a ON a.alias = upper(btrim(c.error_code))
LEFT JOIN mart_egisz.dim_nsi_error_codes n
  ON n.nsi_error_code = COALESCE(a.nsi_error_code, upper(btrim(c.error_code)))
LEFT JOIN mart_egisz.dim_nsi_dictionaries nd ON nd.oid = c.nsi_dictionary_oid
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
CREATE INDEX IF NOT EXISTS idx_document_errors_corpus ON serving_egisz.document_errors (ips_date) WHERE is_error_corpus;

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors IS
'Ошибки текущего состояния документа для документов состояния к выдаче serving_egisz.documents_current. Строка — одна ошибка документа, ключ (dwh_id, error_no): error_type — тип ошибки из правил классификации либо «Не распознано» своего вида; вид, категория, код и атрибуты справочников; is_warning — предупреждение источника. Исходный текст ошибок документа — в mart_egisz.documents.error_text. Статус документа — отдельная колонка: элементы ошибки в подтверждении регистрации статус не меняют. is_error_corpus — элемент входит в корпус ошибок (отказ асинхронного ответа или ошибка связи): отбор для долей и сводок; знаменатели периода — в document_errors_weekly / document_errors_monthly и semd_error_categories_daily, типы ошибок документа — в document_error_types.';

-- Ошибки на уровне документа: строка — один документ с ошибками текущего состояния, списки
-- типов, категорий и видов — по всем его элементам. Нужна потребителям, которым удобнее
-- отбирать документы по типу ошибки без соединения с элементами; с документом связывается
-- по dwh_id (serving_egisz.documents_current). Элемент ошибки в подтверждении регистрации
-- в списки входит, поэтому корпус ошибок обозначен отдельным признаком.
CREATE MATERIALIZED VIEW serving_egisz.document_error_types AS
SELECT
    e.dwh_id,
    COUNT(*)::integer AS errors_count,
    array_agg(DISTINCT e.error_type ORDER BY e.error_type) FILTER (WHERE e.error_type IS NOT NULL) AS error_types,
    array_agg(DISTINCT e.error_category ORDER BY e.error_category) FILTER (WHERE e.error_category IS NOT NULL) AS error_categories,
    array_agg(DISTINCT e.error_kind ORDER BY e.error_kind) FILTER (WHERE e.error_kind IS NOT NULL) AS error_kinds,
    bool_or(e.error_kind = 'Ошибка связи') AS has_network_error,
    bool_or(e.error_kind = 'Ошибка асинхронного ответа') AS has_remd_error,
    bool_or(e.is_error_corpus) AS is_error_corpus
FROM serving_egisz.document_errors e
GROUP BY e.dwh_id
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_document_error_types
    ON serving_egisz.document_error_types (dwh_id);
CREATE INDEX IF NOT EXISTS idx_document_error_types_types
    ON serving_egisz.document_error_types USING gin (error_types);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_error_types IS
'Ошибки на уровне документа: строка — документ с ошибками текущего состояния (ключ dwh_id); errors_count — число элементов, error_types / error_categories / error_kinds — списки различных значений, has_network_error / has_remd_error — есть ли ошибка данного вида, is_error_corpus — документ входит в корпус ошибок. Строится из serving_egisz.document_errors; обновляется refresh_report_marts() после него.';

-- Ошибки связи за период: шлюз не доставил сообщение. Строка — одна ошибка связи, в том
-- числе в сообщениях без связи с документом. Текст ошибки — в mart_egisz.exchangelog_errors.
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
    -- Подпись СЭМД та же, что в document_versions: фильтр «Код СЭМД» дашборда передаёт её.
    CASE
        WHEN st.oid IS NOT NULL AND st.name IS NOT NULL
            THEN st.oid || ' · ' || st.name
        WHEN st.oid IS NOT NULL
            THEN st.oid || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
    END AS semd_label,
    m.egisz_subsystem,
    m.source_action,
    m.error_type,
    m.error_code,
    t.responsibility,
    t.is_retryable
FROM mart_egisz.exchangelog_errors m
LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = m.error_type
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = m.clinic_jid
LEFT JOIN mart_egisz.dim_nsi_semd_types st ON st.oid = m.semd_code
WHERE m.error_source = 'связь';

COMMENT ON VIEW serving_egisz.network_errors IS
'Ошибки связи по времени сообщения: шлюз не доставил сообщение (LOGSTATE = 3). Строка — одна ошибка связи; dwh_id пуст у сообщения без связи с документом. Текст ошибки — в mart_egisz.exchangelog_errors по сообщению (logid, message_at, error_source = связь): ошибка связи у сообщения одна.';

-- Исходный текст ошибок текущего состояния документа в строке документа: элементы последнего
-- асинхронного ответа и ошибки связи после него, в порядке ошибок документа
-- (mart_egisz.document_errors), через « · ». Пересчитывается для документов пакета приёма;
-- NULL в p_dwh_ids — для всех документов.
CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_error_texts(p_dwh_ids text[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    refreshed bigint;
BEGIN
    WITH scope AS (
        SELECT d.dwh_id
        FROM mart_egisz.documents d
        WHERE p_dwh_ids IS NULL OR d.dwh_id = ANY (p_dwh_ids)
    ),
    last_response AS (
        SELECT t.dwh_id, max(t.log_date) AS responded_at
        FROM stg_egisz.exchange_messages t
        JOIN scope s ON s.dwh_id = t.dwh_id
        WHERE t.status IN ('success', 'error')
        GROUP BY t.dwh_id
    ),
    source_errors AS (
        SELECT n.dwh_id, n.message_at, n.logid, 0 AS item_no, 'связь'::text AS error_source, n.error_text
        FROM stg_egisz.network_errors n JOIN scope s ON s.dwh_id = n.dwh_id
        UNION ALL
        SELECT r.dwh_id, r.message_at, r.logid, r.item_no, 'РЭМД', r.message
        FROM stg_egisz.remd_errors r JOIN scope s ON s.dwh_id = r.dwh_id
        UNION ALL
        SELECT h.dwh_id, h.message_at, h.logid, h.item_no, 'ИЭМК', h.code_context
        FROM stg_egisz.ihe_errors h JOIN scope s ON s.dwh_id = h.dwh_id
    ),
    texts AS (
        SELECT
            s.dwh_id,
            string_agg(e.error_text, ' · ' ORDER BY e.message_at, e.logid, e.item_no, e.error_source) AS error_text
        FROM scope s
        LEFT JOIN last_response lr ON lr.dwh_id = s.dwh_id
        LEFT JOIN source_errors e
          ON e.dwh_id = s.dwh_id
         AND e.message_at >= COALESCE(lr.responded_at, '-infinity'::timestamptz)
        GROUP BY s.dwh_id
    )
    UPDATE mart_egisz.documents d
    SET error_text = x.error_text,
        updated_at = now()
    FROM texts x
    WHERE d.dwh_id = x.dwh_id
      AND d.error_text IS DISTINCT FROM x.error_text;
    GET DIAGNOSTICS refreshed = ROW_COUNT;
    RETURN refreshed;
END;
$$;

COMMENT ON FUNCTION mart_egisz.recompute_document_error_texts(text[]) IS
'Пересчитывает исходный текст ошибок текущего состояния в mart_egisz.documents.error_text: тексты источников (stg_egisz.network_errors, remd_errors, ihe_errors) последнего асинхронного ответа и ошибок связи после него через « · ». p_dwh_ids NULL — все документы. Возвращает число изменённых строк.';

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
LEFT JOIN mart_egisz.dim_clinic_oids r ON r.oid = btrim(stg_egisz.clean_text_value(d.org_oid))
WHERE d.dwh_id IS NOT NULL;

COMMENT ON VIEW mart_egisz_admin.document_lineage IS
'Lineage документа: OID и адрес обмена из журнала рядом с ЮЛ, к которому их относит реестр OID.';

-- Контроль качества данных: текущие документы с ответом РЭМД, их происхождение и признаки
-- проверки. Каждое правило записано здесь один раз; OID вне реестра — clinic_oid_unknown
-- documents_current. Материализовано: соединение документной витрины с происхождением на
-- каждом запросе карточки занимало секунды.
CREATE MATERIALIZED VIEW mart_egisz_admin.document_quality AS
SELECT
    q.*,
    (q.is_no_jid OR q.is_oid_unknown OR q.is_no_local_uid OR q.is_no_semd_code OR q.is_success_without_date) AS has_violation
FROM (
    SELECT
        d.dwh_id,
        d.ips_date,
        d.status,
        d.status_detail_label,
        d.clinic_jid,
        d.clinic_label,
        d.semd_code,
        d.semd_name,
        d.semd_label,
        d.semd_local_uid,
        d.clinic_oid,
        l.clinic_oid_xml,
        l.clinic_jid_by_oid,
        l.clinic_host,
        l.clinic_jid_resolve_method,
        (d.clinic_jid IS NULL) AS is_no_jid,
        COALESCE(d.clinic_oid_unknown, false) AS is_oid_unknown,
        (NULLIF(btrim(d.semd_local_uid), '') IS NULL) AS is_no_local_uid,
        (NULLIF(btrim(d.semd_code), '') IS NULL) AS is_no_semd_code,
        (d.status = 'success' AND d.ips_date IS NULL) AS is_success_without_date
    FROM serving_egisz.documents_current d
    JOIN mart_egisz_admin.document_lineage l ON l.dwh_id = d.dwh_id
    WHERE d.status IN ('success', 'async_error')
) q
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_document_quality
    ON mart_egisz_admin.document_quality (dwh_id);
CREATE INDEX IF NOT EXISTS idx_document_quality_ips_date
    ON mart_egisz_admin.document_quality (ips_date);

COMMENT ON MATERIALIZED VIEW mart_egisz_admin.document_quality IS
'Контроль качества данных: строка — текущий документ с ответом РЭМД (dwh_id) с реквизитами, происхождением из document_lineage и признаками проверки: нет JID, OID вне реестра медорганизаций, нет localUid, нет кода СЭМД, успех без даты обработки; has_violation — хотя бы одно нарушение. Обновляется refresh_report_marts().';

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
LEFT JOIN mart_egisz.dim_nsi_organizations n ON n.oid = stg_egisz.clean_text_value(o.fir_oid)
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
    -- Счётчик на грейне логического документа (состояние к выдаче), иначе
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
LEFT JOIN mart_egisz.dim_nsi_semd_types st ON st.oid = f.semd_code;

COMMENT ON VIEW serving_egisz.clinic_semd_activity IS
'Типы СЭМД в обмене клиники: грейн (clinic_jid, semd_code) по документам; последняя отправка, последняя регистрация и число документов.';

-- ---------------------------------------------------------------- section: semd_guides
-- ============================================================================
-- Требования руководств по реализации: какие справочники НСИ обязан использовать
-- документ данного вида. Источник — НСИ 638 и 805, якорь — dim_nsi_semd_types.
-- ============================================================================

-- Одна строка на вид медицинской документации, включая виды без руководства. Иначе вид
-- формата PDF/A, которому руководство не положено, и вид, чьё руководство не заведено
-- в реестре, одинаково пропадали бы из выборки; различает их guide_match.
CREATE OR REPLACE VIEW serving_egisz.semd_guides AS
SELECT
    st.oid AS semd_code,
    st.name AS semd_name,
    st.oid || ' · ' || COALESCE(NULLIF(btrim(st.name), ''), '—') AS semd_label,
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
FROM mart_egisz.dim_nsi_semd_types st
LEFT JOIN mart_egisz.dim_semd_guide_oids r ON r.published_oid = NULLIF(btrim(st.ig_oid), '')
LEFT JOIN mart_egisz.dim_nsi_semd_guides g ON g.oid = r.guide_oid
LEFT JOIN LATERAL (
    SELECT count(*) AS dictionaries_total
    FROM mart_egisz.dim_nsi_semd_guide_dictionaries gd
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
JOIN mart_egisz.dim_nsi_semd_guide_dictionaries gd ON gd.guide_oid = s.guide_oid;

COMMENT ON VIEW serving_egisz.semd_dictionaries IS
'Справочники НСИ, предписанные руководством по реализации для вида медицинской документации: грейн (semd_code, dict_oid).';

CREATE VIEW serving_egisz.error_types AS
SELECT
    t.error_type,
    t.error_kind,
    t.error_category,
    t.responsibility,
    t.is_retryable,
    t.nsi_error_code,
    c.nsi_error_description,
    (t.rule_code IS NOT NULL) AS is_recognized,
    r.definition,
    r.definition_source
FROM mart_egisz.dim_error_types t
LEFT JOIN mart_egisz.dim_nsi_error_codes c ON c.nsi_error_code = t.nsi_error_code
LEFT JOIN mart_egisz.dim_error_rules r ON r.rule_code = t.rule_code;

COMMENT ON VIEW serving_egisz.error_types IS
'Типы ошибок — закрытый список: строка — тип (ключ error_type) с видом, категорией, зоной ответственности, признаком повтора и кодом справочника НСИ «РЭМД. Классификатор кодов сообщений» с его описанием. is_recognized — тип задан правилом классификации; ложь только у типа «Не распознано» своего вида. definition и definition_source — общепринятое определение ошибки и его источник (заданы у ошибок связи).';

CREATE VIEW mart_egisz_admin.unrecognized_errors AS
SELECT
    e.error_kind,
    e.normalized_text,
    count(*) AS error_count,
    count(DISTINCT e.clinic_jid) AS clinic_count,
    min(e.message_at) AS first_seen_at,
    max(e.message_at) AS last_seen_at,
    min(e.error_code) AS error_code_example
FROM mart_egisz.exchangelog_errors e
JOIN mart_egisz.dim_error_types t ON t.error_type = e.error_type
WHERE t.rule_code IS NULL
GROUP BY e.error_kind, e.normalized_text;

COMMENT ON VIEW mart_egisz_admin.unrecognized_errors IS
'Нераспознанные ошибки (тип «Не распознано»), сгруппированные по виду и нормализованному тексту: число ошибок и клиник, первое и последнее появление, пример кода. Контроль полноты правил классификации: строка — кандидат на новое правило в mart_egisz.dim_error_rules.';

-- Версии справочников НСИ в DWH: какой справочник, какая версия и когда загружена.
-- Наименование справочника — из общего перечня справочников НСИ, при отсутствии в нём —
-- по паспорту справочника.
CREATE VIEW serving_egisz.nsi_dictionary_versions AS
SELECT
    v.dictionary_oid,
    COALESCE(n.name, v.passport_name) AS dictionary_name,
    v.dictionary_version,
    v.loaded_at,
    v.record_count,
    v.purpose
FROM (
    SELECT source_oid AS dictionary_oid, max(source_version) AS dictionary_version, max(loaded_at) AS loaded_at,
           count(*) AS record_count, 'Реестр медицинских и фармацевтических организаций Российской Федерации'::text AS passport_name,
           'Сопоставление клиник с медицинскими организациями'::text AS purpose
    FROM mart_egisz.dim_nsi_organizations GROUP BY source_oid
    UNION ALL
    SELECT source_oid, max(source_version), max(updated_at), count(*), 'Электронные медицинские документы',
           'Виды медицинской документации (СЭМД)'
    FROM mart_egisz.dim_nsi_semd_types GROUP BY source_oid
    UNION ALL
    SELECT source_oid, max(source_version), max(loaded_at), count(*),
           'Реестр руководств по реализации структурированных электронных медицинских документов и протоколов информационного взаимодействия',
           'Руководства по реализации видов медицинской документации'
    FROM mart_egisz.dim_nsi_semd_guides GROUP BY source_oid
    UNION ALL
    SELECT source_oid, max(source_version), max(loaded_at), count(*),
           'Реестр справочников, использующихся в руководствах по реализации структурированных электронных медицинских документов',
           'Справочники НСИ, предписанные руководствами'
    FROM mart_egisz.dim_nsi_semd_guide_dictionaries GROUP BY source_oid
    UNION ALL
    SELECT source_oid, max(source_version), max(updated_at), count(*), 'РЭМД. Классификатор кодов сообщений',
           'Коды и описания ошибок регистрации ЭМД'
    FROM mart_egisz.dim_nsi_error_codes GROUP BY source_oid
) v
LEFT JOIN mart_egisz.dim_nsi_dictionaries n ON n.oid = v.dictionary_oid;

COMMENT ON VIEW serving_egisz.nsi_dictionary_versions IS
'Справочники НСИ в DWH: строка — справочник (OID dictionary_oid) с наименованием, версией справочника, датой загрузки, числом записей и назначением в DWH. Наименование — из общего перечня справочников НСИ mart_egisz.dim_nsi_dictionaries, при отсутствии — по паспорту справочника.';

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
    COUNT(DISTINCT d.dwh_id)::bigint AS docs_all,
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
            SELECT 1 FROM mart_egisz.document_errors c
            WHERE c.dwh_id = r.dwh_id AND c.error_kind = 'Ошибка связи'
        ) AS has_network_error,
        date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS week_start
    FROM serving_egisz.document_versions r
    WHERE r.is_current_version
      AND r.ips_date IS NOT NULL
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
'Недельная витрина документов: грейн (week_start = понедельник МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_all — все документы периода, знаменатель доли ошибок связи; docs_success + docs_error = docs_total; docs_network_error — документы с ошибкой связи в текущем состоянии; docs_pending + docs_no_response = docs_sent. Состояния отправки считаются на конец своей недели (МСК), для открытой недели — на текущий момент: строки закрытых недель не меняются между обновлениями. Обновляется refresh_report_marts() после transform.';

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
    p.docs_total,
    p.docs_all,
    (date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
        < date_trunc('week', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_week
FROM mart_egisz.document_errors c
JOIN serving_egisz.document_versions r ON r.dwh_id = c.dwh_id AND r.is_current_version
LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
JOIN serving_egisz.documents_weekly p
  ON p.week_start = date_trunc('week', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
 AND p.clinic_label = r.clinic_label
WHERE r.ips_date IS NOT NULL
  AND (r.status = 'async_error' OR c.error_kind = 'Ошибка связи')
GROUP BY 1, r.clinic_jid, r.clinic_label, c.error_kind, t.error_category, p.docs_total, p.docs_all
WITH DATA;

-- У вида «Ошибка связи» категория пуста: ключ сравнивает пустые значения как равные.
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors_weekly
    ON serving_egisz.document_errors_weekly (week_start, clinic_label, error_kind, error_category) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_document_errors_weekly_week
    ON serving_egisz.document_errors_weekly (week_start);
CREATE INDEX IF NOT EXISTS idx_document_errors_weekly_category
    ON serving_egisz.document_errors_weekly (error_category);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_weekly IS
'Недельная структура ошибок: грейн (week_start, клиника, вид, категория); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории; docs_total и docs_all — знаменатели периода и клиники из documents_weekly (документы с ответом и все документы), одинаковые во всех строках группы: доля = SUM(docs_with_category) / знаменатель по уникальным (week_start, клиника). Обновляется refresh_report_marts() после текущих ошибок документа.';

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
    COUNT(DISTINCT d.dwh_id)::bigint AS docs_all,
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
            SELECT 1 FROM mart_egisz.document_errors c
            WHERE c.dwh_id = r.dwh_id AND c.error_kind = 'Ошибка связи'
        ) AS has_network_error,
        date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS month_start
    FROM serving_egisz.document_versions r
    WHERE r.is_current_version
      AND r.ips_date IS NOT NULL
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
'Месячная витрина документов: грейн (month_start = первое число месяца МСК по ips_date, клиника). Корпус SLI = docs_total (status <> sent); docs_all — все документы периода, знаменатель доли ошибок связи; docs_success + docs_error = docs_total; docs_network_error — документы с ошибкой связи в текущем состоянии; docs_pending + docs_no_response = docs_sent. Состояния отправки считаются на конец своего месяца (МСК), для открытого месяца — на текущий момент: строки закрытых месяцев не меняются между обновлениями. Обновляется refresh_report_marts() после transform.';

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
    p.docs_total,
    p.docs_all,
    (date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
        < date_trunc('month', now() AT TIME ZONE serving_egisz.report_timezone())::date) AS is_complete_month
FROM mart_egisz.document_errors c
JOIN serving_egisz.document_versions r ON r.dwh_id = c.dwh_id AND r.is_current_version
LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
JOIN serving_egisz.documents_monthly p
  ON p.month_start = date_trunc('month', r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date
 AND p.clinic_label = r.clinic_label
WHERE r.ips_date IS NOT NULL
  AND (r.status = 'async_error' OR c.error_kind = 'Ошибка связи')
GROUP BY 1, r.clinic_jid, r.clinic_label, c.error_kind, t.error_category, p.docs_total, p.docs_all
WITH DATA;

-- У вида «Ошибка связи» категория пуста: ключ сравнивает пустые значения как равные.
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_errors_monthly
    ON serving_egisz.document_errors_monthly (month_start, clinic_label, error_kind, error_category) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_document_errors_monthly_month
    ON serving_egisz.document_errors_monthly (month_start);
CREATE INDEX IF NOT EXISTS idx_document_errors_monthly_category
    ON serving_egisz.document_errors_monthly (error_category);

COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_monthly IS
'Месячная структура ошибок: грейн (month_start, клиника, вид, категория); docs_with_category = COUNT(DISTINCT dwh_id) — документ учитывается в каждой своей категории; docs_total и docs_all — знаменатели периода и клиники из documents_monthly (документы с ответом и все документы), одинаковые во всех строках группы: доля = SUM(docs_with_category) / знаменатель по уникальным (month_start, клиника). Обновляется refresh_report_marts() после текущих ошибок документа.';

-- ---------------------------------------------------------------- section: queue history
-- История очереди обработки: состояние на конец каждого отчётного дня (для сегодняшнего —
-- на текущий момент). Очередь и ступень определяют is_pending_at / pending_segment_at, как
-- и у остальных потребителей; здесь они применяются к каждому дню, в который документ мог
-- ждать ответа. Дни перебираются только в пределах лестницы сроков ожидания и не дальше
-- дня первого ответа: документ за терминальным сроком в очередь не входит, а ответивший
-- в тот же день в конце дня в ней уже не числится. Хранятся счётчики документов: доли и
-- скользящие суммы считает потребитель.
-- Пояс и текущий день вычисляются один раз: report_timezone() читает каталог, и вызов на
-- каждой паре «документ — день» стоил бы минут. Пары «документ — день» (миллионы строк)
-- группируются по узким ключам — JID, код СЭМД, код ступени; подписи клиники, СЭМД и
-- ступени присоединяются к итогу: группировка по текстовым подписям уходила в сортировку
-- на диск. Подпись клиники однозначно задаётся JID, подпись СЭМД — кодом.
CREATE MATERIALIZED VIEW serving_egisz.pending_queue_daily AS
WITH calendar AS (
    SELECT
        serving_egisz.report_timezone() AS tz,
        (now() AT TIME ZONE serving_egisz.report_timezone())::date AS today
),
ladder AS (
    SELECT MAX(max_age_minutes) AS max_minutes
    FROM mart_egisz.dim_pending_segments
    WHERE NOT is_no_response
),
queue_documents AS (
    SELECT
        r.clinic_jid,
        r.clinic_name,
        r.clinic_label,
        r.semd_code,
        r.semd_label,
        r.first_sent_at,
        r.first_callback_at,
        (r.first_sent_at AT TIME ZONE c.tz)::date AS first_day,
        LEAST(
            ((r.first_sent_at + make_interval(mins => l.max_minutes)) AT TIME ZONE c.tz)::date,
            (COALESCE(r.first_callback_at, now()) AT TIME ZONE c.tz)::date,
            c.today
        ) AS last_day
    FROM serving_egisz.document_versions r
    CROSS JOIN ladder l
    CROSS JOIN calendar c
    WHERE r.is_current_version
      AND r.first_sent_at IS NOT NULL
),
queue_labels AS (
    SELECT
        clinic_jid,
        semd_code,
        MAX(clinic_name) AS clinic_name,
        MAX(clinic_label) AS clinic_label,
        MAX(semd_label) AS semd_label
    FROM queue_documents
    GROUP BY clinic_jid, semd_code
),
queue_days AS (
    SELECT
        g.snapshot_date,
        q.clinic_jid,
        q.semd_code,
        seg.code AS pending_segment,
        COUNT(*)::bigint AS docs_pending
    FROM queue_documents q
    CROSS JOIN calendar c
    CROSS JOIN LATERAL (
        SELECT d::date AS snapshot_date
        FROM generate_series(q.first_day::timestamp, q.last_day::timestamp, interval '1 day') d
    ) g
    CROSS JOIN LATERAL (
        SELECT LEAST(((g.snapshot_date + 1)::timestamp AT TIME ZONE c.tz), now()) AS ts
    ) anchor
    CROSS JOIN LATERAL serving_egisz.pending_segment_at(q.first_sent_at, anchor.ts) seg
    WHERE serving_egisz.is_pending_at(q.first_sent_at, q.first_callback_at, anchor.ts)
      AND NOT seg.is_no_response
    GROUP BY g.snapshot_date, q.clinic_jid, q.semd_code, seg.code
)
SELECT
    qd.snapshot_date,
    qd.clinic_jid,
    ql.clinic_name,
    ql.clinic_label,
    qd.semd_code,
    ql.semd_label,
    s.code AS pending_segment,
    s.label AS pending_segment_label,
    s.sort_order AS pending_segment_sort,
    qd.docs_pending,
    (qd.snapshot_date < c.today) AS is_complete_day
FROM queue_days qd
CROSS JOIN calendar c
JOIN queue_labels ql
  ON ql.clinic_jid IS NOT DISTINCT FROM qd.clinic_jid
 AND ql.semd_code IS NOT DISTINCT FROM qd.semd_code
JOIN mart_egisz.dim_pending_segments s ON s.code = qd.pending_segment
WITH DATA;

-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label NOT NULL по
-- построению, и REFRESH CONCURRENTLY требует уникальный btree без выражений. Код СЭМД
-- бывает пуст: ключ сравнивает пустые значения как равные.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_queue_daily
    ON serving_egisz.pending_queue_daily (snapshot_date, clinic_label, semd_code, pending_segment) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_pending_queue_daily_clinic_jid
    ON serving_egisz.pending_queue_daily (clinic_jid);

COMMENT ON MATERIALIZED VIEW serving_egisz.pending_queue_daily IS
'История очереди обработки: грейн (snapshot_date — отчётный день МСК, клиника, тип СЭМД, ступень срока ожидания); docs_pending — документов в очереди на конец дня (для текущего дня — на момент обновления), is_complete_day — день закрыт. В очередь входят только ступени лестницы: документы за терминальным порогом («Ответ не получен») не учитываются. Определение очереди — is_pending_at / pending_segment_at. Обновляется refresh_report_marts().';

-- ---------------------------------------------------------------- section: semd error categories
-- Категории ошибок внутри типа СЭМД по дням: грейн (ips_day — отчётный день МСК, клиника,
-- тип СЭМД, вид и категория ошибки). Строка есть у каждой категории справочника, в том
-- числе с нулём документов, поэтому знаменатель одной группы (день, клиника, тип СЭМД)
-- повторяется во всех её категориях: доля категории за любой набор дней —
-- SUM(docs_with_category) / SUM(docs_denominator) по строкам этой категории. Документ с
-- несколькими категориями учитывается в каждой; учитываются отказы и ошибки связи, как у
-- недельной и месячной структуры ошибок.
CREATE MATERIALIZED VIEW serving_egisz.semd_error_categories_daily AS
WITH documents AS (
    SELECT
        r.dwh_id,
        (r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS ips_day,
        r.clinic_jid,
        r.clinic_label,
        r.semd_code,
        r.semd_label,
        r.status
    FROM serving_egisz.documents_current r
    WHERE r.ips_date IS NOT NULL
),
totals AS (
    SELECT
        d.ips_day,
        d.clinic_jid,
        d.clinic_label,
        d.semd_code,
        MAX(d.semd_label) AS semd_label,
        COUNT(*) FILTER (WHERE d.status <> 'sent')::bigint AS docs_total,
        COUNT(*)::bigint AS docs_all
    FROM documents d
    GROUP BY d.ips_day, d.clinic_jid, d.clinic_label, d.semd_code
),
hits AS (
    SELECT
        d.ips_day,
        d.clinic_label,
        COALESCE(d.semd_code, '') AS semd_key,
        c.error_kind,
        COALESCE(t.error_category, '') AS category_key,
        COUNT(DISTINCT d.dwh_id)::bigint AS docs_with_category
    FROM documents d
    JOIN mart_egisz.document_errors c ON c.dwh_id = d.dwh_id
    LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
    WHERE d.status = 'async_error' OR c.error_kind = 'Ошибка связи'
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    tt.ips_day,
    tt.clinic_jid,
    tt.clinic_label,
    tt.semd_code,
    tt.semd_label,
    k.error_kind,
    k.error_category,
    COALESCE(h.docs_with_category, 0)::bigint AS docs_with_category,
    tt.docs_total,
    tt.docs_all,
    -- Отказ считается от документов с ответом РЭМД, ошибка связи — от всех документов.
    CASE WHEN k.error_kind = 'Ошибка связи' THEN tt.docs_all ELSE tt.docs_total END AS docs_denominator
FROM totals tt
CROSS JOIN mart_egisz.dim_error_categories k
LEFT JOIN hits h
  ON h.ips_day = tt.ips_day
 AND h.clinic_label = tt.clinic_label
 AND h.semd_key = COALESCE(tt.semd_code, '')
 AND h.error_kind = k.error_kind
 AND h.category_key = COALESCE(k.error_category, '')
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_semd_error_categories_daily
    ON serving_egisz.semd_error_categories_daily (ips_day, clinic_label, semd_code, error_kind, error_category) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_semd_error_categories_daily_day
    ON serving_egisz.semd_error_categories_daily (ips_day);

COMMENT ON MATERIALIZED VIEW serving_egisz.semd_error_categories_daily IS
'Категории ошибок внутри типа СЭМД: грейн (ips_day — отчётный день МСК по ips_date, клиника, тип СЭМД, вид, категория); строка есть у каждой категории справочника. docs_with_category — документы группы с ошибкой категории (отказ или ошибка связи; документ учитывается в каждой своей категории); docs_total — документы с ответом РЭМД, docs_all — все документы группы; docs_denominator — знаменатель доли: docs_all у ошибок связи, docs_total у отказов. Доля = SUM(docs_with_category) / SUM(docs_denominator) по строкам категории. Обновляется refresh_report_marts().';

-- ---------------------------------------------------------------- section: registration speed
-- Скорость регистрации: сколько документов получили первый ответ в пределах каждой ступени
-- лестницы ожидания. Грейн (ips_day, клиника, тип СЭМД, ступень); ступень «Получен ответ»
-- (порядок 0) — все документы с ответом, основание воронки. Ступень срока ответа подбирает
-- pending_segment_at на момент первого ответа: документ укладывается в каждую ступень не
-- ниже своей. Пороги лестницы — целые минуты, поэтому срок округляется вверх до минуты без
-- смены ступени, и функция вызывается один раз на различный срок, а не на документ.
CREATE MATERIALIZED VIEW serving_egisz.registration_speed_daily AS
WITH answered AS (
    SELECT
        (r.ips_date AT TIME ZONE serving_egisz.report_timezone())::date AS ips_day,
        r.clinic_jid,
        r.clinic_label,
        r.semd_code,
        r.semd_label,
        ceil(EXTRACT(EPOCH FROM (r.first_callback_at - r.first_sent_at)) / 60.0)::integer AS answer_minutes
    FROM serving_egisz.documents_current r
    WHERE r.ips_date IS NOT NULL
      AND r.first_sent_at IS NOT NULL
      AND r.first_callback_at IS NOT NULL
      AND r.first_callback_at >= r.first_sent_at
),
answer_segments AS (
    SELECT m.answer_minutes, seg.sort_order AS answer_segment_sort
    FROM (SELECT DISTINCT answer_minutes FROM answered) m
    CROSS JOIN LATERAL serving_egisz.pending_segment_at(
        'epoch'::timestamptz, 'epoch'::timestamptz + make_interval(mins => m.answer_minutes)
    ) seg
),
stages AS (
    SELECT 'answered'::text AS stage_code, 'Получен ответ'::text AS stage_label, 0::smallint AS stage_sort
    UNION ALL
    SELECT g.code, g.label, g.sort_order
    FROM mart_egisz.dim_pending_segments g
    WHERE NOT g.is_no_response
)
SELECT
    a.ips_day,
    a.clinic_jid,
    a.clinic_label,
    a.semd_code,
    MAX(a.semd_label) AS semd_label,
    s.stage_code,
    s.stage_label,
    s.stage_sort,
    COUNT(*) FILTER (WHERE s.stage_sort = 0 OR g.answer_segment_sort <= s.stage_sort)::bigint AS docs
FROM answered a
JOIN answer_segments g ON g.answer_minutes = a.answer_minutes
CROSS JOIN stages s
GROUP BY a.ips_day, a.clinic_jid, a.clinic_label, a.semd_code, s.stage_code, s.stage_label, s.stage_sort
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_registration_speed_daily
    ON serving_egisz.registration_speed_daily (ips_day, clinic_label, semd_code, stage_code) NULLS NOT DISTINCT;

COMMENT ON MATERIALIZED VIEW serving_egisz.registration_speed_daily IS
'Скорость регистрации в РЭМД: грейн (ips_day — отчётный день МСК по ips_date, клиника, тип СЭМД, ступень лестницы ожидания). docs — документы, первый ответ которых пришёл в пределах ступени от первой отправки; ступень answered («Получен ответ», порядок 0) — все документы с ответом. Ступени — mart_egisz.dim_pending_segments без терминальной. Обновляется refresh_report_marts().';

-- ---------------------------------------------------------------- section: clinic activity
-- Состояние клиник на день: активность каждого JID активной базы на конец каждого отчётного
-- дня (для текущего дня — на момент обновления) и ориентировочные денежные показатели по
-- плоской ставке (mart_egisz.jid_fee_rates). Показатели за произвольный период —
-- serving_egisz.clinic_activity(p_from, p_to). Окна и пороги — mart_egisz.dim_jid_activity_rules. Активная база дня D —
-- JID с документами в окне [D − active_days + 1; D];
-- замолчавший — нет документов в последние quiet_days суток окна; без успехов — не
-- замолчавший, от no_success_min_docs документов за окно и ни одного успешного. Документ
-- относится к дню по ips_date, как во всех витринах.
CREATE MATERIALIZED VIEW serving_egisz.clinic_activity_daily AS
WITH calendar AS (
    SELECT
        serving_egisz.report_timezone() AS tz,
        (now() AT TIME ZONE serving_egisz.report_timezone())::date AS today
),
fee_rates AS (
    SELECT f.*, lead(f.valid_from) OVER (ORDER BY f.valid_from) AS valid_to
    FROM mart_egisz.jid_fee_rates f
),
rules AS (
    SELECT r.*, lead(r.valid_from) OVER (ORDER BY r.valid_from) AS valid_to
    FROM mart_egisz.dim_jid_activity_rules r
),
activity AS (
    SELECT
        r.clinic_jid,
        (r.ips_date AT TIME ZONE c.tz)::date AS activity_day,
        COUNT(*)::bigint AS docs,
        COUNT(*) FILTER (WHERE r.status = 'success')::bigint AS docs_success,
        COUNT(*) FILTER (WHERE r.status IN ('success', 'async_error'))::bigint AS docs_answered
    FROM serving_egisz.document_versions r
    CROSS JOIN calendar c
    WHERE r.is_current_version
      AND r.clinic_jid IS NOT NULL
      AND r.ips_date IS NOT NULL
    GROUP BY 1, 2
),
history AS (
    SELECT MIN(activity_day) AS first_day FROM activity
),
spread AS (
    SELECT a.*, g.snapshot_date::date AS snapshot_date
    FROM activity a
    CROSS JOIN calendar c
    CROSS JOIN LATERAL generate_series(
        a.activity_day::timestamp,
        LEAST(a.activity_day + (SELECT MAX(active_days) FROM mart_egisz.dim_jid_activity_rules) - 1, c.today)::timestamp,
        interval '1 day'
    ) AS g(snapshot_date)
),
per_jid AS (
    SELECT
        s.snapshot_date,
        s.clinic_jid,
        f.jid_monthly_fee,
        r.active_days,
        r.quiet_days,
        r.no_success_min_docs,
        SUM(s.docs)::bigint AS docs_window,
        SUM(s.docs_success)::bigint AS docs_success_window,
        SUM(s.docs_answered)::bigint AS docs_answered_window,
        MAX(s.activity_day) AS last_document_day
    FROM spread s
    JOIN fee_rates f
      ON s.snapshot_date >= f.valid_from
     AND (f.valid_to IS NULL OR s.snapshot_date < f.valid_to)
    JOIN rules r
      ON s.snapshot_date >= r.valid_from
     AND (r.valid_to IS NULL OR s.snapshot_date < r.valid_to)
    WHERE s.activity_day > s.snapshot_date - r.active_days
    GROUP BY s.snapshot_date, s.clinic_jid, f.jid_monthly_fee, r.active_days, r.quiet_days, r.no_success_min_docs
),
flagged AS (
    SELECT
        p.*,
        (p.last_document_day <= p.snapshot_date - p.quiet_days) AS is_silent,
        (p.last_document_day > p.snapshot_date - p.quiet_days
         AND p.docs_success_window = 0
         AND p.docs_window >= p.no_success_min_docs) AS is_no_success
    FROM per_jid p
)
SELECT
    f.snapshot_date,
    f.clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(btrim(f.clinic_jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(btrim(o.name), ''), '—') AS clinic_label,
    o.inn AS clinic_inn,
    f.docs_window,
    f.docs_success_window,
    f.docs_answered_window,
    f.last_document_day,
    f.is_silent,
    f.is_no_success,
    f.jid_monthly_fee AS monthly_fee,
    (f.jid_monthly_fee * 12)::numeric(14, 2) AS annual_fee,
    CASE WHEN f.is_silent OR f.is_no_success THEN f.jid_monthly_fee ELSE 0 END::numeric(12, 2) AS monthly_fee_at_risk,
    CASE WHEN f.is_silent THEN f.jid_monthly_fee ELSE 0 END::numeric(12, 2) AS monthly_fee_silent,
    CASE WHEN f.is_no_success THEN f.jid_monthly_fee ELSE 0 END::numeric(12, 2) AS monthly_fee_no_success,
    (f.snapshot_date < c.today) AS is_complete_day,
    (f.snapshot_date - f.active_days + 1 >= h.first_day) AS is_full_window
FROM flagged f
CROSS JOIN calendar c
CROSS JOIN history h
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = f.clinic_jid
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_clinic_activity_daily
    ON serving_egisz.clinic_activity_daily (snapshot_date, clinic_jid);

COMMENT ON MATERIALIZED VIEW serving_egisz.clinic_activity_daily IS
'Состояние клиник на день: активность и ориентировочные денежные показатели по плоской ставке mart_egisz.jid_fee_rates и правилам активности mart_egisz.dim_jid_activity_rules; грейн (snapshot_date — отчётный день МСК, JID активной базы этого дня). Активная база — JID с документами за active_days суток по день включительно (для текущего дня — на момент обновления); docs_window, docs_success_window, docs_answered_window — документы, успешные и с ответом РЭМД за окно; last_document_day — день последнего документа. is_silent — замолчал: нет документов quiet_days суток; is_no_success — не замолчал, от no_success_min_docs документов и ни одного успешного. monthly_fee — MRR JID, annual_fee — ARR; monthly_fee_at_risk — MRR под риском (замолчавшие и без успехов, JID один раз), monthly_fee_silent и monthly_fee_no_success — по спискам. MRR дня = SUM(monthly_fee) по дню. is_full_window — окно дня целиком лежит в истории. Ставка — порядок величины, а не биллинг. Обновляется refresh_report_marts().';

-- ---------------------------------------------------------------- section: period functions
-- Показатели, зависящие от периода фильтра дашборда. Параметр NULL или пустой массив —
-- без ограничения. Тело функции — один SELECT на STABLE-функциях: планировщик подставляет
-- его в запрос потребителя, как тело представления.

-- Вклад клиник в изменение доли ошибок: период [p_from; p_to) против опорного периода фазы
-- контрольной карты (mart_egisz.dim_control_chart_phases). Фаза — последняя, начатая не позже
-- недели последнего документа периода; её опорный период должен быть закрыт. Определения —
-- README, раздел «Вклад клиник в изменение доли ошибок».
CREATE OR REPLACE FUNCTION serving_egisz.clinic_error_rate_contribution(
    p_from timestamptz,
    p_to timestamptz,
    p_clinic_labels text[] DEFAULT NULL,
    p_semd_labels text[] DEFAULT NULL,
    p_error_types text[] DEFAULT NULL
)
RETURNS TABLE (
    clinic text,
    docs_baseline bigint,
    docs_period bigint,
    rate_baseline numeric,
    rate_period numeric,
    contribution_pp numeric,
    via_rate_pp numeric,
    via_volume_pp numeric,
    top_error_type text,
    row_kind integer,
    sort_key numeric
)
LANGUAGE sql
STABLE
AS $$
WITH calendar AS MATERIALIZED (
    SELECT serving_egisz.report_timezone() AS tz
),
slice AS (
    SELECT d.dwh_id, d.clinic_label, d.status, d.ips_date
    FROM serving_egisz.documents_current d
    WHERE d.ips_date IS NOT NULL
      AND (COALESCE(cardinality(p_clinic_labels), 0) = 0 OR d.clinic_label = ANY (p_clinic_labels))
      AND (COALESCE(cardinality(p_semd_labels), 0) = 0 OR d.semd_label = ANY (p_semd_labels))
),
counted AS (
    SELECT
        s.*,
        s.status = 'async_error'
        AND (COALESCE(cardinality(p_error_types), 0) = 0 OR EXISTS (
            SELECT 1
            FROM serving_egisz.document_errors e
            WHERE e.dwh_id = s.dwh_id
              AND e.error_type = ANY (p_error_types)
        )) AS is_error
    FROM slice s
),
period AS (
    SELECT
        c.clinic_label,
        COUNT(DISTINCT c.dwh_id) FILTER (WHERE c.status <> 'sent') AS docs,
        COUNT(DISTINCT c.dwh_id) FILTER (WHERE c.is_error) AS errs,
        MAX(c.ips_date) AS last_at
    FROM counted c
    WHERE (p_from IS NULL OR c.ips_date >= p_from)
      AND (p_to IS NULL OR c.ips_date < p_to)
    GROUP BY c.clinic_label
),
phase AS MATERIALIZED (
    SELECT
        ph.baseline_start,
        ph.baseline_end,
        ph.baseline_start::timestamp AT TIME ZONE c.tz AS from_ts,
        (ph.baseline_end + 7)::timestamp AT TIME ZONE c.tz AS to_ts
    FROM calendar c
    CROSS JOIN LATERAL (
        SELECT p.baseline_start, p.baseline_end
        FROM mart_egisz.dim_control_chart_phases p
        WHERE p.period_grain = 'week'
          AND p.phase_start <= (SELECT date_trunc('week', MAX(period.last_at) AT TIME ZONE c.tz)::date FROM period)
        ORDER BY p.phase_start DESC
        LIMIT 1
    ) ph
    WHERE ph.baseline_end < date_trunc('week', now() AT TIME ZONE c.tz)::date
),
baseline AS (
    SELECT
        c.clinic_label,
        COUNT(DISTINCT c.dwh_id) FILTER (WHERE c.status <> 'sent') AS docs,
        COUNT(DISTINCT c.dwh_id) FILTER (WHERE c.is_error) AS errs
    FROM counted c
    CROSS JOIN phase ph
    WHERE c.ips_date >= ph.from_ts
      AND c.ips_date < ph.to_ts
    GROUP BY c.clinic_label
),
clinics AS (
    SELECT
        COALESCE(p.clinic_label, b.clinic_label) AS clinic_label,
        COALESCE(b.docs, 0) AS docs0,
        COALESCE(b.errs, 0) AS errs0,
        COALESCE(p.docs, 0) AS docs1,
        COALESCE(p.errs, 0) AS errs1
    FROM period p
    FULL JOIN baseline b ON b.clinic_label = p.clinic_label
    WHERE EXISTS (SELECT 1 FROM phase)
),
totals AS (
    SELECT
        SUM(docs0) AS n0,
        SUM(docs1) AS n1,
        SUM(errs0)::numeric / NULLIF(SUM(docs0), 0) AS p0,
        SUM(errs1)::numeric / NULLIF(SUM(docs1), 0) AS p1
    FROM clinics
),
contrib AS (
    SELECT
        c.*,
        100.0 * ((c.errs1 - t.p0 * c.docs1) / NULLIF(t.n1, 0) - (c.errs0 - t.p0 * c.docs0) / NULLIF(t.n0, 0)) AS total_pp,
        CASE
            WHEN c.docs0 > 0 AND c.docs1 > 0
                THEN 100.0 * c.docs1 / NULLIF(t.n1, 0) * (c.errs1::numeric / c.docs1 - c.errs0::numeric / c.docs0)
            ELSE 0
        END AS rate_pp,
        SIGN(t.p1 - t.p0) AS direction
    FROM clinics c
    CROSS JOIN totals t
),
top_error AS (
    SELECT DISTINCT ON (by_type.clinic_label) by_type.clinic_label, by_type.error_type
    FROM (
        SELECT s.clinic_label, e.error_type, COUNT(DISTINCT e.dwh_id) AS docs
        FROM slice s
        JOIN serving_egisz.document_errors e ON e.dwh_id = s.dwh_id
        WHERE e.status = 'async_error'
          AND e.error_kind = 'Ошибка асинхронного ответа'
          AND (p_from IS NULL OR s.ips_date >= p_from)
          AND (p_to IS NULL OR s.ips_date < p_to)
        GROUP BY s.clinic_label, e.error_type
    ) by_type
    ORDER BY by_type.clinic_label, by_type.docs DESC, by_type.error_type
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM period) THEN 'Нет документов за период'
        WHEN ph.baseline_start IS NULL THEN 'Нет закрытого опорного периода фазы'
        ELSE 'Итого (опорный период ' || to_char(ph.baseline_start, 'DD.MM') || '–'
             || to_char(ph.baseline_end + 6, 'DD.MM.YYYY') || ')'
    END,
    t.n0,
    t.n1,
    ROUND(100 * t.p0, 1),
    ROUND(100 * t.p1, 1),
    ROUND(100 * (t.p1 - t.p0), 1),
    (SELECT ROUND(SUM(ct.rate_pp), 1) FROM contrib ct),
    (SELECT ROUND(SUM(ct.total_pp - ct.rate_pp), 1) FROM contrib ct),
    NULL::text,
    0,
    NULL::numeric
FROM totals t
LEFT JOIN phase ph ON TRUE
UNION ALL
SELECT
    c.clinic_label,
    c.docs0,
    c.docs1,
    ROUND(100.0 * c.errs0 / NULLIF(c.docs0, 0), 1),
    ROUND(100.0 * c.errs1 / NULLIF(c.docs1, 0), 1),
    ROUND(c.total_pp, 1),
    ROUND(c.rate_pp, 1),
    ROUND(c.total_pp - c.rate_pp, 1),
    te.error_type,
    1,
    c.direction * c.total_pp
FROM contrib c
LEFT JOIN top_error te ON te.clinic_label = c.clinic_label
$$;

COMMENT ON FUNCTION serving_egisz.clinic_error_rate_contribution(timestamptz, timestamptz, text[], text[], text[]) IS
'Вклад клиник в изменение доли ошибок: период [p_from; p_to) против опорного периода фазы контрольной карты, срез по клиникам (clinic_label), типам СЭМД (semd_label) и типам ошибки. Строка row_kind 0 — итог: документы с исходом и доля отказов РЭМД в опорном периоде и периоде, изменение доли (contribution_pp) и его части за счёт долей ошибок клиник (via_rate_pp) и за счёт объёма (via_volume_pp); без закрытого опорного периода или без документов — пояснение. Строки row_kind 1 — клиники; sort_key упорядочивает их по направлению общего изменения. top_error_type — самый частый тип отказа РЭМД клиники за период.';

-- Показатели клиник (JID) за период [p_from; p_to): строка — JID с документами в периоде либо
-- в окне active_days перед ним. Правила и ставка — действующие на последний день периода;
-- конец периода в будущем ограничен текущим моментом. Определения — README, раздел
-- «Показатели клиник за период».
CREATE OR REPLACE FUNCTION serving_egisz.clinic_activity(
    p_from timestamptz,
    p_to timestamptz
)
RETURNS TABLE (
    clinic_jid bigint,
    clinic_name text,
    clinic_label text,
    clinic_inn text,
    docs bigint,
    docs_success bigint,
    docs_answered bigint,
    docs_refused bigint,
    docs_pending bigint,
    docs_no_response bigint,
    docs_before bigint,
    first_sent_at timestamptz,
    last_document_at timestamptz,
    is_active boolean,
    is_new boolean,
    is_churned boolean,
    is_silent boolean,
    is_no_success boolean,
    monthly_fee numeric
)
LANGUAGE sql
STABLE
AS $$
WITH bounds AS MATERIALIZED (
    SELECT
        COALESCE(p_from, '-infinity'::timestamptz) AS from_ts,
        LEAST(COALESCE(p_to, 'infinity'::timestamptz), now()) AS to_ts
),
rules AS MATERIALIZED (
    SELECT r.*
    FROM mart_egisz.dim_jid_activity_rules r
    CROSS JOIN bounds b
    WHERE r.valid_from <= (b.to_ts AT TIME ZONE serving_egisz.report_timezone())::date
    ORDER BY r.valid_from DESC
    LIMIT 1
),
fee AS MATERIALIZED (
    SELECT f.jid_monthly_fee
    FROM mart_egisz.jid_fee_rates f
    CROSS JOIN bounds b
    WHERE f.valid_from <= (b.to_ts AT TIME ZONE serving_egisz.report_timezone())::date
    ORDER BY f.valid_from DESC
    LIMIT 1
),
history AS MATERIALIZED (
    SELECT MIN(d.first_sent_at) AS first_at
    FROM mart_egisz.documents d
    WHERE COALESCE(d.is_current_version, true)
      AND NULLIF(btrim(d.dwh_id), '') IS NOT NULL
),
per_jid AS (
    SELECT
        v.clinic_jid,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts) AS docs,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts AND v.status = 'success') AS docs_success,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts AND v.status IN ('success', 'async_error')) AS docs_answered,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts AND v.status = 'async_error') AS docs_refused,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts AND v.sent_state = 'pending') AS docs_pending,
        COUNT(*) FILTER (WHERE v.ips_date >= b.from_ts AND v.sent_state = 'no_response') AS docs_no_response,
        COUNT(*) FILTER (WHERE v.ips_date < b.from_ts) AS docs_before,
        MAX(v.ips_date) FILTER (WHERE v.ips_date >= b.from_ts) AS last_document_at
    FROM serving_egisz.document_versions v
    CROSS JOIN bounds b
    WHERE v.is_current_version
      AND v.clinic_jid IS NOT NULL
      -- Границы — скалярными подзапросами: так они служат условием индекса по ips_date.
      AND v.ips_date >= (SELECT b2.from_ts - make_interval(days => r.active_days) FROM bounds b2 CROSS JOIN rules r)
      AND v.ips_date < (SELECT b2.to_ts FROM bounds b2)
    GROUP BY v.clinic_jid
)
SELECT
    p.clinic_jid,
    o.name,
    COALESCE(NULLIF(btrim(p.clinic_jid::text), ''), '—') || ' · ' || COALESCE(NULLIF(btrim(o.name), ''), '—'),
    o.inn,
    p.docs,
    p.docs_success,
    p.docs_answered,
    p.docs_refused,
    p.docs_pending,
    p.docs_no_response,
    p.docs_before,
    fs.first_sent_at,
    p.last_document_at,
    p.docs > 0,
    COALESCE(fs.first_sent_at >= b.from_ts AND fs.first_sent_at < b.to_ts
             AND fs.first_sent_at >= h.first_at + make_interval(days => r.active_days), false),
    p.docs = 0 AND p.docs_before > 0,
    p.docs > 0 AND p.last_document_at < b.to_ts - make_interval(days => r.quiet_days),
    p.docs >= r.no_success_min_docs AND p.docs_success = 0,
    f.jid_monthly_fee
FROM per_jid p
CROSS JOIN bounds b
CROSS JOIN rules r
CROSS JOIN fee f
CROSS JOIN history h
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = p.clinic_jid
LEFT JOIN LATERAL (
    SELECT MIN(d.first_sent_at) AS first_sent_at
    FROM mart_egisz.documents d
    WHERE d.jid = p.clinic_jid
      AND COALESCE(d.is_current_version, true)
      AND NULLIF(btrim(d.dwh_id), '') IS NOT NULL
) fs ON TRUE
$$;

COMMENT ON FUNCTION serving_egisz.clinic_activity(timestamptz, timestamptz) IS
'Показатели клиник (JID) за период [p_from; p_to): строка — JID с документами в периоде либо в окне active_days перед ним. docs … docs_no_response — документы периода по исходу и состоянию отправки, docs_before — документы окна перед периодом, first_sent_at — первая отправка JID за всю историю. is_active — есть документы в периоде; is_new — первая отправка в периоде и не раньше active_days от начала истории; is_churned — документы только в окне перед периодом; is_silent — активен, но без документов последние quiet_days суток периода; is_no_success — от no_success_min_docs документов и ни одного успешного. Правила — mart_egisz.dim_jid_activity_rules, monthly_fee — mart_egisz.jid_fee_rates на последний день периода.';

-- ---------------------------------------------------------------- section: filter dimensions
-- Значения фильтров «Клиника» и «Тип СЭМД» дашбордов: пары клиника — тип СЭМД текущих версий
-- документов. Подписи собираются так же, как в document_versions, чтобы значение фильтра
-- совпадало со столбцом витрин.
CREATE MATERIALIZED VIEW serving_egisz.clinic_semd_types AS
SELECT
    f.clinic_jid,
    COALESCE(NULLIF(btrim(f.clinic_jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(btrim(o.name), ''), '—') AS clinic_label,
    o.name AS clinic_name,
    f.semd_code,
    CASE
        WHEN st.oid IS NOT NULL AND st.name IS NOT NULL
            THEN st.oid || ' · ' || st.name
        WHEN st.oid IS NOT NULL
            THEN st.oid || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
    END AS semd_label,
    f.documents_total
FROM (
    SELECT
        d.jid AS clinic_jid,
        stg_egisz.normalize_semd_code(d.semd_code) AS semd_code,
        COUNT(*)::bigint AS documents_total
    FROM mart_egisz.documents d
    WHERE COALESCE(d.is_current_version, true)
      AND NULLIF(btrim(d.dwh_id), '') IS NOT NULL
    GROUP BY 1, 2
) f
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = f.clinic_jid
LEFT JOIN mart_egisz.dim_nsi_semd_types st ON st.oid = f.semd_code
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_clinic_semd_types
    ON serving_egisz.clinic_semd_types (clinic_label, semd_code) NULLS NOT DISTINCT;

COMMENT ON MATERIALIZED VIEW serving_egisz.clinic_semd_types IS
'Пары клиника — тип СЭМД текущих версий документов: грейн (clinic_label, semd_code); подписи clinic_label и semd_label совпадают с document_versions; documents_total — число документов пары. Источник значений фильтров «Клиника» и «Тип СЭМД». Обновляется refresh_report_marts().';

-- Статус документа в разрезе состояния отправки — значения фильтра «Статус»: финальный
-- статус либо состояние отправки нефинального, как status_detail в documents_current.
-- Состояние «Ответ не получен» в состояние к выдаче не входит и значением фильтра не служит.
CREATE VIEW serving_egisz.document_status_details AS
SELECT ds.code AS status_detail, ds.label AS status_detail_label, ds.sort_order AS status_detail_sort
FROM mart_egisz.dim_document_statuses ds
WHERE ds.is_final
UNION ALL
SELECT ss.code, ss.label, ds.sort_order + ss.sort_order - 1
FROM mart_egisz.dim_document_statuses ds
CROSS JOIN mart_egisz.dim_sent_states ss
WHERE NOT ds.is_final
  AND ss.code <> 'no_response';

COMMENT ON VIEW serving_egisz.document_status_details IS
'Значения статуса документа с раскрытием состояния отправки (status_detail, status_detail_label, status_detail_sort) — те же, что в documents_current; без состояния «Ответ не получен».';

CREATE VIEW serving_egisz.pending_segments AS
SELECT
    g.code AS pending_segment,
    g.label AS pending_segment_label,
    g.sort_order AS pending_segment_sort
FROM mart_egisz.dim_pending_segments g
WHERE NOT g.is_no_response;

COMMENT ON VIEW serving_egisz.pending_segments IS
'Ступени лестницы ожидания ответа под именами столбцов витрин (pending_segment, pending_segment_label, pending_segment_sort) без ступени за сроком ожидания: значения фильтра «Срок ожидания».';

-- Обновление материализованных витрин — единственное определение их состава и порядка:
-- функцию вызывают DAG-и и сценарий применения схемы. Порядок обязателен: состояние документа
-- к выдаче и список без ответа обновляются одним вызовом — документ входит ровно в один из
-- них; опубликованные ошибки и витрины читают их и текущие ошибки документа. CONCURRENTLY не
-- блокирует чтение дашбордов, но требует наполненного представления — ненаполненное
-- обновляется обычным способом. Статистика собирается сразу после обновления.
CREATE OR REPLACE FUNCTION serving_egisz.refresh_report_marts(
    p_concurrently boolean DEFAULT true,
    p_include_periodic boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    mart regclass;
BEGIN
    FOREACH mart IN ARRAY ARRAY[
        'mart_egisz.document_errors',
        'serving_egisz.documents_current',
        'serving_egisz.documents_no_response',
        'serving_egisz.document_errors',
        'serving_egisz.document_error_types',
        'mart_egisz_admin.document_quality',
        'serving_egisz.documents_weekly',
        'serving_egisz.document_errors_weekly',
        'serving_egisz.documents_monthly',
        'serving_egisz.document_errors_monthly',
        'serving_egisz.pending_queue_daily',
        'serving_egisz.semd_error_categories_daily',
        'serving_egisz.registration_speed_daily',
        'serving_egisz.clinic_activity_daily',
        'serving_egisz.clinic_semd_types'
    ]::regclass[]
    LOOP
        -- Периодические срезы читают уже обновлённые ошибки документа, но их полное
        -- построение не задерживает очередной приём журнала.
        IF NOT p_include_periodic AND mart = ANY(ARRAY[
            'serving_egisz.documents_weekly',
            'serving_egisz.document_errors_weekly',
            'serving_egisz.documents_monthly',
            'serving_egisz.document_errors_monthly'
        ]::regclass[]) THEN
            CONTINUE;
        END IF;
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

-- Обход от подач без документа (в реестре их тысячи) к ответам по ключу relatesToMessage:
-- обратный порядок — от каждого ответа к реестру — стоил поиска на сотнях тысяч сообщений.
-- От подачи берётся последняя по EGMID строка без DOCUMENTID. OFFSET 0 удерживает
-- вложенный цикл: хеш-соединение читало бы все сообщения всех партиций.
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
FROM (
    SELECT DISTINCT ON (m.msgid) m.msgid, m.egmid, m.created_at, m.reply_to
    FROM stg_egisz.message_registry m
    WHERE m.document_uid IS NULL
      AND m.msgid IS NOT NULL
    ORDER BY m.msgid, m.egmid DESC NULLS LAST
) reg
CROSS JOIN LATERAL (
    SELECT t.*
    FROM stg_egisz.exchange_messages t
    WHERE stg_egisz.message_registry_key(t.relates_to_msgid) = reg.msgid
      AND t.relates_to_msgid IS NOT NULL
      AND t.egisz_subsystem IS DISTINCT FROM 'ИЭМК'
    OFFSET 0
) tx
LEFT JOIN mart_egisz.dim_organizations o ON o.jid = tx.jid;

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
-- Порог сигнала — число строк детализации, но не более 500: сортировка по LOGID ничего не
-- меняла в счёте и заставляла строить детализацию целиком.
registry_no_document_recent AS (
    SELECT COUNT(*)::numeric AS cnt
    FROM (
        SELECT 1
        FROM mart_egisz_admin.health_message_registry_no_document
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
no_response_docs AS (
    SELECT COUNT(*) AS cnt
    FROM mart_egisz.documents d, no_response_after c
    WHERE d.status = 'sent' AND d.first_sent_at < c.ts
),
-- Отказы с типом «Не распознано»; их тексты сгруппированы в mart_egisz_admin.unrecognized_errors.
uncovered_types AS (
    SELECT DISTINCT c.dwh_id
    FROM mart_egisz.document_errors c
    JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
    WHERE c.error_kind = 'Ошибка асинхронного ответа'
      AND t.rule_code IS NULL
),
-- Элемент ошибки без строки в справочнике типов — элементы не приведены к текущим правилам.
untyped_errors AS (
    SELECT COUNT(*) AS cnt
    FROM mart_egisz.document_errors c
    LEFT JOIN mart_egisz.dim_error_types t ON t.error_type = c.error_type
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
        ('sent_24h', 'Отправлено без ответа > 24ч', 'yellow', (SELECT COUNT(*)::numeric FROM mart_egisz.documents WHERE status = 'sent' AND first_sent_at < now() - INTERVAL '24 hours'), 'документов', 'documents.status=sent', 'Проверить клиники без ответа ЕГИСЗ и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM mart_egisz.document_errors WHERE error_kind = 'Ошибка связи'), 'документов', 'mart_egisz.document_errors, вид «Ошибка связи»', 'Разобрать типы ошибок связи в serving_egisz.network_errors'),
        ('error_rows', 'Ошибки асинхронного ответа РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM mart_egisz.documents WHERE status = 'async_error'), 'документов', 'documents.status=async_error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('no_response_backlog',
         'Документы без ответа',
         CASE
             WHEN (SELECT cnt FROM no_response_docs) >= 50 THEN 'red'
             WHEN (SELECT cnt FROM no_response_docs) >= 20 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT cnt::numeric FROM no_response_docs),
         'документов',
         'serving_egisz.documents_no_response',
         'Проверить транспорт клиник по списку документов без ответа: ответ по ним уже не ожидается'),
        ('uncovered_error_types',
         'Отказы без правила классификации',
         CASE
             WHEN (SELECT COUNT(*) FROM uncovered_types) >= 1000 THEN 'red'
             WHEN (SELECT COUNT(*) FROM uncovered_types) >= 100 THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM uncovered_types),
         'документов',
         'mart_egisz.document_errors: тип «Не распознано»',
         'Разобрать нормализованные тексты в mart_egisz_admin.unrecognized_errors и завести правило в dim_error_rules'),
        ('untyped_errors',
         'Элементы ошибки без типа в справочнике',
         CASE WHEN (SELECT cnt FROM untyped_errors) >= 1 THEN 'red' ELSE 'green' END,
         (SELECT cnt FROM untyped_errors)::numeric,
         'элементов',
         'mart_egisz.document_errors вне mart_egisz.dim_error_types',
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
-- суточный DAG обслуживания, обновление витрин — задачи refresh_marts обоих DAG. Полные проходы в теле
-- наката пересекались по блокировкам с пятиминутным приёмом и давали взаимоблокировки.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM mart_egisz.documents)
       AND NOT EXISTS (SELECT 1 FROM mart_egisz.document_attributes) THEN
        PERFORM mart_egisz.recompute_document_error_texts(NULL::text[]);
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
ANALYZE mart_egisz.document_errors;
ANALYZE serving_egisz.documents_current;
ANALYZE serving_egisz.documents_no_response;
ANALYZE serving_egisz.document_errors;
ANALYZE serving_egisz.document_error_types;
ANALYZE serving_egisz.documents_weekly;
ANALYZE serving_egisz.document_errors_weekly;
ANALYZE serving_egisz.documents_monthly;
ANALYZE serving_egisz.document_errors_monthly;
ANALYZE serving_egisz.pending_queue_daily;
ANALYZE serving_egisz.semd_error_categories_daily;
ANALYZE serving_egisz.registration_speed_daily;
ANALYZE serving_egisz.clinic_activity_daily;
ANALYZE serving_egisz.clinic_semd_types;
ANALYZE mart_egisz_admin.document_quality;

\echo 'DWH init complete: egisz owns all objects of the EGISZ layer schemas in dwh_bi'
