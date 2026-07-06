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
queue AS (
    SELECT jid::text AS clinic_jid, COUNT(DISTINCT dwh_id)::bigint AS queue_cnt
    FROM public.documents
    WHERE status = 'waiting'
    GROUP BY jid
)
SELECT
    f.clinic_jid AS "JID Клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f.clinic_jid) AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.queue_cnt, 0)::bigint AS "В очереди (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.queue_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.queue_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN queue q ON q.clinic_jid = f.clinic_jid;

CREATE OR REPLACE VIEW public.rpt_health_proxy_db AS
SELECT
    (SELECT COUNT(*) FROM public.documents)::bigint AS "DWH сообщений всего",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'waiting')::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'waiting' AND first_sent_at < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'waiting' AND first_sent_at >= now() - INTERVAL '24 hours' AND first_sent_at < now() - INTERVAL '1 hour')::bigint AS "Очередь 1-24ч",
    (SELECT COUNT(DISTINCT dwh_id) FROM public.documents WHERE status = 'waiting' AND first_sent_at >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
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
-- Доля сообщений, которые classify_async_status не распознал (status='unknown'),
-- за 24ч. Растёт, если РЭМД присылает новый шаблон ответа — сигнал чинить регэкспы.
unknown_24h AS (
    SELECT ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'unknown')
        / NULLIF(COUNT(*), 0),
        1
    ) AS pct
    FROM public.transactions
    WHERE log_date >= now() - INTERVAL '24 hours'
)
SELECT * FROM (
    VALUES
        ('parsed_documents', 'Разложенные документы proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM public.documents), 'документов', 'documents', 'Контроль поступления СЭМД в DWH'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'waiting' AND first_sent_at < now() - INTERVAL '24 hours'), 'документов', 'documents.status=waiting', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'network_error'), 'документов', 'documents.status=network_error', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки асинхронного ответа РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM public.documents WHERE status = 'async_error'), 'документов', 'documents.status=async_error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('pending_backlog_24h',
         'Документы без ответа > 7 дней',
         CASE
             WHEN (SELECT COUNT(*) FROM public.documents WHERE status = 'waiting' AND first_sent_at < now() - INTERVAL '30 days') >= 50 THEN 'red'
             WHEN (SELECT COUNT(*) FROM public.documents WHERE status = 'waiting' AND first_sent_at < now() - INTERVAL '7 days') >= 20  THEN 'yellow'
             ELSE 'green'
         END,
         (SELECT COUNT(*)::numeric FROM public.documents WHERE status = 'waiting' AND first_sent_at < now() - INTERVAL '7 days'),
         'документов > 7 дн.',
         'rpt_documents_waiting (Сегмент ожидания)',
         'Проверить транспорт клиник в дашборде 03; до 3 дн. — норма, >7 дн. — эскалация'),
        ('unknown_high',
         'Доля «Нераспознан» (status=unknown)',
         CASE
             WHEN COALESCE((SELECT pct FROM unknown_24h), 0) >= 5 THEN 'red'
             WHEN COALESCE((SELECT pct FROM unknown_24h), 0) >= 1 THEN 'yellow'
             ELSE 'green'
         END,
         COALESCE((SELECT pct FROM unknown_24h), 0)::numeric,
         '% (за 24ч)',
         'transactions.status=unknown за 24ч',
         'Проверить regex classify_async_status, если появится новый шаблон ответа РЭМД'),
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

UPDATE public.documents d
SET error_text = src.error_text,
    updated_at = now()
FROM (
    SELECT DISTINCT ON (f.dwh_id)
        f.dwh_id,
        NULLIF(btrim(f.error_json_text), '') AS error_text
    FROM public.transactions f
    WHERE NULLIF(btrim(f.error_json_text), '') IS NOT NULL
    ORDER BY f.dwh_id, f.log_date DESC NULLS LAST, f.logid DESC
) src
WHERE d.dwh_id = src.dwh_id
  AND d.error_text IS DISTINCT FROM src.error_text;

-- Полная сборка атрибутов документов при init.
SELECT public.reconcile_document_attributes(NULL::text[]);
-- Полный пересчёт слоя версий при init: уточняет document_group_id / is_current_version /
-- цепочку по всему архиву (бэкфилл singleton в 10_tables — лишь стартовое состояние).
SELECT public.recompute_document_versions(NULL::text[]);
-- rpt_error_breakdown (matview) создан в 80 с данными, но после reconcile атрибутов
-- обновляем, чтобы display-колонки (клиника/СЭМД) отражали финальное состояние.
-- Идёт ПОСЛЕ recompute: rpt_documents (источник джойна matview) = текущие версии.
REFRESH MATERIALIZED VIEW public.rpt_error_breakdown;
ANALYZE public.exchangelog_raw;
ANALYZE public.documents;
ANALYZE public.transactions;
ANALYZE public.document_attributes;
ANALYZE public.rpt_error_breakdown;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
