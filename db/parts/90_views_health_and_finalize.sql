-- ============================================================================
-- 90_views_health_and_finalize.sql — v_health_* views, final GRANT verification, REFRESH MATERIALIZED VIEWs
-- Source: db/dwh_init.sql, lines [1744..1916).
-- Loaded by db/dwh_init.sql via \i db/parts/90_views_health_and_finalize.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
WITH anchor AS (
    -- Use the latest observed fact as the reference point so the "last 24h" window
    -- works on stale / archival data, not only on real-time pipelines.
    SELECT COALESCE(MAX("Обработано IPS"), now()) AS ref_ts
    FROM public.v_egisz_transactions_enriched_ui
),
fact_24h AS (
    SELECT
        "JID клиники",
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS docs_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_cnt
    FROM public.v_egisz_transactions_enriched_ui, anchor
    WHERE "Обработано IPS" >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY 1
),
queue AS (
    SELECT "JID клиники", COUNT(DISTINCT "localUid СЭМД")::bigint AS queue_cnt
    FROM public.v_rpt_documents_no_response_ui
    GROUP BY 1
)
SELECT
    f."JID клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f."JID клиники") AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.queue_cnt, 0)::bigint AS "В очереди (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.queue_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.queue_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN queue q ON q."JID клиники" = f."JID клиники";

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM exchangelog_raw)::bigint AS "Staging: всего строк",
    (SELECT COUNT(*) FROM egisz_messages_raw WHERE egmid IS NULL)::bigint AS "Без EGMID",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui)::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '24 hours' AND "Отправлено" < now() - INTERVAL '1 hour')::bigint AS "Очередь 1–24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT MAX(egmid) FROM egisz_messages_raw) AS "Staging max EGMID",
    (SELECT MAX(created_at) FROM egisz_messages_raw) AS "Staging max Sent",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт курсора",
    (SELECT MAX(last_log_id) FROM elt_state) AS "elt_state.last_log_id",
    (SELECT MAX(last_egmid) FROM elt_state) AS "elt_state.last_egmid (курсор EGISZ_MESSAGES)",
    (SELECT MAX(logid) FROM exchangelog_raw) AS "Staging max ID",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
WITH anchor AS (
    SELECT MAX(log_date) AS last_fact_ts FROM fact_egisz_transactions
)
SELECT * FROM (
    VALUES
        ('raw_rows', 'Raw-строки proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM exchangelog_raw), 'строк', 'exchangelog_raw', 'Контроль поступления журнала EXCHANGELOG'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT "localUid СЭМД")::numeric FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours'), 'документов', 'egisz_messages_raw без callback-факта', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT "Ключ документа (группировка)")::numeric FROM public.v_rpt_network_errors_detail_ui), 'документов', 'EXCHANGELOG LOGSTATE=3 и журнал ошибок', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки регистрации РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM fact_egisz_transactions WHERE status = 'error'), 'строк', 'fact_egisz_transactions.status=error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
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
         'fact_egisz_transactions.log_date',
         'Проверить ELT-цикл, Airflow scheduler и доступ к Firebird')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");

-- Backfill error_code и error_type для уже загруженных фактов после смены
-- парсинга и таксономии.
--   1) Если error_code засорён XML-фрагментом (например, '<' в значении) —
--      повторно извлекаем код из msgtext исправленной egisz_xml_text.
--   2) Перекалькулируем error_type по новой плоской классификации
--      (см. egisz_error_classify); error_summary и error_json_text
--      перестраиваем заодно из freshly-rebuilt errors_json.
-- Идемпотентно: повторный прогон даёт тот же результат.
UPDATE public.fact_egisz_transactions f
SET error_code = COALESCE(
        public.egisz_xml_text(r.msgtext, 'errorCode'),
        public.egisz_xml_text(r.msgtext, 'code'),
        f.error_code
    )
FROM public.exchangelog_raw r
WHERE r.logid = f.exchangelog_log_id
  AND f.error_code IS NOT NULL
  AND f.error_code LIKE '%<%';

-- Backfill error_type только для строк, ещё НЕ классифицированных
-- (новые строки после миграции 2026-05-15 заполняются upsert-ом самой
-- egisz_transform_raw_to_facts; этот UPDATE — safety-net для re-init.)
-- Большие исторические пересчёты делаются отдельной миграцией с батчами,
-- чтобы не упереться в statement_timeout этого скрипта.
UPDATE public.fact_egisz_transactions f
SET error_type = CASE
        WHEN f.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
        ELSE public.egisz_error_classify(
            public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
        )
    END,
    error_summary = public.egisz_error_interpretation_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    ),
    error_json_text = public.egisz_error_messages_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    )
FROM public.exchangelog_raw r
WHERE r.logid = f.exchangelog_log_id
  AND f.status = 'error'
  AND f.error_type IS NULL;

-- Перекалькулировать строки, где error_type содержит переменные данные пациента
-- (имя/фамилия/отчество/пол в квадратных скобках просочились как сырой текст до добавления правил).
UPDATE public.fact_egisz_transactions f
SET error_type = CASE
        WHEN f.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
        ELSE public.egisz_error_classify(
            public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
        )
    END,
    error_summary = public.egisz_error_interpretation_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    ),
    error_json_text = public.egisz_error_messages_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    )
FROM public.exchangelog_raw r
WHERE r.logid = f.exchangelog_log_id
  AND f.status = 'error'
  AND f.error_type ~* '(Имя|Фамилия|Отчество|Пол) пациента в ЭМД \[';

-- Transfer ownership of all public-schema objects to egisz so it can run DDL independently
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

REFRESH MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui;
REFRESH MATERIALIZED VIEW public.v_stg_channel_errors_by_document;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
