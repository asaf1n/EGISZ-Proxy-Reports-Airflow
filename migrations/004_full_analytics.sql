\encoding UTF8
-- Migration 004 — Полная аналитическая система отдела интеграции ЕГИСЗ.
--
-- Создаёт/пересоздаёт SQL-представления, обслуживающие 6 дашбордов Metabase:
--   A. Общая картина сервиса
--   B. Ошибки и качество
--   C. Клиники
--   D. Типы СЭМД
--   E. Архив СЭМД
--   F. Оперативный мониторинг
--
-- Все представления — обычные VIEW (не materialized). Базовый материализованный
-- слой v_egisz_transactions_enriched_ui по-прежнему рефрешится DAG-ом; новые
-- *_ui-представления читают непосредственно fact_egisz_transactions, поэтому
-- актуальны без отдельного REFRESH.
--
-- Запуск:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f migrations/004_full_analytics.sql

SET lock_timeout = '30s';
SET statement_timeout = '60min';

-- =============================================================================
-- 1. ETL run log — для часовой динамики пайплайна и мониторинга деградации
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.etl_run_log (
    run_ts          timestamptz NOT NULL DEFAULT now(),
    docs_processed  integer,
    errors_count    integer,
    duration_ms     integer,
    batch_min_id    bigint,
    batch_max_id    bigint,
    batch_min_egmid bigint,
    batch_max_egmid bigint,
    PRIMARY KEY (run_ts)
);

CREATE INDEX IF NOT EXISTS idx_etl_run_log_run_ts ON public.etl_run_log (run_ts DESC);

ALTER TABLE public.etl_run_log OWNER TO egisz;

-- =============================================================================
-- 2. Подготовка: дропаем старые представления, которые мы пересоздаём
-- =============================================================================
DROP VIEW IF EXISTS public.v_doc_registry_ui          CASCADE;
DROP VIEW IF EXISTS public.v_doc_timeline_ui          CASCADE;
DROP VIEW IF EXISTS public.v_stat_semd_types_ui       CASCADE;
DROP VIEW IF EXISTS public.v_stat_errors_ui           CASCADE;
DROP VIEW IF EXISTS public.v_stat_orgs_ui             CASCADE;
DROP VIEW IF EXISTS public.v_stat_daily_ui            CASCADE;
DROP VIEW IF EXISTS public.v_stat_hourly_ui           CASCADE;
DROP VIEW IF EXISTS public.v_docs_no_response_ui      CASCADE;
DROP VIEW IF EXISTS public.v_service_health_ui        CASCADE;
DROP VIEW IF EXISTS public.v_kpi_summary_ui           CASCADE;

-- =============================================================================
-- 3. Внутренний helper: вычисление doc_key из транзакции
--    (NULLIF + btrim + COALESCE приоритет: localUid > emdrId > doc_number > msgId)
-- =============================================================================
CREATE OR REPLACE FUNCTION public.egisz_doc_key(
    p_local_uid text,
    p_emdr_id text,
    p_doc_number text,
    p_message_id text,
    p_log_id bigint
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(btrim(p_local_uid), ''),
        NULLIF(btrim(p_emdr_id), ''),
        NULLIF(btrim(p_doc_number), ''),
        NULLIF(btrim(p_message_id), ''),
        p_log_id::text
    );
$$;

ALTER FUNCTION public.egisz_doc_key(text, text, text, text, bigint) OWNER TO egisz;

-- =============================================================================
-- 4. v_doc_registry_ui — Реестр документов (одна строка на ЭМД)
--    Используется в дашборде E «Архив СЭМД» и в drill-down из других дашбордов.
-- =============================================================================
CREATE OR REPLACE VIEW public.v_doc_registry_ui AS
WITH per_tx AS (
    SELECT
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.*
    FROM public.fact_egisz_transactions t
),
agg AS (
    SELECT
        doc_key,
        MAX(NULLIF(btrim(local_uid_semd), ''))                 AS local_uid_semd,
        MAX(NULLIF(btrim(emdr_id), ''))                        AS emdr_id,
        MAX(NULLIF(btrim(doc_number), ''))                     AS doc_number,
        MAX(public.egisz_normalize_semd_code(semd_code))       AS semd_code,
        MAX(jid)                                               AS jid,
        MIN(creation_date)                                     AS creation_date,
        MIN(log_date)                                          AS first_sent_date,
        MAX(log_date)                                          AS last_sent_date,
        COUNT(*)                                               AS attempt_count,
        bool_or(status = 'success')                            AS any_success,
        bool_or(status = 'error')                              AS any_error,
        bool_or(NULLIF(btrim(emdr_id), '') IS NOT NULL)        AS has_emdr
    FROM per_tx
    GROUP BY doc_key
),
last_tx AS (
    SELECT DISTINCT ON (doc_key)
        doc_key,
        status        AS final_status_raw,
        error_type    AS final_error_type,
        error_summary AS final_error_summary
    FROM per_tx
    ORDER BY doc_key, log_date DESC NULLS LAST, exchangelog_log_id DESC
)
SELECT
    a.doc_key                                                       AS "Идентификатор документа",
    a.local_uid_semd                                                AS "Локальный UID СЭМД",
    a.emdr_id                                                       AS "ID в РЭМД",
    a.doc_number                                                    AS "Номер документа",
    a.semd_code                                                     AS "Код СЭМД",
    COALESCE(st.name, '(нет в справочнике)')                        AS "Тип СЭМД",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || a.jid::text)    AS "Клиника",
    a.jid::text                                                     AS "JID клиники",
    a.creation_date                                                 AS "Дата создания документа",
    a.first_sent_date                                               AS "Первая отправка",
    a.last_sent_date                                                AS "Последняя отправка",
    a.attempt_count::int                                            AS "Попыток отправки",
    CASE
        WHEN a.any_success THEN 'success'
        WHEN a.any_error   THEN 'error'
        ELSE 'pending'
    END                                                              AS "Итоговый статус",
    l.final_error_type                                              AS "Тип ошибки",
    l.final_error_summary                                           AS "Описание ошибки",
    ROUND(
        EXTRACT(EPOCH FROM (COALESCE(a.last_sent_date, now()) - a.first_sent_date)) / 86400.0,
        2
    )::numeric                                                       AS "Дней в обработке",
    a.has_emdr                                                       AS "Зарегистрирован в РЭМД",
    -- технические поля для drill-down
    a.last_sent_date                                                 AS "Дата последней попытки (сорт.)",
    a.jid                                                            AS "JID (число)"
FROM agg a
LEFT JOIN last_tx l ON l.doc_key = a.doc_key
LEFT JOIN public.dim_semd_types st ON st.code = a.semd_code
LEFT JOIN public.dim_organizations o ON o.jid = a.jid;

COMMENT ON VIEW public.v_doc_registry_ui IS
'Реестр ЭМД (одна строка на документ). Архив для поиска и расследования инцидентов; используется в дашборде E «Архив СЭМД».';

-- =============================================================================
-- 5. v_doc_timeline_ui — Все попытки отправки для конкретного документа
--    Используется как drill-down: WHERE "Идентификатор документа" = {{doc_key}}.
-- =============================================================================
CREATE OR REPLACE VIEW public.v_doc_timeline_ui AS
SELECT
    public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS "Идентификатор документа",
    t.exchangelog_log_id                                         AS "LOGID",
    t.log_date                                                    AS "Время записи",
    t.status                                                      AS "Статус",
    t.message_id                                                  AS "MSGID обмена",
    t.relates_to_id                                               AS "Связанное сообщение",
    t.error_type                                                  AS "Тип ошибки",
    t.error_summary                                               AS "Сводка ошибки",
    t.error_json_text                                             AS "Исходный текст ошибки",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || t.jid::text) AS "Клиника",
    public.egisz_normalize_semd_code(t.semd_code)                 AS "Код СЭМД",
    COALESCE(st.name, '(нет в справочнике)')                      AS "Тип СЭМД",
    t.emdr_id                                                     AS "ID в РЭМД",
    t.local_uid_semd                                              AS "Локальный UID СЭМД"
FROM public.fact_egisz_transactions t
LEFT JOIN public.dim_organizations o ON o.jid = t.jid
LEFT JOIN public.dim_semd_types st ON st.code = public.egisz_normalize_semd_code(t.semd_code);

COMMENT ON VIEW public.v_doc_timeline_ui IS
'История транзакций по конкретному документу (все попытки отправки в хронологии). Drill-down из v_doc_registry_ui.';

-- =============================================================================
-- 6. v_stat_semd_types_ui — Статистика по типам СЭМД (окно 30 дней)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_stat_semd_types_ui AS
WITH period AS (
    SELECT (now() - INTERVAL '30 days') AS since_ts
),
filtered AS (
    SELECT
        public.egisz_normalize_semd_code(t.semd_code) AS code,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.*
    FROM public.fact_egisz_transactions t, period p
    WHERE t.log_date >= p.since_ts
),
docs AS (
    SELECT
        code,
        doc_key,
        COUNT(*)                              AS attempts,
        bool_or(status = 'success')           AS any_success,
        bool_or(status = 'error')             AS any_error,
        MAX(jid)                              AS jid
    FROM filtered
    GROUP BY code, doc_key
),
err_per_code AS (
    SELECT code, error_type, COUNT(*) AS cnt
    FROM filtered
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
    GROUP BY code, error_type
),
top_err AS (
    SELECT DISTINCT ON (code) code, error_type AS top_error_type, cnt AS top_error_count
    FROM err_per_code
    ORDER BY code, cnt DESC, error_type
),
agg AS (
    SELECT
        d.code,
        COUNT(*)::bigint                                                       AS unique_docs,
        SUM(d.attempts)::bigint                                                AS total_sent,
        COUNT(*) FILTER (WHERE d.any_success)::bigint                          AS success_count,
        COUNT(*) FILTER (WHERE d.any_error AND NOT d.any_success)::bigint      AS error_count,
        COUNT(*) FILTER (WHERE NOT d.any_success AND NOT d.any_error)::bigint  AS pending_count,
        AVG(d.attempts)::numeric(10,2)                                         AS avg_attempts,
        COUNT(DISTINCT d.jid)::bigint                                          AS orgs_using
    FROM docs d
    GROUP BY d.code
)
SELECT
    COALESCE(a.code, '(нет кода)')                                       AS "Код СЭМД",
    COALESCE(st.name, a.code, '(неизвестно)')                            AS "Тип СЭМД",
    a.total_sent                                                          AS "Транзакций",
    a.unique_docs                                                         AS "Уникальных документов",
    a.success_count                                                       AS "Успешных",
    a.error_count                                                         AS "С ошибкой",
    a.pending_count                                                       AS "Без ответа",
    ROUND(100.0 * a.success_count / NULLIF(a.unique_docs, 0), 1)::numeric AS "% успеха",
    a.avg_attempts                                                        AS "Среднее попыток",
    a.orgs_using                                                          AS "Клиник использует",
    t.top_error_type                                                      AS "Топ ошибки",
    t.top_error_count                                                     AS "Шт. топ ошибки",
    a.code                                                                 AS "Код СЭМД (ключ)"
FROM agg a
LEFT JOIN public.dim_semd_types st ON st.code = a.code
LEFT JOIN top_err t ON t.code = a.code
ORDER BY a.unique_docs DESC NULLS LAST;

COMMENT ON VIEW public.v_stat_semd_types_ui IS
'Статистика по типам СЭМД за последние 30 дней. Используется в дашборде D.';

-- =============================================================================
-- 7. v_stat_errors_ui — Статистика по ошибкам (паттерны, тренд)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_stat_errors_ui AS
WITH window_30d AS (
    SELECT
        t.error_type,
        t.error_summary,
        t.jid,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.log_date
    FROM public.fact_egisz_transactions t
    WHERE t.status = 'error'
      AND NULLIF(btrim(t.error_type), '') IS NOT NULL
      AND t.log_date >= now() - INTERVAL '30 days'
),
trend_7d AS (
    SELECT error_type, COUNT(*)::bigint AS cnt
    FROM public.fact_egisz_transactions
    WHERE status = 'error'
      AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '7 days'
    GROUP BY error_type
),
trend_prev_7d AS (
    SELECT error_type, COUNT(*)::bigint AS cnt
    FROM public.fact_egisz_transactions
    WHERE status = 'error'
      AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '14 days'
      AND log_date <  now() - INTERVAL '7 days'
    GROUP BY error_type
),
totals AS (
    SELECT COUNT(*)::bigint AS total_errors FROM window_30d
),
agg AS (
    SELECT
        w.error_type,
        MAX(w.error_summary)                          AS error_summary,
        COUNT(*)::bigint                              AS error_count,
        COUNT(DISTINCT w.doc_key)::bigint             AS unique_docs_affected,
        COUNT(DISTINCT w.jid)::bigint                 AS orgs_affected,
        MIN(w.log_date)                               AS first_seen,
        MAX(w.log_date)                               AS last_seen
    FROM window_30d w
    GROUP BY w.error_type
)
SELECT
    a.error_type                                                                       AS "Тип ошибки",
    COALESCE(a.error_summary, a.error_type)                                            AS "Описание ошибки",
    a.error_count                                                                       AS "Всего вхождений",
    a.unique_docs_affected                                                              AS "Уникальных документов",
    a.orgs_affected                                                                     AS "Клиник затронуто",
    ROUND(100.0 * a.error_count / NULLIF((SELECT total_errors FROM totals), 0), 1)::numeric AS "% от всех ошибок",
    a.first_seen                                                                        AS "Впервые увидели",
    a.last_seen                                                                         AS "Последнее появление",
    COALESCE(t7.cnt, 0)                                                                 AS "За последние 7 дней",
    COALESCE(tp.cnt, 0)                                                                 AS "За предыдущие 7 дней",
    COALESCE(t7.cnt, 0) - COALESCE(tp.cnt, 0)                                           AS "Дельта 7д",
    (COALESCE(t7.cnt, 0) > COALESCE(tp.cnt, 0))                                         AS "Растёт"
FROM agg a
LEFT JOIN trend_7d t7      ON t7.error_type = a.error_type
LEFT JOIN trend_prev_7d tp ON tp.error_type = a.error_type
ORDER BY a.error_count DESC;

COMMENT ON VIEW public.v_stat_errors_ui IS
'Паттерны и тренд ошибок за 30 дней с сравнением двух 7-дневных окон. Используется в дашборде B.';

-- =============================================================================
-- 8. v_stat_orgs_ui — Сводная таблица клиник со светофором
-- =============================================================================
CREATE OR REPLACE VIEW public.v_stat_orgs_ui AS
WITH per_tx AS (
    SELECT
        t.*,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key
    FROM public.fact_egisz_transactions t
    WHERE t.jid IS NOT NULL
),
agg_30d AS (
    SELECT
        jid,
        COUNT(*)::bigint                                              AS total_sent,
        COUNT(DISTINCT doc_key)::bigint                               AS unique_docs,
        COUNT(*) FILTER (WHERE status = 'success')::bigint            AS success_count,
        COUNT(*) FILTER (WHERE status = 'error')::bigint              AS error_count,
        COUNT(DISTINCT public.egisz_normalize_semd_code(semd_code))::bigint AS distinct_semd_types
    FROM per_tx
    WHERE log_date >= now() - INTERVAL '30 days'
    GROUP BY jid
),
err_24h AS (
    SELECT jid, COUNT(*)::bigint AS cnt
    FROM per_tx
    WHERE status = 'error' AND log_date >= now() - INTERVAL '24 hours'
    GROUP BY jid
),
err_7d AS (
    SELECT jid, COUNT(*)::bigint AS cnt
    FROM per_tx
    WHERE status = 'error' AND log_date >= now() - INTERVAL '7 days'
    GROUP BY jid
),
last_success AS (
    SELECT jid, MAX(log_date) AS ts
    FROM per_tx
    WHERE status = 'success'
    GROUP BY jid
),
last_error AS (
    SELECT jid, MAX(log_date) AS ts
    FROM per_tx
    WHERE status = 'error'
    GROUP BY jid
),
err_types_30d AS (
    SELECT jid, error_type, COUNT(*) AS cnt, MAX(error_summary) AS summary
    FROM per_tx
    WHERE status = 'error'
      AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '30 days'
    GROUP BY jid, error_type
),
top_err AS (
    SELECT DISTINCT ON (jid) jid, error_type AS top_error_type, summary AS top_error_summary
    FROM err_types_30d
    ORDER BY jid, cnt DESC, error_type
),
no_response AS (
    -- Документы без ответа: messages без записей в fact с теми же MSGID/document_id
    SELECT
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS jid,
        COUNT(*)::bigint AS cnt
    FROM public.egisz_messages_raw m
    LEFT JOIN public.fact_egisz_transactions f
      ON public.egisz_normalize_message_id(f.message_id) = public.egisz_normalize_message_id(m.msgid)
      OR public.egisz_normalize_message_id(f.relates_to_id) = public.egisz_normalize_message_id(m.msgid)
      OR lower(NULLIF(btrim(f.local_uid_semd), '')) = lower(NULLIF(btrim(m.document_id), ''))
    WHERE f.exchangelog_log_id IS NULL
      AND NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '') IS NOT NULL
    GROUP BY 1
),
combined AS (
    SELECT
        a.jid,
        a.total_sent,
        a.unique_docs,
        a.success_count,
        a.error_count,
        a.distinct_semd_types,
        COALESCE(e24.cnt, 0)::bigint AS errors_last_24h,
        COALESCE(e7d.cnt, 0)::bigint AS errors_last_7d,
        ls.ts                        AS last_success_ts,
        le.ts                        AS last_error_ts,
        te.top_error_type,
        te.top_error_summary,
        COALESCE(nr.cnt, 0)::bigint  AS docs_no_response
    FROM agg_30d a
    LEFT JOIN err_24h e24        ON e24.jid = a.jid
    LEFT JOIN err_7d  e7d        ON e7d.jid = a.jid
    LEFT JOIN last_success ls    ON ls.jid  = a.jid
    LEFT JOIN last_error   le    ON le.jid  = a.jid
    LEFT JOIN top_err      te    ON te.jid  = a.jid
    LEFT JOIN no_response  nr    ON nr.jid  = a.jid
),
scored AS (
    SELECT
        c.*,
        ROUND(100.0 * c.success_count / NULLIF(c.total_sent, 0), 1)::numeric AS success_rate_pct,
        ROUND(100.0 * c.error_count   / NULLIF(c.total_sent, 0), 1)::numeric AS error_rate_pct,
        CASE
            WHEN c.last_success_ts IS NULL THEN NULL
            ELSE EXTRACT(EPOCH FROM (now() - c.last_success_ts)) / 86400.0
        END::numeric(10,2) AS days_since_last_success
    FROM combined c
)
SELECT
    s.jid::text                                                            AS "JID клиники",
    s.jid                                                                  AS "JID (число)",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || s.jid::text)           AS "Клиника",
    o.inn                                                                  AS "ИНН",
    s.total_sent                                                            AS "Транзакций за 30д",
    s.unique_docs                                                           AS "Документов за 30д",
    s.success_count                                                         AS "Успешных",
    s.error_count                                                           AS "С ошибкой",
    s.success_rate_pct                                                      AS "% успеха",
    s.error_rate_pct                                                        AS "% ошибок",
    s.errors_last_24h                                                       AS "Ошибок за 24ч",
    s.errors_last_7d                                                        AS "Ошибок за 7д",
    s.distinct_semd_types                                                   AS "Разных типов СЭМД",
    s.top_error_type                                                        AS "Топ ошибки (тип)",
    s.top_error_summary                                                     AS "Топ ошибки (описание)",
    s.last_success_ts                                                       AS "Последний успех",
    s.last_error_ts                                                         AS "Последняя ошибка",
    s.days_since_last_success                                               AS "Дней с последнего успеха",
    s.docs_no_response                                                      AS "Документов без ответа",
    CASE
        WHEN COALESCE(s.error_rate_pct, 0) >= 50
          OR COALESCE(s.days_since_last_success, 999) >= 3 THEN 'CRITICAL'
        WHEN COALESCE(s.error_rate_pct, 0) >= 20
          OR s.errors_last_24h >= 10 THEN 'WARNING'
        ELSE 'OK'
    END                                                                     AS "Состояние",
    CASE
        WHEN COALESCE(s.error_rate_pct, 0) >= 50
          OR COALESCE(s.days_since_last_success, 999) >= 3 THEN 1
        WHEN COALESCE(s.error_rate_pct, 0) >= 20
          OR s.errors_last_24h >= 10 THEN 2
        ELSE 3
    END                                                                     AS "Состояние (сорт.)"
FROM scored s
LEFT JOIN public.dim_organizations o ON o.jid = s.jid
ORDER BY "Состояние (сорт.)" ASC, s.error_count DESC, s.unique_docs DESC;

COMMENT ON VIEW public.v_stat_orgs_ui IS
'Сравнительная таблица клиник со светофором (CRITICAL/WARNING/OK) и сводными метриками за 24ч/7д/30д. Дашборды A и C.';

-- =============================================================================
-- 9. v_stat_daily_ui — Дневная динамика сервиса за 90 дней
-- =============================================================================
CREATE OR REPLACE VIEW public.v_stat_daily_ui AS
SELECT
    date_trunc('day', log_date)::date                                          AS "День",
    COUNT(*)::bigint                                                            AS "Транзакций",
    COUNT(*) FILTER (WHERE status = 'success')::bigint                          AS "Успешных",
    COUNT(*) FILTER (WHERE status = 'error')::bigint                            AS "С ошибкой",
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success')
                  / NULLIF(COUNT(*), 0), 1)::numeric                            AS "% успеха",
    COUNT(DISTINCT CASE WHEN status = 'error' THEN error_type END)::bigint      AS "Уникальных типов ошибок",
    COUNT(DISTINCT jid)::bigint                                                 AS "Активных клиник",
    COUNT(DISTINCT public.egisz_doc_key(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id))::bigint
                                                                                AS "Уникальных документов"
FROM public.fact_egisz_transactions
WHERE log_date >= now() - INTERVAL '90 days'
GROUP BY 1
ORDER BY 1;

COMMENT ON VIEW public.v_stat_daily_ui IS
'Дневной тренд сервиса за 90 дней. Дашборды A, C.';

-- =============================================================================
-- 10. v_stat_hourly_ui — Часовая динамика за последние 48 часов
-- =============================================================================
CREATE OR REPLACE VIEW public.v_stat_hourly_ui AS
SELECT
    date_trunc('hour', log_date)                                                AS "Час",
    COUNT(*)::bigint                                                            AS "Транзакций",
    COUNT(*) FILTER (WHERE status = 'success')::bigint                          AS "Успешных",
    COUNT(*) FILTER (WHERE status = 'error')::bigint                            AS "С ошибкой",
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success')
                  / NULLIF(COUNT(*), 0), 1)::numeric                            AS "% успеха",
    COUNT(DISTINCT jid)::bigint                                                 AS "Активных клиник"
FROM public.fact_egisz_transactions
WHERE log_date >= now() - INTERVAL '48 hours'
GROUP BY 1
ORDER BY 1;

COMMENT ON VIEW public.v_stat_hourly_ui IS
'Почасовой тренд за 48 часов для оперативного мониторинга. Дашборд F.';

-- =============================================================================
-- 11. v_docs_no_response_ui — Документы без ответа от РЭМД (с urgency)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_docs_no_response_ui AS
WITH messages AS (
    SELECT
        m.egmid,
        m.created_at,
        m.msgid,
        m.reply_to,
        m.document_id,
        public.egisz_normalize_semd_code(
            COALESCE(public.egisz_xml_text(r.msgtext, 'kind'),
                     public.egisz_xml_text(r.msgtext, 'KIND'))
        ) AS semd_code_resolved,
        public.egisz_clean_text_value(
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'),
                     public.egisz_xml_text(r.msgtext, 'name'),
                     public.egisz_xml_text(r.msgtext, 'documentName'))
        ) AS semd_name_payload,
        public.egisz_normalize_message_id(m.msgid) AS msgid_norm,
        lower(NULLIF(btrim(m.document_id), '')) AS document_id_norm,
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS reply_to_jid,
        public.egisz_clean_host(m.reply_to) AS reply_to_host
    FROM public.egisz_messages_raw m
    LEFT JOIN LATERAL (
        SELECT er.msgtext
        FROM public.exchangelog_raw er
        WHERE er.msgid IS NOT NULL
          AND public.egisz_normalize_message_id(er.msgid) = public.egisz_normalize_message_id(m.msgid)
        ORDER BY er.logid DESC
        LIMIT 1
    ) r ON TRUE
),
fact_msgs AS (
    SELECT DISTINCT public.egisz_normalize_message_id(message_id)  AS k FROM public.fact_egisz_transactions
        WHERE NULLIF(public.egisz_normalize_message_id(message_id), '') IS NOT NULL
    UNION
    SELECT DISTINCT public.egisz_normalize_message_id(relates_to_id)
        FROM public.fact_egisz_transactions
        WHERE NULLIF(public.egisz_normalize_message_id(relates_to_id), '') IS NOT NULL
),
fact_docs AS (
    SELECT DISTINCT lower(NULLIF(btrim(local_uid_semd), '')) AS k
    FROM public.fact_egisz_transactions
    WHERE lower(NULLIF(btrim(local_uid_semd), '')) IS NOT NULL
),
core AS (
    SELECT
        m.*,
        EXTRACT(EPOCH FROM (now() - m.created_at)) / 3600.0 AS wait_hours
    FROM messages m
    LEFT JOIN fact_msgs fm ON fm.k = m.msgid_norm
    LEFT JOIN fact_docs fd ON fd.k = m.document_id_norm
    WHERE fm.k IS NULL AND fd.k IS NULL
)
SELECT
    c.created_at                                                                    AS "Отправлено",
    c.document_id                                                                   AS "Локальный UID СЭМД",
    c.document_id                                                                   AS "Идентификатор документа",
    c.semd_code_resolved                                                            AS "Код СЭМД",
    COALESCE(st.name, c.semd_name_payload, '(нет в справочнике)')                   AS "Тип СЭМД",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(c.reply_to_jid::text, '?')) AS "Клиника",
    COALESCE(c.reply_to_jid, l.jid)::text                                           AS "JID клиники",
    c.msgid                                                                          AS "MSGID обмена",
    c.egmid                                                                          AS "EGMID",
    ROUND(c.wait_hours::numeric, 1)                                                  AS "Часов ожидания",
    CASE
        WHEN c.wait_hours > 24 THEN 'CRITICAL'
        WHEN c.wait_hours >= 4  THEN 'WARNING'
        ELSE 'PENDING'
    END                                                                              AS "Срочность",
    CASE
        WHEN c.wait_hours > 24 THEN 1
        WHEN c.wait_hours >= 4  THEN 2
        ELSE 3
    END                                                                              AS "Срочность (сорт.)"
FROM core c
LEFT JOIN LATERAL (
    SELECT dl.jid
    FROM public.dim_licenses dl
    WHERE (c.reply_to_jid IS NOT NULL AND dl.jid = c.reply_to_jid)
       OR (c.reply_to_host IS NOT NULL AND public.egisz_clean_host(dl.mo_domen) = c.reply_to_host)
    ORDER BY
        CASE WHEN c.reply_to_jid IS NOT NULL AND dl.jid = c.reply_to_jid THEN 0 ELSE 1 END,
        dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN public.dim_organizations o ON o.jid = COALESCE(c.reply_to_jid, l.jid)
LEFT JOIN public.dim_semd_types st   ON st.code = c.semd_code_resolved;

COMMENT ON VIEW public.v_docs_no_response_ui IS
'Документы, отправленные в РЭМД, но без полученного ответа. Срочность: CRITICAL >24ч, WARNING 4–24ч, PENDING <4ч.';

-- =============================================================================
-- 12. v_service_health_ui — Здоровье пайплайна и сервиса (одна строка)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_service_health_ui AS
WITH last_run AS (
    SELECT MAX(run_ts) AS ts FROM public.etl_run_log
),
last_fact AS (
    SELECT MAX(log_date) AS ts FROM public.fact_egisz_transactions
),
hour_window AS (
    SELECT
        COUNT(*)::bigint                                       AS docs_total,
        COUNT(*) FILTER (WHERE status = 'error')::bigint       AS errors_total
    FROM public.fact_egisz_transactions
    WHERE log_date >= now() - INTERVAL '1 hour'
),
critical_orgs AS (
    SELECT COUNT(*)::bigint AS cnt FROM public.v_stat_orgs_ui WHERE "Состояние" = 'CRITICAL'
),
no_response_crit AS (
    SELECT COUNT(*)::bigint AS cnt FROM public.v_docs_no_response_ui WHERE "Срочность" = 'CRITICAL'
),
freshness AS (
    SELECT
        ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT ts FROM last_run), (SELECT ts FROM last_fact), now()))) / 60.0, 1)::numeric AS pipeline_minutes
)
SELECT
    f.pipeline_minutes                                                              AS "Свежесть, мин",
    CASE
        WHEN f.pipeline_minutes > 60 THEN 'DEAD'
        WHEN f.pipeline_minutes > 30 THEN 'STALE'
        WHEN f.pipeline_minutes > 10 THEN 'STALE'
        ELSE 'OK'
    END                                                                              AS "Статус пайплайна",
    h.docs_total                                                                     AS "Документов за час",
    h.errors_total                                                                   AS "Ошибок за час",
    ROUND(100.0 * h.errors_total / NULLIF(h.docs_total, 0), 1)::numeric              AS "% ошибок за час",
    co.cnt                                                                            AS "Клиник в CRITICAL",
    nr.cnt                                                                            AS "Документов без ответа >24ч",
    (SELECT ts FROM last_run)                                                         AS "Последний запуск ETL",
    (SELECT ts FROM last_fact)                                                        AS "Последний факт"
FROM freshness f
CROSS JOIN hour_window h
CROSS JOIN critical_orgs co
CROSS JOIN no_response_crit nr;

COMMENT ON VIEW public.v_service_health_ui IS
'Здоровье сервиса: свежесть пайплайна, объём за час, ошибки за час, клиники в CRITICAL, документы без ответа >24ч.';

-- =============================================================================
-- 13. v_kpi_summary_ui — Сводные KPI за 30 дней (одна строка)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_kpi_summary_ui AS
WITH org_summary AS (
    SELECT
        COUNT(*)::bigint                                          AS total_orgs,
        COUNT(*) FILTER (WHERE "Состояние" = 'CRITICAL')::bigint  AS orgs_critical,
        COUNT(*) FILTER (WHERE "Состояние" = 'WARNING')::bigint   AS orgs_warning,
        COUNT(*) FILTER (WHERE "Состояние" = 'OK')::bigint        AS orgs_ok
    FROM public.v_stat_orgs_ui
),
tx_30d AS (
    SELECT
        COUNT(*)::bigint                                                                       AS total_sent,
        COUNT(DISTINCT public.egisz_doc_key(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id))::bigint
                                                                                                AS total_docs,
        COUNT(*) FILTER (WHERE status = 'error')::bigint                                       AS total_errors,
        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*), 0), 1)::numeric AS success_rate_pct
    FROM public.fact_egisz_transactions
    WHERE log_date >= now() - INTERVAL '30 days'
),
attempts_per_doc AS (
    SELECT AVG(cnt)::numeric(10,2) AS avg_attempts
    FROM (
        SELECT public.egisz_doc_key(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id) AS dk, COUNT(*) AS cnt
        FROM public.fact_egisz_transactions
        WHERE log_date >= now() - INTERVAL '30 days'
        GROUP BY 1
    ) x
),
top_err AS (
    SELECT error_type, COUNT(*) AS cnt
    FROM public.fact_egisz_transactions
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '30 days'
    GROUP BY error_type
    ORDER BY cnt DESC
    LIMIT 1
),
top_semd AS (
    SELECT
        COALESCE(st.name, code, '(неизвестно)')          AS name,
        cnt                                              AS cnt
    FROM (
        SELECT public.egisz_normalize_semd_code(semd_code) AS code, COUNT(*) AS cnt
        FROM public.fact_egisz_transactions
        WHERE log_date >= now() - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1
    ) x
    LEFT JOIN public.dim_semd_types st ON st.code = x.code
),
top_problem_org AS (
    SELECT "Клиника" AS org_name, "% ошибок" AS error_rate
    FROM public.v_stat_orgs_ui
    WHERE "% ошибок" IS NOT NULL
    ORDER BY "% ошибок" DESC NULLS LAST
    LIMIT 1
),
no_resp AS (
    SELECT
        COUNT(*)::bigint                                                  AS total,
        COUNT(*) FILTER (WHERE "Срочность" = 'CRITICAL')::bigint          AS critical
    FROM public.v_docs_no_response_ui
)
SELECT
    o.total_orgs                                  AS "Всего клиник",
    o.orgs_critical                               AS "Клиник CRITICAL",
    o.orgs_warning                                AS "Клиник WARNING",
    o.orgs_ok                                     AS "Клиник OK",
    t.total_docs                                  AS "Документов за 30д",
    t.total_sent                                  AS "Транзакций за 30д",
    t.success_rate_pct                            AS "% успеха за 30д",
    t.total_errors                                AS "Ошибок за 30д",
    te.error_type                                 AS "Топ-ошибка (тип)",
    n.total                                       AS "Документов без ответа",
    n.critical                                    AS "Без ответа >24ч",
    a.avg_attempts                                AS "Среднее попыток на документ",
    ts.name                                       AS "Самый частый СЭМД",
    ts.cnt                                        AS "Самый частый СЭМД (шт)",
    tpo.org_name                                  AS "Самая проблемная клиника",
    tpo.error_rate                               AS "Её % ошибок"
FROM org_summary o
CROSS JOIN tx_30d t
CROSS JOIN attempts_per_doc a
LEFT JOIN top_err te        ON TRUE
LEFT JOIN top_semd ts       ON TRUE
LEFT JOIN top_problem_org tpo ON TRUE
CROSS JOIN no_resp n;

COMMENT ON VIEW public.v_kpi_summary_ui IS
'Executive KPI за 30 дней (одна строка). Используется в плитках дашборда A.';

-- =============================================================================
-- 14. Передача владения новыми объектами роли egisz
-- =============================================================================
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname IN (
              'etl_run_log',
              'v_doc_registry_ui', 'v_doc_timeline_ui',
              'v_stat_semd_types_ui', 'v_stat_errors_ui', 'v_stat_orgs_ui',
              'v_stat_daily_ui', 'v_stat_hourly_ui',
              'v_docs_no_response_ui', 'v_service_health_ui', 'v_kpi_summary_ui'
          )
    LOOP
        IF r.relkind = 'r' THEN
            EXECUTE format('ALTER TABLE public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'v' THEN
            EXECUTE format('ALTER VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW public.%I OWNER TO egisz', r.relname);
        END IF;
    END LOOP;
END;
$$;

-- =============================================================================
-- 15. Финальная проверка
-- =============================================================================
DO $$
DECLARE
    v_semd_count int;
BEGIN
    PERFORM 1 FROM public.v_doc_registry_ui       LIMIT 1;
    PERFORM 1 FROM public.v_doc_timeline_ui       LIMIT 1;
    PERFORM 1 FROM public.v_stat_semd_types_ui    LIMIT 1;
    PERFORM 1 FROM public.v_stat_errors_ui        LIMIT 1;
    PERFORM 1 FROM public.v_stat_orgs_ui          LIMIT 1;
    PERFORM 1 FROM public.v_stat_daily_ui         LIMIT 1;
    PERFORM 1 FROM public.v_stat_hourly_ui        LIMIT 1;
    PERFORM 1 FROM public.v_docs_no_response_ui   LIMIT 1;
    PERFORM 1 FROM public.v_service_health_ui     LIMIT 1;
    PERFORM 1 FROM public.v_kpi_summary_ui        LIMIT 1;
    PERFORM 1 FROM public.etl_run_log             LIMIT 0;

    SELECT COUNT(*) INTO v_semd_count FROM public.dim_semd_types;
    IF NOT (v_semd_count > 0) THEN
        RAISE EXCEPTION 'dim_semd_types пуст — загрузите справочник СЭМД из db/dwh_init.sql';
    END IF;

    RAISE NOTICE 'Migration 004 verified OK — 10 view(s) + etl_run_log ready (% SEMD codes in dim)', v_semd_count;
END $$;

\echo 'Migration 004 complete: full analytics layer (v_doc_*, v_stat_*, v_docs_no_response_ui, v_service_health_ui, v_kpi_summary_ui) + etl_run_log'
