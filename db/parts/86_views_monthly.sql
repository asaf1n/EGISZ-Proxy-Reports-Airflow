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
-- docs_success + docs_error = docs_total (docs_waiting вне корпуса).
-- Уникальный ключ — clinic_label, а не clinic_jid: jid nullable, а label
-- NOT NULL по построению ('— · —' при пустом jid), и REFRESH CONCURRENTLY
-- требует уникальный btree без выражений.
CREATE MATERIALIZED VIEW public.rpt_documents_monthly AS
SELECT
    date_trunc('month', r.ips_date AT TIME ZONE 'Europe/Moscow')::date AS month_start,
    r.clinic_jid,
    MAX(r.clinic_name) AS clinic_name,
    r.clinic_label,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status <> 'waiting')::bigint AS docs_total,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'success')::bigint AS docs_success,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status IN ('async_error', 'network_error'))::bigint AS docs_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'async_error')::bigint AS docs_async_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'network_error')::bigint AS docs_network_error,
    COUNT(DISTINCT r.dwh_id) FILTER (WHERE r.status = 'waiting')::bigint AS docs_waiting,
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
'Месячная витрина документов: грейн (month_start = первое число месяца МСК по ips_date, клиника). Корпус SLI = docs_total (status <> waiting); docs_success + docs_error = docs_total. Обновляется refresh_report_marts() после transform.';

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
