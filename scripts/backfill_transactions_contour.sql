-- ============================================================================
-- Одноразовый backfill transactions.contour (контур обмена РЭМД/ИЭМК) по всей
-- истории: exchange_contour(source_action, logtext) — см. README §«Классификация
-- ошибок». Новые строки заполняет transform; здесь — только существующие.
-- Идемпотентно: заполняются лишь строки с contour IS NULL, повтор — no-op.
-- Помесячные транзакции (партиции transactions) — короткие блокировки,
-- без конфликтов с идущим transform.
-- ============================================================================

DO $$
DECLARE
    m date;
    n bigint;
BEGIN
    FOR m IN
        SELECT generate_series(date_trunc('month', MIN(log_date)),
                               date_trunc('month', MAX(log_date)),
                               interval '1 month')::date
        FROM public.transactions
    LOOP
        UPDATE public.transactions t
        SET contour = public.exchange_contour(t.source_action, r.logtext)
        FROM public.exchangelog_raw r
        WHERE t.log_date >= m
          AND t.log_date < m + interval '1 month'
          AND t.contour IS NULL
          AND r.logid = t.logid
          AND r.createdate >= t.log_date - interval '1 day'
          AND r.createdate <  t.log_date + interval '1 day'
          AND public.exchange_contour(t.source_action, r.logtext) IS NOT NULL;
        GET DIAGNOSTICS n = ROW_COUNT;
        RAISE NOTICE 'contour backfill %: % rows', m, n;
    END LOOP;
END $$;
