\encoding UTF8
\set ON_ERROR_STOP on
SET search_path = public;
SET lock_timeout = '30s';
SET statement_timeout = '60min';

CREATE TEMP TABLE IF NOT EXISTS error_item_details_cache (
    item jsonb NOT NULL,
    details jsonb NOT NULL
);
-- Длинный текст ответа может превышать предел ключа B-tree. Hash-индекс
-- сравнивает полное значение при коллизии; уникальность обеспечивает один сеанс.
CREATE INDEX IF NOT EXISTS idx_error_item_details_cache
    ON error_item_details_cache USING hash (item);
TRUNCATE pg_temp.error_item_details_cache;

-- Ранее перенесённые элементы могли получить тип из текста ответа вместе с реквизитами
-- экземпляра. Тип строится из класса и сообщения элемента, поэтому ответ заново не разбирается.
CREATE OR REPLACE FUNCTION pg_temp.details_by_class(p_details jsonb)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(jsonb_agg(e || jsonb_build_object('error_type',
               public.error_type_label(e->>'classification_type', e->>'message')) ORDER BY o),
           '[]'::jsonb)
    FROM jsonb_array_elements(p_details) WITH ORDINALITY AS x(e, o);
$$;

CREATE OR REPLACE FUNCTION pg_temp.details_need_class(p_details jsonb)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (SELECT 1 FROM jsonb_array_elements(p_details) e
                   WHERE e->>'error_type' IS DISTINCT FROM
                         public.error_type_label(e->>'classification_type', e->>'message'));
$$;

UPDATE public.documents d
SET error_details = pg_temp.details_by_class(d.error_details),
    error_types = public.error_detail_types(pg_temp.details_by_class(d.error_details))
WHERE d.error_details IS NOT NULL AND pg_temp.details_need_class(d.error_details);

UPDATE public.transactions t
SET error_details = pg_temp.details_by_class(t.error_details),
    error_type = public.error_detail_types(pg_temp.details_by_class(t.error_details))
WHERE t.error_details IS NOT NULL AND pg_temp.details_need_class(t.error_details);

-- Переносится выбранный ответ документа, а не курсор ELT. Источник сохраняет
-- принадлежность code/message одному item; разделять error_text по точке нельзя.
CREATE OR REPLACE FUNCTION pg_temp.backfill_error_details(p_batch_size integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer;
BEGIN
    WITH batch AS MATERIALIZED (
        SELECT d.dwh_id, d.result_logid, r.logid, r.createdate,
               CASE WHEN r.logstate = 3 THEN jsonb_build_array(jsonb_build_object(
                   'code', 'INTEGRATION_LOGSTATE_3',
                   'message', 'Сетевая ошибка: ' || COALESCE(NULLIF(r.logtext, ''), 'нет деталей')))
               ELSE public.build_errors_json('error', t.error_code, t.message, r.msgtext)
               END AS payload
        FROM public.documents d
        JOIN public.transactions t
          ON t.logid = d.result_logid AND t.dwh_id = d.dwh_id
         AND t.log_date = d.last_callback_at
        JOIN public.exchangelog_raw r ON r.logid = t.logid AND r.createdate = t.log_date
        WHERE d.status IN ('async_error', 'network_error')
          AND (d.error_details IS NULL OR (
              d.status = 'network_error'
              AND (d.error_details->0->>'code' IS DISTINCT FROM 'INTEGRATION_LOGSTATE_3'
                   OR d.error_types IS DISTINCT FROM 'Сетевая ошибка')))
        ORDER BY d.dwh_id
        LIMIT p_batch_size
    ),
    items AS MATERIALIZED (
        SELECT DISTINCT e.item
        FROM batch b CROSS JOIN LATERAL jsonb_array_elements(b.payload) AS e(item)
    ),
    new_items AS (
        INSERT INTO error_item_details_cache (item, details)
        SELECT i.item, public.error_details(jsonb_build_array(i.item))
        FROM items i
        WHERE NOT EXISTS (SELECT 1 FROM error_item_details_cache c WHERE c.item = i.item)
        RETURNING item, details
    ),
    all_items AS MATERIALIZED (
        SELECT c.item, c.details FROM error_item_details_cache c JOIN items i USING (item)
        UNION ALL
        SELECT item, details FROM new_items
    ),
    payloads AS MATERIALIZED (
        SELECT p.payload, COALESCE(jsonb_agg(d.detail ORDER BY e.ord, d.ord)
               FILTER (WHERE d.detail IS NOT NULL), '[]'::jsonb) AS details
        FROM (SELECT DISTINCT payload FROM batch) p
        LEFT JOIN LATERAL jsonb_array_elements(p.payload) WITH ORDINALITY AS e(item, ord) ON true
        LEFT JOIN all_items i ON i.item = e.item
        LEFT JOIN LATERAL jsonb_array_elements(i.details) WITH ORDINALITY AS d(detail, ord) ON true
        GROUP BY p.payload
    ),
    updated_transactions AS (
        UPDATE public.transactions t
        SET error_details = p.details, error_type = public.error_detail_types(p.details)
        FROM batch b JOIN payloads p USING (payload)
        WHERE t.logid = b.logid AND t.log_date = b.createdate
        RETURNING t.logid
    )
    UPDATE public.documents d
    SET error_details = p.details, error_types = public.error_detail_types(p.details)
    FROM batch b JOIN payloads p USING (payload)
    WHERE d.dwh_id = b.dwh_id AND d.result_logid = b.result_logid
      AND (d.error_details IS NULL OR (
          d.status = 'network_error'
          AND (d.error_details->0->>'code' IS DISTINCT FROM 'INTEGRATION_LOGSTATE_3'
               OR d.error_types IS DISTINCT FROM 'Сетевая ошибка')));
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

-- Каждый пакет фиксируется отдельно: повторный запуск продолжает с оставшихся NULL.
CREATE OR REPLACE PROCEDURE pg_temp.run_backfill_error_details()
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer;
BEGIN
    LOOP
        affected := pg_temp.backfill_error_details(2000);
        COMMIT;
        RAISE NOTICE 'Перенесено документов: %', affected;
        EXIT WHEN affected = 0;
    END LOOP;
END;
$$;
CALL pg_temp.run_backfill_error_details();
ANALYZE public.documents;
ANALYZE public.transactions;
SELECT count(*) AS documents_without_error_details
FROM public.documents
WHERE status IN ('async_error', 'network_error') AND error_details IS NULL;
SELECT public.refresh_report_marts();
ANALYZE public.rpt_error_breakdown;
ANALYZE public.rpt_documents_weekly;
ANALYZE public.rpt_error_breakdown_weekly;
ANALYZE public.rpt_documents_monthly;
ANALYZE public.rpt_error_breakdown_monthly;
