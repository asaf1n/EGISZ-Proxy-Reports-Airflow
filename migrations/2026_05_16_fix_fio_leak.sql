-- 2026-05-16 — фикс утечки нормализованных лейблов в error_type.
-- Удаляем агрессивную нормализацию ФИО/UUID в egisz_error_interpretation_type
-- и исключение 'Не указан адрес пациента' в egisz_error_interpretation_item.
\set ON_ERROR_STOP on
SET lock_timeout = '60s';
SET statement_timeout = '90min';

BEGIN;

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_item(p_code text, p_message text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    c text;
    m text;
    parts text[];
    chunk text;
    interpreted text;
    out_parts text[] := ARRAY[]::text[];
    deduped text[] := ARRAY[]::text[];
    p text;
BEGIN
    c := upper(btrim(COALESCE(p_code, '')));
    m := btrim(COALESCE(p_message, ''));

    IF m = '' THEN
        IF c <> '' THEN
            RETURN 'Код: ' || c;
        END IF;
        RETURN NULL;
    END IF;

    SELECT r.interpretation
    INTO interpreted
    FROM egisz_error_interpretation_rules r
    WHERE r.is_active
      AND (r.match_code IS NULL OR r.match_code = c)
      AND m ~* r.match_pattern
    ORDER BY r.priority
    LIMIT 1;

    IF interpreted IS NOT NULL THEN
        RETURN interpreted;
    END IF;

    IF c IN ('RUNTIME_ERROR', 'INTERNAL_ERROR') THEN
        RETURN 'Техническая ошибка на стороне РЭМД: повторите отправку позже';
    END IF;
    IF c IN ('CA_INACCESSIBILITY', 'CA_UNAVAILABLE') THEN
        RETURN 'Недоступен сервис проверки подписи/УЦ на стороне РЭМД: повторите отправку позже';
    END IF;
    IF c IN ('ASYNC_RESPONSE_TIMEOUT', 'TIMEOUT') THEN
        RETURN 'Таймаут асинхронной обработки на стороне РЭМД: повторите отправку позже';
    END IF;
    IF c IN ('DISABLED_RMIS', 'NO_RMIS', 'ATTRIBUTE_MISMATCH', 'MIS_NOT_AVAILABLE', 'REGISTRY_ITEM_NOT_FOUND', 'FILE_WAS_NOT_SENT', 'RMIS_ERROR', 'GET_DOCUMENT_FILE_ERROR') THEN
        SELECT r.interpretation
        INTO interpreted
        FROM egisz_error_interpretation_rules r
        WHERE r.is_active
          AND r.match_code = c
        ORDER BY r.priority
        LIMIT 1;
        IF interpreted IS NOT NULL THEN
            RETURN interpreted;
        END IF;
    END IF;

    IF m !~* 'schematron' AND m !~* 'схематрон' THEN
        RETURN m;
    END IF;

    parts := string_to_array(
        regexp_replace(
            m,
            'Ошибка валидации (Schematron|схематрона)\s*:\s*',
            E'\x1E',
            'gi'
        ),
        E'\x1E'
    );

    FOREACH chunk IN ARRAY parts
    LOOP
        chunk := NULLIF(btrim(chunk), '');
        IF chunk IS NULL THEN
            CONTINUE;
        END IF;
        interpreted := public.egisz_error_interpretation_schematron_chunk(chunk);
        IF interpreted IS NOT NULL THEN
            out_parts := array_append(out_parts, interpreted);
        END IF;
    END LOOP;

    IF COALESCE(array_length(out_parts, 1), 0) = 0 THEN
        RETURN COALESCE(interpreted, m);
    END IF;

    FOREACH p IN ARRAY out_parts
    LOOP
        IF p IS NULL OR p = '' OR p = ANY (deduped) THEN
            CONTINUE;
        END IF;
        deduped := array_append(deduped, p);
    END LOOP;

    RETURN array_to_string(deduped, ' - ');
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_type(error_code text, error_message text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    label text;
BEGIN
    label := btrim(COALESCE(public.egisz_error_interpretation_item(error_code, error_message), ''));
    IF label = '' THEN
        RETURN 'Неизвестная ошибка';
    END IF;
    RETURN left(label, 220);
END;
$$;

COMMIT;

-- ============================================================
-- Реклассифицировать ВСЕ error-строки с новой логикой.
-- ============================================================
DO $do$
DECLARE
    batch_size constant int := 5000;
    affected int;
    total_done bigint := 0;
    last_id bigint := -1;
BEGIN
    LOOP
        WITH target AS (
            SELECT f.exchangelog_log_id
            FROM public.fact_egisz_transactions f
            WHERE f.status = 'error'
              AND f.exchangelog_log_id > last_id
            ORDER BY f.exchangelog_log_id
            LIMIT batch_size
        ),
        compute AS (
            SELECT
                t.exchangelog_log_id,
                public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext) AS errs,
                f.error_code AS ec
            FROM target t
            JOIN public.fact_egisz_transactions f USING (exchangelog_log_id)
            JOIN public.exchangelog_raw r ON r.logid = f.exchangelog_log_id
        ),
        upd AS (
            UPDATE public.fact_egisz_transactions f
            SET error_type = CASE
                    WHEN c.ec = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
                    ELSE public.egisz_error_classify(c.errs)
                END,
                error_summary = public.egisz_error_interpretation_row(c.errs),
                error_json_text = public.egisz_error_messages_row(c.errs)
            FROM compute c
            WHERE f.exchangelog_log_id = c.exchangelog_log_id
            RETURNING f.exchangelog_log_id
        )
        SELECT COUNT(*), COALESCE(MAX(exchangelog_log_id), last_id)
        INTO affected, last_id
        FROM upd;

        total_done := total_done + affected;
        RAISE NOTICE 'reclassify batch: % rows (total %)', affected, total_done;
        EXIT WHEN affected = 0;
        COMMIT;
    END LOOP;
    RAISE NOTICE 'reclassify done: total %', total_done;
END
$do$;

REFRESH MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui;
