-- Migration: 2026-05-15 — flatten error_type taxonomy, fix XML extractor greediness.
-- Run as `postgres` so the embedded pg_terminate_backend() can clear Metabase locks.
-- Все шаги идемпотентны.
\set ON_ERROR_STOP on
SET lock_timeout = '60s';
SET statement_timeout = '60min';

-- ============================================================
-- Phase A. Functions + rules (fast, atomic)
-- ============================================================
BEGIN;

CREATE OR REPLACE FUNCTION public.egisz_xml_text(payload text, tag_name text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    safe_tag text;
    match text[];
BEGIN
    IF payload IS NULL OR tag_name IS NULL OR position('<' in payload) = 0 THEN
        RETURN NULL;
    END IF;
    safe_tag := regexp_replace(tag_name, '[^A-Za-z0-9_:-]', '', 'g');
    IF safe_tag = '' THEN
        RETURN NULL;
    END IF;
    match := regexp_match(
        payload,
        '<(?:[A-Za-z0-9_]+:)?' || safe_tag || '(?:\s[^>]*)?>([^<]*)</(?:[A-Za-z0-9_]+:)?' || safe_tag || '>',
        'is'
    );
    IF match IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN NULLIF(btrim(replace(replace(replace(match[1], E'\n', ' '), E'\r', ' '), E'\t', ' ')), '');
END;
$$;

INSERT INTO egisz_error_interpretation_rules (rule_code, priority, match_code, match_pattern, interpretation)
VALUES
    ('cert_org_validity_expired', 56, 'CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', '(?is).*', 'Срок действия сертификата организации истек'),
    ('org_ogrn_frmo_mismatch', 11, NULL, '(?is)(ОГРН|ОКПО|КПП|ИНН).*(СЭМД|ФРМО).*(не совпада|не соответств)|ОГРН МО.*не совпада|ФРМО.*(не совпада|не соответств).*организац', 'Несоответствие данных организации в ФРМО'),
    ('org_generic_fallback', 95, NULL, '(?is)(организаци|ОГРН|ФРМО|лицензи)', 'Ошибки организации')
ON CONFLICT (rule_code) DO UPDATE SET
    priority = EXCLUDED.priority,
    match_code = EXCLUDED.match_code,
    match_pattern = EXCLUDED.match_pattern,
    interpretation = EXCLUDED.interpretation,
    is_active = true,
    updated_at = now();

UPDATE egisz_error_interpretation_rules
SET is_active = false, updated_at = now()
WHERE rule_code = 'remd_async_response';

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_type(error_code text, error_message text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    c text;
    m text;
    s text;
    rule_hit text;
BEGIN
    c := upper(btrim(COALESCE(error_code, '')));
    m := btrim(COALESCE(error_message, ''));
    m := regexp_replace(m, '</?[a-zA-Z][a-zA-Z0-9:]*(?:\s[^>]*)?>?', ' ', 'g');
    m := btrim(regexp_replace(m, '\s+', ' ', 'g'));

    SELECT r.interpretation INTO rule_hit
    FROM egisz_error_interpretation_rules r
    WHERE r.is_active
      AND (r.match_code IS NULL OR r.match_code = c)
      AND m ~* r.match_pattern
    ORDER BY r.priority
    LIMIT 1;

    IF rule_hit IS NOT NULL AND rule_hit <> 'Не указан адрес пациента' THEN
        RETURN rule_hit;
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
        SELECT r.interpretation INTO rule_hit
        FROM egisz_error_interpretation_rules r
        WHERE r.is_active AND r.match_code = c
        ORDER BY r.priority
        LIMIT 1;
        IF rule_hit IS NOT NULL THEN
            RETURN rule_hit;
        END IF;
    END IF;

    s := btrim(COALESCE(public.egisz_error_interpretation_item(error_code, error_message), ''));
    IF s = '' THEN
        RETURN 'Неизвестная ошибка';
    END IF;

    s := regexp_replace(s, '</?[a-zA-Z][a-zA-Z0-9:]*(?:\s[^>]*)?>?', ' ', 'g');
    s := btrim(regexp_replace(s, '\s+', ' ', 'g'));
    IF s = '' THEN RETURN 'Неизвестная ошибка'; END IF;

    s := regexp_replace(s, '\bhttps?://[^\s<>"\)]+', '<url>', 'gi');
    s := regexp_replace(s, '\b(?:(?:gost-[a-z0-9_-]+\.infoclinica\.lan)|(?:\d{1,3}(?:\.\d{1,3}){3})(?::\d{1,5})?)\b', '<host>', 'gi');
    s := regexp_replace(s, '\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '<uuid>', 'gi');
    s := regexp_replace(s, '\b[0-9a-f]{16,}\b', '<hex>', 'gi');
    s := regexp_replace(s, '\b\d{3}-\d{3}-\d{3} \d{2}\b', '<snils>', 'gi');
    s := regexp_replace(s, '\b\d{2}\.\d{2}\.\d{4}\b', '<date>', 'g');
    s := regexp_replace(s, '№\s*[\w\-/]+', '№ <id>', 'gi');
    s := regexp_replace(s, '\b\d{5,}\b', '<n>', 'g');
    s := regexp_replace(s, '(?i)[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.', '<ФИО>', 'g');
    s := regexp_replace(s, '(?i)[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ][а-яё\-]+', '<ФИО>', 'g');
    s := regexp_replace(s, '\[[^\]]+\]', '<поле>', 'g');
    s := regexp_replace(s, '«[^»]+»|"[^"]+"', '<...>', 'g');
    s := regexp_replace(s, '\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?\b', '<dt>', 'g');
    s := regexp_replace(s, '\s+', ' ', 'g');
    RETURN left(btrim(s), 220);
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_error_classify(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    WITH normalized AS (
        SELECT CASE jsonb_typeof(COALESCE(p_errors, '[]'::jsonb))
            WHEN 'array' THEN COALESCE(p_errors, '[]'::jsonb)
            WHEN 'object' THEN jsonb_build_array(COALESCE(p_errors, '{}'::jsonb))
            ELSE '[]'::jsonb
        END AS payload
    ),
    items AS (
        SELECT
            o,
            NULLIF(btrim(public.egisz_error_interpretation_type(e->>'code', e->>'message')), '') AS t
        FROM normalized n
        CROSS JOIN LATERAL jsonb_array_elements(n.payload) WITH ORDINALITY AS x(e, o)
    ),
    first_pos AS (
        SELECT t, MIN(o) AS first_o
        FROM items
        WHERE t IS NOT NULL AND t <> 'Неизвестная ошибка'
        GROUP BY t
    ),
    aggregated AS (
        SELECT string_agg(t, ' · ' ORDER BY first_o) AS types
        FROM first_pos
    )
    SELECT COALESCE(NULLIF(types, ''), 'Неизвестная ошибка') FROM aggregated;
$$;

DROP FUNCTION IF EXISTS public.egisz_error_group_type(text, text);

COMMIT;

-- ============================================================
-- Phase B. Drop dependent views + column (own tx).
-- ============================================================
BEGIN;

DO $do$
BEGIN
    IF current_user = 'postgres' THEN
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'dwh_egisz'
          AND pid <> pg_backend_pid()
          AND application_name LIKE 'Metabase%';
    END IF;
END
$do$;

DROP MATERIALIZED VIEW IF EXISTS public.v_egisz_transactions_enriched_ui CASCADE;
ALTER TABLE public.fact_egisz_transactions DROP COLUMN IF EXISTS error_subtype;

COMMIT;

-- ============================================================
-- Phase C. Heal contaminated error_code (own tx).
-- ============================================================
BEGIN;
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
COMMIT;

-- ============================================================
-- Phase D. Backfill error_type/summary/json_text in batches.
-- ============================================================
DO $do$
DECLARE
    batch_size constant int := 5000;
    affected int;
    total_done bigint := 0;
    last_id bigint := -1;
    max_id bigint;
BEGIN
    SELECT max(exchangelog_log_id) INTO max_id FROM public.fact_egisz_transactions WHERE status = 'error';
    RAISE NOTICE 'Phase D backfill: max_id=%', max_id;

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
        RAISE NOTICE 'Phase D batch: % rows (total %), next-from-id %', affected, total_done, last_id;
        EXIT WHEN affected = 0;
        COMMIT;
    END LOOP;
    RAISE NOTICE 'Phase D done: total updated %', total_done;
END
$do$;
