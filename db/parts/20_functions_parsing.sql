-- ============================================================================
-- 20_functions_parsing.sql — Parsing helpers (xml_text, normalize_message_id, clean_host, ...)
-- Source: db/dwh_init.sql, lines [417..536).
-- Loaded by db/dwh_init.sql via \i db/parts/20_functions_parsing.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

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
    -- NB: inner capture uses `[^<]*` rather than `(.*?)`. In PostgreSQL ARE the
    -- greediness of the entire regex is locked by the FIRST quantifier; the
    -- optional `:?` prefix makes that one greedy and silently turns the
    -- nominally non-greedy `.*?` greedy too, which spilled `<ns2:code>VALIDATION_ERROR</ns2:code>...`
    -- across siblings into a single match. `[^<]*` cannot cross a tag boundary,
    -- so the first matching pair is always returned.
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

CREATE OR REPLACE FUNCTION public.egisz_normalize_message_id(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(regexp_replace(trim(both '<>' from btrim(COALESCE(value, ''))), '^urn:uuid:', '', 'i'), '');
$$;

CREATE INDEX IF NOT EXISTS idx_egisz_messages_msgid_norm ON egisz_messages_raw (public.egisz_normalize_message_id(msgid));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_message_id_norm ON fact_egisz_transactions (public.egisz_normalize_message_id(message_id));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_relates_to_norm ON fact_egisz_transactions (public.egisz_normalize_message_id(relates_to_id));

CREATE OR REPLACE FUNCTION public.safe_cast_timestamptz(p_text text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF NULLIF(btrim(COALESCE(p_text, '')), '') IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN p_text::timestamptz;
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_clean_host(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        regexp_replace(
            btrim(COALESCE(p_text, '')),
            '^(?:https?://)?([^/:?#]+).*$',
            '\1',
            'i'
        ),
        ''
    );
$$;

CREATE OR REPLACE FUNCTION public.egisz_extract_jid_from_endpoint(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF((regexp_match(COALESCE(p_text, ''), 'gost-([0-9]+)', 'i'))[1], '');
$$;

CREATE OR REPLACE FUNCTION public.egisz_clean_text_value(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        btrim(
            regexp_replace(
                regexp_replace(COALESCE(p_text, ''), '<[^>]+>', ' ', 'g'),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    );
$$;

CREATE OR REPLACE FUNCTION public.egisz_normalize_semd_code(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    WITH normalized AS (
        SELECT public.egisz_clean_text_value(p_text) AS value
    )
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN regexp_match(value, '([0-9]+(?:\.[0-9]+)*)') IS NOT NULL THEN (regexp_match(value, '([0-9]+(?:\.[0-9]+)*)'))[1]
        ELSE split_part(value, ' ', 1)
    END
    FROM normalized;
$$;

CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_domen_host ON dim_licenses (public.egisz_clean_host(mo_domen));

