-- ============================================================================
-- 40_functions_errors.sql — Error classification functions + xml_error_items + build_errors_json + semd_type_report_label
-- Loaded by db/dwh_init.sql via \i db/parts/40_functions_errors.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

-- DROP перед CREATE: sep — обязательный параметр, а CREATE OR REPLACE не убирает DEFAULT.
DROP FUNCTION IF EXISTS public.error_join_deduped(text[], text);
CREATE FUNCTION public.error_join_deduped(parts text[], sep text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    deduped text[] := ARRAY[]::text[];
    p text;
BEGIN
    IF parts IS NULL OR COALESCE(array_length(parts, 1), 0) = 0 THEN
        RETURN NULL;
    END IF;

    FOREACH p IN ARRAY parts
    LOOP
        IF p IS NULL OR btrim(p) = '' OR p = ANY (deduped) THEN
            CONTINUE;
        END IF;
        deduped := array_append(deduped, p);
    END LOOP;

    IF COALESCE(array_length(deduped, 1), 0) = 0 THEN
        RETURN NULL;
    END IF;

    RETURN array_to_string(deduped, sep);
END;
$$;

-- Ярусный матчинг: правила проверяются по возрастанию match_tier, первый ярус,
-- давший хотя бы одно совпадение, закрывает поиск (совпадения внутри яруса — все).
-- Ярус 2 (только код) матчится и на пустом message: '' ~* '(?is).*' истинно, поэтому
-- item с пустым текстом и известным кодом классифицируется правилом, а не «Код: X».
CREATE OR REPLACE FUNCTION public.error_matching_rule_labels(p_code text, p_message text)
RETURNS text[]
LANGUAGE sql
STABLE
AS $$
    WITH normalized AS (
        SELECT
            upper(btrim(COALESCE(p_code, ''))) AS c,
            btrim(COALESCE(p_message, '')) AS m
    ),
    matched AS (
        SELECT r.match_tier, r.rule_code, r.interpretation
        FROM normalized n
        JOIN public.dim_error_rules r ON r.is_active
        WHERE CASE r.match_tier
            WHEN 1 THEN n.c <> '' AND r.match_code = n.c AND n.m <> '' AND n.m ~* r.match_pattern
            WHEN 2 THEN n.c <> '' AND r.match_code = n.c AND n.m ~* r.match_pattern
            ELSE n.m <> '' AND n.m ~* r.match_pattern
        END
    ),
    -- Дедуп интерпретаций внутри выигравшего яруса: два правила яруса с одним типом
    -- (напр. общий и уточнённый schematron-паттерн) не должны давать атом дважды.
    winning AS (
        SELECT m.interpretation, min(m.rule_code) AS rule_code
        FROM matched m
        WHERE m.match_tier = (SELECT min(match_tier) FROM matched)
        GROUP BY m.interpretation
    )
    SELECT COALESCE(
        array_agg(r.interpretation ORDER BY r.rule_code),
        ARRAY[]::text[]
    )
    FROM winning r;
$$;

-- Атомарные канонические типы для одного <item> (без склейки в псевдо-тип).
-- ВСЕ возвращаемые значения каноничны (есть в dim_error_type_group): правило из
-- dim_error_rules, code-фолбэк, единый «Ошибка Schematron-валидации» либо
-- «Неизвестная ошибка». Детальный текст (включая номера schematron-правил) остаётся
-- в error_text — rid-детали в каноническом error_type раздували бы таксономию.
CREATE OR REPLACE FUNCTION public.error_item_atoms(p_code text, p_message text)
RETURNS text[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    c text;
    m text;
    rule_labels text[];
BEGIN
    c := upper(btrim(COALESCE(p_code, '')));
    m := btrim(COALESCE(p_message, ''));

    IF m = '' AND c = '' THEN
        RETURN ARRAY[]::text[];
    END IF;

    -- Правила первыми, в том числе при пустом message: code-only правила яруса 2
    -- закрывают item без текста каноническим типом вместо сырого «Код: X».
    rule_labels := public.error_matching_rule_labels(c, m);
    IF COALESCE(array_length(rule_labels, 1), 0) > 0 THEN
        RETURN rule_labels;
    END IF;

    -- Страховка на случай деактивации ярус-2 правил этих кодов. Строки обязаны
    -- совпадать с каноническими типами словаря: вариант с иным суффиксом расщеплял
    -- один логический тип на две строки витрины.
    IF c IN ('RUNTIME_ERROR', 'INTERNAL_ERROR') THEN
        RETURN ARRAY['Техническая ошибка на стороне РЭМД'];
    END IF;
    IF c IN ('CA_INACCESSIBILITY', 'CA_UNAVAILABLE') THEN
        RETURN ARRAY['Недоступен сервис проверки подписи (УЦ) на стороне РЭМД'];
    END IF;
    IF c IN ('ASYNC_RESPONSE_TIMEOUT', 'TIMEOUT') THEN
        RETURN ARRAY['Таймаут асинхронной обработки на стороне РЭМД'];
    END IF;

    -- «Код: X» — только для кодов, не покрытых ни одним правилом (health-сигнал
    -- на непокрытые коды строится по этому остатку).
    IF m = '' THEN
        RETURN ARRAY['Код: ' || c];
    END IF;

    -- Schematron без конкретного правила в dim_error_rules → единый канонический тип.
    IF m ~* 'schematron' OR m ~* 'схематрон' THEN
        RETURN ARRAY['Ошибка Schematron-валидации'];
    END IF;

    RETURN ARRAY['Неизвестная ошибка'];
END;
$$;

-- Нормализация payload ошибок (object|array|прочее → jsonb-массив). Общий вход для
-- всех построчных свёрток ниже — устраняет дублирование CTE normalized.
CREATE OR REPLACE FUNCTION public.error_payload_array(p_errors jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE jsonb_typeof(COALESCE(p_errors, '[]'::jsonb))
        WHEN 'array' THEN COALESCE(p_errors, '[]'::jsonb)
        WHEN 'object' THEN jsonb_build_array(COALESCE(p_errors, '{}'::jsonb))
        ELSE '[]'::jsonb
    END;
$$;

-- Плоская таксономия error_types: каждый <item> → атомы (error_item_atoms),
-- уникальные дедуплицируются и склеиваются через ' · ' (порядок детерминирован:
-- позиция item, затем тип — детерминизм важен для идемпотентности transform).
CREATE OR REPLACE FUNCTION public.error_classify(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        public.error_join_deduped(
            array_agg(btrim(atom) ORDER BY o, btrim(atom))
                FILTER (WHERE NULLIF(btrim(atom), '') IS NOT NULL
                          AND btrim(atom) <> 'Неизвестная ошибка'),
            ' · '
        ),
        'Неизвестная ошибка'
    )
    FROM jsonb_array_elements(public.error_payload_array(p_errors)) WITH ORDINALITY AS x(e, o)
    CROSS JOIN LATERAL unnest(public.error_item_atoms(e->>'code', e->>'message')) AS atom;
$$;

-- Исходные тексты <message> каждого <item>, уникальные через ' · ' в порядке появления.
CREATE OR REPLACE FUNCTION public.error_messages_row(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT public.error_join_deduped(
        array_agg(btrim(e->>'message') ORDER BY o),
        ' · '
    )
    FROM jsonb_array_elements(public.error_payload_array(p_errors)) WITH ORDINALITY AS x(e, o);
$$;

CREATE OR REPLACE FUNCTION public.xml_error_items(payload text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    item_xml text;
    item_code text;
    item_message text;
    result jsonb := '[]'::jsonb;
BEGIN
    IF payload IS NULL OR position('<' in payload) = 0 THEN
        RETURN result;
    END IF;

    FOR item_xml IN
        SELECT part
        FROM regexp_split_to_table(payload, '<(?:[A-Za-z0-9_]+:)?item(?:\s[^>]*)?>', 'i') AS part
    LOOP
        item_code := public.xml_text(item_xml, 'code');
        item_message := public.xml_text(item_xml, 'message');
        IF NULLIF(btrim(COALESCE(item_code, '')), '') IS NOT NULL
           OR NULLIF(btrim(COALESCE(item_message, '')), '') IS NOT NULL THEN
            result := result || jsonb_build_array(jsonb_build_object('code', item_code, 'message', item_message));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;

-- Ошибки контура ИЭМК (IHE XDS.b): RegistryResponse несёт их АТРИБУТАМИ тега
-- <rs:RegistryError errorCode="…" codeContext="…"/>, а не элементами <item>.
-- Атрибуты извлекаются независимо из attr-строки каждого тега — порядок и наличие
-- severity/location не важны. Значение в "" не может содержать сырую кавычку,
-- поэтому [^"]* безопасен; XML-сущности декодируем после захвата (&amp; — последним,
-- иначе двойное декодирование &amp;quot; → ").
CREATE OR REPLACE FUNCTION public.xml_registry_errors(payload text)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    WITH tags AS (
        SELECT t.m[1] AS attrs, t.ord
        FROM regexp_matches(
                 COALESCE(payload, ''),
                 '<(?:[A-Za-z0-9_.-]+:)?RegistryError\y([^>]*?)/?>',
                 'gi'
             ) WITH ORDINALITY AS t(m, ord)
    ),
    parsed AS (
        SELECT
            NULLIF(btrim((regexp_match(attrs, 'errorCode\s*=\s*"([^"]*)"', 'i'))[1]), '') AS code,
            NULLIF(btrim(
                replace(replace(replace(replace(replace(
                    COALESCE((regexp_match(attrs, 'codeContext\s*=\s*"([^"]*)"', 'i'))[1], ''),
                    '&quot;', '"'), '&apos;', ''''), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
            ), '') AS message,
            ord
        FROM tags
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object('code', code, 'message', message) ORDER BY ord)
            FILTER (WHERE code IS NOT NULL OR message IS NOT NULL),
        '[]'::jsonb
    )
    FROM parsed;
$$;

CREATE OR REPLACE FUNCTION public.build_errors_json(
    p_status text,
    p_error_code text,
    p_error_message text,
    p_msgtext text
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    WITH xml_items AS (
        SELECT public.xml_error_items(p_msgtext) AS items
    ),
    registry_items AS (
        -- strpos-гард: regexp-скан payload только когда RegistryError вообще присутствует.
        SELECT CASE
            WHEN strpos(COALESCE(p_msgtext, ''), 'RegistryError') > 0
            THEN public.xml_registry_errors(p_msgtext)
            ELSE '[]'::jsonb
        END AS items
    )
    SELECT CASE
        WHEN p_status <> 'error' THEN '[]'::jsonb
        WHEN jsonb_array_length(x.items) > 0 THEN x.items
        WHEN jsonb_array_length(g.items) > 0 THEN g.items
        WHEN NULLIF(btrim(COALESCE(p_error_code, '')), '') IS NOT NULL
          OR NULLIF(btrim(COALESCE(p_error_message, '')), '') IS NOT NULL
          THEN jsonb_build_array(jsonb_build_object('code', p_error_code, 'message', p_error_message))
        ELSE '[]'::jsonb
    END
    FROM xml_items x
    CROSS JOIN registry_items g;
$$;

-- Сворачивает формулировки LOGSTATE=3 в канонический тип: URL, gost-endpoint,
-- UUID и IP не должны раздувать кардинальность топов на дашбордах 02/04.
CREATE OR REPLACE FUNCTION public.network_error_type(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(
            left(
                btrim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            btrim(COALESCE(p_text, '')),
                                            'https?://[^\s<>"'',;]+',
                                            '<endpoint>',
                                            'gi'
                                        ),
                                        '(?i)gost-[0-9]+\.[a-z0-9._-]+(?::[0-9]+)?',
                                        '<gost-endpoint>'
                                    ),
                                    '(?i)(?:<urn:uuid:|<uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}>?',
                                    '<uuid>'
                                ),
                                '\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?',
                                '<ip>'
                            ),
                            '\[[^\]]{1,200}\]',
                            '[…]'
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ),
                220
            ),
            ''
        ),
        '(без текста)'
    );
$$;
