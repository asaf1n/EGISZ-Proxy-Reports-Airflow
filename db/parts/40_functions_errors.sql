-- ============================================================================
-- 40_functions_errors.sql — Error classification functions + xml_error_items + build_errors_json + semd_type_report_label
-- Source: db/dwh_init.sql, lines [650..1055).
-- Loaded by db/dwh_init.sql via \i db/parts/40_functions_errors.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.error_interpretation_schematron_chunk(p_chunk text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    t text;
    rid text;
BEGIN
    t := btrim(COALESCE(p_chunk, ''));
    IF t = '' THEN
        RETURN NULL;
    END IF;

    rid := (regexp_match(t, 'У[0-9]+(?:[.-][0-9A-Za-z.]+)*'))[1];

    -- Адрес пациента
    IF t ~* 'address:Type' AND t ~* 'patientRole' AND t ~* 'addr' THEN
        RETURN 'Не указан адрес пациента';
    END IF;
    IF t ~* 'patientRole' AND t ~* 'addr' THEN
        RETURN 'Адрес пациента не заполнен или некорректен';
    END IF;

    -- ДУЛ (документ, удостоверяющий личность)
    IF t ~* 'identity:IssueDate' OR (t ~* 'IdentityDoc' AND t ~* 'IssueDate') THEN
        RETURN 'ДУЛ: не заполнена дата выдачи документа';
    END IF;
    IF t ~* 'IdentityCardType' THEN
        RETURN 'ДУЛ: проверьте тип документа / реквизиты удостоверения';
    END IF;

    -- Данные пациента
    IF t ~* 'patientRole' AND t ~* 'birthTime' THEN
        RETURN 'Дата рождения пациента не заполнена или некорректна';
    END IF;
    IF t ~* 'patientRole' AND t ~* '(name|given|family)' THEN
        RETURN 'ФИО пациента не заполнено или некорректно';
    END IF;
    IF t ~* 'patientRole' AND t ~* '(SNILS|СНИЛС)' THEN
        RETURN 'СНИЛС пациента не заполнен или некорректен';
    END IF;

    -- ФРМР / автор / должность
    IF t ~* '(ФРМР|FRMR)' AND t ~* '(должност|specialit|специальност)' THEN
        RETURN 'Должность врача не соответствует данным в ФРМР';
    END IF;
    IF t ~* 'assignedAuthor' AND t ~* '(SNILS|СНИЛС)' THEN
        RETURN 'СНИЛС автора (врача) не заполнен или некорректен';
    END IF;
    IF t ~* 'assignedAuthor' AND t ~* '(specialit|code.*codeSystem|специальност)' THEN
        RETURN 'Специальность врача не соответствует справочнику НСИ';
    END IF;
    IF t ~* 'assignedAuthor' AND t ~* 'representedOrganization' THEN
        RETURN 'Данные организации автора документа не заполнены';
    END IF;

    -- ГИП / пациент
    IF t ~* '(ГИП|GIP)' AND t ~* '(пациент|patient)' THEN
        RETURN 'Данные пациента не соответствуют ГИП';
    END IF;

    -- Заверитель (legalAuthenticator)
    IF t ~* 'legalAuthenticator' THEN
        RETURN 'Данные заверителя документа не заполнены или некорректны';
    END IF;

    -- Хранитель (custodian)
    IF t ~* 'custodian' THEN
        RETURN 'Данные хранителя документа не заполнены';
    END IF;

    -- Дата создания
    IF t ~* 'creationTime' THEN
        RETURN 'Дата/время создания документа не заполнены или некорректны';
    END IF;

    -- Код типа документа
    IF t ~* 'ClinicalDocument/code' THEN
        RETURN 'Код типа документа не соответствует справочнику НСИ';
    END IF;

    -- НСИ / справочник
    IF t ~* '(codeSystem|codeSystemVersion|OID.*справочник|справочник.*OID)' THEN
        RETURN 'Ошибка справочника НСИ в элементе документа';
    END IF;

    -- Fallback: группируем по номеру правила без сырого текста
    IF rid IS NOT NULL THEN
        RETURN 'Ошибка Schematron-валидации (правило ' || rid || ')';
    END IF;

    RETURN 'Ошибка Schematron-валидации (прочие требования)';
END;
$$;

CREATE OR REPLACE FUNCTION public.error_join_deduped(parts text[], sep text DEFAULT ' - ')
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

CREATE OR REPLACE FUNCTION public.error_matching_rule_labels(p_code text, p_message text)
RETURNS text[]
LANGUAGE sql
STABLE
AS $$
    WITH normalized AS (
        SELECT
            upper(btrim(COALESCE(p_code, ''))) AS c,
            btrim(COALESCE(p_message, '')) AS m
    )
    SELECT COALESCE(array_agg(r.interpretation ORDER BY r.rule_code), ARRAY[]::text[])
    FROM normalized n
    JOIN public.dim_error_rules r ON r.is_active
    WHERE n.m <> ''
      AND (r.match_code IS NULL OR r.match_code = n.c)
      AND n.m ~* r.match_pattern;
$$;

CREATE OR REPLACE FUNCTION public.error_interpretation_item(p_code text, p_message text)
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
    rule_labels text[];
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

    rule_labels := public.error_matching_rule_labels(c, m);
    IF COALESCE(array_length(rule_labels, 1), 0) > 0 THEN
        RETURN public.error_join_deduped(rule_labels, ' - ');
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
        interpreted := public.error_interpretation_schematron_chunk(chunk);
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

CREATE OR REPLACE FUNCTION public.error_interpretation_type(error_code text, error_message text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    label text;
BEGIN
    -- egisz_error_interpretation_item возвращает уже канонические лейблы
    -- (правило из dim_error_rules ИЛИ название из
    -- schematron-chunk). Нормализация ФИО/UUID/<...> здесь не нужна и
    -- даже вредна: «case-insensitive» регэксп 3-слов случайно матчит
    -- «Не указан адрес» и калечит лейбл в «<ФИО> пациента». Поэтому
    -- просто обрезаем длину и возвращаем как есть.
    label := btrim(COALESCE(public.error_interpretation_item(error_code, error_message), ''));
    IF label = '' THEN
        RETURN 'Неизвестная ошибка';
    END IF;
    RETURN left(label, 220);
END;
$$;

-- Атомарные канонические типы для одного <item> (без склейки в псевдо-тип).
CREATE OR REPLACE FUNCTION public.error_item_atoms(p_code text, p_message text)
RETURNS text[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    c text;
    m text;
    rule_labels text[];
    parts text[];
    chunk text;
    interpreted text;
    out_parts text[] := ARRAY[]::text[];
    deduped text[] := ARRAY[]::text[];
    p text;
BEGIN
    c := upper(btrim(COALESCE(p_code, '')));
    m := btrim(COALESCE(p_message, ''));

    IF m = '' AND c = '' THEN
        RETURN ARRAY[]::text[];
    END IF;

    IF m = '' THEN
        RETURN ARRAY['Код: ' || c];
    END IF;

    rule_labels := public.error_matching_rule_labels(c, m);
    IF COALESCE(array_length(rule_labels, 1), 0) > 0 THEN
        RETURN rule_labels;
    END IF;

    IF c IN ('RUNTIME_ERROR', 'INTERNAL_ERROR') THEN
        RETURN ARRAY['Техническая ошибка на стороне РЭМД: повторите отправку позже'];
    END IF;
    IF c IN ('CA_INACCESSIBILITY', 'CA_UNAVAILABLE') THEN
        RETURN ARRAY['Недоступен сервис проверки подписи/УЦ на стороне РЭМД: повторите отправку позже'];
    END IF;
    IF c IN ('ASYNC_RESPONSE_TIMEOUT', 'TIMEOUT') THEN
        RETURN ARRAY['Таймаут асинхронной обработки на стороне РЭМД: повторите отправку позже'];
    END IF;

    IF m !~* 'schematron' AND m !~* 'схематрон' THEN
        RETURN ARRAY['Неизвестная ошибка'];
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
        interpreted := public.error_interpretation_schematron_chunk(chunk);
        IF interpreted IS NOT NULL THEN
            out_parts := array_append(out_parts, interpreted);
        END IF;
    END LOOP;

    IF COALESCE(array_length(out_parts, 1), 0) = 0 THEN
        RETURN ARRAY['Неизвестная ошибка'];
    END IF;

    FOREACH p IN ARRAY out_parts
    LOOP
        IF p IS NULL OR p = '' OR p = ANY (deduped) THEN
            CONTINUE;
        END IF;
        deduped := array_append(deduped, p);
    END LOOP;

    RETURN deduped;
END;
$$;

-- Возвращает категорию (~10 групп) для одиночной интерпретации ошибки.
-- Сначала ищет точное совпадение interpretation в таблице правил (с error_category),
-- затем падает на паттерн-матчинг для schematron-chunk текстов и прочих краевых случаев.
CREATE OR REPLACE FUNCTION public.error_category(p_interpretation text)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    cat text;
    t   text;
BEGIN
    t := btrim(COALESCE(p_interpretation, ''));
    IF t = '' OR t = 'Неизвестная ошибка' THEN
        RETURN 'Прочие';
    END IF;

    SELECT r.error_category INTO cat
    FROM dim_error_rules r
    WHERE r.is_active AND r.interpretation = t
    LIMIT 1;

    IF cat IS NOT NULL THEN
        RETURN cat;
    END IF;

    -- Паттерн-матчинг для schematron-chunk текстов и прочих неканонических строк
    RETURN CASE
        WHEN t ~* '(сетевая ошибка|network error)' THEN 'Ошибки связи'
        WHEN t ~* '(техническая ошибка.*рэмд|рэмд не смог|таймаут.*рэмд|внутренн.*ошибка|невозможно обработать)' THEN 'Технические ошибки РЭМД'
        WHEN t ~* '(xsd|xml.*валид|xml.*parse|разбора xml|схематрон|schematron|хранитель|заверитель|дата.*создания документа|телефон|привязана.*рмис|организ.*автор|код.*типа документа)' THEN 'Ошибки структуры и валидации'
        WHEN t ~* '(справочник|нси.*код|нси.*версия|codeSystem)' THEN 'Ошибки справочника НСИ'
        WHEN t ~* '(подпис|сертификат|crl|ocsp|УЦ.*рэмд|рэмд.*УЦ|ЭП истёк|ЭП отозван)' THEN 'Ошибки ЭП и сертификатов'
        WHEN t ~* '(организаци.*рэмд|рмис|лицензи|фрмо|огрн|зарегистрирована в рэмд)' THEN 'Ошибки организации / ИС'
        WHEN t ~* '(файл эмд|предоставляющей ис|getDocumentFile|запись эмд не найдена)' THEN 'Ошибки получения файла ЭМД'
        WHEN t ~* '(зарегистрирован в рэмд|метаописание|идентификатор.*рэмд|вид документа не актуален|дублирующий запрос|неверный формат запроса|аннулирован|доступ.*запрещ|тип сэмд.*рэмд)' THEN 'Ошибки регистрации в РЭМД'
        WHEN t ~* '(медработник|врач.*фрмр|фрмр.*врач|должность.*врач|отчество.*врач|автор.*снилс|автор.*специальн|автор.*организ|frmr)' THEN 'Данные медработника'
        WHEN t ~* '(пациент|patient|ГИП|GIP|ДУЛ|СНИЛС|snils|рождения|имя.*пациент|получатель)' THEN 'Данные пациента'
        ELSE 'Прочие'
    END;
END;
$$;

-- Плоская таксономия error_type. Каждый <ns2:item> даёт один или несколько
-- атомарных типов (error_item_atoms); уникальные типы дедуплицируются и
-- объединяются через ' · '. Если ни один item не дал интерпретации —
-- возвращается 'Неизвестная ошибка'.
CREATE OR REPLACE FUNCTION public.error_classify(p_errors jsonb)
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
            NULLIF(btrim(atom), '') AS t
        FROM normalized n
        CROSS JOIN LATERAL jsonb_array_elements(n.payload) WITH ORDINALITY AS x(e, o)
        CROSS JOIN LATERAL unnest(
            public.error_item_atoms(e->>'code', e->>'message')
        ) AS atom
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

CREATE OR REPLACE FUNCTION public.error_interpretation_row(p_errors jsonb)
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
            NULLIF(btrim(public.error_interpretation_item(e->>'code', e->>'message')), '') AS t
        FROM normalized n
        CROSS JOIN LATERAL jsonb_array_elements(n.payload) WITH ORDINALITY AS x(e, o)
    ),
    first_pos AS (
        SELECT t, MIN(o) AS first_o
        FROM items
        WHERE t IS NOT NULL
        GROUP BY t
    )
    SELECT NULLIF(string_agg(t, ' · ' ORDER BY first_o), '')
    FROM first_pos;
$$;

CREATE OR REPLACE FUNCTION public.error_messages_row(p_errors jsonb)
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
            NULLIF(btrim(e->>'message'), '') AS t
        FROM normalized n
        CROSS JOIN LATERAL jsonb_array_elements(n.payload) WITH ORDINALITY AS x(e, o)
    ),
    first_pos AS (
        SELECT t, MIN(o) AS first_o
        FROM items
        WHERE t IS NOT NULL
        GROUP BY t
    )
    SELECT NULLIF(string_agg(t, ' · ' ORDER BY first_o), '')
    FROM first_pos;
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
    )
    SELECT CASE
        WHEN p_status <> 'error' THEN '[]'::jsonb
        WHEN jsonb_array_length(items) > 0 THEN items
        WHEN NULLIF(btrim(COALESCE(p_error_code, '')), '') IS NOT NULL
          OR NULLIF(btrim(COALESCE(p_error_message, '')), '') IS NOT NULL
          THEN jsonb_build_array(jsonb_build_object('code', p_error_code, 'message', p_error_message))
        ELSE '[]'::jsonb
    END
    FROM xml_items;
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
