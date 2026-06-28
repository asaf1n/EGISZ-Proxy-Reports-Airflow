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

-- error_interpretation_type() удалён как неиспользуемый: канонический тип даёт
-- error_item_atoms (для error_type), а человекочитаемую сводку — error_interpretation_item.
DROP FUNCTION IF EXISTS public.error_interpretation_type(text, text);

-- Атомарные канонические типы для одного <item> (без склейки в псевдо-тип).
-- ВСЕ возвращаемые значения каноничны (есть в dim_error_type_group): правило из
-- dim_error_rules, code-фолбэк, единый «Ошибка Schematron-валидации» либо
-- «Неизвестная ошибка». Детальная человекочитаемая расшифровка schematron-чанков
-- живёт отдельно в error_interpretation_item (поле error_summary), а не в
-- каноническом error_type — иначе rid-детали раздували бы таксономию.
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

    -- Schematron без конкретного правила в dim_error_rules → единый канонический тип.
    IF m ~* 'schematron' OR m ~* 'схематрон' THEN
        RETURN ARRAY['Ошибка Schematron-валидации'];
    END IF;

    RETURN ARRAY['Неизвестная ошибка'];
END;
$$;

-- Возвращает категорию (~10 групп) для одиночной интерпретации ошибки.
-- Единый источник истины — dim_error_type_group (тип PK → группа). Паттерн-матчинг
-- ниже остаётся подстраховкой для исторических/неканонических строк до канонизации.
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

    SELECT g.error_category INTO cat
    FROM dim_error_type_group g
    WHERE g.error_type = t
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
-- Нормализация payload ошибок (object|array|прочее → jsonb-массив). Общий вход для
-- всех построчных свёрток ниже — устраняет тройное дублирование CTE normalized.
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

-- Сводка ошибки: интерпретация каждого <item> (с детальной расшифровкой Schematron),
-- уникальные склеиваются через ' · ' в порядке первого появления.
CREATE OR REPLACE FUNCTION public.error_interpretation_row(p_errors jsonb)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT public.error_join_deduped(
        array_agg(btrim(public.error_interpretation_item(e->>'code', e->>'message')) ORDER BY o),
        ' · '
    )
    FROM jsonb_array_elements(public.error_payload_array(p_errors)) WITH ORDINALITY AS x(e, o);
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
-- Нормализация лейбла БЕЗ обращения к словарю (чистые LIKE/CASE, IMMUTABLE): сводит
-- исторические/неканонические лейблы прежнего chunk-движка к канону (адрес, ДУЛ, единый
-- schematron, «в ФРМР»/«в элементе документа»). Вынесена отдельно, чтобы горячий путь
-- (rpt_error_breakdown) канонизировал атомы set-based JOIN'ом к dim_error_type_group,
-- а не коррелированным EXISTS на каждый из сотен тысяч атомов.
CREATE OR REPLACE FUNCTION public.error_atom_normalize(p_atom text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN btrim(COALESCE(p_atom, '')) = '' THEN NULL
        WHEN btrim(p_atom) = 'Адрес пациента не заполнен или некорректен'
            THEN 'Не указан адрес пациента'
        WHEN btrim(p_atom) LIKE 'ДУЛ:%'
            THEN 'Документ, удостоверяющий личность пациента: некорректные реквизиты'
        WHEN btrim(p_atom) = 'Должность врача не соответствует данным в ФРМР'
            THEN 'Должность врача не соответствует данным ФРМР'
        WHEN btrim(p_atom) = 'Ошибка справочника НСИ в элементе документа'
            THEN 'Ошибка справочника НСИ'
        WHEN btrim(p_atom) LIKE 'Ошибка Schematron-валидации%'
            THEN 'Ошибка Schematron-валидации'
        ELSE btrim(p_atom)
    END;
$$;

-- Канонизация одного атома типа ошибки на ЧТЕНИИ: нормализация + проверка принадлежности
-- к dim_error_type_group. Чинит таксономию без переобработки архива. Для горячих витрин
-- предпочтительнее set-based JOIN (см. rpt_error_breakdown); эта скалярная версия —
-- для построчного списка документа (canonical_error_list).
CREATE OR REPLACE FUNCTION public.canonical_error_atom(p_atom text)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    WITH norm AS (SELECT public.error_atom_normalize(p_atom) AS a)
    SELECT CASE
        WHEN n.a IS NULL THEN NULL
        WHEN EXISTS (
            SELECT 1 FROM public.dim_error_type_group g WHERE g.error_type = n.a
        ) THEN n.a
        WHEN n.a LIKE 'Код: %' THEN n.a
        ELSE 'Неизвестная ошибка'
    END
    FROM norm n;
$$;

-- Канонизирует и дедуплицирует полный список типов документа (documents.error_type
-- через ' · ' / legacy ' - ') для документной витрины. Порядок — по первому появлению.
-- Один источник канона с canonical_error_atom: rpt_documents.error_types и
-- rpt_error_breakdown показывают согласованную таксономию на одних и тех же данных.
CREATE OR REPLACE FUNCTION public.canonical_error_list(p_csv text)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    WITH atoms AS (
        SELECT public.canonical_error_atom(btrim(atom)) AS a, ord
        FROM unnest(
            string_to_array(
                regexp_replace(
                    COALESCE(NULLIF(btrim(p_csv), ''), 'Неизвестная ошибка'),
                    ' - ', ' · ', 'g'
                ),
                ' · '
            )
        ) WITH ORDINALITY AS u(atom, ord)
    ),
    dedup AS (
        SELECT a, MIN(ord) AS first_ord
        FROM atoms
        WHERE a IS NOT NULL
        GROUP BY a
    )
    SELECT NULLIF(string_agg(a, ' · ' ORDER BY first_ord), '')
    FROM dedup;
$$;

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
