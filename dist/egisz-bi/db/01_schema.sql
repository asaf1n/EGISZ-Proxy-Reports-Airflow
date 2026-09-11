-- ============================================================================
-- 01_schema.sql — bootstrap, tables, partitions, indexes, dictionaries
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: bootstrap
-- ============================================================================
-- 00_bootstrap.sql — заголовок, пояс роли, гранты.
-- Подключается из db/dwh_init.sql через \i db/01_schema.sql.
-- Идемпотентно; выполняется под ролью egisz (владелец dwh_egisz).
-- ============================================================================

\encoding UTF8
-- Инициализация DWH для отчётности EGISZ. Запускать под ролью egisz против dwh_egisz;
-- повторный прогон безопасен. Все части dwh_init выполняются под ролью egisz.
--
-- Предусловия на уровне администратора БД:
--   CREATE ROLE egisz LOGIN PASSWORD '...';
--   CREATE DATABASE dwh_egisz OWNER egisz;   -- egisz как владелец получает public-схему
--
-- Usage:
--   psql -U egisz -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql

-- Пояс отчётности задаётся здесь и только здесь. Наивное Firebird-время
-- (EXCHANGELOG.CREATEDATE, лицензии) пишется как timestamptz; без фиксированного пояса
-- сессии сутки «уехали» бы на границе. Роль вправе менять собственные параметры сессии,
-- поэтому egisz выполняет это сам.
--
-- Отчётный слой не повторяет это значение литералом: границы недель и месяцев считает
-- report_timezone(), которая читает пояс текущей сессии. Конвейер работает под ролью egisz
-- и получает пояс отсюда; Metabase выставляет пояс сессии из своей настройки
-- report-timezone. Смена пояса выполняется в этих двух точках, правки SQL не требует.
ALTER ROLE egisz SET timezone TO 'Europe/Moscow';

-- egisz — владелец dwh_egisz и public (через pg_database_owner), права уже есть; GRANT
-- идемпотентен и фиксирует контракт для среды, где владение выдано иначе.
GRANT CONNECT ON DATABASE dwh_egisz TO egisz;
GRANT USAGE, CREATE ON SCHEMA public TO egisz;

-- ---------------------------------------------------------------- section: tables
-- ============================================================================
-- 10_tables.sql — Tables, dim_semd_types seed, fact + indexes
-- Loaded by db/dwh_init.sql via \i db/01_schema.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- ============================================================================

-- Конвейер по существу ETL (выгрузка → загрузка → разбор в факты), поэтому таблица
-- состояния называется etl_state. Курсор назван по фазе и объекту, по которому считает:
-- extract_logid_cursor — позиция выгрузки в журнале шлюза (EXCHANGELOG.LOGID),
-- extract_egmid_cursor — там же по реестру подач (EGISZ_MESSAGES.EGMID),
-- transform_logid_cursor — позиция разбора в exchangelog_raw. Объекты разные, поэтому
-- отметки самостоятельные. Все курсоры продвигает только egisz_etl_dag, через GREATEST.
CREATE TABLE IF NOT EXISTS etl_state (
    pipeline text PRIMARY KEY,
    extract_logid_cursor bigint DEFAULT 0,
    transform_logid_cursor bigint DEFAULT 0,
    extract_egmid_cursor bigint DEFAULT 0,
    updated_at timestamptz DEFAULT now()
);

INSERT INTO etl_state (pipeline)
VALUES ('egisz')
ON CONFLICT (pipeline) DO NOTHING;

-- Каденция задач задаётся расписанием DAG, а не отметками в базе.
DROP TABLE IF EXISTS etl_job_runs;

-- Stored-column migrations below may drop old names (result_msgid, request_msgid,
-- message_id, relates_to_id). Existing rpt objects from previous releases depend on
-- those columns, so remove report-layer dependents before ALTER TABLE ... DROP COLUMN.
DROP VIEW IF EXISTS public.rpt_health_by_clinic CASCADE;
DROP VIEW IF EXISTS public.rpt_health_signals CASCADE;
DROP VIEW IF EXISTS public.rpt_health_message_registry_no_document CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.rpt_health_sync CASCADE;
DROP VIEW IF EXISTS public.rpt_health_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_network_errors CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_documents_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.rpt_error_breakdown_monthly CASCADE;
DO $$
DECLARE
    kind "char";
BEGIN
    SELECT c.relkind INTO kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'rpt_error_breakdown';

    IF kind = 'm' THEN
        DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE;
    ELSIF kind IS NOT NULL THEN
        DROP VIEW public.rpt_error_breakdown CASCADE;
    END IF;
END $$;
DROP VIEW IF EXISTS public.rpt_documents CASCADE;
DROP VIEW IF EXISTS public.rpt_document_versions CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_sent CASCADE;
DROP VIEW IF EXISTS public.rpt_document_file_request CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;
DROP VIEW IF EXISTS public.rpt_clinic_semd_licenses CASCADE;
DROP VIEW IF EXISTS public.rpt_clinic_semd_activity CASCADE;

-- Реестр подач шлюза (EGISZ_MESSAGES): одна строка источника по EGMID.
-- msgid — ключ подачи; document_uid — localUid РЭМД. Для ИЭМК document_uid не задан.
CREATE TABLE IF NOT EXISTS dim_message_document (
    source_egmid bigint,
    msgid text,
    document_uid text,
    reply_to text,
    created_at timestamptz,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE dim_message_document DROP CONSTRAINT IF EXISTS dim_message_document_pkey;
ALTER TABLE dim_message_document ALTER COLUMN msgid DROP NOT NULL;
ALTER TABLE dim_message_document ALTER COLUMN document_uid DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_message_document_egmid_unique
    ON dim_message_document (source_egmid);

CREATE OR REPLACE FUNCTION public.dim_message_document_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF COALESCE(NEW.reply_to, '') ~ ':9921(\D|$)' THEN
        NEW.document_uid := NULL;
    ELSE
        NEW.document_uid := lower(NULLIF(btrim(NEW.document_uid), ''));
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dim_message_document_guard ON public.dim_message_document;
CREATE TRIGGER trg_dim_message_document_guard
BEFORE INSERT OR UPDATE ON public.dim_message_document
FOR EACH ROW
EXECUTE FUNCTION public.dim_message_document_guard();

UPDATE public.dim_message_document
SET document_uid = NULL
WHERE NULLIF(btrim(document_uid), '') IS NOT NULL
  AND COALESCE(reply_to, '') ~ ':9921(\D|$)';

CREATE TABLE IF NOT EXISTS exchangelog_raw (
    logid bigint PRIMARY KEY,
    logdate timestamptz,
    createdate timestamptz,
    msgid text,
    logstate integer,
    logtext text,
    msgtext text,
    uri text,
    loaded_at timestamptz DEFAULT now()
);

ALTER TABLE exchangelog_raw ADD COLUMN IF NOT EXISTS createdate timestamptz;
-- URI вызова задаёт подсистему ЕГИСЗ: /emdr/callback — РЭМД, /ips/callback — ИЭМК.
ALTER TABLE exchangelog_raw ADD COLUMN IF NOT EXISTS uri text;

-- Маркер попытки парсинга (по LOGID). parse_targets в transform_raw_to_facts должен
-- отличать «ещё не парсили» от «парсили, но payload без реквизитов»: строки без
-- msgid/localUid/emdrId/getDocumentFile не проходят фильтр вставки в transactions,
-- и анти-джойн по transactions.xml_parsed_at перепарсивал их каждым полножурнальным
-- lookback'ом reconcile (~65 тыс. строк, ~5,9 мс/строка ≈ 6,4 мин на окно).
CREATE TABLE IF NOT EXISTS exchangelog_parse_attempts (
    logid bigint PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS documents (
    dwh_id text PRIMARY KEY,
    local_uid text,
    emdr_id text,
    semd_code text,
    status text,
    msgid text,
    relates_to_msgid text,
    result_logid bigint,
    document_created_at timestamptz,
    registered_at timestamptz,
    error_types text,
    error_text text,
    patient_hash text,
    doctor_hash text,
    request_logid bigint,
    first_sent_at timestamptz,
    first_callback_at timestamptz,
    last_callback_at timestamptz,
    last_status text,
    jid bigint,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE documents ADD COLUMN IF NOT EXISTS local_uid text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS emdr_id text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS semd_code text;
ALTER TABLE documents ALTER COLUMN semd_code DROP NOT NULL;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS status_category text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS msgid text;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'documents'
                 AND column_name = 'result_msgid') THEN
        EXECUTE 'UPDATE public.documents SET msgid = COALESCE(msgid, result_msgid) WHERE msgid IS NULL';
    END IF;
END $$;
ALTER TABLE documents DROP COLUMN IF EXISTS result_msgid;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS relates_to_msgid text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS result_logid bigint;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS document_created_at timestamptz;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS registered_at timestamptz;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS error_types text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS error_text text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS patient_hash text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doctor_hash text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS request_logid bigint;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS first_sent_at timestamptz;
-- Отметка первого ответа ЕГИСЗ. Выход документа из очереди обработки определяет именно
-- она: last_callback_at несёт последний ответ и перезаписывается каждым повторным
-- коллбэком, поэтому документ, отвеченный за секунды, числился бы в очереди до последнего
-- повтора.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS first_callback_at timestamptz;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_callback_at timestamptz;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_status text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS jid bigint;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS org_oid text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS jid_resolve_method text;
-- Число подач документа в ЕГИСЗ (строк реестра dim_message_document на этот localUid).
-- Повторная подача не меняет localUid, поэтому счётчик живёт на экземпляре документа.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS attempt_count integer;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();
-- status_category удалён: полностью выводится из status, downstream-потребителей нет.
ALTER TABLE documents DROP COLUMN IF EXISTS status_category;

-- Слой версий/логического документа.
-- dwh_id (PK) — ЭКЗЕМПЛЯР/ВЕРСИЯ (localUid), меняется при каждой правке/ре-выгрузке.
-- Логический документ собирается по (clinic jid + тип СЭМД + documentNumber=PROTOCOLID).
-- Проверено на базе: пара (jid, doc_number) всегда несёт ровно ОДИН semd_code (это ключ
-- ДОКУМЕНТА, не случая), max 7 версий на группу; CDA setId в журнал не попадает и источником
-- не отдаётся — не используем.
--   doc_number                 — PROTOCOLID (номер протокола/ИБ в МИС), ключ группировки версий
--   document_group_id          — 'd:'||jid||'|'||semd||'|'||docnum (группа) либо dwh_id (singleton)
--   document_group_confidence  — провенанс группы: 'doc_number' | 'singleton'
--   semd_version_number        — порядковый номер версии в группе
--   superseded_by_dwh_id /     — цепочка версий между экземплярами
--     supersedes_dwh_id
--   is_current_version         — текущая (последняя) версия своей группы
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_number text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS document_group_id text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS document_group_confidence text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS semd_version_number integer;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS superseded_by_dwh_id text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS supersedes_dwh_id text;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS is_current_version boolean;

CREATE TABLE IF NOT EXISTS dim_organizations (
    jid bigint PRIMARY KEY,
    name text,
    inn text,
    address text,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE dim_organizations ADD COLUMN IF NOT EXISTS fir_oid text;
ALTER TABLE dim_organizations ADD COLUMN IF NOT EXISTS nsi_name text;

COMMENT ON COLUMN dim_organizations.name IS
'Наименование организации из CASH/JPERSONS.';
COMMENT ON COLUMN dim_organizations.fir_oid IS
'OID медицинской организации. Ведущий источник — справочник ФРМО (НСИ 1461); синхронизация справочников добирает значение из JPERSONS.FIR_OID только там, где OID ещё не известен, и никогда не затирает его пустым.';
COMMENT ON COLUMN dim_organizations.nsi_name IS
'Наименование медицинской организации из НСИ для аудита сопоставления с CASH.';

CREATE TABLE IF NOT EXISTS dim_nsi_organization (
    nsi_id bigint PRIMARY KEY,
    oid text UNIQUE,
    source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.11.1461',
    source_version text NOT NULL,
    name_full text,
    name_short text,
    medical_subject_id integer,
    medical_subject_name text,
    inn text,
    kpp text,
    ogrn text,
    region_id integer,
    region_name text,
    organization_type integer,
    mo_dept_id integer,
    mo_dept_name text,
    delete_date date,
    delete_reason text,
    create_date date,
    modify_date date,
    mo_level text,
    mo_agency_kind_id integer,
    mo_agency_kind text,
    post_index text,
    aoid_area text,
    aoid_street text,
    houseid text,
    addr_region_id integer,
    addr_region_name text,
    area_name text,
    prefix_area text,
    street_name text,
    prefix_street text,
    house text,
    building text,
    struct text,
    latitude numeric,
    longitude numeric,
    founder text,
    profile_agency_kind_id integer,
    profile_agency_kind text,
    cadastral_number text,
    old_oid text,
    parent_id text,
    raw_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz DEFAULT now()
);

COMMENT ON TABLE dim_nsi_organization IS
'НСИ 1.2.643.5.1.13.13.11.1461 «ФРМО. Справочник медицинских организаций»; полный снимок версии источника.';
COMMENT ON COLUMN dim_nsi_organization.parent_id IS
'parentId из НСИ: OID родительской записи, а не внутренний nsi_id.';

CREATE TABLE IF NOT EXISTS dim_document_status (
    code text PRIMARY KEY,
    label text NOT NULL,
    sort_order smallint NOT NULL,
    is_final boolean NOT NULL
);

INSERT INTO dim_document_status (code, label, sort_order, is_final)
VALUES
    ('success', 'Успешно зарегистрирован', 1, true),
    ('async_error', 'Ошибка асинхронного ответа РЭМД', 2, true),
    ('network_error', 'Ошибка связи', 3, true),
    ('sent', 'Отправлено', 4, false)
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    sort_order = EXCLUDED.sort_order,
    is_final = EXCLUDED.is_final;

DELETE FROM dim_document_status
WHERE code NOT IN ('success', 'async_error', 'network_error', 'sent');

-- Ступени возраста обработки для нефинального статуса 'sent'. Ступень ищется как первая
-- по sort_order с max_age_minutes >= возраста; терминальная ступень (max_age_minutes IS NULL)
-- замыкает лестницу. Точка перехода в состояние «Без ответа» задаётся is_no_response —
-- ужесточение порога выполняется UPDATE по справочнику, без правки представлений.
CREATE TABLE IF NOT EXISTS dim_pending_segments (
    code text PRIMARY KEY,
    label text NOT NULL,
    max_age_minutes integer,
    sort_order smallint NOT NULL,
    is_no_response boolean NOT NULL
);

INSERT INTO dim_pending_segments (code, label, max_age_minutes, sort_order, is_no_response)
VALUES
    ('p_5m', 'до 5 минут', 5, 1, false),
    ('p_1h', 'до 1 часа', 60, 2, false),
    ('p_6h', 'до 6 часов', 360, 3, false),
    ('p_12h', 'до 12 часов', 720, 4, false),
    ('p_24h', 'до 24 часов', 1440, 5, false),
    ('p_72h', 'до 3 суток', 4320, 6, false),
    ('p_7d', 'до 7 суток', 10080, 7, false),
    -- Граница утилизации в «Без ответа» — последняя нетерминальная ступень. 15 суток:
    -- наблюдаемый максимум срока ответа 10.1 суток, позже 15 суток не приходило ни одного.
    -- Замер цензурирован глубиной окна приёма, поэтому порог взят с запасом, но вдвое ниже
    -- самого окна (EGISZ_EXTRACT_DEPTH_DAYS = 30): совпади он с глубиной хранения, документ
    -- не успевал бы стать терминальным, пока лежит в DWH.
    ('p_15d', 'до 15 суток', 21600, 8, false),
    ('p_over', 'свыше 15 суток', NULL, 9, true)
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    max_age_minutes = EXCLUDED.max_age_minutes,
    sort_order = EXCLUDED.sort_order,
    is_no_response = EXCLUDED.is_no_response;

DELETE FROM dim_pending_segments
WHERE code NOT IN ('p_5m', 'p_1h', 'p_6h', 'p_12h', 'p_24h', 'p_72h', 'p_7d', 'p_15d', 'p_over');

CREATE TABLE IF NOT EXISTS dim_sent_state (
    code text PRIMARY KEY,
    label text NOT NULL,
    sort_order smallint NOT NULL
);

-- Код состояния остаётся no_response — это таксономия модели состояний отправки.
-- Наименование говорит и об исходе, и о судьбе документа: ответа не будет, запись
-- выводится из аналитики и подлежит очистке.
INSERT INTO dim_sent_state (code, label, sort_order)
VALUES
    ('pending', 'В обработке', 1),
    ('no_response', 'Ответ не получен (утилизирован)', 2)
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    sort_order = EXCLUDED.sort_order;

DELETE FROM dim_sent_state WHERE code NOT IN ('pending', 'no_response');

CREATE TABLE IF NOT EXISTS dim_licenses (
    id bigint PRIMARY KEY,
    service_type integer,
    jid bigint,
    mo_uid text,
    mo_domen text,
    bdate date,
    fdate date,
    kind text,
    modifydate timestamptz,
    updated_at timestamptz DEFAULT now()
);

-- Parsed MSGTEXT и метаданные строки журнала хранятся в transactions (xml_* / source_*).
-- grain transaction: PK (logid, log_date).

-- ФНСИ выгружает НСИ 1520 с переставленными полями: GIT_LINK несёт OID руководства по реализации,
-- а IMPLEMENTATION_GUIDE — ссылку на портал ЕГИСЗ. Колонка названа по содержанию, иначе соединение
-- с реестром руководств выглядит соединением по ссылке и «исправляется» обратно первым же читателем.
DO $$
BEGIN
    IF to_regclass('public.dim_semd_types') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'dim_semd_types'
             AND column_name = 'git_link'
       )
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'dim_semd_types'
             AND column_name = 'ig_oid'
       )
    THEN
        EXECUTE 'ALTER TABLE public.dim_semd_types RENAME COLUMN git_link TO ig_oid';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS dim_semd_types (
    code text PRIMARY KEY,
    type_code text,
    name text NOT NULL,
    level text,
    format_code text,
    start_date date,
    end_date date,
    implementation_guide text,
    ig_oid text,
    oid text,
    version text,
    updated_at timestamptz DEFAULT now()
);

INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, ig_oid)
VALUES
    ('4', '8', 'Медицинская справка о допуске к управлению транспортными средствами (CDA) Редакция 1', '3', '2', DATE '2018-10-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/2927', '1.2.643.5.1.13.13.15.43.1'),
    ('5', '6', 'Протокол инструментального исследования (PDF/A-1)', '0', '1', DATE '2018-07-04', DATE '2024-01-01', NULL, NULL),
    ('6', '5', 'Протокол консультации (PDF/A-1)', '0', '1', DATE '2018-07-04', DATE '2024-01-01', NULL, NULL),
    ('7', '7', 'Протокол лабораторного исследования (PDF/A-1)', '0', '1', DATE '2018-07-04', DATE '2024-01-01', NULL, NULL),
    ('8', '36', 'Протокол телемедицинской консультации (PDF/A-1)', '0', '1', DATE '2018-08-13', DATE '2024-01-01', NULL, NULL),
    ('13', '13', 'Медицинское свидетельство о смерти (CDA) Редакция 2', '3', '2', DATE '2018-10-16', DATE '2021-08-31', 'https://portal.egisz.rosminzdrav.ru/materials/2931', '1.2.643.5.1.13.13.15.35.2'),
    ('15', '6', 'Протокол инструментального исследования (CDA) Редакция 1', '3', '2', DATE '2019-02-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3291', '1.2.643.5.1.13.13.15.17.1'),
    ('16', '5', 'Протокол консультации (CDA) Редакция 2', '3', '2', DATE '2019-02-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2937', '1.2.643.5.1.13.13.15.13.2'),
    ('17', '7', 'Протокол лабораторного исследования (CDA) Редакция 2', '3', '2', DATE '2019-02-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2939', '1.2.643.5.1.13.13.15.18.2'),
    ('33', '33', 'Медицинское свидетельство о рождении (CDA) Редакция 3', '3', '2', DATE '2018-10-16', DATE '2022-02-16', 'https://portal.egisz.rosminzdrav.ru/materials/2929', '1.2.643.5.1.13.13.15.39.3'),
    ('34', '34', 'Направление на медико-социальную экспертизу медицинской организацией (CDA) Редакция 4', '3', '2', DATE '2018-10-16', DATE '2022-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/2947', '1.2.643.5.1.13.13.15.4.4'),
    ('35', '35', 'Сведения о результатах проведенной медико-социальной экспертизы (CDA) Редакция 2', '3', '2', DATE '2018-10-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3477', '1.2.643.5.1.13.13.15.5.2'),
    ('37', '37', 'Льготный рецепт на лекарственный препарат и специальное питание (CDA) Редакция 1', '3', '2', DATE '2020-11-25', DATE '2021-03-15', 'https://portal.egisz.rosminzdrav.ru/materials/3741', '1.2.643.5.1.13.13.15.1.1'),
    ('38', '38', 'Отпуск по рецепту на лекарственный препарат и специальное питание (CDA) Редакция 1', '3', '2', DATE '2020-11-25', DATE '2021-03-10', 'https://portal.egisz.rosminzdrav.ru/materials/3739', '1.2.643.5.1.13.13.15.2.1'),
    ('40', '36', 'Протокол телемедицинской консультации (CDA) Редакция 1', '3', '2', DATE '2019-11-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3479', '1.2.643.5.1.13.13.15.15.1'),
    ('41', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 3', '3', '2', DATE '2020-09-14', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2943', '1.2.643.5.1.13.13.15.25.3'),
    ('42', '2', 'Эпикриз по законченному случаю амбулаторный (CDA) Редакция 3', '3', '2', DATE '2020-09-14', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2945', '1.2.643.5.1.13.13.15.26.3'),
    ('43', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 2', '3', '2', DATE '2020-09-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/2933', '1.2.643.5.1.13.13.15.31.2'),
    ('44', '10', 'Выписной эпикриз из родильного дома (CDA) Редакция 2', '3', '2', DATE '2020-09-14', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2925', '1.2.643.5.1.13.13.15.27.2'),
    ('45', '11', 'Протокол гемотрансфузии (CDA) Редакция 2', '3', '2', DATE '2020-09-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/2935', '1.2.643.5.1.13.13.15.24.2'),
    ('46', '12', 'Протокол прижизненного патологоанатомического исследования (CDA) Редакция 1', '3', '2', DATE '2020-09-14', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2941', '1.2.643.5.1.13.13.15.21.1'),
    ('47', '14', 'Медицинское свидетельство о перинатальной смерти (CDA) Редакция 1', '3', '2', DATE '2020-09-08', DATE '2021-08-31', 'https://portal.egisz.rosminzdrav.ru/materials/3605', '1.2.643.5.1.13.13.15.37.1'),
    ('50', '39', 'Медицинская справка (врачебное профессионально-консультативное заключение) (CDA) Редакция 1', '3', '2', DATE '2020-12-10', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3757', '1.2.643.5.1.13.13.15.45.1'),
    ('51', '40', 'Карта профилактического медицинского осмотра несовершеннолетнего (PDF/A-1)', '0', '1', DATE '2020-10-17', NULL, NULL, NULL),
    ('52', '41', 'Медицинская карта пациента, получающего медицинскую помощь в амбулаторных условиях (PDF/A-1)', '0', '1', DATE '2020-10-17', NULL, NULL, NULL),
    ('53', '42', 'Контрольная карта диспансерного наблюдения (PDF/A-1)', '0', '1', DATE '2020-10-17', NULL, NULL, NULL),
    ('54', '44', 'Контрольная карта диспансеризации (профилактических медицинских осмотров) (PDF/A-1)', '0', '1', DATE '2020-10-17', NULL, NULL, NULL),
    ('55', '45', 'Медицинское заключение об отсутствии медицинских противопоказаний к владению оружием (PDF/A-1)', '0', '1', DATE '2020-10-17', DATE '2022-01-27', NULL, NULL),
    ('56', '46', 'Медицинское заключение об отсутствии в организме человека наркотических средств, психотропных веществ и их метаболитов (PDF/A-1)', '0', '1', DATE '2020-10-17', DATE '2022-01-27', NULL, NULL),
    ('57', '13', 'Медицинское свидетельство о смерти (CDA) Редакция 4', '3', '2', DATE '2020-12-15', DATE '2021-08-31', 'https://portal.egisz.rosminzdrav.ru/materials/3753', '1.2.643.5.1.13.13.15.35.4'),
    ('58', '13', 'Медицинское свидетельство о смерти (CDA) Редакция 5', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3815', '1.2.643.5.1.13.13.15.35.5'),
    ('59', '14', 'Медицинское свидетельство о перинатальной смерти (CDA) Редакция 2', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3817', '1.2.643.5.1.13.13.15.37.2'),
    ('60', '38', 'Отпуск по рецепту на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 2', '3', '2', DATE '2021-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3819', '1.2.643.5.1.13.13.15.2.2'),
    ('61', '37', 'Льготный рецепт на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 2', '3', '2', DATE '2021-03-15', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3821', '1.2.643.5.1.13.13.15.1.2'),
    ('62', '86', 'Рецепт на лекарственный препарат (CDA) Редакция 1', '3', '2', DATE '2021-03-15', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3823', '1.2.643.5.1.13.13.15.3.1'),
    ('63', '45', 'Медицинское заключение об отсутствии медицинских противопоказаний к владению оружием (CDA) Редакция 1', '3', '2', DATE '2021-04-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3827', '1.2.643.5.1.13.13.15.41.1'),
    ('64', '46', 'Медицинское заключение об отсутствии в организме человека наркотических средств, психотропных веществ и их метаболитов (CDA) Редакция 1', '3', '2', DATE '2021-04-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3829', '1.2.643.5.1.13.13.15.42.1'),
    ('65', '47', 'Справка для получения путевки на санаторно-курортное лечение (CDA) Редакция 1', '3', '2', DATE '2021-04-12', DATE '2025-02-01', 'https://portal.egisz.rosminzdrav.ru/materials/3831', '1.2.643.5.1.13.13.15.8.1'),
    ('66', '108', 'Протокол хирургической операции (PDF/A-1)', '0', '1', DATE '2021-04-06', NULL, NULL, NULL),
    ('67', '109', 'Протокол медицинской манипуляции (PDF/A1)', '0', '1', DATE '2021-04-06', DATE '2024-01-01', NULL, NULL),
    ('68', '5', 'Протокол консультации (CDA) Редакция 3', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3845', '1.2.643.5.1.13.13.15.13.3'),
    ('69', '11', 'Протокол гемотрансфузии (CDA) Редакция 3', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3847', '1.2.643.5.1.13.13.15.24.3'),
    ('70', '89', 'Справка о результатах химико-токсикологических исследований (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3837', '1.2.643.5.1.13.13.15.19.1'),
    ('71', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2022-12-07', 'https://portal.egisz.rosminzdrav.ru/materials/3839', '1.2.643.5.1.13.13.15.54.1'),
    ('72', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3841', '1.2.643.5.1.13.13.15.56.1'),
    ('73', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3843', '1.2.643.5.1.13.13.15.57.1'),
    ('74', '12', 'Протокол прижизненного патологоанатомического исследования (CDA) Редакция 2', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3833', '1.2.643.5.1.13.13.15.21.2'),
    ('75', '7', 'Протокол лабораторного исследования (CDA) Редакция 4', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3835', '1.2.643.5.1.13.13.15.18.4'),
    ('76', '33', 'Медицинское свидетельство о рождении (CDA) Редакция 4', '3', '2', DATE '2021-04-26', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3849', '1.2.643.5.1.13.13.15.39.4'),
    ('77', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 4', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3903', '1.2.643.5.1.13.13.15.25.4'),
    ('78', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2025-02-01', 'https://portal.egisz.rosminzdrav.ru/materials/3905', '1.2.643.5.1.13.13.15.68.1'),
    ('79', '142', 'Справка о прохождении медицинского освидетельствования в психоневрологическом диспансере (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3907', '1.2.643.5.1.13.13.15.59.1'),
    ('80', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3909', '1.2.643.5.1.13.13.15.56.2'),
    ('81', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3909', '1.2.643.5.1.13.13.15.58.2'),
    ('82', '69', 'Медицинское заключение о принадлежности несовершеннолетнего к медицинской группе для занятий физической культурой (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.52.2'),
    ('83', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.54.2'),
    ('84', '91', 'Медицинская справка в бассейн (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.53.2'),
    ('85', '57', 'Направление на консультацию и во вспомогательные кабинеты (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3913', '1.2.643.5.1.13.13.15.32.1'),
    ('86', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2025-02-01', 'https://portal.egisz.rosminzdrav.ru/materials/3915', '1.2.643.5.1.13.13.15.67.1'),
    ('87', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3917', '1.2.643.5.1.13.13.15.44.1'),
    ('88', '56', 'Медицинская справка (для выезжающего за границу) (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3919', '1.2.643.5.1.13.13.15.48.1'),
    ('89', '10', 'Выписной эпикриз из родильного дома (CDA) Редакция 3', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3921', '1.2.643.5.1.13.13.15.27.3'),
    ('90', '6', 'Протокол инструментального исследования (CDA) Редакция 2', '3', '2', DATE '2021-06-30', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3923', '1.2.643.5.1.13.13.15.17.2'),
    ('91', '74', 'Карта вызова скорой медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-06-30', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3925', '1.2.643.5.1.13.13.15.72.1'),
    ('92', '2', 'Эпикриз по законченному случаю амбулаторный (CDA) Редакция 4', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3927', '1.2.643.5.1.13.13.15.26.4'),
    ('93', '121', 'Протокол цитологического исследования (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3929', '1.2.643.5.1.13.13.15.20.1'),
    ('94', '85', 'Протокол консультации в рамках диспансерного наблюдения (CDA) Редакция 3', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3845', '1.2.643.5.1.13.13.15.14.3'),
    ('95', '91', 'Медицинская справка в бассейн (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3839', '1.2.643.5.1.13.13.15.53.1'),
    ('96', '141', 'Сведения о результатах диспансеризации или профилактического медицинского осмотра (CDA) Редакция 1', '3', '2', DATE '2021-07-08', DATE '2023-09-01', 'https://portal.egisz.rosminzdrav.ru/materials/3901', '1.2.643.5.1.13.13.15.74.1'),
    ('97', '241', 'Направление на госпитализацию для оказания высокотехнологичной медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-09-28', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3973', '1.2.643.5.1.13.13.15.33.1'),
    ('98', '346', 'Направление на госпитализацию для оказания специализированной медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-09-28', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3973', '1.2.643.5.1.13.13.15.34.1'),
    ('99', '347', 'Выписка из протокола врачебной комиссии (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3993', '1.2.643.5.1.13.13.15.75.1'),
    ('100', '52', 'Справка об оплате медицинских услуг для предоставления в налоговые органы Российской Федерации (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3991', '1.2.643.5.1.13.13.15.69.1'),
    ('101', '73', 'Медицинское заключение о допуске к выполнению работ на высоте, верхолазных работ, работ, связанных с подъемом на высоту, а также по обслуживанию подъемных сооружений (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3989', '1.2.643.5.1.13.13.15.55.1'),
    ('102', '344', 'Справка об отказе в направлении на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2022-12-27', 'https://portal.egisz.rosminzdrav.ru/materials/3987', '1.2.643.5.1.13.13.15.6.1'),
    ('103', '51', 'Медицинское заключение по результатам предварительного (периодического) медицинского осмотра (обследования) (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3985', '1.2.643.5.1.13.13.15.47.1'),
    ('104', '59', 'Экстренное извещение об инфекционном заболевании, пищевом, остром профессиональном отравлении, необычной реакции на прививку (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3983', '1.2.643.5.1.13.13.15.70.1'),
    ('105', '53', 'Сертификат профилактических прививок (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3981', '1.2.643.5.1.13.13.15.46.1'),
    ('106', '343', 'Справка о постановке на учет по беременности (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3979', '1.2.643.5.1.13.13.15.60.1'),
    ('107', '66', 'Справка донору об освобождении от работы в день кровосдачи и предоставлении ему дополнительного дня отдыха (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/3977', '1.2.643.5.1.13.13.15.49.1'),
    ('108', '352', 'Уведомление о причинах возврата направления на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4017', '1.2.643.5.1.13.13.15.7.1'),
    ('109', '34', 'Направление на медико-социальную экспертизу (CDA) Редакция 5', '3', '2', DATE '2022-01-01', DATE '2023-03-15', 'https://portal.egisz.rosminzdrav.ru/materials/4011', '1.2.643.5.1.13.13.15.4.5'),
    ('110', '6', 'Протокол инструментального исследования (CDA) Редакция 3', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4021', '1.2.643.5.1.13.13.15.17.3'),
    ('111', '85', 'Протокол консультации в рамках диспансерного наблюдения (CDA) Редакция 4', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4023', '1.2.643.5.1.13.13.15.14.4'),
    ('112', '37', 'Льготный рецепт на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 3', '3', '2', DATE '2021-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4025', '1.2.643.5.1.13.13.15.1.3'),
    ('113', '353', 'Документ, содержащий сведения медицинского свидетельства о смерти в бумажной форме (CDA) Редакция 5', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3815', '1.2.643.5.1.13.13.15.36.5'),
    ('114', '354', 'Документ, содержащий сведения медицинского свидетельства о перинатальной смерти в бумажной форме (CDA) Редакция 2', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3817', '1.2.643.5.1.13.13.15.38.2'),
    ('115', '74', 'Карта вызова скорой медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-02-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4043', '1.2.643.5.1.13.13.15.72.2'),
    ('116', '362', 'Уведомление о выявлении противопоказаний или аннулировании медицинских заключений к владению оружием (CDA) Редакция 1', '3', '2', DATE '2022-02-15', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4049', '1.2.643.5.1.13.13.15.62.1'),
    ('117', '45', 'Медицинское заключение об отсутствии медицинских противопоказаний к владению оружием (CDA) Редакция 2', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4055', '1.2.643.5.1.13.13.15.41.2'),
    ('118', '33', 'Документ, содержащий сведения медицинского свидетельства о рождении в бумажной форме (CDA) Редакция 4', '3', '2', DATE '2021-02-21', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3849', '1.2.643.5.1.13.13.15.39.4'),
    ('119', '5', 'Протокол консультации (CDA) Редакция 4', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4023', '1.2.643.5.1.13.13.15.13.4'),
    ('120', '374', 'Согласие гражданина (его законного или уполномоченного представителя) на направление и проведение медико-социальной экспертизы (PDF/A-1)', '0', '1', DATE '2022-07-18', DATE '2023-03-29', NULL, NULL),
    ('121', '34', 'Направление на медико-социальную экспертизу (CDA) Редакция 6', '3', '2', DATE '2022-11-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4283', '1.2.643.5.1.13.13.15.4.6'),
    ('122', '141', 'Сведения о результатах диспансеризации или профилактического медицинского осмотра (CDA) Редакция 2', '3', '2', DATE '2023-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4099', '1.2.643.5.1.13.13.15.74.2'),
    ('123', '241', 'Направление на госпитализацию для оказания высокотехнологичной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-11-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4257', '1.2.643.5.1.13.13.15.33.2'),
    ('124', '346', 'Направление на госпитализацию для оказания специализированной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-11-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4255', '1.2.643.5.1.13.13.15.34.2'),
    ('125', '13', 'Медицинское свидетельство о смерти (CDA) Редакция 6', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4325', '1.2.643.5.1.13.13.15.35.6'),
    ('126', '353', 'Документ, содержащий сведения медицинского свидетельства о смерти в бумажной форме (CDA) Редакция 6', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4325', '1.2.643.5.1.13.13.15.36.6'),
    ('127', '14', 'Медицинское свидетельство о перинатальной смерти (CDA) Редакция 3', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4327', '1.2.643.5.1.13.13.15.37.3'),
    ('128', '354', 'Документ, содержащий сведения медицинского свидетельства о перинатальной смерти в бумажной форме (CDA) Редакция 3', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4327', '1.2.643.5.1.13.13.15.38.3'),
    ('129', '340', 'Результаты профилактического медицинского осмотра / диспансеризации (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4415', '1.2.643.5.1.13.13.15.28.1'),
    ('130', '352', 'Уведомление о причинах возврата направления на медико-социальную экспертизу в медицинскую организацию (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4329', '1.2.643.5.1.13.13.15.7.2'),
    ('131', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4311', '1.2.643.5.1.13.13.15.67.3'),
    ('132', '80', 'Талон на оказание высокотехнологичной медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2023-04-20', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4125', '1.2.643.5.1.13.13.15.73.1'),
    ('133', '351', 'Этапный эпикриз (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4115', '1.2.643.5.1.13.13.15.30.1'),
    ('134', '345', 'Предоперационный эпикриз (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4107', '1.2.643.5.1.13.13.15.29.1'),
    ('135', '350', 'Выписка из истории болезни (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4113', '1.2.643.5.1.13.13.15.61.1'),
    ('136', '72', 'Экстренное извещение о случае острого отравления химической этиологии (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4123', '1.2.643.5.1.13.13.15.71.1'),
    ('137', '48', 'Санаторно-курортная карта (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4117', '1.2.643.5.1.13.13.15.9.1'),
    ('138', '375', 'Программа дополнительного обследования гражданина (CDA) Редакция 1', '3', '2', DATE '2023-02-06', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4285', '1.2.643.5.1.13.13.15.40.1'),
    ('139', '89', 'Справка о результатах химико-токсикологических исследований (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4433', '1.2.643.5.1.13.13.15.19.2'),
    ('140', '38', 'Отпуск по рецепту на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4317', '1.2.643.5.1.13.13.15.2.4'),
    ('141', '37', 'Льготный рецепт на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4319', '1.2.643.5.1.13.13.15.1.4'),
    ('142', '368', 'Заключение об установлении факта поствакцинального осложнения (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4275', '1.2.643.5.1.13.13.15.64.1'),
    ('143', '367', 'Заключение лечебного учреждения о нуждаемости престарелого гражданина в постоянном постороннем уходе (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4273', '1.2.643.5.1.13.13.15.63.1'),
    ('144', '369', 'Заключение врачебной комиссии медицинской организации, оказывающей лечебно-профилактическую помощь, о нуждаемости ветерана в обеспечении протезами (кроме зубных протезов), протезно-ортопедическими изделиями (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4277', '1.2.643.5.1.13.13.15.65.1'),
    ('145', '370', 'Справка о наличии медицинских показаний, в соответствии с которыми ребенок не посещает дошкольную организацию или организацию, осуществляющую образовательную деятельность по основным общеобразовательным программам, в период учебного процесса (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4279', '1.2.643.5.1.13.13.15.66.1'),
    ('146', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2023-04-20', DATE '2025-02-01', 'https://portal.egisz.rosminzdrav.ru/materials/4313', '1.2.643.5.1.13.13.15.68.3'),
    ('147', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 5', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4417', '1.2.643.5.1.13.13.15.25.5'),
    ('148', '86', 'Рецепт на лекарственный препарат (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4321', '1.2.643.5.1.13.13.15.3.2'),
    ('149', '69', 'Медицинское заключение о принадлежности несовершеннолетнего к медицинской группе для занятий физической культурой (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4331', '1.2.643.5.1.13.13.15.52.3'),
    ('150', '91', 'Медицинская справка в бассейн (CDA) Редакция 3', '3', '2', DATE '2023-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4297', '1.2.643.5.1.13.13.15.53.3'),
    ('151', '47', 'Справка для получения путевки на санаторно-курортное лечение (CDA) Редакция 2', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4315', '1.2.643.5.1.13.13.15.8.2'),
    ('152', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4333', '1.2.643.5.1.13.13.15.54.3'),
    ('153', '56', 'Медицинская справка (для выезжающего за границу) (CDA) Редакция 2', '3', '2', DATE '2023-08-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4335', '1.2.643.5.1.13.13.15.48.2'),
    ('154', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 4', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4337', '1.2.643.5.1.13.13.15.56.4'),
    ('155', '67', 'Справка об отсутствии медицинских противопоказаний для работы с использованием сведений, составляющих государственную тайну (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4119', '1.2.643.5.1.13.13.15.50.1'),
    ('156', '68', 'Заключение о результатах медицинского освидетельствования граждан, намеревающихся усыновить (удочерить), взять под опеку (попечительство), в приемную или патронатную семью детей-сирот и детей, оставшихся без попечения родителей (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4121', '1.2.643.5.1.13.13.15.51.1'),
    ('157', '142', 'Справка о прохождении медицинского освидетельствования в психоневрологическом диспансере (CDA) Редакция 2', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4419', '1.2.643.5.1.13.13.15.59.2'),
    ('158', '347', 'Выписка из протокола решения врачебной комиссии (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4307', '1.2.643.5.1.13.13.15.75.2'),
    ('159', '113', 'Статистическая карта выбывшего из медицинской организации, оказывающей медицинскую помощь в стационарных условиях, в условиях дневного стационара (CDA) Редакция 1', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4421', '1.2.643.5.1.13.13.15.76.1'),
    ('160', '372', 'Протокол телемедицинской консультации для трансграничных телемедицинских решений (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4423', '1.2.643.5.1.13.13.15.16.1'),
    ('161', '50', 'Санаторно-курортная карта для детей (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4111', '1.2.643.5.1.13.13.15.10.1'),
    ('162', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4127', '1.2.643.5.1.13.13.15.11.1'),
    ('163', '109', 'Протокол медицинской манипуляции (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4411', '1.2.643.5.1.13.13.15.23.1'),
    ('164', '59', 'Экстренное извещение об инфекционном заболевании, пищевом, остром профессиональном отравлении, необычной реакции на прививку (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4425', '1.2.643.5.1.13.13.15.70.2'),
    ('165', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4127', '1.2.643.5.1.13.13.15.12.1'),
    ('166', '39', 'Медицинская справка (врачебное профессионально-консультативное заключение) (CDA) Редакция 2', '3', '2', DATE '2023-08-28', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/4101', '1.2.643.5.1.13.13.15.45.2'),
    ('167', '33', 'Медицинское свидетельство о рождении (CDA) Редакция 5', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4059', '1.2.643.5.1.13.13.15.39.5'),
    ('168', '33', 'Документ, содержащий сведения медицинского свидетельства о рождении в бумажной форме (CDA) Редакция 5', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4059', '1.2.643.5.1.13.13.15.39.5'),
    ('169', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 4', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4339', '1.2.643.5.1.13.13.15.58.4'),
    ('170', '53', 'Сертификат профилактических прививок (CDA) Редакция 2', '3', '2', DATE '2023-08-28', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4095', '1.2.643.5.1.13.13.15.46.2'),
    ('171', '8', 'Медицинское заключение о наличии (об отсутствии) у водителей транспортных средств медицинских противопоказаний, медицинских показаний или медицинских ограничений к управлению транспортными средствами (CDA) Редакция 3', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4109', '1.2.643.5.1.13.13.15.43.3'),
    ('172', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 2', '3', '2', DATE '2023-08-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4341', '1.2.643.5.1.13.13.15.57.2'),
    ('173', '11', 'Протокол гемотрансфузии (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4427', '1.2.643.5.1.13.13.15.24.4'),
    ('174', '6', 'Протокол инструментального исследования (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4429', '1.2.643.5.1.13.13.15.17.4'),
    ('175', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 2', '3', '2', DATE '2023-08-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4343', '1.2.643.5.1.13.13.15.44.2'),
    ('176', '121', 'Протокол цитологического исследования (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4373', '1.2.643.5.1.13.13.15.20.2'),
    ('177', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4345', '1.2.643.5.1.13.13.15.31.3'),
    ('178', '48', 'Санаторно-курортная карта (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4347', '1.2.643.5.1.13.13.15.9.2'),
    ('179', '50', 'Санаторно-курортная карта для детей (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4349', '1.2.643.5.1.13.13.15.10.2'),
    ('180', '46', 'Медицинское заключение об отсутствии в организме человека наркотических средств, психотропных веществ и их метаболитов (CDA) Редакция 2', '3', '2', DATE '2027-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4351', '1.2.643.5.1.13.13.15.42.2'),
    ('181', '254', 'Протокол патолого-анатомического вскрытия (CDA) Редакция 1', '3', '2', DATE '2023-08-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4353', '1.2.643.5.1.13.13.15.22.1'),
    ('182', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 2', '3', '2', DATE '2023-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4299', '1.2.643.5.1.13.13.15.11.2'),
    ('183', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 2', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4301', '1.2.643.5.1.13.13.15.12.2'),
    ('184', '184', 'Извещение о больном с впервые в жизни установленным диагнозом злокачественного новообразования (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4355', '1.2.643.5.1.13.13.15.80.1'),
    ('185', '57', 'Направление на консультацию и во вспомогательные кабинеты (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4357', '1.2.643.5.1.13.13.15.32.2'),
    ('186', '7', 'Протокол лабораторного исследования (CDA) Редакция 5', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4431', '1.2.643.5.1.13.13.15.18.5'),
    ('187', '35', 'Сведения о результатах проведенной медико-социальной экспертизы (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4359', '1.2.643.5.1.13.13.15.5.3'),
    ('188', '54', 'Заключение медицинского учреждения о наличии (отсутствии) заболевания, препятствующего поступлению на государственную гражданскую службу Российской Федерации и муниципальную службу или ее прохождению (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4361', '1.2.643.5.1.13.13.15.81.1'),
    ('189', '108', 'Протокол оперативного вмешательства (операции) (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4363', '1.2.643.5.1.13.13.15.77.1'),
    ('190', '371', 'Протокол консилиума врачей (онкологического) (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4375', '1.2.643.5.1.13.13.15.79.1'),
    ('191', '341', 'Осмотр лечащим врачом, врачом-специалистом, заведующим отделением, лечащим врачом совместно с врачом-специалистом, лечащим врачом совместно с заведующим отделением (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4365', '1.2.643.5.1.13.13.15.78.1'),
    ('192', '77', 'Справка о количестве донаций донорской крови и ее компонентов (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4367', '1.2.643.5.1.13.13.15.82.1'),
    ('193', '52', 'Справка об оплате медицинских услуг для предоставления в налоговые органы Российской Федерации (CDA) Редакция 2', '3', '2', DATE '2023-08-14', DATE '2025-07-01', 'https://portal.egisz.rosminzdrav.ru/materials/4377', '1.2.643.5.1.13.13.15.69.2'),
    ('194', '51', 'Медицинское заключение по результатам предварительного (периодического) медицинского осмотра (обследования) (CDA) Редакция 2', '3', '2', DATE '2023-07-31', DATE '2024-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4413', '1.2.643.5.1.13.13.15.47.2'),
    ('195', '350', 'Выписка из истории болезни (CDA) Редакция 2', '3', '2', DATE '2023-10-26', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4475', '1.2.643.5.1.13.13.15.61.2'),
    ('196', '39', 'Медицинская справка (врачебное профессионально-консультативное заключение) (CDA) Редакция 3', '3', '2', DATE '2023-10-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4477', '1.2.643.5.1.13.13.15.45.3'),
    ('197', '73', 'Медицинское заключение о допуске к выполнению работ на высоте, верхолазных работ, работ, связанных с подъемом на высоту, а также по обслуживанию подъемных сооружений (CDA) Редакция 2', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4451', '1.2.643.5.1.13.13.15.55.2'),
    ('198', '381', 'Первичный осмотр врачом приемного отделения (дежурным врачом или лечащим врачом) (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4453', '1.2.643.5.1.13.13.15.86.1'),
    ('199', '396', 'Извещение о поступлении (обращении) пациента, а также в случае смерти пациента, личность которого не установлена (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4455', '1.2.643.5.1.13.13.15.89.1'),
    ('200', '351', 'Этапный эпикриз (CDA) Редакция 2', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4457', '1.2.643.5.1.13.13.15.30.2'),
    ('201', '113', 'Статистическая карта выбывшего из медицинской организации, оказывающей медицинскую помощь в стационарных условиях, в условиях дневного стационара (CDA) Редакция 2', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4459', '1.2.643.5.1.13.13.15.76.2'),
    ('202', '107', 'Направление на лабораторное исследование (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4461', '1.2.643.5.1.13.13.15.85.1'),
    ('203', '79', 'Медицинская справка (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4463', '1.2.643.5.1.13.13.15.98.1'),
    ('204', '480', 'Медицинское заключение (CDA) Редакция 1', '3', '2', DATE '2023-11-21', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4483', '1.2.643.5.1.13.13.15.105.1'),
    ('205', '10', 'Выписной эпикриз из родильного дома (CDA) Редакция 4', '3', '2', DATE '2023-12-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4485', '1.2.643.5.1.13.13.15.27.4'),
    ('206', '3', 'Направление на госпитализацию, обследование, консультацию (CDA) Редакция 4', '3', '2', DATE '2023-12-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4495', '1.2.643.5.1.13.13.15.31.4'),
    ('207', '376', 'Направление на проведение неонатального скрининга (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4461', '1.2.643.5.1.13.13.15.107.1'),
    ('208', '78', 'Справка о состоянии здоровья по месту требования (CDA) Редакция 1', '3', '2', DATE '2023-09-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4463', '1.2.643.5.1.13.13.15.84.1'),
    ('209', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 4', '3', '2', DATE '2027-06-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4503', '1.2.643.5.1.13.13.15.67.4'),
    ('210', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 4', '3', '2', DATE '2023-12-08', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4487', '1.2.643.5.1.13.13.15.68.4'),
    ('211', '250', 'Протокол на случай выявления у больного запущенной формы злокачественного новообразования (CDA) Редакция 1', '3', '2', DATE '2023-12-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4505', '1.2.643.5.1.13.13.15.95.1'),
    ('212', '362', 'О наличии оснований для внеочередного медицинского освидетельствования и об аннулировании действующего медицинского заключения об отсутствии медицинских противопоказаний к владению оружием (при его наличии) (CDA) Редакция 2', '3', '2', DATE '2023-12-19', DATE '2024-05-01', 'https://portal.egisz.rosminzdrav.ru/materials/4507', '1.2.643.5.1.13.13.15.62.2'),
    ('213', '142', 'Справка о прохождении медицинского освидетельствования в психоневрологическом диспансере (CDA) Редакция 3', '3', '2', DATE '2023-12-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4489', '1.2.643.5.1.13.13.15.59.3'),
    ('214', '12', 'Протокол прижизненного патолого-анатомического исследования биопсийного (операционного) материала (CDA) Редакция 3', '3', '2', DATE '2024-02-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4551', '1.2.643.5.1.13.13.15.21.3'),
    ('215', '66', 'Справка донору об освобождении от работы в день кроводачи и предоставлении ему дополнительного дня отдыха (CDA) Редакция 2', '3', '2', DATE '2023-12-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4519', '1.2.643.5.1.13.13.15.49.2'),
    ('216', '343', 'Справка о постановке на учет по беременности (CDA) Редакция 2', '3', '2', DATE '2023-12-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4521', '1.2.643.5.1.13.13.15.60.2'),
    ('217', '345', 'Предоперационный эпикриз (CDA) Редакция 2', '3', '2', DATE '2024-01-25', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4527', '1.2.643.5.1.13.13.15.29.2'),
    ('218', '498', 'Заключение межведомственного экспертного совета об установлении причинной связи развившихся заболеваний ребенка с последствиями радиоактивного облучения одного из родителей вследствие ЧАЭС (CDA) Редакция 1', '3', '2', DATE '2023-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4515', '1.2.643.5.1.13.13.15.109.1'),
    ('219', '500', 'Заключение межведомственного экспертного совета об установлении причинной связи смерти кормильца с последствиями чернобыльской катастрофы (вследствие лучевой болезни и других заболеваний) (CDA) Редакция 1', '3', '2', DATE '2023-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4517', '1.2.643.5.1.13.13.15.110.1'),
    ('220', '53', 'Сертификат о профилактических прививках (CDA) Редакция 3', '3', '2', DATE '2024-03-07', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4561', '1.2.643.5.1.13.13.15.46.3'),
    ('221', '389', 'Лист назначений и их выполнение (CDA) Редакция 1', '3', '2', DATE '2024-01-25', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4529', '1.2.643.5.1.13.13.15.96.1'),
    ('222', '93', 'Направление на прижизненное патолого-анатомическое исследование биопсийного (операционного) материала (CDA) Редакция 1', '3', '2', DATE '2024-01-09', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4523', '1.2.643.5.1.13.13.15.101.1'),
    ('223', '72', 'Экстренное извещение о случае острого отравления химической этиологии (CDA) Редакция 2', '3', '2', DATE '2023-12-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4493', '1.2.643.5.1.13.13.15.71.2'),
    ('224', '6', 'Протокол инструментального исследования (CDA) Редакция 5', '3', '2', DATE '2024-01-25', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4491', '1.2.643.5.1.13.13.15.17.5'),
    ('225', '386', 'Эпикриз родов (CDA) Редакция 1', '3', '2', DATE '2024-03-07', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4555', '1.2.643.5.1.13.13.15.83.1'),
    ('226', '75', 'Извещение на ребенка с врожденными пороками развития (CDA) Редакция 1', '3', '2', DATE '2024-02-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4543', '1.2.643.5.1.13.13.15.94.1'),
    ('227', '5', 'Протокол консультации (CDA) Редакция 5', '3', '2', DATE '2024-03-07', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4557', '1.2.643.5.1.13.13.15.13.5'),
    ('228', '340', 'Результаты профилактического медицинского осмотра / диспансеризации (CDA) Редакция 2', '3', '2', DATE '2024-02-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4501', '1.2.643.5.1.13.13.15.28.2'),
    ('229', '80', 'Талон на оказание высокотехнологичной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2024-03-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4567', '1.2.643.5.1.13.13.15.73.2'),
    ('230', '502', 'Медицинское заключение по результатам медицинского осмотра работника для предоставления в подсистему ЭЛМК (CDA) Редакция 1', '3', '2', DATE '2024-03-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4565', '1.2.643.5.1.13.13.15.111.1'),
    ('231', '370', 'Справка о наличии медицинских показаний, в соответствии с которыми ребенок не посещает дошкольную организацию или организацию, осуществляющую образовательную деятельность по основным общеобразовательным программам, в период учебного процесса (CDA) Редакция 2', '3', '2', DATE '2024-02-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4553', '1.2.643.5.1.13.13.15.66.2'),
    ('232', '368', 'Заключение об установлении факта поствакцинального осложнения (CDA) Редакция 2', '3', '2', DATE '2027-06-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4559', '1.2.643.5.1.13.13.15.64.2'),
    ('233', '2', 'Эпикриз по законченному случаю амбулаторный (CDA) Редакция 5', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4581', '1.2.643.5.1.13.13.15.26.5'),
    ('234', '384', 'Переводной эпикриз (CDA) Редакция 1', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4583', '1.2.643.5.1.13.13.15.87.1'),
    ('235', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 6', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4585', '1.2.643.5.1.13.13.15.25.6'),
    ('236', '385', 'Посмертный эпикриз (CDA) Редакция 1', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4573', '1.2.643.5.1.13.13.15.93.1'),
    ('237', '378', 'Протокол осмотра мультидисциплинарной реабилитационной команды (CDA) Редакция 1', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4575', '1.2.643.5.1.13.13.15.92.1'),
    ('238', '379', 'Этапный реабилитационный эпикриз (CDA) Редакция 1', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4577', '1.2.643.5.1.13.13.15.91.1'),
    ('239', '380', 'Заключительный реабилитационный эпикриз (CDA) Редакция 1', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4579', '1.2.643.5.1.13.13.15.90.1'),
    ('240', '367', 'Заключение лечебного учреждения о нуждаемости престарелого гражданина в постоянном постороннем уходе (CDA) Редакция 2', '3', '2', DATE '2024-03-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4571', '1.2.643.5.1.13.13.15.63.2'),
    ('241', '365', 'Направление тела умершего в патолого-анатомическое отделение (CDA) Редакция 1', '3', '2', DATE '2024-03-21', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4569', '1.2.643.5.1.13.13.15.106.1'),
    ('242', '254', 'Протокол патолого-анатомического вскрытия (CDA) Редакция 2', '3', '2', DATE '2024-04-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4587', '1.2.643.5.1.13.13.15.22.2'),
    ('243', '458', 'Протокол патолого-анатомического вскрытия плода, мертворожденного или новорожденного (CDA) Редакция 1', '3', '2', DATE '2024-04-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4589', '1.2.643.5.1.13.13.15.108.1'),
    ('244', '503', 'Сопроводительный лист станции (отделения) скорой медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2024-04-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4619', '1.2.643.5.1.13.13.15.112.1'),
    ('245', '504', 'Талон к сопроводительному листу станции (отделения) скорой медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2024-05-02', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4621', '1.2.643.5.1.13.13.15.113.1'),
    ('246', '11', 'Протокол трансфузии (CDA) Редакция 5', '3', '2', DATE '2024-06-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4711', '1.2.643.5.1.13.13.15.24.5'),
    ('247', '56', 'Медицинская справка (для выезжающего за границу) (CDA) Редакция 3', '3', '2', DATE '2024-06-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4635', '1.2.643.5.1.13.13.15.48.3'),
    ('248', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 3', '3', '2', DATE '2024-07-08', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4719', '1.2.643.5.1.13.13.15.44.3'),
    ('249', '67', 'Справка об отсутствии медицинских противопоказаний для работы с использованием сведений, составляющих государственную тайну (CDA) Редакция 2', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4707', '1.2.643.5.1.13.13.15.50.2'),
    ('250', '68', 'Заключение о результатах медицинского освидетельствования граждан, намеревающихся усыновить (удочерить), взять под опеку (попечительство), в приемную или патронатную семью детей-сирот и детей, оставшихся без попечения родителей (CDA) Редакция 2', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4709', '1.2.643.5.1.13.13.15.51.2'),
    ('251', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 5', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4705', '1.2.643.5.1.13.13.15.56.5'),
    ('252', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 3', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4701', '1.2.643.5.1.13.13.15.57.3'),
    ('253', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 5', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4703', '1.2.643.5.1.13.13.15.58.5'),
    ('254', '506', 'Протокол кесарева сечения (CDA) Редакция 1', '3', '2', DATE '2024-09-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4745', '1.2.643.5.1.13.13.15.114.1'),
    ('255', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 5', '3', '2', DATE '2024-07-23', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4721', '1.2.643.5.1.13.13.15.31.5'),
    ('256', '508', 'Заключение по результатам микробиологического исследования (CDA) Редакция 1', '3', '2', DATE '2024-07-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4723', '1.2.643.5.1.13.13.15.120.1'),
    ('257', '509', 'Выписка из протокола решения врачебной комиссии для направления на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2024-08-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4727', '1.2.643.5.1.13.13.15.118.1'),
    ('262', '510', 'Протокол по результатам дистанционного наблюдения за состоянием здоровья пациента (CDA) Редакция 1', '3', '2', DATE '2024-11-11', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4769', '1.2.643.5.1.13.13.15.123.1'),
    ('266', '179', 'Медицинское заключение о допуске к участию в физкультурных и спортивных мероприятиях (учебно-тренировочных мероприятиях и спортивных соревнованиях), мероприятиях по оценке выполнения нормативов испытаний (тестов) Всероссийского физкультурно-спортивного комплекса "Готов к труду и обороне" (ГТО) (CDA) Редакция 1', '3', '2', DATE '2024-09-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4743', '1.2.643.5.1.13.13.15.124.1'),
    ('268', '382', 'Протокол анестезиологического пособия (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4801', '1.2.643.5.1.13.13.15.100.1'),
    ('269', '512', 'Протокол копрологического исследования (CDA) Редакция 1', '3', '2', DATE '2024-11-11', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4767', '1.2.643.5.1.13.13.15.125.1'),
    ('270', '518', 'Протокол трансторакальной эхокардиографии (CDA) Редакция 1', '3', '2', DATE '2024-11-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4773', '1.2.643.5.1.13.13.15.126.1'),
    ('271', '532', 'План радиологического/радиотерапевтического лечения (CDA) Редакция 1', '3', '2', DATE '2025-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4819', '1.2.643.5.1.13.13.15.129.1'),
    ('272', '533', 'Протокол радиологического/радиотерапевтического лечения (CDA) Редакция 1', '3', '2', DATE '2025-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4823', '1.2.643.5.1.13.13.15.130.1'),
    ('273', '534', 'План противоопухолевой лекарственной терапии (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4799', '1.2.643.5.1.13.13.15.131.1'),
    ('274', '535', 'Протокол противоопухолевой лекарственной терапии (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4803', '1.2.643.5.1.13.13.15.132.1'),
    ('275', '507', 'Протокол эндоскопического исследования толстой кишки (CDA) Редакция 1', '3', '2', DATE '2024-11-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4771', '1.2.643.5.1.13.13.15.115.1'),
    ('276', '531', 'Справка о наличии показаний к протезированию (CDA) Редакция 1', '3', '2', DATE '2024-11-21', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4741', '1.2.643.5.1.13.13.15.128.1'),
    ('277', '252', 'Карта проведения реанимации и интенсивной терапии (CDA) Редакция 1', '3', '2', DATE '2025-03-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4815', '1.2.643.5.1.13.13.15.133.1'),
    ('278', '48', 'Санаторно-курортная карта (CDA) Редакция 3', '3', '2', DATE '2025-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4783', '1.2.643.5.1.13.13.15.9.3'),
    ('279', '50', 'Санаторно-курортная карта для детей (CDA) Редакция 3', '3', '2', DATE '2025-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4785', '1.2.643.5.1.13.13.15.10.3'),
    ('280', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 3', '3', '2', DATE '2025-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4787', '1.2.643.5.1.13.13.15.11.3'),
    ('281', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 3', '3', '2', DATE '2025-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4789', '1.2.643.5.1.13.13.15.12.3'),
    ('282', '511', 'Направление на дистанционное наблюдение за состоянием здоровья пациента (CDA) Редакция 1', '3', '2', DATE '2024-12-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4781', '1.2.643.5.1.13.13.15.134.1'),
    ('283', '109', 'Протокол медицинской манипуляции (CDA) Редакция 2', '3', '2', DATE '2025-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4797', '1.2.643.5.1.13.13.15.23.2'),
    ('284', '121', 'Протокол цитологического исследования (CDA) Редакция 3', '3', '2', DATE '2025-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4795', '1.2.643.5.1.13.13.15.20.3'),
    ('285', '540', 'Протокол цитологического исследования материала, полученного при профилактическом гинекологическом осмотре, скрининге (CDA) Редакция 1', '3', '2', DATE '2025-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4821', '1.2.643.5.1.13.13.15.136.1'),
    ('286', '539', 'Протокол эндоскопического исследования верхних отделов желудочно-кишечного тракта (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4805', '1.2.643.5.1.13.13.15.135.1'),
    ('287', '383', 'Предоперационный осмотр врачом-анестезиологом-реаниматологом (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4807', '1.2.643.5.1.13.13.15.88.1'),
    ('288', '390', 'Карта проведения анестезиологического пособия (CDA) Редакция 1', '3', '2', DATE '2025-05-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4809', '1.2.643.5.1.13.13.15.127.1'),
    ('289', '85', 'Протокол консультации в рамках диспансерного наблюдения (CDA) Редакция 6', '3', '2', DATE '2026-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5020', '1.2.643.5.1.13.13.15.14.6'),
    ('290', '5', 'Протокол консультации (CDA) Редакция 7', '3', '2', DATE '2026-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5055', '1.2.643.5.1.13.13.15.13.7'),
    ('291', '241', 'Направление на госпитализацию для оказания высокотехнологичной медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2025-12-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4881', '1.2.643.5.1.13.13.15.33.3'),
    ('292', '346', 'Направление на госпитализацию для оказания специализированной медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2025-07-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4879', '1.2.643.5.1.13.13.15.34.3'),
    ('293', '91', 'Медицинская справка в бассейн (CDA) Редакция 4', '3', '2', DATE '2025-10-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4991', '1.2.643.5.1.13.13.15.53.4'),
    ('294', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 4', '3', '2', DATE '2025-10-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4993', '1.2.643.5.1.13.13.15.54.4'),
    ('296', '462', 'Заключение о состоянии здоровья ребенка-сироты, ребенка, оставшегося без попечения родителей, помещаемого под надзор в организацию для детей-сирот и детей, оставшихся без попечения родителей (CDA) Редакция 1', '3', '2', DATE '2025-11-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5009', '1.2.643.5.1.13.13.15.142.1'),
    ('297', '547', 'Направление на цитологическое исследование (CDA) Редакция 1', '3', '2', DATE '2026-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5056', '1.2.643.5.1.13.13.15.143.1'),
    ('298', '548', 'Направление на цитологическое исследование материала, полученного при профилактическом гинекологическом осмотре, скрининге (CDA) Редакция 1', '3', '2', DATE '2026-01-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5019', '1.2.643.5.1.13.13.15.144.1'),
    ('299', '546', 'Переводной эпикриз на ребенка, достигшего возраста 18 лет, из детской поликлиники в поликлинику для взрослого населения (CDA) Редакция 1', '3', '2', DATE '2026-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5140', '1.2.643.5.1.13.13.15.141.1'),
    ('301', '2', 'Эпикриз по законченному случаю амбулаторный (CDA) Редакция 6', '3', '2', DATE '2027-09-01', NULL, NULL, '1.2.643.5.1.13.13.15.26.6'),
    ('302', '69', 'Медицинское заключение о принадлежности несовершеннолетнего к медицинской группе для занятий физической культурой (CDA) Редакция 4', '3', '2', DATE '2025-10-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4994', '1.2.643.5.1.13.13.15.52.4'),
    ('303', '541', 'Протокол результата ультразвукового исследования щитовидной железы и околощитовидных желез (CDA) Редакция 1', '3', '2', DATE '2026-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5028', '1.2.643.5.1.13.13.15.137.1'),
    ('305', '475', 'Медицинское заключение о наличии (об отсутствии) у трактористов, машинистов и водителей самоходных машин (кандидатов в трактористы, машинисты и водители самоходных машин) медицинских противопоказаний, медицинских показаний или медицинских ограничений к управлению самоходными машинами (CDA) Редакция 1', '3', '2', DATE '2025-11-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5010', '1.2.643.5.1.13.13.15.146.1'),
    ('306', '51', 'Медицинское заключение по результатам предварительного (периодического) медицинского осмотра (обследования) (CDA) Редакция 3', '3', '2', DATE '2025-11-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5011', '1.2.643.5.1.13.13.15.47.3'),
    ('307', '349', 'Протокол консилиума врачей (CDA) Редакция 1', '3', '2', DATE '2025-11-17', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5012', '1.2.643.5.1.13.13.15.148.1'),
    ('308', '10', 'Выписной эпикриз из родильного дома (CDA) Редакция 5', '3', '2', DATE '2026-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5027', '1.2.643.5.1.13.13.15.27.5'),
    ('309', '254', 'Протокол патолого-анатомического вскрытия умершего ребенка в возрасте свыше семи дней жизни или умершего взрослого (CDA) Редакция 3', '3', '2', DATE '2025-12-25', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5017', '1.2.643.5.1.13.13.15.22.3'),
    ('310', '458', 'Протокол патолого-анатомического вскрытия плода, мертворожденного или умершего ребенка в возрасте до семи дней жизни включительно (CDA) Редакция 2', '3', '2', DATE '2026-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5139', '1.2.643.5.1.13.13.15.108.2'),
    ('311', '3', 'Направление для оказания медицинской помощи (CDA) Редакция 6', '3', '2', DATE '2026-04-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5038', '1.2.643.5.1.13.13.15.31.6'),
    ('312', '347', 'Выписка из протокола решения врачебной комиссии (CDA) Редакция 3', '3', '2', DATE '2025-11-17', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5013', '1.2.643.5.1.13.13.15.75.3'),
    ('313', '176', 'Медицинское заключение о пригодности или непригодности к выполнению отдельных видов работ (CDA) Редакция 1', '3', '2', DATE '2026-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5070', '1.2.643.5.1.13.13.15.155.1'),
    ('314', '564', 'Медицинское заключение о наличии или об отсутствии профессионального заболевания (CDA) Редакция 1', '3', '2', DATE '2026-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5024', '1.2.643.5.1.13.13.15.156.1'),
    ('315', '543', 'Протокол коронарографии (CDA) Редакция 1', '3', '2', DATE '2026-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5103', '1.2.643.5.1.13.13.15.139.1'),
    ('316', '544', 'Протокол чрескожного коронарного вмешательства (CDA) Редакция 1', '3', '2', DATE '2026-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5029', '1.2.643.5.1.13.13.15.140.1'),
    ('317', '431', 'Талон дополнений к контрольной карте диспансерного наблюдения больного злокачественным новообразованием (CDA) Редакция 1', '3', '2', DATE '2026-02-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5102', '1.2.643.5.1.13.13.15.145.1'),
    ('318', '182', 'Медицинское заключение об отсутствии медицинских противопоказаний к исполнению обязанностей частного охранника (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5025', '1.2.643.5.1.13.13.15.157.1'),
    ('319', '569', 'Медицинское заключение об отсутствии медицинских противопоказаний к осуществлению частной детективной деятельности (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5071', '1.2.643.5.1.13.13.15.158.1'),
    ('320', '570', 'Сообщение об аннулировании действующего медицинского заключения об отсутствии медицинских противопоказаний к исполнению обязанностей частного охранника (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5026', '1.2.643.5.1.13.13.15.159.1'),
    ('321', '571', 'Сообщение об аннулировании действующего медицинского заключения об отсутствии медицинских противопоказаний к осуществлению частной детективной деятельности (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5072', '1.2.643.5.1.13.13.15.160.1'),
    ('322', '551', 'Сведения о медицинском осмотре по репродуктивному здоровью (CDA) Редакция 1', '3', '2', DATE '2026-03-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5141', '1.2.643.5.1.13.13.15.151.1'),
    ('323', '542', 'Протокол ультразвукового исследования предстательной железы (CDA) Редакция 1', '3', '2', DATE '2026-04-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5040', '1.2.643.5.1.13.13.15.138.1'),
    ('324', '553', 'Эпикриз в стационаре выписной (онкологический) (CDA) Редакция 1', '3', '2', DATE '2026-04-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5034', '1.2.643.5.1.13.13.15.154.1'),
    ('327', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 4', '3', '2', DATE '2026-07-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5042', '1.2.643.5.1.13.13.15.11.4'),
    ('329', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 4', '3', '2', DATE '2026-07-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5039', '1.2.643.5.1.13.13.15.12.4'),
    ('330', '47', 'Справка для получения путевки на санаторно-курортное лечение (CDA) Редакция 3', '3', '2', DATE '2026-07-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5277', '1.2.643.5.1.13.13.15.8.3'),
    ('331', '241', 'Направление на госпитализацию для оказания высокотехнологичной и специализированной медицинской помощи (CDA) Редакция 4', '3', '2', DATE '2026-07-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5257', '1.2.643.5.1.13.13.15.33.4'),
    ('332', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 8', '3', '2', DATE '2026-09-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5018', '1.2.643.5.1.13.13.15.25.8'),
    ('333', '385', 'Посмертный эпикриз (CDA) Редакция 2', '3', '2', DATE '2026-09-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5276', '1.2.643.5.1.13.13.15.93.2'),
    ('334', '347', 'Выписка из протокола решения врачебной комиссии (CDA) Редакция 4', '3', '2', DATE '2026-08-10', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5016', '1.2.643.5.1.13.13.15.75.4'),
    ('337', '449', 'Извещение о раненом, скончавшемся в течение 30 суток после дорожно-транспортного происшествия (CDA) Редакция 1', '3', '2', DATE '2026-10-10', NULL, NULL, '1.2.643.5.1.13.13.15.164.1'),
    ('338', '505', 'Извещение о раненом в дорожно-транспортном происшествии, обратившемся или доставленном в медицинскую организацию (CDA) Редакция 1', '3', '2', DATE '2026-10-10', NULL, NULL, '1.2.643.5.1.13.13.15.165.1'),
    ('339', '500', 'Заключение межведомственного экспертного совета об установлении (отказе в установлении) причинной связи заболеваний, инвалидности и смерти граждан, подвергшихся воздействию радиационного фактора (CDA) Редакция 2', '3', '2', DATE '2026-11-10', NULL, NULL, '1.2.643.5.1.13.13.15.110.2'),
    ('340', '576', 'Медицинское заключение об отсутствии инфекционных заболеваний, представляющих опасность для окружающих, и заболевания, вызываемого вирусом иммунодефицита человека (ВИЧ-инфекции) (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5284', '1.2.643.5.1.13.13.15.166.1'),
    ('341', '577', 'Медицинское заключение о наличии инфекционных заболеваний, представляющих опасность для окружающих, и заболевания, вызываемого вирусом иммунодефицита человека (ВИЧ-инфекции) (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5283', '1.2.643.5.1.13.13.15.167.1'),
    ('342', '578', 'Сообщение о выявлении у иностранного гражданина инфекционного заболевания, представляющего опасность для окружающих, или заболевания, вызываемого вирусом иммунодефицита человека (ВИЧ-инфекции) (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5286', '1.2.643.5.1.13.13.15.168.1'),
    ('343', '579', 'Сообщение о наличии факта употребления иностранным гражданином наркотических средств или психотропных веществ без назначения врача либо новых потенциально опасных психоактивных веществ (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5285', '1.2.643.5.1.13.13.15.169.1'),
    ('344', '394', 'Медицинское заключение о наличии (отсутствии) факта употребления иностранным гражданином наркотических средств или психотропных веществ без назначения врача либо новых потенциально опасных психоактивных веществ (CDA) Редакция 1', '3', '2', DATE '2026-09-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/5282', '1.2.643.5.1.13.13.15.170.1')
ON CONFLICT (code) DO UPDATE SET
    type_code = EXCLUDED.type_code,
    name = EXCLUDED.name,
    level = EXCLUDED.level,
    format_code = EXCLUDED.format_code,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    implementation_guide = EXCLUDED.implementation_guide,
    ig_oid = EXCLUDED.ig_oid,
    oid = EXCLUDED.code,
    updated_at = now();

UPDATE dim_semd_types
SET oid = code
WHERE oid IS DISTINCT FROM code;

CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid ON dim_semd_types (oid) WHERE oid IS NOT NULL;

COMMENT ON COLUMN dim_semd_types.ig_oid IS
    'OID руководства по реализации СЭМД: ключ соединения с dim_nsi_semd_guide. В выгрузке ФНСИ лежит в поле GIT_LINK.';
COMMENT ON COLUMN dim_semd_types.implementation_guide IS
    'Ссылка на материалы портала ЕГИСЗ. В выгрузке ФНСИ поля GIT_LINK и IMPLEMENTATION_GUIDE переставлены относительно содержания.';

-- Схемой не наполняется: снимок кладёт scripts/load_nsi_semd_guides.py, поэтому на свежем
-- контуре таблица пуста до первого запуска загрузчика.
CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide (
    oid text PRIMARY KEY,
    semd_id integer,
    full_name text NOT NULL,
    release_number smallint,
    format text,
    git_pub_date date,
    git_link text,
    source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.99.2.638',
    source_version text NOT NULL,
    raw_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz DEFAULT now()
);

COMMENT ON TABLE dim_nsi_semd_guide IS
    'НСИ 1.2.643.5.1.13.13.99.2.638 «Реестр руководств по реализации структурированных электронных медицинских документов и протоколов информационного взаимодействия»; полный снимок версии источника.';
COMMENT ON COLUMN dim_nsi_semd_guide.semd_id IS
    'SEMD_ID источника — номер ветви в собственном OID руководства, а не код вида медицинской документации из НСИ 1520. Ключом соединения не является.';
COMMENT ON COLUMN dim_nsi_semd_guide.git_link IS
    'Ссылка на git.minzdrav.gov.ru. Здесь поле источника названо по содержанию — в отличие от одноимённого поля НСИ 1520, где лежит OID (см. dim_semd_types.ig_oid).';

-- Поле OID_SYNONYM из НСИ 638: дополнительные OID, под которыми выгрузка публикует то же
-- руководство. Ни порядка их появления, ни признака, что синоним больше не принимается РЭМД,
-- выгрузка не несёт, поэтому реестр синонимы только разрешает и ничего о них не утверждает.
-- Соединение с dim_semd_types закрывается основными OID; синонимы нужны на случай, когда
-- очередной выпуск НСИ 1520 сошлётся на синоним: без них вид документации потерял бы набор
-- справочников молча. Так же устроен dim_nsi_error_code_alias.
CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide_alias (
    alias_oid text PRIMARY KEY,
    guide_oid text NOT NULL REFERENCES dim_nsi_semd_guide (oid) ON DELETE CASCADE,
    loaded_at timestamptz DEFAULT now()
);

COMMENT ON TABLE dim_nsi_semd_guide_alias IS
    'OID_SYNONYM из НСИ 638: дополнительные OID того же руководства. Разрешаются в основной OID представлением dim_semd_guide_oid.';

CREATE INDEX IF NOT EXISTS idx_dim_nsi_semd_guide_alias_guide
    ON dim_nsi_semd_guide_alias (guide_oid);

-- Наименование, редакция и синонимы OID руководства здесь не повторяются: в источнике они
-- выводятся из OID руководства и дословно совпадают с реестром руководств.
CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide_dictionary (
    guide_oid text NOT NULL REFERENCES dim_nsi_semd_guide (oid) ON DELETE CASCADE,
    dict_oid text NOT NULL,
    source_id text NOT NULL,
    dict_name text NOT NULL,
    dict_version text,
    dict_ids_systemname text,
    source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.99.2.805',
    source_version text NOT NULL,
    raw_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (guide_oid, dict_oid)
);

COMMENT ON TABLE dim_nsi_semd_guide_dictionary IS
    'НСИ 1.2.643.5.1.13.13.99.2.805 «Реестр справочников, использующихся в руководствах по реализации структурированных электронных медицинских документов»; грейн — пара (руководство, справочник).';
COMMENT ON COLUMN dim_nsi_semd_guide_dictionary.dict_version IS
    'Версия справочника из источника. Значение «*» означает «любая версия», а не «версия неизвестна», и сохраняется дословно.';
COMMENT ON COLUMN dim_nsi_semd_guide_dictionary.dict_ids_systemname IS
    'Имя поля-идентификатора внутри справочника (ID, CODE, MKB_CODE, oid): им СЭМД ссылается на запись справочника.';
COMMENT ON COLUMN dim_nsi_semd_guide_dictionary.raw_json IS
    'Запись источника целиком. Разрешённые подмножества значений (COLLECTION) отдельной таблицей не разворачиваются и доступны только здесь.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_nsi_semd_guide_dictionary_source_id
    ON dim_nsi_semd_guide_dictionary (source_id);
CREATE INDEX IF NOT EXISTS idx_dim_nsi_semd_guide_dictionary_dict_oid
    ON dim_nsi_semd_guide_dictionary (dict_oid);

-- Справочник «РЭМД. Классификатор кодов сообщений» — источник истины для кодов и
-- наименований ошибок регистрационного пути. Наполнение — выгрузка ФНСИ, описания
-- приводятся дословно (включая опечатки справочника): расхождение с оригиналом
-- сделало бы сверку с ответом РЭМД неоднозначной.
CREATE TABLE IF NOT EXISTS dim_nsi_error_code (
    nsi_error_code text PRIMARY KEY,
    nsi_error_id integer NOT NULL,
    nsi_error_description text NOT NULL,
    contour text NOT NULL,
    oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.99.2.305',
    version text NOT NULL DEFAULT '3.18',
    updated_at timestamptz DEFAULT now()
);

COMMENT ON TABLE dim_nsi_error_code IS
    'НСИ 1.2.643.5.1.13.13.99.2.305 «РЭМД. Классификатор кодов сообщений», версия 3.18';

-- FRLLO_RELISE_POSITION_ERROR в справочнике задвоена (ID 85 и 88 с разными описаниями);
-- берётся запись с меньшим ID.
INSERT INTO dim_nsi_error_code (nsi_error_code, nsi_error_id, nsi_error_description, contour)
VALUES
    ('ACCESS_DENIED', 1, 'У запрашивающей РМИС/МИС нет разрешения на получение документа', 'регистрация СЭМД'),
    ('ADDITIONAL_INFO_REQUIRED', 64, 'Для формирования запрошенного в рамках услуги "заказ справки он-лайн" документа недостаточно сведений, гражданину необходимо обратиться с личным визитом для прохождения дополнительных исследований', 'заказ справок онлайн'),
    ('AOGUID_DIFFERENT', 69, 'Уникальный идентификатор адресного объекта [AOGUID], переданного в СЭМД, не совпадает с адресом [AOGUID], полученным в результате проверки уникального идентификатора дома [HOUSEGUID] в ФИАС', 'регистрация СЭМД'),
    ('AOGUID_NOT_FOUND', 66, 'Уникальный идентификатор адресного объекта [AOGUID], переданного в СЭМД, не найден в ФИАС', 'регистрация СЭМД'),
    ('ASYNC_RESPONSE_TIMEOUT', 97, 'Превышено ожидание асинхронного ответа от проверяющей системы', 'регистрация СЭМД'),
    ('ATTRIBUTE_MISMATCH', 2, 'Из предоставляющей РМИС/МИС передан документ, метаописание которого не соответствует зарегистрированному', 'регистрация СЭМД'),
    ('CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', 4, 'Не удалось построить цепочку сертификатов до аккредитованного удостоверяющего центра (сертификат сотрудника выдан не аккредитованным УЦ или один из сертификатов цепочки не действителен)', 'регистрация СЭМД'),
    ('CANT_REG_VERSION', 5, 'Регистрация версии документа невозможна', 'регистрация СЭМД'),
    ('CAN_NOT_ASSOCIATE', 3, 'Невозможно связать документы. Создание недопустимой связи документов', 'регистрация СЭМД'),
    ('CA_INACCESSIBILITY', 62, 'Адрес OCSP-службы не указан или недоступен и недоступнен CRL', 'регистрация СЭМД'),
    ('DIGEST_MISMATCH', 6, 'Хеш-сумма документа, полученного из предоставляющей системы, не соответсвует зарегистрированной в РЭМД', 'регистрация СЭМД'),
    ('DISABLED_RMIS', 7, 'РМИС/МИС зарегистрирована в РЭМД но не активна', 'регистрация СЭМД'),
    ('DOC_DATE_MISMATCH_CERT_NOT_AFTER', 8, 'Сертификат ЭП недействителен на дату создания документа (документ создан позже окончания срока действия сертификата)', 'регистрация СЭМД'),
    ('DOC_DATE_MISMATCH_CERT_NOT_BEFORE', 9, 'Сертификат ЭП недействителен на дату создания документа (документ создан раньше начала срока действия сертификата)', 'регистрация СЭМД'),
    ('DUPLICATE_PATIENT_FOUND', 100, 'По локальному идентификатору в ГИП найдено более одной записи', 'регистрация СЭМД'),
    ('FILE_WAS_NOT_SENT', 56, 'ИС не передала файл ЭМД', 'регистрация СЭМД'),
    ('FRLLO_BENEFIT_SOURCE_ERROR', 78, 'Информационная система не является владельцем сведений о назначении для категории льготы', 'ФРЛЛО'),
    ('FRLLO_CITIZEN_BENEFIT_ERROR', 80, 'У гражданина не найдены сведения по коду льготы', 'ФРЛЛО'),
    ('FRLLO_CITIZEN_IDENTIFY_ERROR', 75, 'Переданы некорректные идентификаторы документов гражданина', 'ФРЛЛО'),
    ('FRLLO_CITIZEN_REGION_ERROR', 81, 'У гражданина отсутствуют льготы, в субъекте РФ, указанном в СЭМД', 'ФРЛЛО'),
    ('FRLLO_CITIZEN_SEARCH_ERROR', 76, 'Сведения о гражданине в регистре не найдены', 'ФРЛЛО'),
    ('FRLLO_COMISSION_INFO_ERROR', 82, 'Отсутствуют сведения о врачебной комиссии при назначении лекарственного препарата по торговому наименованию', 'ФРЛЛО'),
    ('FRLLO_DIC_ERROR', 72, 'Неверный код термина для значения, определяемого по справочнику', 'ФРЛЛО'),
    ('FRLLO_EXPIRE_DATE_ERROR', 84, 'Дата срока действия не согласуется со сроком действия согласно справочнику ФНСИ 1.2.643.5.1.13.13.99.2.608', 'ФРЛЛО'),
    ('FRLLO_NOT_CORRECT_TYPE', 93, 'Передан СЭМД с типом, для которого не предусмотрена проверка в ФРЛЛО', 'ФРЛЛО'),
    ('FRLLO_ORGANIZATION_ERROR', 79, 'Не переданы сведения об организации, назначившей мед. продукцию, или переданы противоречивые сведения об организации', 'ФРЛЛО'),
    ('FRLLO_RECIPE_DATE_ERROR', 83, 'В СЭМД не корректно передана дата назначения', 'ФРЛЛО'),
    ('FRLLO_RECIPE_IDENTIFY_ERROR', 86, 'Отсутствуют сведения о переданном назначении мед. Продукции', 'ФРЛЛО'),
    ('FRLLO_RECIPE_POSITION_ERROR', 77, 'Не передан код назначенной мед. продукции или передана неоднозначная информация о коде назначенной мед. Продукции', 'ФРЛЛО'),
    ('FRLLO_RELEASE_ORGANIZATION_ERROR', 87, 'Не переданы сведения об организации, отпустившей мед. продукцию, или переданы противоречивые сведения об организации', 'ФРЛЛО'),
    ('FRLLO_RELISE_DATE_ERROR', 89, 'Не корректно передана дата отпуска', 'ФРЛЛО'),
    ('FRLLO_RELISE_POSITION_ERROR', 85, 'Не передан код отпущенной мед. продукции, либо передан неоднозначный код отпущенной мед. продукции', 'ФРЛЛО'),
    ('FRLLO_RELISE_QTY_ERROR', 90, 'Количество отпущенных потребительских упаковок не согласуется с кол-вом потребительских единиц', 'ФРЛЛО'),
    ('FRLLO_REQUIRED_CITIZEN_ERROR', 73, 'Не переданы обязательные сведения о гражданине Имя или Фамилия или Отчество и пол, дата рождения', 'ФРЛЛО'),
    ('FRLLO_REQUIRED_IDENTIFY_ERROR', 74, 'Не передано ни одного идентификатора гражданина', 'ФРЛЛО'),
    ('FRLLO_SEMD_FLK_ERROR', 92, 'СЭМД не прошел ФЛК, либо не направлялся на ФЛК', 'ФРЛЛО'),
    ('FRLLO_TRANSPORT_ERROR', 91, 'Используется некорректный механизм передачи сведений', 'ФРЛЛО'),
    ('FRLLO_UNKNOWN_SYSTEM', 94, 'Не удалось определить информационную систему, сформировавшую СЭМД по справочнику 1.2.643.5.1.13.13.99.2.622', 'ФРЛЛО'),
    ('FRLLO_VALIDATION_ERROR', 71, 'Неверный формат передаваемого значения (формат/диапазон даты, маска/длинна строки)', 'ФРЛЛО'),
    ('GET_DOCUMENT_FILE_ERROR', 61, 'Ошибка при получении файла документа из предоставляющей системы', 'регистрация СЭМД'),
    ('HOUSEGUID_NOT_FOUND', 68, 'Уникальный идентификатор дома [HOUSEGUID], переданного в СЭМД, не найден в ФИАС', 'регистрация СЭМД'),
    ('INCONSISTENT_DIGESTS', 10, 'ЭП при одинаковых алгоритмах хеширования содержат разные хеш-суммы документов. ЭП от разных документов', 'регистрация СЭМД'),
    ('INTERNAL_ERROR', 11, 'Внутренняя ошибка', 'регистрация СЭМД'),
    ('INVALID_CERT_KEY_USAGE', 12, 'Область использования ключа ЭП не соответствует предъявляемым требованиям', 'регистрация СЭМД'),
    ('INVALID_CONTENT', 13, 'Из предоставляющей РМИС/МИС передан документ, формат файла которого не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('INVALID_DICTIONARY', 114, 'Для документа вида [] недопустимо использование справочника []', 'регистрация СЭМД'),
    ('INVALID_DICTIONARY_MAPPING', 113, 'Справочник OID [], версия []. Не удалось найти поле, отвечающее за код справочника', 'регистрация СЭМД'),
    ('INVALID_DICTIONARY_OID', 115, 'Справочник OID []. Справочник с указанным кодом отсутствует', 'регистрация СЭМД'),
    ('INVALID_DICTIONARY_VERSION', 116, 'Справочник OID []. Версия [] недопустима для данного документа вида []', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_FAMILY', 106, 'Фамилия [] медицинского работника в запросе на регистрацию отличается от фамилии [] в СЭМД. СНИЛС []', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_ID', 111, 'Локальный идентификатор медицинского работника в запросе на регистрацию отличается от уникального идентификатора медицинского работника в СЭМД. СНИЛС []', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_INFO', 110, 'Медицинский работник [] из запроса на регистрацию сведений не найден в СЭМД', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_NAME', 107, 'Имя [] медицинского работника в запросе на регистрацию отличается от имени [] в СЭМД. СНИЛС []', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_PATRONYMIC', 108, 'Отчество [] медицинского работника в запросе на регистрацию отличается от отчества [] в СЭМД. СНИЛС []', 'регистрация СЭМД'),
    ('INVALID_DOCTOR_SNILS', 112, 'Медицинский работник [] не найден в СЭМД или в запросе на регистрацию', 'регистрация СЭМД'),
    ('INVALID_DOC_CONTENT_TYPE', 55, 'Документ не соответствует допустимому формату (для вида документов)', 'регистрация СЭМД'),
    ('INVALID_ELEMENT_VALUE_CODE', 117, 'Справочник OID [], версия []. Значение с кодом [] отсутствует', 'регистрация СЭМД'),
    ('INVALID_ELEMENT_VALUE_NAME', 118, 'Справочник OID [], версия []. Наименование элемента [] не соответствует наименованию элемента в НСИ []', 'регистрация СЭМД'),
    ('INVALID_PLUGGABLE_ATTRS', 14, 'Дополнительные атрибуты документа не соответствуют схеме дополнительных атрибутов вида документов', 'регистрация СЭМД'),
    ('IPS_VALIDATION_WARNING', 101, 'Замечание от подсистемы ИПС по проверке данных запроса', 'регистрация СЭМД'),
    ('LEGAL_AUTHENTICATOR_NOT_FOUND', 109, 'Медицинский работник [], придавший документу юридическую силу, не найден в запросе на регистрацию сведений', 'регистрация СЭМД'),
    ('MIS_ERROR', 15, 'Ошибка сервиса системы, предоставляющей документ', 'регистрация СЭМД'),
    ('MIS_NOT_AVAILABLE', 16, 'Сервис системы, предоставляющей документ, не доступен', 'регистрация СЭМД'),
    ('MULTIPLE_SIGNERS', 52, 'В контейнере ЭП указано более одного подписанта', 'регистрация СЭМД'),
    ('NOT_UNIQUE_ASSOCIATION', 24, 'Регистрируемая связь документов уже существует', 'регистрация СЭМД'),
    ('NOT_UNIQUE_ITEM', 65, 'В запросе listDocKindSendRequest (сервис "заказ справок онлайн" на предоставление списка МО на ЕПГУ) есть конфликтующие записи по виду документов с пересечением дат доступности документов для заказа гражданами', 'заказ справок онлайн'),
    ('NOT_UNIQUE_PROVIDED_ID', 25, 'Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован', 'регистрация СЭМД'),
    ('NO_DEPARTMENT', 54, 'Отсутствует информация о подразделении организации', 'регистрация СЭМД'),
    ('NO_DOCUMENT_KIND_ON_DATE', 17, 'Дата создания документа находится вне периода, допустимого для вида документов', 'регистрация СЭМД'),
    ('NO_END_ENTITY_CERTIFICATE', 18, 'В ЭП отсутствует сертификат проверки подписи', 'регистрация СЭМД'),
    ('NO_ORG_ON_DATE', 50, 'МО недействительна на дату создания документа', 'регистрация СЭМД'),
    ('NO_RMIS', 19, 'РМИС/МИС не зарегистрирована в РЭМД', 'регистрация СЭМД'),
    ('NO_ROLE_POLICY_ON_DATE', 20, 'В указанную дату для роли недоступно подписание документов указанного вида', 'регистрация СЭМД'),
    ('NO_SIGNATURE', 21, 'Отсутствуют подписи документа', 'регистрация СЭМД'),
    ('NO_SNILS', 22, 'Наличие СНИЛС пациента не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('NO_SPECIALITY', 23, 'Наличие специальности подписанта не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('OBJECT_NOT_FOUND', 26, 'Не найдена запись справочника', 'регистрация СЭМД'),
    ('ORDER_ALREADY_PROCESSED', 127, 'По данному заказу уже был отправлен статус на витрину, отличный от переданного', 'заказ справок онлайн'),
    ('ORDER_NOT_FOUND', 128, 'В РЭМД не найден заказ с переданным идентификатором', 'заказ справок онлайн'),
    ('ORG_NOT_FOUND_IN_FRMO', 27, 'Организация не найдена в ФРМО', 'регистрация СЭМД'),
    ('ORG_SIGNATURE_OCCURRENCE_MISMATCH', 28, 'Наличие подписи организации не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('PATIENT_ALREADY_REGISTERED', 60, 'Внутренняя ошибка ГИП при создании пациента', 'регистрация СЭМД'),
    ('PATIENT_CREATION_ERROR', 29, 'Ошибка при создании пациента в ГИП', 'регистрация СЭМД'),
    ('PATIENT_MPI_MISMATCH', 30, 'Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП', 'регистрация СЭМД'),
    ('PATIENT_NAME_NOT_FOUND', 98, 'Имя пациента в составе сведений о пациенте обязательно', 'регистрация СЭМД'),
    ('PATIENT_NOT_FOUND', 63, 'МО-получатель заказа на оформление документа онлайн не может идентифицировать пациента. По полученным персональным данным пациента в МО данные не найдены', 'заказ справок онлайн'),
    ('PATIENT_OCCURRENCE_MISMATCH', 31, 'Наличие сведений о пациенте не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('PATIENT_SURNAME_NOT_FOUND', 99, 'Фамилия пациента в составе сведений о пациенте обязательна', 'регистрация СЭМД'),
    ('PERSONAL_SIG_CERT_NOT_ACTUAL_ON_CHECK_DT', 125, 'Сертификат сотрудника недействителен на дату проверки документа', 'регистрация СЭМД'),
    ('PERSONAL_SIG_CERT_NOT_ACTUAL_ON_DOC_CREATION_DT', 105, 'Сертификат сотрудника недействителен на дату создания документа', 'регистрация СЭМД'),
    ('PERSON_CARD_NOT_FOUND', 32, 'Личное дело сотрудника отсутствует в ФРМР', 'регистрация СЭМД'),
    ('PERSON_NOT_FOUND', 33, 'Сотрудник не найден в ФРМР', 'регистрация СЭМД'),
    ('PERSON_POST_IN_FRMR_MISMATCH', 34, 'Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР', 'регистрация СЭМД'),
    ('PLUGGABLE_ATTRS_OCCURRENCE_MISMATCH', 35, 'Наличие дополнительных атрибутов документа не соответстветсвует требованиям вида документов', 'регистрация СЭМД'),
    ('POSITION_TO_ROLE_MISMATCH', 36, 'Несоотствие должности и роли подписанта', 'регистрация СЭМД'),
    ('RATE_LIMIT', 95, 'Достигнут защитный лимит, просьба повторить через минуту или позже', 'регистрация СЭМД'),
    ('RECEPIENT_FAMILY_MISMATCH', 122, 'Фамилия получателя в запросе на регистрацию отличается от фамилия в СЭМД', 'регистрация СЭМД'),
    ('RECEPIENT_INFO_MISMATCH', 120, 'Получатель из запроса на регистрацию сведений не найден в СЭМД', 'регистрация СЭМД'),
    ('RECEPIENT_NAME_MISMATCH', 123, 'Имя получателя в запросе на регистрацию отличается от имени в СЭМД', 'регистрация СЭМД'),
    ('RECEPIENT_PATRONYMIC_MISMATCH', 124, 'Отчество получателя в запросе на регистрацию отличается от отчества в СЭМД', 'регистрация СЭМД'),
    ('RECEPIENT_SNILS_MISMATCH', 121, 'СНИЛС получателя в запросе на регистрацию отличается от СНИЛС в СЭМД', 'регистрация СЭМД'),
    ('REGION_CODE_DIFFERENT', 67, 'Регион адресного объекта [CODE], переданного в СЭМД, не совпадает с регионом [REGIONCODE], полученным в результате проверки уникального идентификатора адресного объекта в ФИАС [AOGUID]', 'регистрация СЭМД'),
    ('REGISTRY_ITEM_NOT_FOUND', 37, 'Запрашиваемая запись регистра не найдена', 'регистрация СЭМД'),
    ('RESTRICT_NEW_VERSION', 70, 'Для вида документа запрещено регистрировать новую версию', 'регистрация СЭМД'),
    ('RMIS_ERROR', 59, 'Ошибка ответа от сервиса системы в getDocumentFileResponse, предоставляющей документ', 'регистрация СЭМД'),
    ('RMIS_REGION_MISMATCH', 38, 'Регион организации не соответствует региону РМИС/МИС', 'регистрация СЭМД'),
    ('ROLE_OCCURRENCE_MISMATCH', 39, 'Число ЭП сотрудников с требуемой ролью не соответствует требованиям вида документов', 'регистрация СЭМД'),
    ('RUNTIME_ERROR', 40, 'Непредвиденная ошибка', 'регистрация СЭМД'),
    ('SCHEMA_PROCESSING_ERROR', 103, 'Внутренняя ошибка обработки шаблона валидации', 'регистрация СЭМД'),
    ('SERIES_REQUIRED', 58, 'Необходимо указать серию документа', 'регистрация СЭМД'),
    ('SERIES_REQUIRED_WRONG_SERVICE_VERSION', 57, 'Невозможно зарегистрировать ЭМД с обязательным указанием серии документа с помощью версии сервиса 3.0', 'регистрация СЭМД'),
    ('SIGNATURE_DECODING_ERROR', 41, 'Ошибка декодирования ЭП', 'регистрация СЭМД'),
    ('SIGNATURE_DUPLICATION', 51, 'Дублирование подписи', 'регистрация СЭМД'),
    ('SIGNATURE_VERIFICATION_ERROR', 42, 'Подпись не верна', 'регистрация СЭМД'),
    ('SIGNER_ORG_MISMATCH', 43, 'Организация подписанта отлична от организации, регистрирующей документ (и РМИС/МИС не имеет на это разрешения)', 'регистрация СЭМД'),
    ('TIME_EXPIRED_ERROR', 126, 'Истекло время выполнения заказа в рамках сервиса "Заказ справок онлайн"', 'заказ справок онлайн'),
    ('UNKNOWN_ALGORITHM', 44, 'Неподдерживаемый алгоритм подписи', 'регистрация СЭМД'),
    ('VALIDATION_ERROR', 49, 'Ошибка валидации значения', 'регистрация СЭМД'),
    ('VALSYS_INTERNAL_ERROR', 119, 'Внутренняя ошибка при проверке ЭМД в проверяющей системе', 'регистрация СЭМД'),
    ('VALSYS_REJECT', 96, 'Ошибка отправки запроса на валидацию в проверяющую систему', 'регистрация СЭМД'),
    ('VALUE_MISMATCH_METADATA_AND_CERTIFICATE', 45, 'Несоответствие данных (сотрудника либо МО) в сообщении и в сертификате ЭП', 'регистрация СЭМД'),
    ('VALUE_MISMATCH_METADATA_AND_FRMR', 46, 'Переданные данные сотрудника не соответствуют данным, зарегистрированным в ФРМР', 'регистрация СЭМД'),
    ('WRONG_CREATION_DATE', 47, 'Дата создания документа больше даты регистрации', 'регистрация СЭМД'),
    ('WRONG_MESSAGE_ID', 48, 'Асинхронный запрос файла ЭМД с указанным messageID не найден', 'регистрация СЭМД'),
    ('WRONG_SIGNATURE_FORMAT', 53, 'Неподдерживаемый формат ЭП', 'регистрация СЭМД'),
    ('XML_VALIDATION_ERROR', 104, 'Ошибка при трансформации СЭМД для проверки (Schematron)', 'регистрация СЭМД'),
    ('XML_VALIDATOR_ERROR', 102, 'Внутренняя ошибка валидации СЭМД', 'регистрация СЭМД')
ON CONFLICT (nsi_error_code) DO UPDATE SET
    nsi_error_id = EXCLUDED.nsi_error_id,
    nsi_error_description = EXCLUDED.nsi_error_description,
    contour = EXCLUDED.contour,
    oid = EXCLUDED.oid,
    version = EXCLUDED.version,
    updated_at = now();

CREATE INDEX IF NOT EXISTS idx_dim_nsi_error_code_contour ON dim_nsi_error_code (contour);

-- РЭМД отдаёт RECIPIENT_*, тогда как в справочнике закреплено написание RECEPIENT_*.
-- Синоним разрешается до сопоставления с правилами, поэтому правило заводится
-- на каноничную мнемонику справочника.
CREATE TABLE IF NOT EXISTS dim_nsi_error_code_alias (
    alias text PRIMARY KEY,
    nsi_error_code text NOT NULL REFERENCES dim_nsi_error_code (nsi_error_code),
    updated_at timestamptz DEFAULT now()
);

INSERT INTO dim_nsi_error_code_alias (alias, nsi_error_code)
VALUES
    ('RECIPIENT_INFO_MISMATCH', 'RECEPIENT_INFO_MISMATCH'),
    ('RECIPIENT_SNILS_MISMATCH', 'RECEPIENT_SNILS_MISMATCH'),
    ('RECIPIENT_FAMILY_MISMATCH', 'RECEPIENT_FAMILY_MISMATCH'),
    ('RECIPIENT_NAME_MISMATCH', 'RECEPIENT_NAME_MISMATCH'),
    ('RECIPIENT_PATRONYMIC_MISMATCH', 'RECEPIENT_PATRONYMIC_MISMATCH')
ON CONFLICT (alias) DO UPDATE SET
    nsi_error_code = EXCLUDED.nsi_error_code,
    updated_at = now();

-- Наименования справочников ФНСИ по OID. Нужен только для подписи предмета отказа
-- (rpt_error_breakdown.error_type): РЭМД называет справочник одним OID, и без расшифровки
-- разбивка нечитаема.
--
-- Наполнение — снимок НСИ 1.2.643.5.1.13.13.99.2.805 «Реестр справочников, использующихся
-- в руководствах по реализации СЭМД»: наименования опубликованы Минздравом и приводятся
-- дословно, поэтому принадлежность справочника больше не выводится из содержания отказов.
-- Записи вписаны литералами, а не выбраны из dim_nsi_semd_guide_dictionary: тот снимок
-- наполняется скриптом уже после наката схемы и на чистой базе пуст.
--
-- Реестр покрывает не все OID отказов: РЭМД ссылается и на справочники вне 805
-- (например 1.2.643.5.1.13.13.11.1379). Присоединяется внешним соединением — OID без
-- наименования показывается как есть, а не прячется из разбивки.
CREATE TABLE IF NOT EXISTS dim_nsi_dictionary (
    oid text PRIMARY KEY,
    name text NOT NULL,
    short_name text,
    source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.99.2.805',
    source_version text NOT NULL DEFAULT '',
    updated_at timestamptz DEFAULT now()
);

-- Реестр развёрнут раньше этих колонок, поэтому они добавляются на месте. Редакция
-- источника умолчанием колонки не задаётся: на уже развёрнутой базе ADD COLUMN IF NOT
-- EXISTS ничего не делает, и умолчание застыло бы на прежней редакции.
ALTER TABLE dim_nsi_dictionary ADD COLUMN IF NOT EXISTS short_name text;
ALTER TABLE dim_nsi_dictionary
    ADD COLUMN IF NOT EXISTS source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.99.2.805';
ALTER TABLE dim_nsi_dictionary
    ADD COLUMN IF NOT EXISTS source_version text NOT NULL DEFAULT '';

COMMENT ON TABLE dim_nsi_dictionary IS
    'Наименования справочников ФНСИ по OID для подписи предмета отказа (rpt_error_breakdown.error_type). Снимок НСИ 1.2.643.5.1.13.13.99.2.805: наименования дословны. Реестр не покрывает справочники вне 805 — недостающий OID показывается без расшифровки.';
COMMENT ON COLUMN dim_nsi_dictionary.name IS
    'Наименование из НСИ 805 дословно. Расхождение с источником сделало бы сверку неоднозначной.';
COMMENT ON COLUMN dim_nsi_dictionary.short_name IS
    'Краткая подпись для витрины. Заводится только там, где в отрасли устоялось короткое написание: официальные наименования доходят до 181 символа и в подписи типа нечитаемы.';

INSERT INTO dim_nsi_dictionary (oid, name, source_version)
SELECT v.oid, v.name, '6.19'
FROM (VALUES
    ('1.2.643.5.1.13.2.1.1.384', 'Классификатор форм туберкулеза по локализации'),
    ('1.2.643.5.1.13.13.11.1002', 'Должности медицинских и фармацевтических работников'),
    ('1.2.643.5.1.13.13.11.1005', 'Международная статистическая классификация болезней и проблем, связанных со здоровьем (10-й пересмотр)'),
    ('1.2.643.5.1.13.13.11.1006', 'Степень тяжести состояния пациента'),
    ('1.2.643.5.1.13.13.11.1007', 'Вид случая госпитализации или обращения (первичный, повторный)'),
    ('1.2.643.5.1.13.13.11.1008', 'Место оказания медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1009', 'Виды медицинских направлений'),
    ('1.2.643.5.1.13.13.11.1021', 'Тип родственной связи'),
    ('1.2.643.5.1.13.13.11.1033', 'Виды анестезии'),
    ('1.2.643.5.1.13.13.11.1034', 'Виды медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1035', 'Виды полиса обязательного медицинского страхования'),
    ('1.2.643.5.1.13.13.11.1036', 'Виды травм по способу получения'),
    ('1.2.643.5.1.13.13.11.1038', 'Социальные группы населения в учетной медицинской документации'),
    ('1.2.643.5.1.13.13.11.1039', 'Источники оплаты медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1040', 'Пол пациента'),
    ('1.2.643.5.1.13.13.11.1041', 'Тип установления инвалидности (впервые, повторно)'),
    ('1.2.643.5.1.13.13.11.1042', 'Вид места жительства'),
    ('1.2.643.5.1.13.13.11.1044', 'Причины выдачи документа о временной нетрудоспособности'),
    ('1.2.643.5.1.13.13.11.1045', 'Причины прекращения диспансерного наблюдения'),
    ('1.2.643.5.1.13.13.11.1046', 'Результаты обращения'),
    ('1.2.643.5.1.13.13.11.1047', 'Статусы диспансерного наблюдения'),
    ('1.2.643.5.1.13.13.11.1048', 'Учетные группы аппаратуры, используемой при операциях'),
    ('1.2.643.5.1.13.13.11.1049', 'Характер заболевания'),
    ('1.2.643.5.1.13.13.11.1052', 'Обстоятельства посещения'),
    ('1.2.643.5.1.13.13.11.1053', 'Группы инвалидности'),
    ('1.2.643.5.1.13.13.11.1054', 'Степень выраженности ограничений категорий жизнедеятельности человека'),
    ('1.2.643.5.1.13.13.11.1055', 'Основные виды стойких расстройств функций организма человека, обусловленных заболеваниями, последствиями травм или дефектами'),
    ('1.2.643.5.1.13.13.11.1056', 'Степень выраженности стойких расстройств функций организма человека'),
    ('1.2.643.5.1.13.13.11.1057', 'Основные категории жизнедеятельности человека'),
    ('1.2.643.5.1.13.13.11.1058', 'Привычки и зависимости'),
    ('1.2.643.5.1.13.13.11.1059', 'Потенциально-опасные для здоровья социальные факторы'),
    ('1.2.643.5.1.13.13.11.1060', 'Перечень вредных и (или) опасных производственных факторов и работ, при выполнении которых проводятся обязательные предварительные и периодические медицинские осмотры (обследования)'),
    ('1.2.643.5.1.13.13.11.1061', 'Группы крови для учета сигнальной информации о пациенте'),
    ('1.2.643.5.1.13.13.11.1062', 'Характер течения заболевания'),
    ('1.2.643.5.1.13.13.11.1063', 'Основные клинические проявления патологических реакций для сбора аллергоанамнеза'),
    ('1.2.643.5.1.13.13.11.1064', 'Тип патологической реакции для сбора аллергоанамнеза'),
    ('1.2.643.5.1.13.13.11.1066', 'Номенклатура специальностей специалистов, имеющих медицинское и фармацевтическое образование'),
    ('1.2.643.5.1.13.13.11.1069', 'Номенклатура коечного фонда медицинской организации'),
    ('1.2.643.5.1.13.13.11.1070', 'Номенклатура медицинских услуг'),
    ('1.2.643.5.1.13.13.11.1072', 'Перечень подразделений и кабинетов медицинской организации'),
    ('1.2.643.5.1.13.13.11.1077', 'Виды нозологических единиц диагноза'),
    ('1.2.643.5.1.13.13.11.1078', 'Иммунобиологические лекарственные препараты'),
    ('1.2.643.5.1.13.13.11.1079', 'Виды медицинских изделий, имплантируемых в организм человека, и иных устройств для пациентов с ограниченными возможностями'),
    ('1.2.643.5.1.13.13.11.1080', 'Федеральный справочник лабораторных исследований. Справочник лабораторных тестов'),
    ('1.2.643.5.1.13.13.11.1081', 'Федеральный справочник лабораторных исследований. Справочник лабораторных материалов и образцов'),
    ('1.2.643.5.1.13.13.11.1085', 'Группы диспансерного наблюдения в медицинских противотуберкулезных организациях'),
    ('1.2.643.5.1.13.13.11.1087', 'Федеральный справочник лабораторных исследований. Справочник бактерий'),
    ('1.2.643.5.1.13.13.11.1088', 'Федеральный справочник лабораторных исследований. Справочник грибов'),
    ('1.2.643.5.1.13.13.11.1117', 'Федеральный справочник лабораторных исследований. Группы лабораторных исследований'),
    ('1.2.643.5.1.13.13.11.1358', 'Единицы измерения'),
    ('1.2.643.5.1.13.13.11.1367', 'Действующие вещества лекарственных препаратов для медицинского применения, в том числе необходимых для льготного обеспечения граждан лекарственными средствами'),
    ('1.2.643.5.1.13.13.11.1386', 'Компоненты крови'),
    ('1.2.643.5.1.13.13.11.1388', 'Показания к гемотрансфузии'),
    ('1.2.643.5.1.13.13.11.1437', 'Федеральный справочник лабораторных исследований. Профили лабораторных исследований'),
    ('1.2.643.5.1.13.13.11.1461', 'Реестр медицинских и фармацевтических организаций Российской Федерации'),
    ('1.2.643.5.1.13.13.11.1466', 'Лекарственные формы лекарственных препаратов, в том числе необходимых для льготного обеспечения граждан лекарственными средствами'),
    ('1.2.643.5.1.13.13.11.1468', 'Пути введения лекарственных препаратов, в том числе для льготного обеспечения граждан лекарственными средствами'),
    ('1.2.643.5.1.13.13.11.1470', 'Исходы госпитализации'),
    ('1.2.643.5.1.13.13.11.1471', 'Федеральный справочник инструментальных диагностических исследований'),
    ('1.2.643.5.1.13.13.11.1473', 'Выявленные патологии'),
    ('1.2.643.5.1.13.13.11.1474', 'Причины инвалидности'),
    ('1.2.643.5.1.13.13.11.1475', 'Результаты индивидуальной программы реабилитации инвалидов'),
    ('1.2.643.5.1.13.13.11.1476', 'Федеральный справочник лабораторных исследований. Профили лабораторных исследований. Иерархическое представление'),
    ('1.2.643.5.1.13.13.11.1477', 'Анатомические локализации'),
    ('1.2.643.5.1.13.13.11.1485', 'Осложнения лечения онкологических заболеваний'),
    ('1.2.643.5.1.13.13.11.1486', 'Международная классификация болезней – Онкология (3 издание). Морфологические коды'),
    ('1.2.643.5.1.13.13.11.1487', 'Международная классификация болезней – Онкология (3 издание). Топографические коды'),
    ('1.2.643.5.1.13.13.11.1488', 'Степени тяжести осложнений хирургических операций'),
    ('1.2.643.5.1.13.13.11.1489', 'Алфавитный указатель к Международной статистической классификации болезней и проблем, связанных со здоровьем (10-й пересмотр, том 3)'),
    ('1.2.643.5.1.13.13.11.1490', 'Период, в течение которого гражданин находился на инвалидности на дату направления на медико - социальную экспертизу'),
    ('1.2.643.5.1.13.13.11.1491', 'Исходы заболеваний'),
    ('1.2.643.5.1.13.13.11.1492', 'Типы телосложения'),
    ('1.2.643.5.1.13.13.11.1493', 'Виды высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1495', 'Кратность питания'),
    ('1.2.643.5.1.13.13.11.1496', 'Пути поступления пациента на госпитализацию'),
    ('1.2.643.5.1.13.13.11.1497', 'Причины отказов в госпитализации'),
    ('1.2.643.5.1.13.13.11.1498', 'Уровни образования'),
    ('1.2.643.5.1.13.13.11.1502', 'Качество препарата для цитологического исследования'),
    ('1.2.643.5.1.13.13.11.1503', 'Срочность оперативного вмешательства'),
    ('1.2.643.5.1.13.13.11.1504', 'Тип адреса пациента'),
    ('1.2.643.5.1.13.13.11.1505', 'Тип мазка, полученного при гинекологическом осмотре, скрининге'),
    ('1.2.643.5.1.13.13.11.1506', 'Цели проведения врачебной комиссии (консилиума врачей)'),
    ('1.2.643.5.1.13.13.11.1508', 'Форма проведения консилиума врачей (врачебной комиссии)'),
    ('1.2.643.5.1.13.13.11.1509', 'Цитологические признаки патологии материала, полученного при гинекологическом осмотре, скрининге'),
    ('1.2.643.5.1.13.13.11.1510', 'Способ получения биологического материала для цитологического исследования'),
    ('1.2.643.5.1.13.13.11.1512', 'Решение о госпитализации в медицинскую организацию при поступлении'),
    ('1.2.643.5.1.13.13.11.1514', 'Перечень клинических шкал и опросников'),
    ('1.2.643.5.1.13.13.11.1515', 'Параметры клинических шкал и опросников'),
    ('1.2.643.5.1.13.13.11.1516', 'Интерпретация результатов оценки по клиническим шкалам и опросникам'),
    ('1.2.643.5.1.13.13.11.1518', 'Методы лечения онкологических заболеваний'),
    ('1.2.643.5.1.13.13.11.1520', 'Электронные медицинские документы'),
    ('1.2.643.5.1.13.13.11.1522', 'Виды медицинской документации'),
    ('1.2.643.5.1.13.13.11.1523', 'Оценка тонов сердца пациента'),
    ('1.2.643.5.1.13.13.11.1524', 'Повод, по которому поступил вызов бригады скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1525', 'Порядок вызова скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1526', 'Место получения вызова бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1527', 'Причины выезда бригады скорой медицинской помощи с опозданием'),
    ('1.2.643.5.1.13.13.11.1528', 'Место вызова бригады скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1529', 'Причина несчастного случая при вызове скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1530', 'Оценка поведения пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1531', 'Оценка сознания пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1532', 'Оценка зрачков пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1533', 'Оценка кожных покровов пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1534', 'Оценка дыхания пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1535', 'Оценка хрипов у пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1536', 'Оценка шумов сердца пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1537', 'Оценка пульса пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1538', 'Описание языка пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1539', 'Описание живота пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1540', 'Оценка размеров печени пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1541', 'Осложнения диагноза (сопутствующих заболеваний и/или состояний) при оказании скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1542', 'Оценка эффективности мероприятий при осложнении, проводимых бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1543', 'Результат оказания скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1544', 'Способ доставки больного в автомобиль скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1545', 'Результат выполненного выезда бригады скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1546', 'Причины безрезультатного выезда бригады скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1547', 'Описание характера одышки у пациента бригадой скорой медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1548', 'Этапы медицинской реабилитации'),
    ('1.2.643.5.1.13.13.11.1549', 'Категории сложности прижизненного патолого-анатомического исследования биопсийного (операционного) материала'),
    ('1.2.643.5.1.13.13.11.1550', 'Категории сложности патолого-анатомического вскрытия'),
    ('1.2.643.5.1.13.13.11.1551', 'Формы оказания медицинской помощи'),
    ('1.2.643.5.1.13.13.11.1554', 'Этапы талона на оказание высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.13', 'Типы лабораторных исследований'),
    ('1.2.643.5.1.13.13.99.2.14', 'Родственные и иные связи'),
    ('1.2.643.5.1.13.13.99.2.15', 'Семейное положение'),
    ('1.2.643.5.1.13.13.99.2.16', 'Классификатор образования для медицинских свидетельств'),
    ('1.2.643.5.1.13.13.99.2.17', 'Занятость'),
    ('1.2.643.5.1.13.13.99.2.18', 'Доношенность новорожденного'),
    ('1.2.643.5.1.13.13.99.2.19', 'Вид медицинского свидетельства о смерти'),
    ('1.2.643.5.1.13.13.99.2.20', 'Типы мест наступления смерти'),
    ('1.2.643.5.1.13.13.99.2.21', 'Род причины смерти'),
    ('1.2.643.5.1.13.13.99.2.22', 'Тип медицинского работника, установившего причины смерти'),
    ('1.2.643.5.1.13.13.99.2.23', 'Основания для установления причины смерти'),
    ('1.2.643.5.1.13.13.99.2.24', 'Связь смерти с ДТП'),
    ('1.2.643.5.1.13.13.99.2.25', 'Связь смерти с беременностью'),
    ('1.2.643.5.1.13.13.99.2.26', 'Справочник медицинских ограничений к управлению ТС'),
    ('1.2.643.5.1.13.13.99.2.27', 'Справочник медицинских показаний к управлению ТС'),
    ('1.2.643.5.1.13.13.99.2.28', 'Справочник наличия медицинских показаний, ограничений и противопоказаний к управлению ТС'),
    ('1.2.643.5.1.13.13.99.2.30', 'Тип места рождения ребёнка'),
    ('1.2.643.5.1.13.13.99.2.31', 'Тип родов (плодность)'),
    ('1.2.643.5.1.13.13.99.2.32', 'Тип лица, принимавшего роды'),
    ('1.2.643.5.1.13.13.99.2.33', 'Способ получения биопсийного (операционного) материала для прижизненного патолого-анатомического исследования'),
    ('1.2.643.5.1.13.13.99.2.34', 'Характер патологического процесса в биопсийном (операционном) материале для прижизненного патолого-анатомического исследования'),
    ('1.2.643.5.1.13.13.99.2.35', 'Виды окрасок, реакций, определений для патолого-анатомических исследований'),
    ('1.2.643.5.1.13.13.99.2.39', 'Порядок выполнения прижизненных патолого-анатомических исследований'),
    ('1.2.643.5.1.13.13.99.2.43', 'Льготные категории населения'),
    ('1.2.643.5.1.13.13.99.2.46', 'Цели телемедицинской консультации'),
    ('1.2.643.5.1.13.13.99.2.48', 'Документы, удостоверяющие личность'),
    ('1.2.643.5.1.13.13.99.2.65', 'ФРБТ. Классификатор клинических форм больных туберкулезом'),
    ('1.2.643.5.1.13.13.99.2.70', 'ФРБТ. Справочник значений бактериовыделения'),
    ('1.2.643.5.1.13.13.99.2.71', 'ФРБТ. Классификатор методов выявления туберкулеза'),
    ('1.2.643.5.1.13.13.99.2.80', 'ФРБТ. Классификатор фаз заболевания больных туберкулезом'),
    ('1.2.643.5.1.13.13.99.2.83', 'ФРБТ. Классификатор локализации туберкулеза'),
    ('1.2.643.5.1.13.13.99.2.94', 'ФРБТ. Классификатор исходов курса химиотерапии при лечении туберкулеза'),
    ('1.2.643.5.1.13.13.99.2.114', 'ФРМО. Справочник структурных подразделений'),
    ('1.2.643.5.1.13.13.99.2.115', 'ФРМО. Справочник отделений и кабинетов'),
    ('1.2.643.5.1.13.13.99.2.127', 'РР. Локализации отдаленных метастазов (при IV стадии заболевания)'),
    ('1.2.643.5.1.13.13.99.2.128', 'РР. Методы подтверждения диагноза'),
    ('1.2.643.5.1.13.13.99.2.129', 'РР. Обстоятельства выявления опухоли'),
    ('1.2.643.5.1.13.13.99.2.131', 'РР. Причины незавершенности радикального лечения'),
    ('1.2.643.5.1.13.13.99.2.132', 'РР. Способы облучения, применяющиеся при лучевой терапии злокачественных новообразований'),
    ('1.2.643.5.1.13.13.99.2.133', 'РР. Виды лучевой терапии, применяющиеся при лечении злокачественных новообразований'),
    ('1.2.643.5.1.13.13.99.2.134', 'РР. Методы лучевой терапии, применяющиеся при лечении злокачественных новообразований'),
    ('1.2.643.5.1.13.13.99.2.135', 'РР. Радиомодификаторы, применяющиеся при лучевой терапии злокачественных новообразований'),
    ('1.2.643.5.1.13.13.99.2.136', 'РР. Этапы лечения злокачественных новообразований'),
    ('1.2.643.5.1.13.13.99.2.140', 'РР. Обстоятельства взятия на диспансерный учет'),
    ('1.2.643.5.1.13.13.99.2.141', 'РР. Виды первично-множественных опухолей'),
    ('1.2.643.5.1.13.13.99.2.142', 'РР. Сведения о проведении аутопсии'),
    ('1.2.643.5.1.13.13.99.2.144', 'РР. Причины поздней диагностики онкологического заболевания'),
    ('1.2.643.5.1.13.13.99.2.146', 'РР. Клинические группы больных злокачественными новообразованиями'),
    ('1.2.643.5.1.13.13.99.2.147', 'Цели направления на медико-социальную экспертизу'),
    ('1.2.643.5.1.13.13.99.2.148', 'Оценки состояния для медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.150', 'РР. Причины снятия с диспансерного учета'),
    ('1.2.643.5.1.13.13.99.2.166', 'Кодируемые поля CDA документов'),
    ('1.2.643.5.1.13.13.99.2.176', 'СКЛ. Справочник профилей санаторно-курортных организаций'),
    ('1.2.643.5.1.13.13.99.2.183', 'ФОМС F002 Единый реестр страховых медицинских организаций, осуществляющих деятельность в сфере обязательного медицинского страхования'),
    ('1.2.643.5.1.13.13.99.2.186', 'Регистр органов управления здравоохранением по субъектам Российской Федерации'),
    ('1.2.643.5.1.13.13.99.2.197', 'Секции электронных медицинских документов'),
    ('1.2.643.5.1.13.13.99.2.206', 'Субъекты Российской Федерации'),
    ('1.2.643.5.1.13.13.99.2.216', 'ФРБТ. Классификатор методов тестирования на множественную лекарственную устойчивость'),
    ('1.2.643.5.1.13.13.99.2.240', 'Трансплантология. Типы родственной связи'),
    ('1.2.643.5.1.13.13.99.2.254', 'Степень тяжести осложнений при гемотрансфузии'),
    ('1.2.643.5.1.13.13.99.2.255', 'Виды реакции и (или) осложнения у реципиентов, обусловленные трансфузией'),
    ('1.2.643.5.1.13.13.99.2.256', 'Срочность госпитализации'),
    ('1.2.643.5.1.13.13.99.2.257', 'Справочник кодов интерпретации результатов'),
    ('1.2.643.5.1.13.13.99.2.258', 'Справочник приоритетов'),
    ('1.2.643.5.1.13.13.99.2.259', 'Справочник типов инструментальных исследований'),
    ('1.2.643.5.1.13.13.99.2.262', 'Витальные параметры'),
    ('1.2.643.5.1.13.13.99.2.267', 'Шаблоны CDA документов'),
    ('1.2.643.5.1.13.13.99.2.279', 'Исходы беременности'),
    ('1.2.643.5.1.13.13.99.2.285', 'Уровень конфиденциальности медицинского документа'),
    ('1.2.643.5.1.13.13.99.2.286', 'Причины отсутствия информации (NullFlavor)'),
    ('1.2.643.5.1.13.13.99.2.287', 'Категории и подкатегории транспортных средств'),
    ('1.2.643.5.1.13.13.99.2.289', 'ФРМО. Номенклатура медицинских организаций по виду медицинской деятельности'),
    ('1.2.643.5.1.13.13.99.2.291', 'ФРМО. Профиль бригады скорой помощи'),
    ('1.2.643.5.1.13.13.99.2.307', 'Исходы случаев госпитализации'),
    ('1.2.643.5.1.13.13.99.2.313', 'Документы, удостоверяющие полномочия законного (уполномоченного) представителя'),
    ('1.2.643.5.1.13.13.99.2.314', 'Отношение к воинской обязанности'),
    ('1.2.643.5.1.13.13.99.2.315', 'Категории гражданства'),
    ('1.2.643.5.1.13.13.99.2.322', 'Условия оказания медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.325', 'Срок, на который установлена степень утраты профессиональной трудоспособности'),
    ('1.2.643.5.1.13.13.99.2.350', 'Статус выполнения медицинской услуги'),
    ('1.2.643.5.1.13.13.99.2.354', 'Статусы семейного состояния'),
    ('1.2.643.5.1.13.13.99.2.358', 'Срок, на который установлена инвалидность'),
    ('1.2.643.5.1.13.13.99.2.360', 'Модели пациента при оказании высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.368', 'Роли сотрудников при подписании медицинских документов, в том числе в электронном виде'),
    ('1.2.643.5.1.13.13.99.2.390', 'Состояния алкогольного, наркотического и ненаркотического опьянения'),
    ('1.2.643.5.1.13.13.99.2.398', 'Классификатор нарушений состояния здоровья несовершеннолетнего'),
    ('1.2.643.5.1.13.13.99.2.408', 'Время доставки больного в стационар от начала заболевания (получения травмы)'),
    ('1.2.643.5.1.13.13.99.2.412', 'ДС. Заболевания по инвалидности'),
    ('1.2.643.5.1.13.13.99.2.425', 'Сроки постановки на учет по поводу беременности'),
    ('1.2.643.5.1.13.13.99.2.432', 'Номенклатура медицинских организаций'),
    ('1.2.643.5.1.13.13.99.2.437', 'Способы родоразрешения'),
    ('1.2.643.5.1.13.13.99.2.449', 'Классификатор осложнений операции'),
    ('1.2.643.5.1.13.13.99.2.452', 'Изделия медицинского назначения и медицинской техники'),
    ('1.2.643.5.1.13.13.99.2.513', 'Справочник типов медицинских изделий по классификации Росздравнадзора'),
    ('1.2.643.5.1.13.13.99.2.520', 'ДС. Ведомства'),
    ('1.2.643.5.1.13.13.99.2.526', 'Медицинские группы для занятий физической культурой'),
    ('1.2.643.5.1.13.13.99.2.527', 'ДС. Диспансерное наблюдение'),
    ('1.2.643.5.1.13.13.99.2.528', 'ДС. Типы организаций для лечения'),
    ('1.2.643.5.1.13.13.99.2.530', 'Типы инвалидности'),
    ('1.2.643.5.1.13.13.99.2.536', 'Местонахождение и причины выбытия несовершеннолетнего из стационара'),
    ('1.2.643.5.1.13.13.99.2.538', 'Справочник регистрационных удостоверений и моделей по классификации Росздравнадзора'),
    ('1.2.643.5.1.13.13.99.2.540', 'Лекарственные препараты. Товарные позиции. ЕСКЛП с кодами КТРУ'),
    ('1.2.643.5.1.13.13.99.2.541', 'Льготные категории граждан'),
    ('1.2.643.5.1.13.13.99.2.545', 'Страны мира'),
    ('1.2.643.5.1.13.13.99.2.546', 'TNM. Стадирование злокачественных опухолей'),
    ('1.2.643.5.1.13.13.99.2.547', 'TNM. Описание категорий'),
    ('1.2.643.5.1.13.13.99.2.549', 'Начало родовой деятельности'),
    ('1.2.643.5.1.13.13.99.2.552', 'Положение плода'),
    ('1.2.643.5.1.13.13.99.2.553', 'Предлежание плода'),
    ('1.2.643.5.1.13.13.99.2.555', 'Тип телемедицинской консультации'),
    ('1.2.643.5.1.13.13.99.2.566', 'Состав схем противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.569', 'ДС. Выполнение индивидуальной программы реабилитации'),
    ('1.2.643.5.1.13.13.99.2.581', 'Результат направления на высокотехнологичную медицинскую помощь'),
    ('1.2.643.5.1.13.13.99.2.582', 'ВИМИС. Рекомендации по результатам оказания высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.589', 'Наступление летального исхода относительно времени родов'),
    ('1.2.643.5.1.13.13.99.2.590', 'Исход родов'),
    ('1.2.643.5.1.13.13.99.2.603', 'ФРЛЛО. Справочник специализированного питания'),
    ('1.2.643.5.1.13.13.99.2.604', 'ФРЛЛО. Справочник медицинских изделий согласно каталогу товаров, работ, услуг для обеспечения государственных и муниципальных нужд'),
    ('1.2.643.5.1.13.13.99.2.605', 'Виды предоставляемых льгот'),
    ('1.2.643.5.1.13.13.99.2.608', 'Срок действия рецепта'),
    ('1.2.643.5.1.13.13.99.2.609', 'Приоритет исполнения рецепта'),
    ('1.2.643.5.1.13.13.99.2.611', 'Узлы СМНН. ЕСКЛП'),
    ('1.2.643.5.1.13.13.99.2.612', 'ЕСКЛП. Потребительские единицы измерения'),
    ('1.2.643.5.1.13.13.99.2.619', 'Реакции на ввод вакцины'),
    ('1.2.643.5.1.13.13.99.2.620', 'Виды лечения пациента при оказании высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.621', 'Методы лечения пациента при оказании высокотехнологичной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.637', 'Отсроченное обслуживание'),
    ('1.2.643.5.1.13.13.99.2.638', 'Реестр руководств по реализации структурированных электронных медицинских документов и протоколов информационного взаимодействия'),
    ('1.2.643.5.1.13.13.99.2.647', 'Схемы противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.651', 'Тип назначений льготного рецепта'),
    ('1.2.643.5.1.13.13.99.2.654', 'Причины отказа отпуска'),
    ('1.2.643.5.1.13.13.99.2.663', 'ВИМИС. Место наблюдения беременной'),
    ('1.2.643.5.1.13.13.99.2.667', 'ВИМИС. Расположение плаценты по отношению к рубцу на матке'),
    ('1.2.643.5.1.13.13.99.2.668', 'ВИМИС. Расположение плаценты по отношению к шейке матки'),
    ('1.2.643.5.1.13.13.99.2.680', 'ВИМИС. Форма таза у беременной'),
    ('1.2.643.5.1.13.13.99.2.683', 'Кварталы, месяцы и времена года'),
    ('1.2.643.5.1.13.13.99.2.684', 'Климатические факторы в месте проживания'),
    ('1.2.643.5.1.13.13.99.2.685', 'Климаты в месте проживания'),
    ('1.2.643.5.1.13.13.99.2.686', 'Дополнительная запись к причине инвалидности'),
    ('1.2.643.5.1.13.13.99.2.687', 'Периодичность отпуска лекарственных препаратов'),
    ('1.2.643.5.1.13.13.99.2.688', 'Способ выявления заболевания'),
    ('1.2.643.5.1.13.13.99.2.692', 'Алфавитный указатель к Международной статистической классификации болезней и проблем, связанных со здоровьем (10-й пересмотр, том 3, внешние причины заболеваемости и смертности)'),
    ('1.2.643.5.1.13.13.99.2.700', 'Должности медицинских работников для ФРМСС'),
    ('1.2.643.5.1.13.13.99.2.706', 'СМП. Справочник видов работ и услуг'),
    ('1.2.643.5.1.13.13.99.2.713', 'Сопоставление кодов льгот с льготными категориями граждан из Федерального закона №178-ФЗ'),
    ('1.2.643.5.1.13.13.99.2.719', 'СМП. Типы оповещения пациента'),
    ('1.2.643.5.1.13.13.99.2.723', 'Типы медицинских карт'),
    ('1.2.643.5.1.13.13.99.2.724', 'Типы документов оснований'),
    ('1.2.643.5.1.13.13.99.2.725', 'Перечень заключений в медицинских документах'),
    ('1.2.643.5.1.13.13.99.2.726', 'Типы документированных событий'),
    ('1.2.643.5.1.13.13.99.2.727', 'ВИМИС. Виды острого коронарного синдрома'),
    ('1.2.643.5.1.13.13.99.2.731', 'ВИМИС. Виды инфаркта миокарда на основании последующих изменений на ЭКГ'),
    ('1.2.643.5.1.13.13.99.2.735', 'ВИМИС. Типы инфаркта миокарда'),
    ('1.2.643.5.1.13.13.99.2.736', 'ВИМИС. Категории риска неблагоприятного исхода при остром коронарном синдроме без подъема сегмента ST'),
    ('1.2.643.5.1.13.13.99.2.739', 'ВИМИС. Виды инфаркта миокарда на основании наличия инфаркта миокарда в анамнезе'),
    ('1.2.643.5.1.13.13.99.2.743', 'Методы химико-токсикологических исследований'),
    ('1.2.643.5.1.13.13.99.2.750', 'Круг добра. Типы заявлений'),
    ('1.2.643.5.1.13.13.99.2.762', 'FIGO. Классификация для стадирования злокачественных опухолей в акушерстве и гинекологии'),
    ('1.2.643.5.1.13.13.99.2.764', 'Общероссийский классификатор информации о населении: образовательные учреждения'),
    ('1.2.643.5.1.13.13.99.2.765', 'Медицинские группы для занятий несовершеннолетними физической культурой'),
    ('1.2.643.5.1.13.13.99.2.766', 'Группы здоровья'),
    ('1.2.643.5.1.13.13.99.2.768', 'ВИМИС. Респираторная поддержка'),
    ('1.2.643.5.1.13.13.99.2.770', 'Источник медицинской информации'),
    ('1.2.643.5.1.13.13.99.2.771', 'Оценка полового развития'),
    ('1.2.643.5.1.13.13.99.2.772', 'Наличие инвалидности'),
    ('1.2.643.5.1.13.13.99.2.773', 'Цели прохождения медицинского освидетельствования в психоневрологическом диспансере'),
    ('1.2.643.5.1.13.13.99.2.778', 'Латеральность'),
    ('1.2.643.5.1.13.13.99.2.779', 'Тип искусственной вентиляции легких'),
    ('1.2.643.5.1.13.13.99.2.780', 'Тип консилиума врачей'),
    ('1.2.643.5.1.13.13.99.2.781', 'Характеристики противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.782', 'Циклы противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.783', 'Область воздействия лучевой терапии'),
    ('1.2.643.5.1.13.13.99.2.784', 'Поводы обращения при онкологических заболеваниях'),
    ('1.2.643.5.1.13.13.99.2.785', 'Медицинские процедуры и манипуляции'),
    ('1.2.643.5.1.13.13.99.2.786', 'Противопоказания или отказы от методов лечения и диагностики'),
    ('1.2.643.5.1.13.13.99.2.795', 'Степень обоснованности диагноза'),
    ('1.2.643.5.1.13.13.99.2.797', 'Типы консультаций'),
    ('1.2.643.5.1.13.13.99.2.799', 'Федеральный справочник инструментальных диагностических исследований. Методы инструментальных исследований'),
    ('1.2.643.5.1.13.13.99.2.807', 'Методы окрашивания цитологических препаратов'),
    ('1.2.643.5.1.13.13.99.2.810', 'Группы нервно-психического развития детей в соответствии с эпикризными сроками'),
    ('1.2.643.5.1.13.13.99.2.811', 'Оценка нервно-психического развития детей'),
    ('1.2.643.5.1.13.13.99.2.812', 'Федеральный справочник хирургических операций'),
    ('1.2.643.5.1.13.13.99.2.814', 'Федеральный справочник хирургических операций. Тип доступа'),
    ('1.2.643.5.1.13.13.99.2.822', 'Мероприятия профилактического медицинского осмотра, диспансеризации и углубленной диспансеризации'),
    ('1.2.643.5.1.13.13.99.2.824', 'Этапы иммунизации в рамках национального календаря профилактических прививок и календаря профилактических прививок по эпидемическим показаниям'),
    ('1.2.643.5.1.13.13.99.2.830', 'Заболевание или другие обстоятельства, послужившие причиной смерти'),
    ('1.2.643.5.1.13.13.99.2.836', 'Сведения о возможных способах инфицирования'),
    ('1.2.643.5.1.13.13.99.2.837', 'ВИМИС. Сведения о наиболее вероятном источнике инфекции'),
    ('1.2.643.5.1.13.13.99.2.847', 'Медицинские услуги и дорогостоящее лечение'),
    ('1.2.643.5.1.13.13.99.2.848', 'Группы иммунобиологических препаратов для сертификата профилактических прививок'),
    ('1.2.643.5.1.13.13.99.2.855', 'Профессии рабочих и должностей служащих'),
    ('1.2.643.5.1.13.13.99.2.856', 'Местонахождение граждан для медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.857', 'Медицинские обследования для медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.858', 'Условия проживания'),
    ('1.2.643.5.1.13.13.99.2.859', 'Механизмы и пути передачи инфекционных заболеваний'),
    ('1.2.643.5.1.13.13.99.2.864', 'ВМП. Формы финансирования'),
    ('1.2.643.5.1.13.13.99.2.866', 'Причины возврата направления на медико-социальную экспертизу'),
    ('1.2.643.5.1.13.13.99.2.875', 'Виды противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.894', 'Место приобретения яда'),
    ('1.2.643.5.1.13.13.99.2.895', 'Место происшествия'),
    ('1.2.643.5.1.13.13.99.2.896', 'Обстоятельства отравления'),
    ('1.2.643.5.1.13.13.99.2.897', 'Характер отравления'),
    ('1.2.643.5.1.13.13.99.2.910', 'ВИМИC. Этапы операции кесарева сечения'),
    ('1.2.643.5.1.13.13.99.2.911', 'ВИМИC. Тип разреза матки при кесаревом сечении'),
    ('1.2.643.5.1.13.13.99.2.912', 'ВИМИC. Типы хирургических швов'),
    ('1.2.643.5.1.13.13.99.2.913', 'ВИМИC. Вид кожного разреза при кесаревом сечении'),
    ('1.2.643.5.1.13.13.99.2.914', 'ВИМИC. Особенности выполнения этапа хирургической операции'),
    ('1.2.643.5.1.13.13.99.2.915', 'ВИМИС. Значения клинических параметров, оцениваемых при родоразрешении'),
    ('1.2.643.5.1.13.13.99.2.916', 'Токсичные вещества, наиболее часто встречающиеся при острых отравлениях'),
    ('1.2.643.5.1.13.13.99.2.920', 'Тип медицинского работника, установившего диагноз острого отравления химической этиологии'),
    ('1.2.643.5.1.13.13.99.2.921', 'Результаты санаторно-курортного лечения'),
    ('1.2.643.5.1.13.13.99.2.939', 'Группы заболеваний и состояний для оплаты специализированной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.941', 'Виды врачебных подкомиссий'),
    ('1.2.643.5.1.13.13.99.2.943', 'МКФ. Негативная шкала для обозначения величины и выраженности нарушения'),
    ('1.2.643.5.1.13.13.99.2.946', 'МКФ. Реализация и потенциальная способность'),
    ('1.2.643.5.1.13.13.99.2.947', 'МКФ. Негативная и позитивная шкала, обозначающая степень выраженности фактора окружающей среды в виде барьера или облегчения'),
    ('1.2.643.5.1.13.13.99.2.948', 'Сопоставление кодов льгот с льготными категориями граждан'),
    ('1.2.643.5.1.13.13.99.2.950', 'Общероссийский классификатор занятий'),
    ('1.2.643.5.1.13.13.99.2.951', 'Международная классификация функционирования, ограничений жизнедеятельности и здоровья'),
    ('1.2.643.5.1.13.13.99.2.955', 'СКЛ. Обоснования необходимости сопровождения пациентов'),
    ('1.2.643.5.1.13.13.99.2.960', 'Поствакцинальные осложнения'),
    ('1.2.643.5.1.13.13.99.2.961', 'Перечень заболеваний, наличие которых дает право на обучение по основным общеобразовательным программам на дому'),
    ('1.2.643.5.1.13.13.99.2.966', 'Дефекты догоспитального этапа'),
    ('1.2.643.5.1.13.13.99.2.970', 'Предпочтительная форма проведения медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.971', 'Виды статистических карт выбывшего из стационара'),
    ('1.2.643.5.1.13.13.99.2.972', 'Дополнительные мероприятия профилактического медицинского осмотра и диспансеризации несовершеннолетних'),
    ('1.2.643.5.1.13.13.99.2.980', 'Качество изображения при трансторакальной эхокардиографии'),
    ('1.2.643.5.1.13.13.99.2.981', 'Факторы риска и другие патологические состояния и заболевания, выявленные при проведении профилактического медицинского осмотра (диспансеризации)'),
    ('1.2.643.5.1.13.13.99.2.982', 'Оценка физического развития несовершеннолетних в рамках профилактического медицинского осмотра'),
    ('1.2.643.5.1.13.13.99.2.983', 'Режимы работы аппарата УЗИ'),
    ('1.2.643.5.1.13.13.99.2.984', 'Ритмы сердца'),
    ('1.2.643.5.1.13.13.99.2.985', 'Идентификаторы классификации злокачественных опухолей'),
    ('1.2.643.5.1.13.13.99.2.991', 'Способ получения уведомления о проведении медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.998', 'Степени стенозов и недостаточностей клапанов сердца'),
    ('1.2.643.5.1.13.13.99.2.1008', 'Формы документов'),
    ('1.2.643.5.1.13.13.99.2.1009', 'Мероприятия по дополнительному обследованию гражданина'),
    ('1.2.643.5.1.13.13.99.2.1010', 'Основания для проведения медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.1011', 'Цели проведения медико-социальной экспертизы'),
    ('1.2.643.5.1.13.13.99.2.1012', 'Факторы, ограничивающие проведение реабилитационных мероприятий'),
    ('1.2.643.5.1.13.13.99.2.1013', 'Факторы риска проведения реабилитационных мероприятий'),
    ('1.2.643.5.1.13.13.99.2.1014', 'Реабилитационный потенциал'),
    ('1.2.643.5.1.13.13.99.2.1016', 'Двигательные режимы пациентов'),
    ('1.2.643.5.1.13.13.99.2.1017', 'Методы искусственного прерывания беременности'),
    ('1.2.643.5.1.13.13.99.2.1019', 'Федеральный справочник лабораторных исследований. Справочник лабораторных материалов'),
    ('1.2.643.5.1.13.13.99.2.1022', 'ВИМИС. Время пересечения пуповины после рождения'),
    ('1.2.643.5.1.13.13.99.2.1040', 'ВИМИС. Методы контрацепции'),
    ('1.2.643.5.1.13.13.99.2.1046', 'Степень дифференцировки опухоли'),
    ('1.2.643.5.1.13.13.99.2.1047', 'Молекулярные маркеры'),
    ('1.2.643.5.1.13.13.99.2.1049', 'ВМП. Профили медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.1054', 'ВИМИС. Продолжительность родов'),
    ('1.2.643.5.1.13.13.99.2.1057', 'Типы поступления пациента в стационарной медицинской карте'),
    ('1.2.643.5.1.13.13.99.2.1063', 'Причины отмены выполнения лабораторного исследования'),
    ('1.2.643.5.1.13.13.99.2.1064', 'Федеральный справочник лабораторных исследований. Уточнение места взятия материала'),
    ('1.2.643.5.1.13.13.99.2.1065', 'Способ взятия лабораторного материала'),
    ('1.2.643.5.1.13.13.99.2.1069', 'Классификация родов по сроку беременности'),
    ('1.2.643.5.1.13.13.99.2.1073', 'Биологические факторы, влияющие на проведение диагностических исследований'),
    ('1.2.643.5.1.13.13.99.2.1075', 'Характер изменения трудоспособности в результате обращения'),
    ('1.2.643.5.1.13.13.99.2.1079', 'Виды структурированных электронных медицинских документов'),
    ('1.2.643.5.1.13.13.99.2.1085', 'Дееспособность гражданина'),
    ('1.2.643.5.1.13.13.99.2.1088', 'Решения врачебной комиссии'),
    ('1.2.643.5.1.13.13.99.2.1091', 'МКФ. Уровни определителя'),
    ('1.2.643.5.1.13.13.99.2.1095', 'Антимикробные препараты, которые используются для определения чувствительности'),
    ('1.2.643.5.1.13.13.99.2.1096', 'Медицинские работники, участвующие в формировании документа "Лист назначений и их выполнение"'),
    ('1.2.643.5.1.13.13.99.2.1097', 'Тип передаваемого материала для прижизненного патолого-анатомического исследования'),
    ('1.2.643.5.1.13.13.99.2.1100', 'Характеристики физикальных исследований'),
    ('1.2.643.5.1.13.13.99.2.1102', 'Характеристики патологий'),
    ('1.2.643.5.1.13.13.99.2.1108', 'Сведения об итогах проведения профилактических прививок у несовершеннолетних'),
    ('1.2.643.5.1.13.13.99.2.1111', 'Группа заболеваний, для диагностики, профилактики и лечения которых используются иммунобиологические препараты'),
    ('1.2.643.5.1.13.13.99.2.1115', 'Сопоставление заключительного клинического и патолого-анатомического диагнозов'),
    ('1.2.643.5.1.13.13.99.2.1116', 'Ведущие клинические синдромы в процессе умирания'),
    ('1.2.643.5.1.13.13.99.2.1117', 'Назначения направления тела умершего в патолого-анатомическое отделение'),
    ('1.2.643.5.1.13.13.99.2.1120', 'Стандарты определения чувствительности микроорганизмов к антибактериальным препаратам'),
    ('1.2.643.5.1.13.13.99.2.1124', 'Показания к применению иммунобиологических лекарственных препаратов'),
    ('1.2.643.5.1.13.13.99.2.1126', 'Методы определения чувствительности к антимикробным препаратам'),
    ('1.2.643.5.1.13.13.99.2.1127', 'Соответствие лабораторных тестов, антимикробных препаратов и методов определения чувствительности'),
    ('1.2.643.5.1.13.13.99.2.1131', 'Дополнительные характеристики при определении чувствительности к антимикробным препаратам'),
    ('1.2.643.5.1.13.13.99.2.1132', 'Дополнительные лабораторные тесты, используемые в рамках микробиологического исследования'),
    ('1.2.643.5.1.13.13.99.2.1133', 'Лабораторные тесты на обнаружение микроорганизма в рамках микробиологического исследования'),
    ('1.2.643.5.1.13.13.99.2.1134', 'Способы извлечения плода при кесаревом сечении'),
    ('1.2.643.5.1.13.13.99.2.1142', 'Виды спорта'),
    ('1.2.643.5.1.13.13.99.2.1143', 'Типы допуска к участию в физкультурных и спортивных мероприятиях'),
    ('1.2.643.5.1.13.13.99.2.1144', 'Типы рекомендаций для пациентов при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1145', 'Тип программы при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1146', 'Технологические характеристики проведенного исследования при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1147', 'Расчетные показатели, оцениваемые при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1148', 'Параметры, оцениваемые при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1149', 'Клинически значимые события при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1150', 'Параметры для анализа данных при дистанционном наблюдении'),
    ('1.2.643.5.1.13.13.99.2.1152', 'Лабораторные тесты, используемые в рамках копрологического исследования'),
    ('1.2.643.5.1.13.13.99.2.1153', 'Дополнительные действия в рамках эндоскопического исследования'),
    ('1.2.643.5.1.13.13.99.2.1154', 'Этапы подготовки к эндоскопическому исследованию нижних отделов желудочно-кишечного тракта'),
    ('1.2.643.5.1.13.13.99.2.1155', 'Этапы анестезиологического пособия и оперативного вмешательства (операции)'),
    ('1.2.643.5.1.13.13.99.2.1156', 'Компоненты газовой смеси для ингаляционной анестезии'),
    ('1.2.643.5.1.13.13.99.2.1157', 'Классификация технических средств реабилитации (изделий) в рамках федерального перечня реабилитационных мероприятий, технических средств реабилитации и услуг, предоставляемых инвалиду'),
    ('1.2.643.5.1.13.13.99.2.1168', 'Терминологическая система Бетесда'),
    ('1.2.643.5.1.13.13.99.2.1169', 'Категории сложности клинического лабораторного исследования'),
    ('1.2.643.5.1.13.13.99.2.1170', 'Цели проведения лечения при злокачественном новообразовании'),
    ('1.2.643.5.1.13.13.99.2.1171', 'Технологии планирования (виртуального моделирования) в лучевой терапии'),
    ('1.2.643.5.1.13.13.99.2.1172', 'Способы подведения ионизирующего излучения'),
    ('1.2.643.5.1.13.13.99.2.1173', 'Технологии конформного облучения'),
    ('1.2.643.5.1.13.13.99.2.1174', 'Методы визуального контроля (верификации) при проведении лучевой терапии'),
    ('1.2.643.5.1.13.13.99.2.1175', 'Режимы проведения лучевой терапии'),
    ('1.2.643.5.1.13.13.99.2.1176', 'Радиоактивные источники'),
    ('1.2.643.5.1.13.13.99.2.1177', 'Виды брахитерапии'),
    ('1.2.643.5.1.13.13.99.2.1178', 'Способы внутривенного введения лекарственных препаратов'),
    ('1.2.643.5.1.13.13.99.2.1179', 'Виды интубации трахеи'),
    ('1.2.643.5.1.13.13.99.2.1180', 'Место пробуждения пациента в рамках проведения анестезиологического пособия'),
    ('1.2.643.5.1.13.13.99.2.1194', 'Причины задержки начала курса противоопухолевой лекарственной терапии'),
    ('1.2.643.5.1.13.13.99.2.1196', 'Параметры наркозного аппарата и аппарата искусственной вентиляции легких'),
    ('1.2.643.5.1.13.13.99.2.1197', 'Параметры гидробаланса'),
    ('1.2.643.5.1.13.13.99.2.1202', 'Виды телемедицинских технологий'),
    ('1.2.643.5.1.13.13.99.2.1223', 'Транспортная среда для передачи материала в рамках цитологического исследования'),
    ('1.2.643.5.1.13.13.99.2.1227', 'Федеральный справочник лабораторных исследований. Справочник вирусов'),
    ('1.2.643.5.1.13.13.99.2.1236', 'Медицинские показания к управлению самоходными машинами'),
    ('1.2.643.5.1.13.13.99.2.1237', 'Категории и подкатегории самоходных машин'),
    ('1.2.643.5.1.13.13.99.2.1238', 'Группы риска развития профессиональных заболеваний'),
    ('1.2.643.5.1.13.13.99.2.1240', 'Условия назначения незарегистрированного в Российской Федерации лекарственного препарата'),
    ('1.2.643.5.1.13.13.99.2.1241', 'Фазы химиотерапевтического лечения'),
    ('1.2.643.5.1.13.13.99.2.1242', 'Причины снятия бациллярного статуса'),
    ('1.2.643.5.1.13.13.99.2.1243', 'Профессиональные заболевания'),
    ('1.2.643.5.1.13.13.99.2.1246', 'Виды шунтов коронарных артерий'),
    ('1.2.643.5.1.13.13.99.2.1247', 'Типы коронарного кровоснабжения'),
    ('1.2.643.5.1.13.13.99.2.1249', 'Состояние опухолевого процесса при осмотре'),
    ('1.2.643.5.1.13.13.99.2.1251', 'Результат аутопсии'),
    ('1.2.643.5.1.13.13.99.2.1255', 'Программы финансирования'),
    ('1.2.643.5.1.13.13.99.2.1256', 'Обоснования (показания) в рамках направления на оказание медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.1257', 'Визуализация анатомических структур при проведении ультразвукового исследования'),
    ('1.2.643.5.1.13.13.99.2.1258', 'Рекомендации по результатам оказания высокотехнологичной медицинской помощи и специализированной медицинской помощи'),
    ('1.2.643.5.1.13.13.99.2.1259', 'Коды мер социальной защиты (поддержки) по категориям граждан, имеющим право на получение государственной социальной помощи в виде набора социальных услуг'),
    ('1.2.643.5.1.13.13.99.2.1263', 'СУПП. Причина досрочного прекращения санаторно-курортного лечения'),
    ('1.2.643.5.1.13.13.99.2.1269', 'Отношение к беременности'),
    ('1.2.643.5.1.13.13.99.2.1270', 'Госпитализация во время беременности'),
    ('1.2.643.5.1.13.13.99.2.1271', 'Госпитализация для родоразрешения'),
    ('1.2.643.5.1.13.13.99.2.1272', 'Заболевания беременной'),
    ('1.2.643.5.1.13.13.99.2.1273', 'Место завершения беременности'),
    ('1.2.643.5.1.13.13.99.2.1274', 'Время выявления экстрагенитальных заболеваний'),
    ('1.2.643.5.1.13.13.99.2.1275', 'Ответственность со стороны пациентки'),
    ('1.2.643.5.1.13.13.99.2.1276', 'Оперативные вмешательства'),
    ('1.2.643.5.1.13.13.99.2.1277', 'Осложнения и факторы риска в карте донесения о случае материнской смерти'),
    ('1.2.643.5.1.13.13.99.2.1282', 'Наименование экспертого совета'),
    ('1.2.643.5.1.13.13.99.2.1283', 'Вопросы поставленные перед экспертным советом'),
    ('1.2.643.5.1.13.13.99.2.1284', 'Статус лица подвергшегося воздействию радиации'),
    ('1.2.643.5.1.13.13.99.3.44', 'ФОМС F020. Справочник организаций, осуществляющих полномочия страховых медицинских организаций')
) AS v(oid, name)
ON CONFLICT (oid) DO UPDATE SET
    name = EXCLUDED.name,
    source_oid = EXCLUDED.source_oid,
    source_version = EXCLUDED.source_version,
    updated_at = now();

-- Справочник, выведенный из обращения новой редакцией 805, обязан уйти из реестра: иначе
-- подпись показывала бы наименование, которого в источнике уже нет. Редакция повторена
-- здесь намеренно — сид объявляет её сам, без опоры на умолчание колонки.
DELETE FROM dim_nsi_dictionary WHERE source_version <> '6.19';

-- Краткая подпись задаётся списком целиком: снятая из списка запись возвращается к
-- официальному наименованию, а не остаётся с прежним сокращением.
UPDATE dim_nsi_dictionary d
SET short_name = c.short_name,
    updated_at = now()
FROM (
    SELECT r.oid, c.short_name
    FROM dim_nsi_dictionary r
    LEFT JOIN (VALUES
        ('1.2.643.5.1.13.13.11.1005', 'МКБ-10')
    ) AS c(oid, short_name) ON c.oid = r.oid
) c
WHERE d.oid = c.oid AND d.short_name IS DISTINCT FROM c.short_name;

CREATE TABLE IF NOT EXISTS transactions (
    logid bigint PRIMARY KEY,
    dwh_id text,
    log_date timestamptz,
    msgid text,
    relates_to_msgid text,
    local_uid_semd text,
    emdr_id text,
    doc_number text,
    org_oid text,
    status text,
    message text,
    jid bigint,
    semd_code text,
    error_code text,
    creation_date timestamptz,
    loaded_at timestamptz DEFAULT now()
);

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS dwh_id text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS creation_date timestamptz;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS error_type text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS error_json_text text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS patient_name_masked text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS snils_masked text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS doctor_name text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS patient_hash text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS doctor_hash text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS message text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS jid_resolve_method text;
DO $$
DECLARE
    has_msgid boolean;
    has_source_norm boolean;
    has_message_id boolean;
    has_source_msgid boolean;
    msgid_has_data boolean := false;
BEGIN
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'msgid')
      INTO has_msgid;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'source_message_id_norm')
      INTO has_source_norm;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'message_id')
      INTO has_message_id;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'source_msgid')
      INTO has_source_msgid;

    IF has_msgid THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM public.transactions WHERE msgid IS NOT NULL LIMIT 1)'
          INTO msgid_has_data;
    END IF;

    IF NOT has_msgid AND has_source_norm THEN
        ALTER TABLE public.transactions RENAME COLUMN source_message_id_norm TO msgid;
    ELSIF has_msgid AND NOT msgid_has_data AND has_source_norm THEN
        ALTER TABLE public.transactions DROP COLUMN msgid;
        ALTER TABLE public.transactions RENAME COLUMN source_message_id_norm TO msgid;
    ELSIF NOT has_msgid AND has_message_id THEN
        ALTER TABLE public.transactions RENAME COLUMN message_id TO msgid;
    ELSIF has_msgid AND NOT msgid_has_data AND has_message_id THEN
        ALTER TABLE public.transactions DROP COLUMN msgid;
        ALTER TABLE public.transactions RENAME COLUMN message_id TO msgid;
    ELSE
        ALTER TABLE public.transactions ADD COLUMN IF NOT EXISTS msgid text;
        IF has_source_norm THEN
            EXECUTE 'UPDATE public.transactions SET msgid = source_message_id_norm WHERE msgid IS NULL AND source_message_id_norm IS NOT NULL';
        END IF;
        IF has_message_id THEN
            EXECUTE 'UPDATE public.transactions SET msgid = message_id WHERE msgid IS NULL AND message_id IS NOT NULL';
        END IF;
        IF has_source_msgid THEN
            EXECUTE 'UPDATE public.transactions SET msgid = NULLIF(regexp_replace(trim(both ''<>'' from btrim(source_msgid)), ''^urn:uuid:'', '''', ''i''), '''') WHERE msgid IS NULL AND source_msgid IS NOT NULL';
        END IF;
    END IF;
END $$;
ALTER TABLE transactions DROP COLUMN IF EXISTS source_msgid;
ALTER TABLE transactions DROP COLUMN IF EXISTS source_message_id_norm;
ALTER TABLE transactions DROP COLUMN IF EXISTS message_id;
DO $$
DECLARE
    has_relates_to_msgid boolean;
    has_xml_relates_to boolean;
    has_relates_to_id boolean;
    relates_to_has_data boolean := false;
BEGIN
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'relates_to_msgid')
      INTO has_relates_to_msgid;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'xml_relates_to_id')
      INTO has_xml_relates_to;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = 'transactions'
                     AND column_name = 'relates_to_id')
      INTO has_relates_to_id;

    IF has_relates_to_msgid THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM public.transactions WHERE relates_to_msgid IS NOT NULL LIMIT 1)'
          INTO relates_to_has_data;
    END IF;

    IF NOT has_relates_to_msgid AND has_xml_relates_to THEN
        ALTER TABLE public.transactions RENAME COLUMN xml_relates_to_id TO relates_to_msgid;
    ELSIF has_relates_to_msgid AND NOT relates_to_has_data AND has_xml_relates_to THEN
        ALTER TABLE public.transactions DROP COLUMN relates_to_msgid;
        ALTER TABLE public.transactions RENAME COLUMN xml_relates_to_id TO relates_to_msgid;
    ELSIF NOT has_relates_to_msgid AND has_relates_to_id THEN
        ALTER TABLE public.transactions RENAME COLUMN relates_to_id TO relates_to_msgid;
    ELSIF has_relates_to_msgid AND NOT relates_to_has_data AND has_relates_to_id THEN
        ALTER TABLE public.transactions DROP COLUMN relates_to_msgid;
        ALTER TABLE public.transactions RENAME COLUMN relates_to_id TO relates_to_msgid;
    ELSE
        ALTER TABLE public.transactions ADD COLUMN IF NOT EXISTS relates_to_msgid text;
        IF has_xml_relates_to THEN
            EXECUTE 'UPDATE public.transactions SET relates_to_msgid = xml_relates_to_id WHERE relates_to_msgid IS NULL AND xml_relates_to_id IS NOT NULL';
        END IF;
        IF has_relates_to_id THEN
            EXECUTE 'UPDATE public.transactions SET relates_to_msgid = relates_to_id WHERE relates_to_msgid IS NULL AND relates_to_id IS NOT NULL';
        END IF;
    END IF;
END $$;
ALTER TABLE transactions DROP COLUMN IF EXISTS relates_to_id;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'transactions'
                 AND column_name = 'xml_relates_to_id') THEN
        ALTER TABLE public.transactions DROP COLUMN xml_relates_to_id;
    END IF;
END $$;
-- transactions.processed_at (ELT now()) → loaded_at: «обработано IPS» — это бизнес-дата
-- ips_date (rpt_documents), а это поле фиксирует момент загрузки строки в ELT.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'transactions'
                 AND column_name = 'processed_at') THEN
        ALTER TABLE public.transactions RENAME COLUMN processed_at TO loaded_at;
    END IF;
END $$;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS loaded_at timestamptz DEFAULT now();
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source_action text;
-- Подсистема ЕГИСЗ ('РЭМД'|'ИЭМК'|NULL) — см. egisz_subsystem().
-- Переименование, а не пара «добавить + скопировать + удалить»: перенос значений
-- переписал бы каждую строку партиционированной таблицы, RENAME меняет только каталог.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'transactions'
                 AND column_name = 'contour')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = 'transactions'
                         AND column_name = 'egisz_subsystem') THEN
        ALTER TABLE public.transactions RENAME COLUMN contour TO egisz_subsystem;
    END IF;
END $$;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS egisz_subsystem text;
ALTER TABLE transactions DROP COLUMN IF EXISTS contour;
-- Правило связки ответа с документом.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS link_method text;
UPDATE public.transactions
SET link_method = NULL
WHERE egisz_subsystem = 'ИЭМК'
  AND dwh_id IS NULL
  AND link_method = 'message_registry_no_document';
-- Снятые реквизиты: callback_url дублировал LOGTEXT, semd_name всегда пуст
-- (наименование берётся из dim_semd_types), xml_jid потребителей не имеет.
ALTER TABLE transactions DROP COLUMN IF EXISTS callback_url;
ALTER TABLE transactions DROP COLUMN IF EXISTS semd_name;
ALTER TABLE transactions DROP COLUMN IF EXISTS xml_jid;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_dwh_id text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_local_uid text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_emdr_id text;
ALTER TABLE transactions DROP COLUMN IF EXISTS xml_relates_to_id;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_semd_code text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_doc_number text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_org_oid text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_error_code text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_message text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_raw_status text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_document_status text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_creation_date timestamptz;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_patient_name text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_snils text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_doctor_name text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_has_fault_marker boolean;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_has_register_response boolean;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_has_register_result boolean;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_has_processing_marker boolean;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_has_error_ilike boolean;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS xml_parsed_at timestamptz;

DROP TABLE IF EXISTS public.dim_exchangelog_refs CASCADE;

-- ============================================================================
-- Range partitioning (monthly) for monotonic time-series tables.
-- PK must include the partition key: PostgreSQL enforces UNIQUE/PK only when
-- the partition column is part of the constraint. logid / logid
-- remain globally unique in practice; composite keys preserve ON CONFLICT upserts.
-- ============================================================================

DO $$
DECLARE
    relkind "char";
BEGIN
    SELECT c.relkind
    INTO relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'exchangelog_raw';

    IF relkind IS NOT NULL AND relkind <> 'p' THEN
        UPDATE public.exchangelog_raw
        SET createdate = COALESCE(createdate, logdate, loaded_at, timestamptz '1970-01-01')
        WHERE createdate IS NULL;

        CREATE TABLE public.exchangelog_raw_partitioned (
            logid bigint NOT NULL,
            logdate timestamptz,
            createdate timestamptz NOT NULL DEFAULT now(),
            msgid text,
            logstate integer,
            logtext text,
            msgtext text,
            uri text,
            loaded_at timestamptz DEFAULT now(),
            PRIMARY KEY (logid, createdate)
        ) PARTITION BY RANGE (createdate);

        INSERT INTO public.exchangelog_raw_partitioned (
            logid, logdate, createdate, msgid, logstate, logtext, msgtext, uri, loaded_at
        )
        SELECT
            logid,
            logdate,
            COALESCE(createdate, logdate, loaded_at, timestamptz '1970-01-01'),
            msgid,
            logstate,
            logtext,
            msgtext,
            uri,
            loaded_at
        FROM public.exchangelog_raw;

        DROP TABLE public.exchangelog_raw;
        ALTER TABLE public.exchangelog_raw_partitioned RENAME TO exchangelog_raw;
    END IF;
END
$$;

DO $$
DECLARE
    relkind "char";
BEGIN
    SELECT c.relkind
    INTO relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'transactions';

    IF relkind IS NOT NULL AND relkind <> 'p' THEN
        UPDATE public.transactions
        SET log_date = COALESCE(log_date, loaded_at, creation_date, now())
        WHERE log_date IS NULL;

        CREATE TABLE public.transactions_partitioned (
            LIKE public.transactions INCLUDING DEFAULTS
        ) PARTITION BY RANGE (log_date);

        ALTER TABLE public.transactions_partitioned
            DROP CONSTRAINT IF EXISTS transactions_pkey;
        ALTER TABLE public.transactions_partitioned
            ADD PRIMARY KEY (logid, log_date);
        ALTER TABLE public.transactions_partitioned
            ALTER COLUMN log_date SET NOT NULL;

        INSERT INTO public.transactions_partitioned
        SELECT *
        FROM public.transactions;

        DROP TABLE public.transactions;
        ALTER TABLE public.transactions_partitioned RENAME TO transactions;
    END IF;
END
$$;

-- Обслуживание месячных партиций. Партиции создаются на окно назад и вперёд от текущего
-- месяца; DEFAULT-партиции нет намеренно: строка, осевшая в ней, запрещает последующее
-- создание партиции своего месяца, и накат схемы падает. Вместо неё — расчёт границ по
-- фактическому содержимому таблицы, чтобы окно всегда покрывало имеющиеся данные.
-- Вызывается накатом схемы и суточной задачей maintain_partitions.
--
-- Сетка месяцев считается в наивном UTC и приводится к timestamptz только в момент
-- выпуска границы: якорь сетки обязан быть константой. Неявное приведение наивного
-- значения к timestamptz берёт часовой пояс сессии, и один и тот же месяц получает
-- разную границу в зависимости от того, кто и откуда вызвал функцию, — соседние месяцы
-- перестают стыковаться (перекрытие ломает CREATE, зазор ломает вставку).
--
-- Перечень обслуживаемых таблиц берётся из системного каталога, а не задаётся списком:
-- каталог уже знает, что партиционировано по диапазону времени, и второй перечень
-- расходился бы с ним молча.
DROP FUNCTION IF EXISTS public.ensure_time_partitions(integer, integer);
CREATE OR REPLACE FUNCTION public.ensure_time_partitions(
    p_grid_months_back integer DEFAULT 12,
    p_grid_months_ahead integer DEFAULT 24
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    spec record;
    part_start timestamp;
    part_end timestamp;
    part_name text;
    window_start timestamp;
    window_end timestamp;
    data_start timestamp;
    data_end timestamp;
    created integer := 0;
BEGIN
    FOR spec IN
        SELECT c.relname AS table_name, a.attname AS key_column
        FROM pg_partitioned_table p
        JOIN pg_class c ON c.oid = p.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0]
        WHERE n.nspname = 'public'
          AND p.partstrat = 'r'
          AND p.partnatts = 1
          AND a.atttypid IN ('timestamptz'::regtype, 'timestamp'::regtype)
        ORDER BY c.relname
    LOOP
        window_start := date_trunc('month', timezone('UTC', now())) - (p_grid_months_back || ' months')::interval;
        window_end := date_trunc('month', timezone('UTC', now())) + (p_grid_months_ahead || ' months')::interval;

        -- Данные могут выходить за окно: без покрывающей партиции такая строка
        -- не вставится вовсе, поэтому окно расширяется до фактического диапазона.
        EXECUTE format(
            'SELECT date_trunc(''month'', timezone(''UTC'', min(%I))),'
            ' date_trunc(''month'', timezone(''UTC'', max(%I))) FROM public.%I',
            spec.key_column, spec.key_column, spec.table_name
        ) INTO data_start, data_end;

        window_start := LEAST(window_start, COALESCE(data_start, window_start));
        window_end := GREATEST(window_end, COALESCE(data_end, window_end));

        part_start := window_start;
        WHILE part_start <= window_end LOOP
            part_end := part_start + INTERVAL '1 month';
            part_name := format('%s_y%sm%s', spec.table_name,
                                to_char(part_start, 'YYYY'), to_char(part_start, 'MM'));
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relname = part_name
            ) THEN
                EXECUTE format(
                    'CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',
                    part_name, spec.table_name,
                    part_start AT TIME ZONE 'UTC', part_end AT TIME ZONE 'UTC'
                );
                created := created + 1;
            END IF;
            part_start := part_end;
        END LOOP;
    END LOOP;

    RETURN created;
END;
$$;

COMMENT ON FUNCTION public.ensure_time_partitions(integer, integer) IS
'Достраивает месячную сетку партиций всех таблиц public, партиционированных по диапазону '
'одного временного столбца. Границы месяцев считаются в наивном UTC: якорь сетки не должен '
'зависеть от часового пояса сессии. Параметры задают глубину подготовленной сетки назад и '
'вперёд от текущего месяца; функция только создаёт партиции — хранение не ограничивает '
'и ничего не удаляет.';

-- DEFAULT-партиции не используются: строки переносятся в месячные партиции,
-- DEFAULT-партиция отцепляется и удаляется после переноса строк. Отбор идёт по каталогу:
-- DEFAULT-партиция опознаётся своей границей, а не соглашением об имени.
DO $$
DECLARE
    spec record;
    moved bigint;
    data_start timestamp;
    data_end timestamp;
    part_start timestamp;
    part_end timestamp;
    part_name text;
BEGIN
    FOR spec IN
        SELECT parent.relname AS table_name,
               child.relname AS default_name,
               a.attname AS key_column
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = parent.relnamespace
        JOIN pg_partitioned_table p ON p.partrelid = parent.oid
        JOIN pg_attribute a ON a.attrelid = parent.oid AND a.attnum = p.partattrs[0]
        WHERE n.nspname = 'public'
          AND p.partstrat = 'r'
          AND p.partnatts = 1
          AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
    LOOP
        EXECUTE format(
            'SELECT count(*), date_trunc(''month'', timezone(''UTC'', min(%I))),'
            ' date_trunc(''month'', timezone(''UTC'', max(%I))) FROM public.%I',
            spec.key_column, spec.key_column, spec.default_name
        ) INTO moved, data_start, data_end;

        EXECUTE format('ALTER TABLE public.%I DETACH PARTITION public.%I',
                       spec.table_name, spec.default_name);

        IF moved > 0 AND data_start IS NOT NULL THEN
            -- Диапазон берётся из самой отцепленной таблицы: после DETACH её строк
            -- в родителе уже нет, и расчёт по родителю их не покроет.
            part_start := data_start;
            WHILE part_start <= data_end LOOP
                part_end := part_start + INTERVAL '1 month';
                part_name := format('%s_y%sm%s', spec.table_name,
                                    to_char(part_start, 'YYYY'), to_char(part_start, 'MM'));
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relname = part_name
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',
                        part_name, spec.table_name,
                        part_start AT TIME ZONE 'UTC', part_end AT TIME ZONE 'UTC'
                    );
                END IF;
                part_start := part_end;
            END LOOP;

            EXECUTE format('INSERT INTO public.%I SELECT * FROM public.%I',
                           spec.table_name, spec.default_name);
        END IF;

        EXECUTE format('DROP TABLE public.%I', spec.default_name);
    END LOOP;
END
$$;

SELECT public.ensure_time_partitions(12, 24);

-- msgid/logstate на raw не использовались ни одним запросом. createdate — ключ
-- партиционирования; logid — ключ watermark/transform (батч и lookback идут по LOGID,
-- без индекса на logid Postgres обходит все партиции на каждом JOIN).
DROP INDEX IF EXISTS idx_exchangelog_raw_msgid;
DROP INDEX IF EXISTS idx_exchangelog_raw_logstate;
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_createdate ON exchangelog_raw (createdate);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_logid ON exchangelog_raw (logid);
CREATE INDEX IF NOT EXISTS idx_documents_semd_code ON documents (semd_code);
CREATE INDEX IF NOT EXISTS idx_documents_local_uid ON documents (local_uid);
CREATE INDEX IF NOT EXISTS idx_documents_emdr_id ON documents (emdr_id);
-- Резолвинг callback→документ использует нормализованный emdr_id.
CREATE INDEX IF NOT EXISTS idx_documents_emdr_id_norm
    ON documents (lower(NULLIF(btrim(emdr_id), '')))
    WHERE NULLIF(btrim(emdr_id), '') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_last_callback_at ON documents (last_callback_at);
-- Членство в очереди обработки читает отметку первого ответа на любом моменте времени.
CREATE INDEX IF NOT EXISTS idx_documents_first_callback_at ON documents (first_callback_at);
-- Инкрементальное сопровождение document_attributes читает документы по updated_at.
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents (updated_at);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents (status);
CREATE INDEX IF NOT EXISTS idx_documents_jid ON documents (jid);
CREATE INDEX IF NOT EXISTS idx_documents_org_oid ON documents (org_oid) WHERE org_oid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_first_sent_at ON documents (first_sent_at);
CREATE INDEX IF NOT EXISTS idx_documents_document_created_at ON documents (document_created_at);
CREATE INDEX IF NOT EXISTS idx_documents_registered_at ON documents (registered_at);
DROP INDEX IF EXISTS idx_documents_callback_log_id;
CREATE INDEX IF NOT EXISTS idx_documents_result_logid ON documents (result_logid);
-- Слой версий: rpt по умолчанию фильтрует по is_current_version; transform пересобирает
-- группу по document_group_id для затронутых батчем экземпляров.
CREATE INDEX IF NOT EXISTS idx_documents_doc_number ON documents (doc_number);
CREATE INDEX IF NOT EXISTS idx_documents_group_id ON documents (document_group_id);
CREATE INDEX IF NOT EXISTS idx_documents_group_current
    ON documents (document_group_id, is_current_version);
CREATE INDEX IF NOT EXISTS idx_documents_is_current_version
    ON documents (is_current_version) WHERE is_current_version;
CREATE INDEX IF NOT EXISTS idx_dim_organizations_fir_oid
    ON dim_organizations (fir_oid)
    WHERE fir_oid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_inn
    ON dim_nsi_organization (inn)
    WHERE inn IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_ogrn
    ON dim_nsi_organization (ogrn)
    WHERE ogrn IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_active_mo
    ON dim_nsi_organization (inn, oid)
    WHERE delete_date IS NULL
      AND parent_id IS NULL
      AND oid LIKE '1.2.643.5.1.13.13.12.2.%';

-- Инициализация слоя версий: документ без группы получает singleton-группу.
UPDATE documents SET
    document_group_id         = COALESCE(document_group_id, dwh_id),
    document_group_confidence = COALESCE(document_group_confidence, 'singleton'),
    semd_version_number       = COALESCE(semd_version_number, 1),
    is_current_version        = COALESCE(is_current_version, true)
WHERE is_current_version IS NULL OR document_group_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_transactions_log_date ON transactions (log_date);
-- Составной ключ покрывает «последняя транзакция документа» (recompute_document_attributes
-- берёт её дважды на документ) и заменяет одиночный индекс по dwh_id.
DROP INDEX IF EXISTS idx_transactions_dwh_id;
CREATE INDEX IF NOT EXISTS idx_transactions_dwh_id_recent
    ON transactions (dwh_id, log_date DESC, logid DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_jid ON transactions (jid);
-- Ненормализованные дубли нормализованных ключей и индексы под снятые правила привязки:
-- связывание идёт через dim_message_document и documents.emdr_id, поиска по этим
-- колонкам в transactions больше нет. На партиционированной таблице каждый такой индекс
-- множится на число партиций и оплачивается при вставке.
DROP INDEX IF EXISTS idx_transactions_message_id;
DROP INDEX IF EXISTS idx_transactions_local_uid;
DROP INDEX IF EXISTS idx_transactions_local_uid_norm;
DROP INDEX IF EXISTS idx_transactions_emdr_id;
DROP INDEX IF EXISTS idx_transactions_relates_to;
DROP INDEX IF EXISTS idx_transactions_source_message_id_norm;
DROP INDEX IF EXISTS idx_transactions_xml_local_uid_norm;
DROP INDEX IF EXISTS idx_transactions_xml_emdr_id_norm;
CREATE INDEX IF NOT EXISTS idx_transactions_error_type ON transactions (error_type);
CREATE INDEX IF NOT EXISTS idx_transactions_patient_hash ON transactions (patient_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_doctor_hash ON transactions (doctor_hash);
-- Scoped semd backfill: DISTINCT ON (dwh_id) по последней транзакции с semd_code.
CREATE INDEX IF NOT EXISTS idx_transactions_dwh_id_semd
    ON transactions (dwh_id, log_date DESC, logid DESC)
    WHERE NULLIF(btrim(semd_code), '') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_licenses_jid ON dim_licenses (jid);
CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_uid ON dim_licenses (mo_uid);
CREATE INDEX IF NOT EXISTS idx_transactions_xml_dwh_id ON transactions (xml_dwh_id);
CREATE INDEX IF NOT EXISTS idx_transactions_xml_parsed_at ON transactions (xml_parsed_at);
-- Сигнал здоровья читает последние размеченные ответы по LOGID.
DROP INDEX IF EXISTS idx_transactions_link_method;
DROP INDEX IF EXISTS idx_transactions_link_method_loaded_at;
CREATE INDEX IF NOT EXISTS idx_transactions_link_method_logid
    ON transactions (link_method, logid DESC)
    WHERE link_method IS NOT NULL;
-- Индексы правил, не входящих в текущий контракт связывания.
DROP INDEX IF EXISTS idx_transactions_source_action_gdf;
DROP INDEX IF EXISTS idx_transactions_gdf_jid_logid;

-- Реестр подач: связь msgid→document_uid и подсчёт попыток подачи документа.
DROP INDEX IF EXISTS idx_dim_message_document_egmid;
CREATE INDEX IF NOT EXISTS idx_dim_message_document_msgid
    ON dim_message_document (msgid, source_egmid DESC)
    WHERE msgid IS NOT NULL;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_dim_message_document_uid'
          AND indexdef NOT ILIKE '%WHERE (document_uid IS NOT NULL)%'
    ) THEN
        DROP INDEX public.idx_dim_message_document_uid;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_dim_message_document_uid
    ON dim_message_document (document_uid)
    WHERE document_uid IS NOT NULL;

-- Инициализация отметки первого ответа по журналу ответов. Предикат самоограничен:
-- документ с ответом всегда несёт last_callback_at, поэтому после первого прогона строк
-- для заполнения не остаётся, а ожидающие ответа под него не подпадают. Документы,
-- ответы которых старше глубины хранения transactions, получают last_callback_at —
-- единственную известную отметку ответа.
UPDATE documents d
SET first_callback_at = LEAST(
        COALESCE(cb.first_callback_at, d.last_callback_at),
        COALESCE(d.last_callback_at, cb.first_callback_at)
    )
FROM (
    SELECT dwh_id, min(log_date) AS first_callback_at
    FROM transactions
    WHERE status IN ('success', 'error')
      AND NULLIF(btrim(dwh_id), '') IS NOT NULL
    GROUP BY dwh_id
) cb
WHERE cb.dwh_id = d.dwh_id
  AND d.first_callback_at IS NULL
  AND d.last_callback_at IS NOT NULL;

UPDATE documents
SET first_callback_at = last_callback_at
WHERE first_callback_at IS NULL
  AND last_callback_at IS NOT NULL;

-- Инициализация маркера попытки парсинга по распарсенным строкам transactions.
INSERT INTO exchangelog_parse_attempts (logid)
SELECT logid FROM transactions WHERE xml_parsed_at IS NOT NULL
ON CONFLICT (logid) DO NOTHING;

-- Статистика нужна планировщику анти-джойна parse_targets сразу после массового бэкфилла.
ANALYZE exchangelog_parse_attempts;
