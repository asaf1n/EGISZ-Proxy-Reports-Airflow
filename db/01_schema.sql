-- ============================================================================
-- 01_schema.sql — bootstrap, tables, partitions, indexes, dictionaries
-- Loaded by db/dwh_init.sql. Идемпотентен: повторный прогон не меняет состояние.
-- ============================================================================

-- ---------------------------------------------------------------- section: bootstrap
-- ============================================================================
-- 00_bootstrap.sql — заголовок, пояс роли, гранты.
-- Подключается из db/dwh_init.sql через \i db/01_schema.sql.
-- Идемпотентно; выполняется под ролью egisz (владелец dwh_egisz).
-- Контракт схемы — README.md §DWH-модель.
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

-- Пин пояса роли на МСК: наивное Firebird-время (EXCHANGELOG.CREATEDATE, лицензии) пишется
-- как timestamptz; без фиксированного пояса сессии сутки «уехали» бы на границе. Роль вправе
-- менять собственные параметры сессии, поэтому egisz выполняет это сам.
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
-- Контракт схемы — README.md §DWH-модель.
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

-- Слой версий/логического документа (README §«Версии и идентичность документа»).
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
'OID медицинской организации из НСИ; sync_dictionaries не заполняет это поле из JPERSONS.';
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

CREATE TABLE IF NOT EXISTS dim_semd_types (
    code text PRIMARY KEY,
    type_code text,
    name text NOT NULL,
    level text,
    format_code text,
    start_date date,
    end_date date,
    implementation_guide text,
    git_link text,
    oid text,
    version text,
    updated_at timestamptz DEFAULT now()
);

INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, git_link)
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
    ('43', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 2', '3', '2', DATE '2020-09-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/2933', '1.2.643.5.1.13.13.15.31.2'),
    ('44', '10', 'Выписной эпикриз из родильного дома (CDA) Редакция 2', '3', '2', DATE '2020-09-14', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/2925', '1.2.643.5.1.13.13.15.27.2'),
    ('45', '11', 'Протокол гемотрансфузии (CDA) Редакция 2', '3', '2', DATE '2020-09-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/2935', '1.2.643.5.1.13.13.15.24.2'),
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
    ('62', '86', 'Рецепт на лекарственный препарат (CDA) Редакция 1', '3', '2', DATE '2021-03-15', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3823', '1.2.643.5.1.13.13.15.3.1'),
    ('63', '45', 'Медицинское заключение об отсутствии медицинских противопоказаний к владению оружием (CDA) Редакция 1', '3', '2', DATE '2021-04-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3827', '1.2.643.5.1.13.13.15.41.1'),
    ('64', '46', 'Медицинское заключение об отсутствии в организме человека наркотических средств, психотропных веществ и их метаболитов (CDA) Редакция 1', '3', '2', DATE '2021-04-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3829', '1.2.643.5.1.13.13.15.42.1'),
    ('65', '47', 'Справка для получения путевки на санаторно-курортное лечение (CDA) Редакция 1', '3', '2', DATE '2021-04-12', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3831', '1.2.643.5.1.13.13.15.8.1'),
    ('66', '108', 'Протокол хирургической операции (PDF/A-1)', '0', '1', DATE '2021-04-06', NULL, NULL, NULL),
    ('67', '109', 'Протокол медицинской манипуляции (PDF/A1)', '0', '1', DATE '2021-04-06', DATE '2024-01-01', NULL, NULL),
    ('68', '5', 'Протокол консультации (CDA) Редакция 3', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3845', '1.2.643.5.1.13.13.15.13.3'),
    ('69', '11', 'Протокол гемотрансфузии (CDA) Редакция 3', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3847', '1.2.643.5.1.13.13.15.24.3'),
    ('70', '89', 'Справка о результатах химико-токсикологических исследований (CDA) Редакция 1', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3837', '1.2.643.5.1.13.13.15.19.1'),
    ('71', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2022-12-07', 'https://portal.egisz.rosminzdrav.ru/materials/3839', '1.2.643.5.1.13.13.15.54.1'),
    ('72', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 1', '3', '2', DATE '2021-04-16', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/3841', '1.2.643.5.1.13.13.15.56.1'),
    ('73', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 1', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3843', '1.2.643.5.1.13.13.15.57.1'),
    ('74', '12', 'Протокол прижизненного патологоанатомического исследования (CDA) Редакция 2', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3833', '1.2.643.5.1.13.13.15.21.2'),
    ('75', '7', 'Протокол лабораторного исследования (CDA) Редакция 4', '3', '2', DATE '2021-04-16', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3835', '1.2.643.5.1.13.13.15.18.4'),
    ('76', '33', 'Медицинское свидетельство о рождении (CDA) Редакция 4', '3', '2', DATE '2021-04-26', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3849', '1.2.643.5.1.13.13.15.39.4'),
    ('77', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 4', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3903', '1.2.643.5.1.13.13.15.25.4'),
    ('78', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3905', '1.2.643.5.1.13.13.15.68.1'),
    ('79', '142', 'Справка о прохождении медицинского освидетельствования в психоневрологическом диспансере (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3907', '1.2.643.5.1.13.13.15.59.1'),
    ('80', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 2', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3909', '1.2.643.5.1.13.13.15.56.2'),
    ('81', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 2', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3909', '1.2.643.5.1.13.13.15.58.2'),
    ('82', '69', 'Медицинское заключение о принадлежности несовершеннолетнего к медицинской группе для занятий физической культурой (CDA) Редакция 2', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.52.2'),
    ('83', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 2', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.54.2'),
    ('84', '91', 'Медицинская справка в бассейн (CDA) Редакция 2', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3911', '1.2.643.5.1.13.13.15.53.2'),
    ('85', '57', 'Направление на консультацию и во вспомогательные кабинеты (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3913', '1.2.643.5.1.13.13.15.32.1'),
    ('86', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3915', '1.2.643.5.1.13.13.15.67.1'),
    ('87', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3917', '1.2.643.5.1.13.13.15.44.1'),
    ('88', '56', 'Медицинская справка (для выезжающего за границу) (CDA) Редакция 1', '3', '2', DATE '2021-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3919', '1.2.643.5.1.13.13.15.48.1'),
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
    ('100', '52', 'Справка об оплате медицинских услуг для предоставления в налоговые органы Российской Федерации (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3991', '1.2.643.5.1.13.13.15.69.1'),
    ('101', '73', 'Медицинское заключение о допуске к выполнению работ на высоте, верхолазных работ, работ, связанных с подъемом на высоту, а также по обслуживанию подъемных сооружений (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3989', '1.2.643.5.1.13.13.15.55.1'),
    ('102', '344', 'Справка об отказе в направлении на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2021-11-04', DATE '2022-12-27', 'https://portal.egisz.rosminzdrav.ru/materials/3987', '1.2.643.5.1.13.13.15.6.1'),
    ('103', '51', 'Медицинское заключение по результатам предварительного (периодического) медицинского осмотра (обследования) (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3985', '1.2.643.5.1.13.13.15.47.1'),
    ('104', '59', 'Экстренное извещение об инфекционном заболевании, пищевом, остром профессиональном отравлении, необычной реакции на прививку (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3983', '1.2.643.5.1.13.13.15.70.1'),
    ('105', '53', 'Сертификат профилактических прививок (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3981', '1.2.643.5.1.13.13.15.46.1'),
    ('106', '343', 'Справка о постановке на учет по беременности (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3979', '1.2.643.5.1.13.13.15.60.1'),
    ('107', '66', 'Справка донору об освобождении от работы в день кровосдачи и предоставлении ему дополнительного дня отдыха (CDA) Редакция 1', '3', '2', DATE '2021-11-04', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3977', '1.2.643.5.1.13.13.15.49.1'),
    ('108', '352', 'Уведомление о причинах возврата направления на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4017', '1.2.643.5.1.13.13.15.7.1'),
    ('109', '34', 'Направление на медико-социальную экспертизу (CDA) Редакция 5', '3', '2', DATE '2022-01-01', DATE '2023-03-15', 'https://portal.egisz.rosminzdrav.ru/materials/4011', '1.2.643.5.1.13.13.15.4.5'),
    ('110', '6', 'Протокол инструментального исследования (CDA) Редакция 3', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4021', '1.2.643.5.1.13.13.15.17.3'),
    ('111', '85', 'Протокол консультации в рамках диспансерного наблюдения (CDA) Редакция 4', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4023', '1.2.643.5.1.13.13.15.14.4'),
    ('112', '37', 'Льготный рецепт на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 3', '3', '2', DATE '2021-12-27', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4025', '1.2.643.5.1.13.13.15.1.3'),
    ('113', '353', 'Документ, содержащий сведения медицинского свидетельства о смерти в бумажной форме (CDA) Редакция 5', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3815', '1.2.643.5.1.13.13.15.36.5'),
    ('114', '354', 'Документ, содержащий сведения медицинского свидетельства о перинатальной смерти в бумажной форме (CDA) Редакция 2', '3', '2', DATE '2021-03-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3817', '1.2.643.5.1.13.13.15.38.2'),
    ('115', '74', 'Карта вызова скорой медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-02-03', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4043', '1.2.643.5.1.13.13.15.72.2'),
    ('116', '362', 'Уведомление о выявлении противопоказаний или аннулировании медицинских заключений к владению оружием (CDA) Редакция 1', '3', '2', DATE '2022-02-15', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4049', '1.2.643.5.1.13.13.15.62.1'),
    ('117', '45', 'Медицинское заключение об отсутствии медицинских противопоказаний к владению оружием (CDA) Редакция 2', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4055', '1.2.643.5.1.13.13.15.41.2'),
    ('118', '33', 'Документ, содержащий сведения медицинского свидетельства о рождении в бумажной форме (CDA) Редакция 4', '3', '2', DATE '2021-02-21', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/3849', '1.2.643.5.1.13.13.15.39.4'),
    ('119', '5', 'Протокол консультации (CDA) Редакция 4', '3', '2', DATE '2022-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4023', '1.2.643.5.1.13.13.15.13.4'),
    ('120', '374', 'Согласие гражданина (его законного или уполномоченного представителя) на направление и проведение медико-социальной экспертизы (PDF/A-1)', '0', '1', DATE '2022-07-18', DATE '2023-03-29', NULL, NULL),
    ('121', '34', 'Направление на медико-социальную экспертизу (CDA) Редакция 6', '3', '2', DATE '2022-11-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4283', '1.2.643.5.1.13.13.15.4.6'),
    ('122', '141', 'Сведения о результатах диспансеризации или профилактического медицинского осмотра (CDA) Редакция 2', '3', '2', DATE '2023-01-01', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4099', '1.2.643.5.1.13.13.15.74.2'),
    ('123', '241', 'Направление на госпитализацию для оказания высокотехнологичной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-11-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4257', '1.2.643.5.1.13.13.15.33.2'),
    ('124', '346', 'Направление на госпитализацию для оказания специализированной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2022-11-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4255', '1.2.643.5.1.13.13.15.34.2'),
    ('125', '13', 'Медицинское свидетельство о смерти (CDA) Редакция 6', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4325', '1.2.643.5.1.13.13.15.35.6'),
    ('126', '353', 'Документ, содержащий сведения медицинского свидетельства о смерти в бумажной форме (CDA) Редакция 6', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4325', '1.2.643.5.1.13.13.15.36.6'),
    ('127', '14', 'Медицинское свидетельство о перинатальной смерти (CDA) Редакция 3', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4327', '1.2.643.5.1.13.13.15.37.3'),
    ('128', '354', 'Документ, содержащий сведения медицинского свидетельства о перинатальной смерти в бумажной форме (CDA) Редакция 3', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4327', '1.2.643.5.1.13.13.15.38.3'),
    ('129', '340', 'Эпикриз по результатам диспансеризации / профилактического медицинского осмотра (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4415', '1.2.643.5.1.13.13.15.28.1'),
    ('130', '352', 'Уведомление о причинах возврата направления на медико-социальную экспертизу в медицинскую организацию (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4329', '1.2.643.5.1.13.13.15.7.2'),
    ('131', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4311', '1.2.643.5.1.13.13.15.67.3'),
    ('132', '80', 'Талон на оказание высокотехнологичной медицинской помощи (CDA) Редакция 1', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4125', '1.2.643.5.1.13.13.15.73.1'),
    ('133', '351', 'Этапный эпикриз (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4115', '1.2.643.5.1.13.13.15.30.1'),
    ('134', '345', 'Предоперационный эпикриз (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4107', '1.2.643.5.1.13.13.15.29.1'),
    ('135', '350', 'Выписка из истории болезни (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4113', '1.2.643.5.1.13.13.15.61.1'),
    ('136', '72', 'Экстренное извещение о случае острого отравления химической этиологии (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4123', '1.2.643.5.1.13.13.15.71.1'),
    ('137', '48', 'Санаторно-курортная карта (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4117', '1.2.643.5.1.13.13.15.9.1'),
    ('138', '375', 'Программа дополнительного обследования гражданина (CDA) Редакция 1', '3', '2', DATE '2023-02-06', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4285', '1.2.643.5.1.13.13.15.40.1'),
    ('139', '89', 'Справка о результатах химико-токсикологических исследований (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4433', '1.2.643.5.1.13.13.15.19.2'),
    ('140', '38', 'Отпуск по рецепту на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4317', '1.2.643.5.1.13.13.15.2.4'),
    ('141', '37', 'Льготный рецепт на лекарственный препарат, изделие медицинского назначения и специализированный продукт лечебного питания (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4319', '1.2.643.5.1.13.13.15.1.4'),
    ('142', '368', 'Заключение об установлении факта поствакцинального осложнения (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4275', '1.2.643.5.1.13.13.15.64.1'),
    ('143', '367', 'Заключение лечебного учреждения о нуждаемости престарелого гражданина в постоянном постороннем уходе (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4273', '1.2.643.5.1.13.13.15.63.1'),
    ('144', '369', 'Справка о наличии показаний к протезированию (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4277', '1.2.643.5.1.13.13.15.65.1'),
    ('145', '370', 'Справка о наличии медицинских показаний, в соответствии с которыми ребенок не посещает дошкольную организацию или организацию, осуществляющую образовательную деятельность по основным общеобразовательным программам, в период учебного процесса (CDA) Редакция 1', '3', '2', DATE '2022-10-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4279', '1.2.643.5.1.13.13.15.66.1'),
    ('146', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 3', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4313', '1.2.643.5.1.13.13.15.68.3'),
    ('147', '1', 'Эпикриз в стационаре выписной (CDA) Редакция 5', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4417', '1.2.643.5.1.13.13.15.25.5'),
    ('148', '86', 'Рецепт на лекарственный препарат (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4321', '1.2.643.5.1.13.13.15.3.2'),
    ('149', '69', 'Медицинское заключение о принадлежности несовершеннолетнего к медицинской группе для занятий физической культурой (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4331', '1.2.643.5.1.13.13.15.52.3'),
    ('150', '91', 'Медицинская справка в бассейн (CDA) Редакция 3', '3', '2', DATE '2023-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4297', '1.2.643.5.1.13.13.15.53.3'),
    ('151', '47', 'Справка для получения путевки на санаторно-курортное лечение (CDA) Редакция 2', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4315', '1.2.643.5.1.13.13.15.8.2'),
    ('152', '71', 'Медицинское заключение об отсутствии противопоказаний к занятию определенными видами спорта (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4333', '1.2.643.5.1.13.13.15.54.3'),
    ('153', '56', 'Медицинская справка (для выезжающего за границу) (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4335', '1.2.643.5.1.13.13.15.48.2'),
    ('154', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 4', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4337', '1.2.643.5.1.13.13.15.56.4'),
    ('155', '67', 'Справка об отсутствии медицинских противопоказаний для работы с использованием сведений, составляющих государственную тайну (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4119', '1.2.643.5.1.13.13.15.50.1'),
    ('156', '68', 'Заключение о результатах медицинского освидетельствования граждан, намеревающихся усыновить (удочерить), взять под опеку (попечительство), в приемную или патронатную семью детей-сирот и детей, оставшихся без попечения родителей (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4121', '1.2.643.5.1.13.13.15.51.1'),
    ('157', '142', 'Справка о прохождении медицинского освидетельствования в психоневрологическом диспансере (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4419', '1.2.643.5.1.13.13.15.59.2'),
    ('158', '347', 'Выписка из протокола решения врачебной комиссии (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4307', '1.2.643.5.1.13.13.15.75.2'),
    ('159', '113', 'Статистическая карта выбывшего из медицинской организации, оказывающей медицинскую помощь в стационарных условиях, в условиях дневного стационара (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4421', '1.2.643.5.1.13.13.15.76.1'),
    ('160', '372', 'Протокол телемедицинской консультации для трансграничных телемедицинских решений (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4423', '1.2.643.5.1.13.13.15.16.1'),
    ('161', '50', 'Санаторно-курортная карта для детей (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4111', '1.2.643.5.1.13.13.15.10.1'),
    ('162', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4127', '1.2.643.5.1.13.13.15.11.1'),
    ('163', '109', 'Протокол медицинской манипуляции (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4411', '1.2.643.5.1.13.13.15.23.1'),
    ('164', '59', 'Экстренное извещение об инфекционном заболевании, пищевом, остром профессиональном отравлении, необычной реакции на прививку (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4425', '1.2.643.5.1.13.13.15.70.2'),
    ('165', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 1', '3', '2', DATE '2023-06-30', DATE '2023-06-30', 'https://portal.egisz.rosminzdrav.ru/materials/4127', '1.2.643.5.1.13.13.15.12.1'),
    ('166', '39', 'Медицинская справка (врачебное профессионально-консультативное заключение) (CDA) Редакция 2', '3', '2', DATE '2023-08-28', DATE '2024-08-01', 'https://portal.egisz.rosminzdrav.ru/materials/4101', '1.2.643.5.1.13.13.15.45.2'),
    ('167', '33', 'Медицинское свидетельство о рождении (CDA) Редакция 5', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4059', '1.2.643.5.1.13.13.15.39.5'),
    ('168', '33', 'Документ, содержащий сведения медицинского свидетельства о рождении в бумажной форме (CDA) Редакция 5', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4059', '1.2.643.5.1.13.13.15.39.5'),
    ('169', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 4', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4339', '1.2.643.5.1.13.13.15.58.4'),
    ('170', '53', 'Сертификат профилактических прививок (CDA) Редакция 2', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4095', '1.2.643.5.1.13.13.15.46.2'),
    ('171', '8', 'Медицинское заключение о наличии (об отсутствии) у водителей транспортных средств медицинских противопоказаний, медицинских показаний или медицинских ограничений к управлению транспортными средствами (CDA) Редакция 3', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4109', '1.2.643.5.1.13.13.15.43.3'),
    ('172', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4341', '1.2.643.5.1.13.13.15.57.2'),
    ('173', '11', 'Протокол гемотрансфузии (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4427', '1.2.643.5.1.13.13.15.24.4'),
    ('174', '6', 'Протокол инструментального исследования (CDA) Редакция 4', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4429', '1.2.643.5.1.13.13.15.17.4'),
    ('175', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4343', '1.2.643.5.1.13.13.15.44.2'),
    ('176', '121', 'Протокол цитологического исследования (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4373', '1.2.643.5.1.13.13.15.20.2'),
    ('177', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4345', '1.2.643.5.1.13.13.15.31.3'),
    ('178', '48', 'Санаторно-курортная карта (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4347', '1.2.643.5.1.13.13.15.9.2'),
    ('179', '50', 'Санаторно-курортная карта для детей (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4349', '1.2.643.5.1.13.13.15.10.2'),
    ('180', '46', 'Медицинское заключение об отсутствии в организме человека наркотических средств, психотропных веществ и их метаболитов (CDA) Редакция 2', '3', '2', DATE '2025-12-31', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4351', '1.2.643.5.1.13.13.15.42.2'),
    ('181', '254', 'Протокол патолого-анатомического вскрытия (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4353', '1.2.643.5.1.13.13.15.22.1'),
    ('182', '357', 'Обратный талон санаторно-курортной карты (CDA) Редакция 2', '3', '2', DATE '2023-06-30', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4299', '1.2.643.5.1.13.13.15.11.2'),
    ('183', '361', 'Обратный талон санаторно-курортной карты для детей (CDA) Редакция 2', '3', '2', DATE '2023-04-20', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4301', '1.2.643.5.1.13.13.15.12.2'),
    ('184', '184', 'Извещение о больном с впервые в жизни установленным диагнозом злокачественного новообразования (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4355', '1.2.643.5.1.13.13.15.80.1'),
    ('185', '57', 'Направление на консультацию и во вспомогательные кабинеты (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4357', '1.2.643.5.1.13.13.15.32.2'),
    ('186', '7', 'Протокол лабораторного исследования (CDA) Редакция 5', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4431', '1.2.643.5.1.13.13.15.18.5'),
    ('187', '35', 'Сведения о результатах проведенной медико-социальной экспертизы (CDA) Редакция 3', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4359', '1.2.643.5.1.13.13.15.5.3'),
    ('188', '54', 'Заключение медицинского учреждения о наличии отсутствии заболевания, препятствующего поступлению на государственную гражданскую службу Российской Федерации и муниципальную службу или ее прохождению (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4361', '1.2.643.5.1.13.13.15.81.1'),
    ('189', '108', 'Протокол оперативного вмешательства (операции) (CDA) Редакция 1', '3', '2', DATE '2023-08-28', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4363', '1.2.643.5.1.13.13.15.77.1'),
    ('190', '371', 'Протокол консилиума врачей (онкологического) (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4375', '1.2.643.5.1.13.13.15.79.1'),
    ('191', '341', 'Осмотр лечащим врачом, врачом-специалистом, заведующим отделением, лечащим врачом совместно с врачом-специалистом, лечащим врачом совместно с заведующим отделением (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4365', '1.2.643.5.1.13.13.15.78.1'),
    ('192', '77', 'Справка о количестве кроводач, плазмодач (CDA) Редакция 1', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4367', '1.2.643.5.1.13.13.15.82.1'),
    ('193', '52', 'Справка об оплате медицинских услуг для предоставления в налоговые органы Российской Федерации (CDA) Редакция 2', '3', '2', DATE '2023-08-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4377', '1.2.643.5.1.13.13.15.69.2'),
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
    ('209', '81', 'Направление к месту лечения для получения медицинской помощи (CDA) Редакция 4', '3', '2', DATE '2023-12-08', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4503', '1.2.643.5.1.13.13.15.67.4'),
    ('210', '106', 'Талон № 2 на получение специальных талонов (именных направлений) на проезд к месту лечения для получения медицинской помощи (CDA) Редакция 4', '3', '2', DATE '2023-12-08', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4487', '1.2.643.5.1.13.13.15.68.4'),
    ('211', '250', 'Протокол на случай выявления у больного запущенной формы злокачественного новообразования (CDA) Редакция 1', '3', '2', DATE '2023-12-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4505', '1.2.643.5.1.13.13.15.95.1'),
    ('212', '362', 'О наличии оснований для внеочередного медицинского освидетельствования и об аннулировании действующего медицинского заключения об отсутствии медицинских противопоказаний к владению оружием (при его наличии) (CDA) Редакция 2', '3', '2', DATE '2023-12-19', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4507', '1.2.643.5.1.13.13.15.62.2'),
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
    ('228', '340', 'Эпикриз по результатам диспансеризации/профилактического медицинского осмотра (CDA) Редакция 2', '3', '2', DATE '2024-02-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4501', '1.2.643.5.1.13.13.15.28.2'),
    ('229', '80', 'Талон на оказание высокотехнологичной медицинской помощи (CDA) Редакция 2', '3', '2', DATE '2024-03-18', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4567', '1.2.643.5.1.13.13.15.73.2'),
    ('230', '502', 'Медицинское заключение по результатам медицинского осмотра работника для предоставления в подсистему ЭЛМК (CDA) Редакция 1', '3', '2', DATE '2024-03-14', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4565', '1.2.643.5.1.13.13.15.111.1'),
    ('231', '370', 'Справка о наличии медицинских показаний, в соответствии с которыми ребенок не посещает дошкольную организацию или организацию, осуществляющую образовательную деятельность по основным общеобразовательным программам, в период учебного процесса (CDA) Редакция 2', '3', '2', DATE '2024-02-29', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4553', '1.2.643.5.1.13.13.15.66.2'),
    ('232', '368', 'Заключение об установлении факта поствакцинального осложнения (CDA) Редакция 2', '3', '2', DATE '2024-03-07', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4559', '1.2.643.5.1.13.13.15.64.2'),
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
    ('248', '49', 'Медицинская справка о состоянии здоровья ребенка, отъезжающего в организацию отдыха детей и их оздоровления (CDA) Редакция 3', '3', '2', DATE '2024-07-08', NULL, NULL, '1.2.643.5.1.13.13.15.44.3'),
    ('249', '67', 'Справка об отсутствии медицинских противопоказаний для работы с использованием сведений, составляющих государственную тайну (CDA) Редакция 2', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4707', '1.2.643.5.1.13.13.15.50.2'),
    ('250', '68', 'Заключение о результатах медицинского освидетельствования граждан, намеревающихся усыновить (удочерить), взять под опеку (попечительство), в приемную или патронатную семью детей-сирот и детей, оставшихся без попечения родителей (CDA) Редакция 2', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4709', '1.2.643.5.1.13.13.15.51.2'),
    ('251', '88', 'Справка об отсутствии контактов с инфекционными больными (CDA) Редакция 5', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4705', '1.2.643.5.1.13.13.15.56.5'),
    ('252', '90', 'Справка о состоянии на учете в диспансере (CDA) Редакция 3', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4701', '1.2.643.5.1.13.13.15.57.3'),
    ('253', '122', 'Справка о временной нетрудоспособности студента, учащегося техникума, профессионально-технического училища, о болезни, карантине и прочих причинах отсутствия ребенка, посещающего школу, детское дошкольное учреждение (CDA) Редакция 5', '3', '2', DATE '2024-06-24', NULL, 'https://portal.egisz.rosminzdrav.ru/materials/4703', '1.2.643.5.1.13.13.15.58.5'),
    ('254', '506', 'Протокол кесарева сечения (CDA) Редакция 1', '3', '2', DATE '2024-09-27', NULL, NULL, '1.2.643.5.1.13.13.15.114.1'),
    ('255', '3', 'Направление на госпитализацию, восстановительное лечение, обследование, консультацию (CDA) Редакция 5', '3', '2', DATE '2024-07-23', NULL, NULL, '1.2.643.5.1.13.13.15.31.5'),
    ('256', '508', 'Заключение по результатам микробиологического исследования (CDA) Редакция 1', '3', '2', DATE '2024-07-29', NULL, NULL, '1.2.643.5.1.13.13.15.120.1'),
    ('257', '509', 'Выписка из протокола решения врачебной комиссии для направления на медико-социальную экспертизу (CDA) Редакция 1', '3', '2', DATE '2024-08-12', NULL, NULL, '1.2.643.5.1.13.13.15.118.1'),
    ('262', '510', 'Медицинское заключение по дистанционному наблюдению за состоянием здоровья пациента (CDA) Редакция 1', '3', '2', DATE '2024-09-26', NULL, NULL, '1.2.643.5.1.13.13.15.123.1'),
    ('266', '179', 'Медицинское заключение о допуске к участию в физкультурных и спортивных мероприятиях (учебно-тренировочных мероприятиях и спортивных соревнованиях), мероприятиях по оценке выполнения нормативов испытаний (тестов) Всероссийского физкультурно-спортивного комплекса "Готов к труду и обороне" (ГТО) (CDA) Редакция 1', '3', '2', DATE '2024-09-20', NULL, NULL, '1.2.643.5.1.13.13.15.124.1')
ON CONFLICT (code) DO UPDATE SET
    type_code = EXCLUDED.type_code,
    name = EXCLUDED.name,
    level = EXCLUDED.level,
    format_code = EXCLUDED.format_code,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    implementation_guide = EXCLUDED.implementation_guide,
    git_link = EXCLUDED.git_link,
    oid = EXCLUDED.code,
    updated_at = now();

UPDATE dim_semd_types
SET oid = code
WHERE oid IS DISTINCT FROM code;

CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid ON dim_semd_types (oid) WHERE oid IS NOT NULL;

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
CREATE OR REPLACE FUNCTION public.ensure_time_partitions(
    p_months_back integer DEFAULT 12,
    p_months_ahead integer DEFAULT 24
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    spec record;
    part_start timestamptz;
    part_end timestamptz;
    part_name text;
    window_start timestamptz;
    window_end timestamptz;
    data_start timestamptz;
    data_end timestamptz;
    created integer := 0;
BEGIN
    FOR spec IN
        SELECT * FROM (VALUES
            ('exchangelog_raw', 'createdate'),
            ('transactions', 'log_date')
        ) AS t(table_name, key_column)
    LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = spec.table_name
              AND c.relkind = 'p'
        );

        window_start := date_trunc('month', timezone('UTC', now())) - (p_months_back || ' months')::interval;
        window_end := date_trunc('month', timezone('UTC', now())) + (p_months_ahead || ' months')::interval;

        -- Данные могут выходить за окно: без покрывающей партиции такая строка
        -- не вставится вовсе, поэтому окно расширяется до фактического диапазона.
        EXECUTE format(
            'SELECT date_trunc(''month'', min(%I)), date_trunc(''month'', max(%I)) FROM public.%I',
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
                    part_name, spec.table_name, part_start, part_end
                );
                created := created + 1;
            END IF;
            part_start := part_end;
        END LOOP;
    END LOOP;

    RETURN created;
END;
$$;

-- DEFAULT-партиции не используются: строки переносятся в месячные партиции,
-- DEFAULT-партиция отцепляется и удаляется после переноса строк.
DO $$
DECLARE
    spec record;
    moved bigint;
    data_start timestamptz;
    data_end timestamptz;
    part_start timestamptz;
    part_end timestamptz;
    part_name text;
BEGIN
    FOR spec IN
        SELECT * FROM (VALUES
            ('exchangelog_raw', 'exchangelog_raw_default', 'createdate'),
            ('transactions', 'transactions_default', 'log_date')
        ) AS t(table_name, default_name, key_column)
    LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = spec.default_name
        );

        EXECUTE format(
            'SELECT count(*), date_trunc(''month'', min(%I)), date_trunc(''month'', max(%I)) FROM public.%I',
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
                        part_name, spec.table_name, part_start, part_end
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

-- Инициализация маркера попытки парсинга по распарсенным строкам transactions.
INSERT INTO exchangelog_parse_attempts (logid)
SELECT logid FROM transactions WHERE xml_parsed_at IS NOT NULL
ON CONFLICT (logid) DO NOTHING;

-- Статистика нужна планировщику анти-джойна parse_targets сразу после массового бэкфилла.
ANALYZE exchangelog_parse_attempts;
