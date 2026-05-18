\encoding UTF8
-- DWH initialization script for EGISZ proxy reports.
-- Run once (and re-run safely on updates) as PostgreSQL superuser against dwh_egisz.
--
-- Prerequisites — execute as superuser against the 'postgres' database:
--   CREATE ROLE egisz LOGIN PASSWORD 'egisz';
--   CREATE DATABASE dwh_egisz;
--
-- Usage:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql

SET lock_timeout = '30s';
SET statement_timeout = '60min';

-- Idempotent role creation
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'egisz') THEN
        EXECUTE format('CREATE ROLE egisz LOGIN PASSWORD %L', 'egisz');
    END IF;
END;
$$;

GRANT CONNECT ON DATABASE dwh_egisz TO egisz;
GRANT USAGE, CREATE ON SCHEMA public TO egisz;

CREATE TABLE IF NOT EXISTS elt_state (
    pipeline text PRIMARY KEY,
    last_log_id bigint DEFAULT 0,
    last_egmid bigint DEFAULT 0,
    updated_at timestamptz DEFAULT now()
);

DO $$
BEGIN
    IF to_regclass('public.exchangelog_raw') IS NULL AND to_regclass('public.egisz_raw') IS NOT NULL THEN
        ALTER TABLE public.egisz_raw RENAME TO exchangelog_raw;
    ELSIF to_regclass('public.exchangelog_raw') IS NULL AND to_regclass('public.exchangelog') IS NOT NULL THEN
        ALTER TABLE public.exchangelog RENAME TO exchangelog_raw;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS exchangelog_raw (
    logid bigint PRIMARY KEY,
    logdate timestamptz,
    createdate timestamptz,
    msgid text,
    logstate integer,
    logtext text,
    msgtext text,
    loaded_at timestamptz DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'exchangelog_raw'
          AND column_name = 'createdate'
    ) THEN
        ALTER TABLE exchangelog_raw ADD COLUMN createdate timestamptz;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS egisz_messages_raw (
    egmid bigint PRIMARY KEY,
    created_at timestamptz,
    msgid text,
    reply_to text,
    document_id text,
    loaded_at timestamptz DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'egisz_messages_raw'
          AND column_name = 'loaded_at'
    ) THEN
        ALTER TABLE egisz_messages_raw ADD COLUMN loaded_at timestamptz DEFAULT now();
    END IF;
END
$$;
-- Columns jid/kind/msgtext were always NULL (not present in Firebird EGISZ_MESSAGES);
-- they are dropped after DROP VIEW block below to avoid breaking mat-view dependencies.

CREATE TABLE IF NOT EXISTS dim_organizations (
    jid integer PRIMARY KEY,
    name text,
    inn text,
    address text,
    updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_licenses (
    id bigint PRIMARY KEY,
    service_type integer,
    jid integer,
    mo_uid text,
    mo_domen text,
    bdate date,
    fdate date,
    kind text,
    modifydate timestamptz,
    updated_at timestamptz DEFAULT now()
);

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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'type_code'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN type_code text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'level'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN level text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'format_code'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN format_code text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'start_date'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN start_date date;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'end_date'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN end_date date;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'implementation_guide'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN implementation_guide text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'git_link'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN git_link text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'oid'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN oid text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'version'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN version text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'dim_semd_types' AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE dim_semd_types ADD COLUMN updated_at timestamptz DEFAULT now();
    END IF;
END
$$;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_semd_types'
          AND column_name = 'oid'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE dim_semd_types ALTER COLUMN oid DROP NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_semd_types'
          AND column_name = 'oid'
          AND column_default IS NOT NULL
    ) THEN
        ALTER TABLE dim_semd_types ALTER COLUMN oid DROP DEFAULT;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_semd_types'
          AND column_name = 'version'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE dim_semd_types ALTER COLUMN version DROP NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_semd_types'
          AND column_name = 'version'
          AND column_default IS NOT NULL
    ) THEN
        ALTER TABLE dim_semd_types ALTER COLUMN version DROP DEFAULT;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_semd_types'
          AND column_name = 'source_url'
    ) THEN
        ALTER TABLE dim_semd_types DROP COLUMN source_url;
    END IF;
END
$$;

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
    updated_at = now();

CREATE TABLE IF NOT EXISTS fact_egisz_transactions (
    exchangelog_log_id bigint PRIMARY KEY REFERENCES exchangelog_raw(logid),
    log_date timestamptz,
    message_id text,
    relates_to_id text,
    local_uid_semd text,
    emdr_id text,
    doc_number text,
    org_oid text,
    status text,
    error_message text,
    callback_url text,
    egmid bigint,
    jid integer,
    semd_code text,
    semd_name text,
    error_code text,
    creation_date timestamptz,
    processed_at timestamptz DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'egmid'
    ) THEN
        ALTER TABLE fact_egisz_transactions ADD COLUMN egmid bigint;
    END IF;
END
$$;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'errors_json'
    ) THEN
        ALTER TABLE fact_egisz_transactions DROP COLUMN errors_json CASCADE;
    END IF;
END
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'creation_date'
    ) THEN
        ALTER TABLE fact_egisz_transactions ADD COLUMN creation_date timestamptz;
    END IF;
END
$$;
-- error_subtype упразднён: его роль теперь играет плоский error_type
-- (см. egisz_error_classify). Колонка удаляется идемпотентно.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'error_subtype'
    ) THEN
        ALTER TABLE fact_egisz_transactions DROP COLUMN error_subtype;
    END IF;
END
$$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'error_type'
    ) THEN
        ALTER TABLE fact_egisz_transactions ADD COLUMN error_type text;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'error_summary'
    ) THEN
        ALTER TABLE fact_egisz_transactions ADD COLUMN error_summary text;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fact_egisz_transactions'
          AND column_name = 'error_json_text'
    ) THEN
        ALTER TABLE fact_egisz_transactions ADD COLUMN error_json_text text;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_msgid ON exchangelog_raw (msgid);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_msgid_norm ON exchangelog_raw (public.egisz_normalize_message_id(msgid));
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_logstate ON exchangelog_raw (logstate);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_createdate ON exchangelog_raw (createdate);
CREATE INDEX IF NOT EXISTS idx_egisz_messages_msgid ON egisz_messages_raw (msgid);
CREATE INDEX IF NOT EXISTS idx_egisz_messages_document_id ON egisz_messages_raw (document_id);
CREATE INDEX IF NOT EXISTS idx_egisz_messages_document_id_norm ON egisz_messages_raw (lower(NULLIF(btrim(document_id), '')));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_log_date ON fact_egisz_transactions (log_date);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_status ON fact_egisz_transactions (status);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_jid ON fact_egisz_transactions (jid);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_message_id ON fact_egisz_transactions (message_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_local_uid ON fact_egisz_transactions (local_uid_semd);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_local_uid_norm ON fact_egisz_transactions (lower(NULLIF(btrim(local_uid_semd), '')));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_emdr_id ON fact_egisz_transactions (emdr_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_relates_to ON fact_egisz_transactions (relates_to_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_error_type ON fact_egisz_transactions (error_type);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_egmid ON fact_egisz_transactions (egmid);
CREATE INDEX IF NOT EXISTS idx_dim_licenses_jid ON dim_licenses (jid);
CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_uid ON dim_licenses (mo_uid);

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

CREATE TABLE IF NOT EXISTS egisz_error_interpretation_rules (
    rule_code text PRIMARY KEY,
    priority integer NOT NULL,
    match_code text,
    match_pattern text NOT NULL,
    interpretation text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    updated_at timestamptz DEFAULT now()
);

INSERT INTO egisz_error_interpretation_rules (rule_code, priority, match_code, match_pattern, interpretation)
VALUES
    ('schematron_patient_address_type', 10, 'VALIDATION_ERROR', '(?is)(Schematron|схематрон).*patientRole.*addr.*address:Type', 'Не указан адрес пациента'),
    ('schematron_org_not_linked_rmis', 11, 'VALIDATION_ERROR', '(?is)не привязана к РМИС', 'Организация не привязана к РМИС'),
    ('schematron_telecom_missing', 12, 'VALIDATION_ERROR', '(?is)(telecom).*(не пустым значением|@value)|Ошибка заполнения номера телефона', 'Некорректно заполнен телефон'),
    ('xsd_validation', 20, NULL, '(?is)(\bcvc-|XML_VALIDATION_ERROR|xsd|Invalid content was found|not complete|not valid)', 'Ошибка XSD-валидации XML'),
    ('document_already_registered', 25, 'NOT_UNIQUE_PROVIDED_ID', '(?is).*', 'Документ уже зарегистрирован в РЭМД'),
    ('patient_data_gip', 26, 'PATIENT_MPI_MISMATCH', '(?is).*', 'Данные пациента не соответствуют ГИП'),
    ('doctor_position_frmr', 27, 'PERSON_POST_IN_FRMR_MISMATCH', '(?is).*', 'Должность врача не соответствует данным ФРМР'),
    ('person_not_found_frmr', 28, 'PERSON_NOT_FOUND', '(?is).*', 'Медработник не найден в ФРМР'),
    ('staff_data_frmr', 29, 'VALUE_MISMATCH_METADATA_AND_FRMR', '(?is).*', 'Данные медработника не соответствуют ФРМР'),
    ('signature_metadata_certificate', 30, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР'),
    ('signature_metadata_certificate_mismatch', 31, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is).*', 'Данные подписи не соответствуют данным документа'),
    ('nsi_dictionary_version', 32, 'INVALID_DICTIONARY_OID', '(?is).*', 'Неактуальная версия справочника НСИ'),
    ('nsi_dictionary_code', 33, 'INVALID_ELEMENT_VALUE_CODE', '(?is).*', 'Код отсутствует в справочнике НСИ'),
    ('nsi_dictionary_name', 34, 'INVALID_ELEMENT_VALUE_NAME', '(?is).*', 'Наименование не соответствует справочнику НСИ'),
    ('nsi_dictionary_value', 35, NULL, '(?is)(Справочник OID|codeSystem|codeSystemVersion|верси[яи].*справочник|значени[ея].*НСИ|не соответствует наименованию элемента в НСИ|справочн.*значен)', 'Ошибка справочника НСИ'),
    ('rmis_registration_disabled', 40, 'DISABLED_RMIS', '(?is).*', 'ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС'),
    ('rmis_registration_missing', 41, 'NO_RMIS', '(?is).*', 'ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные'),
    ('document_metadata_mismatch', 50, 'ATTRIBUTE_MISMATCH', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД'),
    ('document_provider_unavailable', 51, 'MIS_NOT_AVAILABLE', '(?is).*', 'Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile'),
    ('document_registry_item_missing', 52, 'REGISTRY_ITEM_NOT_FOUND', '(?is).*', 'Запрашиваемая запись ЭМД не найдена в предоставляющей ИС'),
    ('document_file_not_sent', 53, 'FILE_WAS_NOT_SENT', '(?is).*', 'ИС не передала файл ЭМД в ответе getDocumentFile'),
    ('document_provider_response_error', 54, 'RMIS_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС'),
    ('document_file_get_error', 55, 'GET_DOCUMENT_FILE_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС'),
    ('document_file_runtime_error', 56, NULL, '(?is)(getDocumentFile|получения файла ЭМД|файлового хранилища)', 'Не удалось получить файл ЭМД из предоставляющей ИС'),
    ('signature_certificate_chain', 60, NULL, '(?is)(CANT_BUILD_CERT_CHAIN|цепочк.*сертификат|аккредитованн.*УЦ)', 'Недействительный сертификат подписи'),
    ('signature_doc_date_mismatch', 61, NULL, '(?is)(DOC_DATE_MISMATCH_CERT_NOT_BEFORE|сертификат.*не действителен.*дат[уы] создания)', 'Сертификат подписи недействителен на дату создания документа'),
    ('signature_verification_error', 62, 'SIGNATURE_VERIFICATION_ERROR', '(?is).*', 'Не удалось проверить электронную подпись'),
    ('person_snils', 70, NULL, '(?is)(СНИЛС|SNILS)', 'СНИЛС не найден или не соответствует данным пациента/медработника'),
    ('doctor_position_frmr_text', 71, NULL, '(?is)(ФРМР|FRMR).*(должност|specialit|специальност)|(должност|specialit|специальност).*(ФРМР|FRMR)|(должност|specialit|специальност).*(не соответств|не совпад|не найден)', 'Должность врача не соответствует данным ФРМР'),
    ('patient_data_gip_text', 72, NULL, '(?is)(ГИП|GIP).*(пациент|patient)|(пациент|patient).*(ГИП|GIP)|(данн|сведени).*(пациент|patient).*(не соответств|не совпад|не найден)', 'Данные пациента не соответствуют ГИП'),
    ('person_frmr', 73, NULL, '(?is)(ФРМР|медработник|автор|author)', 'Данные медработника не соответствуют ФРМР'),
    ('recipient_mismatch', 74, 'RECIPIENT_INFO_MISMATCH', '(?is).*', 'Получатель из запроса не найден в СЭМД'),
    ('document_kind_not_actual', 75, 'NO_DOCUMENT_KIND_ON_DATE', '(?is).*', 'Вид документа не актуален на дату создания'),
    ('object_not_found', 76, 'OBJECT_NOT_FOUND', '(?is).*', 'Подразделение или запись справочника не найдены на дату документа'),
    ('doctor_patronymic_mismatch', 77, 'INVALID_DOCTOR_PATRONYMIC', '(?is).*', 'Отчество врача не соответствует данным СЭМД'),
    ('runtime_request_processing', 79, 'RUNTIME_ERROR', '(?is)Невозможно обработать запрос', 'РЭМД не смог обработать запрос'),
    ('remd_internal', 80, NULL, '(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб)', 'Техническая ошибка на стороне РЭМД'),
    -- Schematron VALIDATION_ERROR — уточнённые паттерны по полям CDA
    ('schematron_author_specialty', 13, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*code.*codeSystem|assignedAuthor.*specialit|специальност.*автор|автор.*специальност)', 'Специальность врача не соответствует справочнику НСИ'),
    ('schematron_author_snils', 14, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*(SNILS|СНИЛС|snils)|author.*(СНИЛС|snils))', 'СНИЛС автора (врача) не заполнен или некорректен'),
    ('schematron_patient_birth', 15, 'VALIDATION_ERROR', '(?is)(patientRole.*birthTime|birthTime.*patient)', 'Дата рождения пациента не заполнена или некорректна'),
    ('schematron_patient_name', 16, 'VALIDATION_ERROR', '(?is)(patientRole.*(name|given|family)|(given|family).*patientRole)', 'ФИО пациента не заполнено или некорректно'),
    ('schematron_patient_snils', 17, 'VALIDATION_ERROR', '(?is)(patientRole.*(SNILS|СНИЛС)|patient.*(SNILS|СНИЛС))', 'СНИЛС пациента не заполнен или некорректен'),
    ('schematron_legal_auth', 18, 'VALIDATION_ERROR', '(?is)legalAuthenticator', 'Данные заверителя документа не заполнены или некорректны'),
    ('schematron_creation_time', 19, 'VALIDATION_ERROR', '(?is)(creationTime.*(не заполнен|некорректн|не указан|обязател))', 'Дата/время создания документа не заполнены или некорректны'),
    ('schematron_doc_code', 21, 'VALIDATION_ERROR', '(?is)(ClinicalDocument/code|тип документа.*(справочник|OID|codeSystem))', 'Код типа документа не соответствует справочнику НСИ'),
    ('schematron_custodian', 22, 'VALIDATION_ERROR', '(?is)(custodian|representedCustodianOrganization)', 'Данные хранителя документа не заполнены'),
    ('schematron_org_repr', 23, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*representedOrganization|representedOrganization.*author)', 'Данные организации автора документа не заполнены'),
    -- Ошибки регистрации/поиска документов в РЭМД
    ('document_not_found_remd', 36, 'DOCUMENT_NOT_FOUND', '(?is).*', 'Документ не найден в РЭМД'),
    ('invalid_emdr_id', 37, 'INVALID_EMDR_ID', '(?is).*', 'Неверный идентификатор документа РЭМД'),
    ('organization_not_found', 38, 'ORGANIZATION_NOT_FOUND', '(?is).*', 'Организация не найдена в реестре РЭМД'),
    ('access_denied_remd', 39, 'ACCESS_DENIED', '(?is).*', 'Доступ к операции запрещён в РЭМД'),
    ('duplicate_request', 42, 'DUPLICATE_REQUEST', '(?is).*', 'Дублирующий запрос'),
    ('unsupported_document_type', 43, 'UNSUPPORTED_DOCUMENT_TYPE', '(?is).*', 'Неподдерживаемый тип СЭМД в РЭМД'),
    ('invalid_request_format', 44, 'INVALID_REQUEST_FORMAT', '(?is).*', 'Неверный формат запроса'),
    ('organization_license_not_found', 45, 'ORGANIZATION_LICENSE_NOT_FOUND', '(?is).*', 'Лицензия организации не найдена'),
    ('invalid_snils_code', 46, 'INVALID_SNILS', '(?is).*', 'Неверный формат или контрольная сумма СНИЛС'),
    ('organization_not_registered', 47, 'ORGANIZATION_NOT_REGISTERED', '(?is).*', 'Организация не зарегистрирована в РЭМД'),
    -- Ошибки сертификата и подписи
    ('certificate_expired', 57, NULL, '(?is)(сертификат.*истёк|истекш.*сертификат|срок.*действи.*сертификат.*истёк|certificate.*expired)', 'Сертификат ЭП истёк'),
    ('certificate_revoked', 58, NULL, '(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)', 'Сертификат ЭП отозван'),
    ('crl_unavailable', 63, NULL, '(?is)(CRL|список.*отозванн|OCSP|сервис.*проверк.*сертификат)', 'Недоступен сервис проверки статуса сертификата (CRL/OCSP)'),
    -- Таймаут и УЦ (code-based, дополнение к уже существующим)
    ('async_response_timeout_code', 64, 'ASYNC_RESPONSE_TIMEOUT', '(?is).*', 'Таймаут асинхронной обработки на стороне РЭМД'),
    ('ca_unavailable_code', 65, 'CA_UNAVAILABLE', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД'),
    ('ca_inaccessibility_code', 66, 'CA_INACCESSIBILITY', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД'),
    -- Аннулирование, текстовые паттерны
    ('document_revoked_text', 67, NULL, '(?is)(аннулирован.*документ|документ.*аннулирован)', 'Документ аннулирован'),
    ('xml_parse_error', 68, NULL, '(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)', 'Ошибка разбора XML-структуры документа'),
    ('snils_invalid_text', 69, NULL, '(?is)(СНИЛС.*неверн|неверн.*СНИЛС|СНИЛС.*контрольн|контрольн.*СНИЛС)', 'Неверный формат или контрольная сумма СНИЛС'),
    ('transport_network', 90, NULL, '(?is)(network|connection|transport|timeout|timed out|соединени|таймаут|сетевая ошибка)', 'Сетевая ошибка'),
    -- Additional canonical mappings to suppress raw-text leakage in error_type
    ('cvc_datatype_extended', 24, NULL, '(?is)cvc-datatype-valid|cvc-pattern-valid|cvc-type|cvc-complex-type|cvc-attribute|cvc-elt|cvc-identity-constraint|cvc-particle|cvc-enumeration-valid', 'Ошибка XSD-валидации XML'),
    ('attribute_not_found_code', 50, 'ATTRIBUTE_NOT_FOUND', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД'),
    ('role_occurrence_mismatch_code', 31, 'ROLE_OCCURRENCE_MISMATCH', '(?is).*', 'Подпись роли не соответствует требованиям РЭМД'),
    ('object_not_found_text_extra', 76, NULL, '(?is)Подразделение.*(идентификатор|не найден)|подразделение.*не найден', 'Подразделение или запись справочника не найдены на дату документа'),
    ('recipient_text_extra', 74, NULL, '(?is)RECIPIENT_INFO_MISMATCH|Получатель.*не найден', 'Получатель из запроса не найден в СЭМД'),
    ('dul_patient_text', 78, NULL, '(?is)ДУЛ[^А-Яа-я]|реквизит.*удостоверени', 'Документ, удостоверяющий личность пациента: некорректные реквизиты'),
    ('patient_birth_text', 15, NULL, '(?is)Дата рождения пациента|birthTime', 'Дата рождения пациента не заполнена или некорректна'),
    ('remd_runtime_internal', 80, NULL, '(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб|невозможно обработать)', 'Техническая ошибка на стороне РЭМД'),
    -- Сертификат организации: специальный case для распознанного кода РЭМД
    ('cert_org_validity_expired', 56, 'CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', '(?is).*', 'Срок действия сертификата организации истек'),
    -- Несоответствие данных организации в ФРМО (ОГРН и подобные)
    ('org_ogrn_frmo_mismatch', 11, NULL, '(?is)(ОГРН|ОКПО|КПП|ИНН).*(СЭМД|ФРМО).*(не совпада|не соответств)|ОГРН МО.*не совпада|ФРМО.*(не совпада|не соответств).*организац', 'Несоответствие данных организации в ФРМО'),
    -- Generic fallback для прочих организационных ошибок
    ('org_generic_fallback', 95, NULL, '(?is)(организаци|ОГРН|ФРМО|лицензи)', 'Ошибки организации')
ON CONFLICT (rule_code) DO UPDATE SET
    priority = EXCLUDED.priority,
    match_code = EXCLUDED.match_code,
    match_pattern = EXCLUDED.match_pattern,
    interpretation = EXCLUDED.interpretation,
    is_active = true,
    updated_at = now();

-- Деактивируем generic-фолбэк, который раньше отдавал «Ошибка регистрации в РЭМД».
-- При отсутствии конкретного типа теперь подставляется «Неизвестная ошибка»
-- в egisz_error_classify (см. ниже).
UPDATE egisz_error_interpretation_rules
SET is_active = false, updated_at = now()
WHERE rule_code = 'remd_async_response';

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_schematron_chunk(p_chunk text)
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
    -- egisz_error_interpretation_item возвращает уже канонические лейблы
    -- (правило из egisz_error_interpretation_rules ИЛИ название из
    -- schematron-chunk). Нормализация ФИО/UUID/<...> здесь не нужна и
    -- даже вредна: «case-insensitive» регэксп 3-слов случайно матчит
    -- «Не указан адрес» и калечит лейбл в «<ФИО> пациента». Поэтому
    -- просто обрезаем длину и возвращаем как есть.
    label := btrim(COALESCE(public.egisz_error_interpretation_item(error_code, error_message), ''));
    IF label = '' THEN
        RETURN 'Неизвестная ошибка';
    END IF;
    RETURN left(label, 220);
END;
$$;

-- Плоская таксономия error_type. Каждый <ns2:item> в асинхронном ответе РЭМД
-- классифицируется через egisz_error_interpretation_type (правила из
-- egisz_error_interpretation_rules); уникальные типы дедуплицируются и
-- объединяются через ' · '. Если ни один item не дал интерпретации —
-- возвращается 'Неизвестная ошибка'.
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

-- Старая функция-бакетатор egisz_error_group_type убрана: десятка bucket-категорий
-- заменена плоской таксономией (см. egisz_error_classify). Возможные внешние
-- ссылки на неё уже переписаны в этом же файле.
DROP FUNCTION IF EXISTS public.egisz_error_group_type(text, text);

CREATE OR REPLACE FUNCTION public.egisz_error_interpretation_row(p_errors jsonb)
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
            NULLIF(btrim(public.egisz_error_interpretation_item(e->>'code', e->>'message')), '') AS t
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

CREATE OR REPLACE FUNCTION public.egisz_error_messages_row(p_errors jsonb)
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

CREATE OR REPLACE FUNCTION public.egisz_xml_error_items(payload text)
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
        item_code := public.egisz_xml_text(item_xml, 'code');
        item_message := public.egisz_xml_text(item_xml, 'message');
        IF NULLIF(btrim(COALESCE(item_code, '')), '') IS NOT NULL
           OR NULLIF(btrim(COALESCE(item_message, '')), '') IS NOT NULL THEN
            result := result || jsonb_build_array(jsonb_build_object('code', item_code, 'message', item_message));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION public.egisz_build_errors_json(
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
        SELECT public.egisz_xml_error_items(p_msgtext) AS items
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

CREATE OR REPLACE FUNCTION public.egisz_semd_type_report_label(semd_code text, semd_name text)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    WITH resolved AS (
        SELECT
            public.egisz_normalize_semd_code(semd_code) AS code,
            COALESCE(
                d.name,
                CASE
                    WHEN public.egisz_clean_text_value(semd_name) IS NOT NULL
                     AND public.egisz_clean_text_value(semd_name) !~ '^\d+$'
                     AND public.egisz_clean_text_value(semd_name) <> public.egisz_normalize_semd_code(semd_code)
                    THEN public.egisz_clean_text_value(semd_name)
                    ELSE NULL
                END
            ) AS display_name
        FROM (SELECT public.egisz_normalize_semd_code(semd_code) AS code) n
        LEFT JOIN public.dim_semd_types d ON d.code = n.code
    )
    SELECT CASE
        WHEN code IS NULL AND display_name IS NULL THEN '(неизвестно)'
        WHEN code IS NULL THEN display_name
        WHEN display_name IS NULL THEN code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
        ELSE code || ' · ' || display_name
    END
    FROM resolved;
$$;

CREATE OR REPLACE FUNCTION public.egisz_transform_raw_to_facts(
    min_log_id bigint,
    max_log_id bigint,
    min_egmid bigint DEFAULT 0,
    max_egmid bigint DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer := 0;
BEGIN
    WITH candidate_log_ids AS (
        -- LOG-id window: rows in the freshly extracted EXCHANGELOG batch
        SELECT r.logid
        FROM exchangelog_raw r
        WHERE r.logid > min_log_id
          AND r.logid <= max_log_id

        UNION

        -- EGMID window: re-process EXCHANGELOG rows whose linked EGISZ_MESSAGES row
        -- arrived in the current batch (late callback to an older request).
        SELECT DISTINCT r.logid
        FROM exchangelog_raw r
        JOIN egisz_messages_raw em
          ON em.egmid > min_egmid
         AND em.egmid <= max_egmid
         AND (
              public.egisz_normalize_message_id(em.msgid) IN (
                  public.egisz_normalize_message_id(r.msgid),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'messageId')),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesToMessage')),
                  public.egisz_normalize_message_id(public.egisz_xml_text(r.msgtext, 'relatesTo'))
              )
              OR lower(NULLIF(btrim(em.document_id), '')) IN (
                  lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'localUid')), '')),
                  lower(NULLIF(btrim(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')), ''))
              )
         )
    ),
    raw_parsed AS (
        SELECT
            r.logid,
            r.logdate,
            r.createdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'messageId'), r.msgid)) AS message_id,
            public.egisz_normalize_message_id(COALESCE(public.egisz_xml_text(r.msgtext, 'relatesToMessage'), public.egisz_xml_text(r.msgtext, 'relatesTo'))) AS relates_to_id,
            public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_xml,
            public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_xml,
            public.egisz_xml_text(r.msgtext, 'kind') AS kind_xml,
            public.egisz_xml_text(r.msgtext, 'KIND') AS kind_upper_xml,
            public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id,
            public.egisz_xml_text(r.msgtext, 'documentNumber') AS doc_number,
            COALESCE(public.egisz_xml_text(r.msgtext, 'organization'), public.egisz_xml_text(r.msgtext, 'organizationOid')) AS org_oid,
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'), public.egisz_xml_text(r.msgtext, 'name'), public.egisz_xml_text(r.msgtext, 'documentName')) AS semd_name,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorCode'), public.egisz_xml_text(r.msgtext, 'code')) AS error_code,
            COALESCE(public.egisz_xml_text(r.msgtext, 'errorMessage'), public.egisz_xml_text(r.msgtext, 'message'), public.egisz_xml_text(r.msgtext, 'faultstring')) AS xml_message,
            lower(COALESCE(public.egisz_xml_text(r.msgtext, 'status'), '')) AS raw_status,
            NULLIF((regexp_match(COALESCE(r.logtext, '') || ' ' || COALESCE(r.msgtext, ''), 'gost-([0-9]+)', 'i'))[1], '')::integer AS jid_from_payload,
            public.safe_cast_timestamptz(COALESCE(public.egisz_xml_text(r.msgtext, 'creationDateTime'), public.egisz_xml_text(r.msgtext, 'creationDate'))) AS creation_date
        FROM exchangelog_raw r
        JOIN candidate_log_ids c ON c.logid = r.logid
        WHERE COALESCE(public.egisz_xml_text(r.msgtext, 'action'), '') <> 'getDocumentFile'
    ),
    parsed AS (
        SELECT
            r.logid,
            COALESCE(m.created_at, r.createdate) AS logdate,
            r.msgid,
            r.logstate,
            r.logtext,
            r.msgtext,
            r.message_id,
            r.relates_to_id,
            COALESCE(r.local_uid_xml, r.document_id_xml, m.document_id) AS local_uid_semd,
            r.emdr_id,
            r.doc_number,
            r.org_oid,
            public.egisz_normalize_semd_code(COALESCE(r.kind_xml, r.kind_upper_xml)) AS semd_code,
            public.egisz_clean_text_value(r.semd_name) AS semd_name,
            r.error_code,
            r.xml_message,
            r.raw_status,
            r.jid_from_payload,
            r.creation_date,
            m.egmid,
            m.license_jid AS message_jid,
            public.egisz_normalize_semd_code(m.license_kind) AS message_kind
        FROM raw_parsed r
        LEFT JOIN LATERAL (
            SELECT candidate.*
            FROM (
                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 0 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE lower(NULLIF(btrim(em.document_id), '')) IN (
                    lower(NULLIF(btrim(r.local_uid_xml), '')),
                    lower(NULLIF(btrim(r.document_id_xml), '')),
                    lower(NULLIF(btrim(r.emdr_id), ''))
                )

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 1 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE public.egisz_normalize_message_id(em.msgid) = r.relates_to_id

                UNION ALL

                SELECT em.egmid, em.created_at, em.msgid, em.reply_to, em.document_id,
                       l.jid AS license_jid, l.kind AS license_kind, 2 AS priority
                FROM egisz_messages_raw em
                LEFT JOIN dim_licenses l
                  ON public.egisz_clean_host(l.mo_domen) = public.egisz_clean_host(em.reply_to)
                WHERE public.egisz_normalize_message_id(em.msgid) = r.message_id
            ) candidate
            ORDER BY candidate.priority, candidate.egmid DESC
            LIMIT 1
        ) m ON TRUE
    ),
    enriched AS (
        SELECT
            p.*,
            COALESCE(p.message_jid, p.jid_from_payload) AS resolved_jid,
            COALESCE(p.semd_code, p.message_kind) AS resolved_semd_code,
            CASE
                WHEN p.logstate = 3 THEN 'error'
                WHEN p.raw_status LIKE '%success%' THEN 'success'
                WHEN p.raw_status LIKE '%error%' OR COALESCE(p.msgtext, '') ILIKE '%error%' THEN 'error'
                ELSE 'unknown'
            END AS final_status,
            CASE
                WHEN p.logstate = 3 THEN 'Сетевая ошибка: ' || COALESCE(NULLIF(p.logtext, ''), 'нет деталей')
                ELSE p.xml_message
            END AS final_error_message
        FROM parsed p
    ),
    with_errors AS (
        SELECT
            e.*,
            public.egisz_build_errors_json(e.final_status, e.error_code, e.final_error_message, e.msgtext) AS built_errors_json
        FROM enriched e
    )
    INSERT INTO fact_egisz_transactions (
        exchangelog_log_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id,
        doc_number, org_oid, status, error_message, callback_url, egmid, jid, semd_code,
        semd_name, error_code, creation_date, processed_at,
        error_type, error_summary, error_json_text
    )
    SELECT
        e.logid, e.logdate, e.message_id, e.relates_to_id, e.local_uid_semd, e.emdr_id,
        e.doc_number, e.org_oid, e.final_status, e.final_error_message, e.logtext, e.egmid,
        e.resolved_jid, e.resolved_semd_code, e.semd_name, e.error_code,
        e.creation_date, now(),
        CASE
            WHEN e.final_status = 'error' AND e.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
            WHEN e.final_status = 'error' THEN public.egisz_error_classify(e.built_errors_json)
            WHEN e.final_status = 'success' THEN 'Успешно'
            ELSE COALESCE(e.final_status, 'unknown')
        END,
        public.egisz_error_interpretation_row(e.built_errors_json),
        public.egisz_error_messages_row(e.built_errors_json)
    FROM with_errors e
    ON CONFLICT (exchangelog_log_id) DO UPDATE SET
        log_date = EXCLUDED.log_date,
        message_id = EXCLUDED.message_id,
        relates_to_id = EXCLUDED.relates_to_id,
        local_uid_semd = EXCLUDED.local_uid_semd,
        emdr_id = EXCLUDED.emdr_id,
        doc_number = EXCLUDED.doc_number,
        org_oid = EXCLUDED.org_oid,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        callback_url = EXCLUDED.callback_url,
        egmid = EXCLUDED.egmid,
        jid = EXCLUDED.jid,
        semd_code = EXCLUDED.semd_code,
        semd_name = EXCLUDED.semd_name,
        error_code = EXCLUDED.error_code,
        creation_date = EXCLUDED.creation_date,
        processed_at = now(),
        error_type = EXCLUDED.error_type,
        error_summary = EXCLUDED.error_summary,
        error_json_text = EXCLUDED.error_json_text;
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

DROP VIEW IF EXISTS public.v_health_by_clinic_ui;
DROP VIEW IF EXISTS public.v_health_signals_ui;
DROP VIEW IF EXISTS public.v_health_proxy_db_ui;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_network_errors_detail_ui;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_stg_channel_errors_by_document;
DROP VIEW IF EXISTS public.v_rpt_error_interpretations_ui;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_rpt_documents_no_response_ui CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_rpt_documents_no_response_ui;  -- in case it was previously created as MV
DROP VIEW IF EXISTS public.v_egisz_transactions_full;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_egisz_transactions_enriched_ui CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_egisz_transactions_enriched_ui;

-- Drop legacy columns after dependent views are gone.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'egisz_messages_raw'
          AND column_name = 'jid'
    ) THEN
        ALTER TABLE egisz_messages_raw DROP COLUMN jid;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'egisz_messages_raw'
          AND column_name = 'kind'
    ) THEN
        ALTER TABLE egisz_messages_raw DROP COLUMN kind;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'egisz_messages_raw'
          AND column_name = 'msgtext'
    ) THEN
        ALTER TABLE egisz_messages_raw DROP COLUMN msgtext;
    END IF;
END
$$;

CREATE MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui AS
SELECT
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    t.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    t.message_id AS "MSGID обмена",
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День",
    t.log_date::date AS "День (тренд)",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.status AS "Статус",
    t.error_type AS "Тип ошибки",
    t.error_summary AS "Сводка ошибки",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    public.egisz_normalize_semd_code(t.semd_code) AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_clean_text_value(t.semd_name) IS NOT NULL
             AND public.egisz_clean_text_value(t.semd_name) !~ '^\d+$'
             AND public.egisz_clean_text_value(t.semd_name) <> public.egisz_normalize_semd_code(t.semd_code)
            THEN public.egisz_clean_text_value(t.semd_name)
            ELSE NULL
        END,
        CASE
            WHEN public.egisz_normalize_semd_code(t.semd_code) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid)::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid)::text) AS "Наименование клиники",
    t.jid::text AS "JID из журнала (gost, число)",
    o.name AS "Медицинская организация",
    t.org_oid AS "OID организации",
    l.mo_uid AS "OID клиники",
    public.egisz_clean_host(t.callback_url) AS "Хост клиники (VPN ГОСТ)",
    o.inn AS "ИНН клиники",
    l.mo_domen AS "Токен gost (нецифр., для отображения)",
    l.jid::text AS "JID (EGISZ_LICENSES)",
    CASE WHEN t.jid IS NOT NULL AND l.jid IS NOT NULL AND t.jid <> l.jid THEN 'да' ELSE 'нет' END AS "Расхождение источников JID",
    t.creation_date AS "Создание СЭМД",
    public.egisz_extract_jid_from_endpoint(m.reply_to) AS "JID из gost в REPLYTO",
    public.egisz_clean_host(m.reply_to) AS "Токен gost (REPLYTO)",
    t.local_uid_semd AS "localUid СЭМД",
    t.local_uid_semd AS "Идентификатор документа (localUid)",
    t.relates_to_id AS "Связанное сообщение",
    lower(NULLIF(btrim(t.relates_to_id), '')) AS "Связанное сообщение (канон)",
    lower(NULLIF(btrim(t.local_uid_semd), '')) AS "localUid СЭМД (канон)",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.emdr_id AS "Регистрационный номер РЭМД",
    t.doc_number AS "DOCUMENTID",
    t.error_json_text AS "Исходный текст ошибки",
    t.exchangelog_log_id AS transaction_id,
    COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid) AS clinic_id,
    public.egisz_normalize_semd_code(t.semd_code) AS service_id
FROM fact_egisz_transactions t
LEFT JOIN egisz_messages_raw m ON m.egmid = t.egmid
LEFT JOIN LATERAL (
    SELECT candidate.*
    FROM (
        (SELECT dl.*, 0 AS _prio
         FROM dim_licenses dl
         WHERE t.org_oid IS NOT NULL AND dl.mo_uid = t.org_oid
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 1 AS _prio
         FROM dim_licenses dl
         WHERE t.jid IS NOT NULL AND dl.jid = t.jid
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 2 AS _prio
         FROM dim_licenses dl
         WHERE public.egisz_extract_jid_from_endpoint(m.reply_to) IS NOT NULL
           AND dl.jid::text = public.egisz_extract_jid_from_endpoint(m.reply_to)
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
        UNION ALL
        (SELECT dl.*, 3 AS _prio
         FROM dim_licenses dl
         WHERE public.egisz_clean_host(m.reply_to) IS NOT NULL
           AND public.egisz_clean_host(dl.mo_domen) = public.egisz_clean_host(m.reply_to)
         ORDER BY dl.modifydate DESC NULLS LAST, dl.id DESC LIMIT 1)
    ) candidate
    ORDER BY _prio, modifydate DESC NULLS LAST, id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN public.dim_semd_types st ON st.code = public.egisz_normalize_semd_code(t.semd_code)
LEFT JOIN dim_organizations o ON COALESCE(t.jid, NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer, l.jid) = o.jid;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_transaction_id
    ON public.v_egisz_transactions_enriched_ui (transaction_id);
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_day
    ON public.v_egisz_transactions_enriched_ui ("День");
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_jid
    ON public.v_egisz_transactions_enriched_ui ("JID клиники");
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_status
    ON public.v_egisz_transactions_enriched_ui ("Статус");
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_localuid_norm
    ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("localUid СЭМД"), '')));
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_emdrid_norm
    ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("Рег. номер РЭМД (emdrid)"), '')));
CREATE INDEX IF NOT EXISTS idx_v_egisz_transactions_enriched_ui_relates_to_norm
    ON public.v_egisz_transactions_enriched_ui (lower(NULLIF(btrim("Связанное сообщение"), '')));

CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui AS
SELECT
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День (тренд)",
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.local_uid_semd AS "localUid СЭМД",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.relates_to_id AS "Связанное сообщение",
    t.jid::text AS "JID клиники",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    t.status AS "Статус",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN COALESCE(NULLIF(t.error_json_text, ''), '(нет текста)')
        ELSE ''
    END AS "Исходный текст ошибки",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN COALESCE(NULLIF(t.error_summary, ''), 'Неизвестная ошибка')
        ELSE ''
    END AS "Интерпретация ошибки",
    CASE
        WHEN t.status = 'success' THEN 'Успешно'
        WHEN t.status = 'error' THEN t.error_type
        ELSE ''
    END AS "Тип ошибки",
    1::bigint AS "Порядок ошибки"
FROM fact_egisz_transactions t
WHERE t.status = 'error'

UNION ALL

SELECT
    t.log_date AS "Обработано IPS",
    t.log_date::date AS "День (тренд)",
    t.exchangelog_log_id::text AS "LOGID журнала EXCHANGELOG",
    COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id, t.doc_number, t.message_id, t.exchangelog_log_id::text) AS "Документ (ключ учёта)",
    t.local_uid_semd AS "localUid СЭМД",
    t.emdr_id AS "Рег. номер РЭМД (emdrid)",
    t.relates_to_id AS "Связанное сообщение",
    t.jid::text AS "JID клиники",
    public.egisz_semd_type_report_label(t.semd_code, t.semd_name) AS "Тип СЭМД (код · НСИ)",
    t.status AS "Статус",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Исходный текст ошибки",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Интерпретация ошибки",
    CASE WHEN t.status = 'success' THEN 'Успешно' ELSE '' END AS "Тип ошибки",
    NULL::bigint AS "Порядок ошибки"
FROM fact_egisz_transactions t
WHERE t.status <> 'error' OR t.error_summary IS NULL;

CREATE OR REPLACE VIEW public.v_stg_channel_errors_by_document AS
SELECT
    r.logid AS id,
    COALESCE(r.createdate, r.loaded_at) AS created_at,
    CASE WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3' ELSE 'PARSE_ERROR' END AS error_code,
    COALESCE(NULLIF(r.logtext, ''), NULLIF(r.msgtext, ''), '(без текста)') AS message,
    CASE WHEN r.logstate = 3 THEN 'network' ELSE 'async_response' END AS error_top_type,
    CASE WHEN r.logstate = 3 THEN 'Сетевая ошибка' ELSE 'Неизвестная ошибка' END AS error_global_subcategory,
    CASE WHEN r.logstate = 3 THEN 'Ошибка связи' ELSE 'Неизвестная ошибка' END AS error_group_label_ru,
    r.logid AS exchangelog_log_id,
    r.msgid AS journal_msgid,
    m.egmid AS egisz_messages_egmid,
    COALESCE(
        x.relates_to_message_msgtext,
        x.relates_to_msgtext,
        x.relates_to_message_logtext
    ) AS relates_to_hint,
    COALESCE(
        x.local_uid_msgtext,
        x.document_id_msgtext,
        m.document_id
    ) AS local_uid_hint,
    x.emdr_id_msgtext AS emdr_id_hint,
    COALESCE(
        x.local_uid_msgtext,
        x.document_id_msgtext,
        x.emdr_id_msgtext,
        x.relates_to_message_msgtext,
        x.relates_to_msgtext,
        m.document_id,
        r.msgid,
        r.logid::text
    ) AS document_group_key,
    COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext) AS relates_to_id
FROM exchangelog_raw r
LEFT JOIN LATERAL (
    SELECT
        public.egisz_xml_text(r.msgtext, 'relatesToMessage') AS relates_to_message_msgtext,
        public.egisz_xml_text(r.msgtext, 'relatesTo') AS relates_to_msgtext,
        public.egisz_xml_text(r.logtext, 'relatesToMessage') AS relates_to_message_logtext,
        public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_msgtext,
        public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_msgtext,
        public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id_msgtext
) x ON TRUE
LEFT JOIN LATERAL (
    SELECT em.*
    FROM egisz_messages_raw em
    WHERE lower(NULLIF(btrim(em.document_id), '')) IN (
            lower(NULLIF(btrim(x.local_uid_msgtext), '')),
            lower(NULLIF(btrim(x.document_id_msgtext), '')),
            lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
          )
       OR public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext))
       OR public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(r.msgid)
    ORDER BY
        CASE
            WHEN lower(NULLIF(btrim(em.document_id), '')) IN (
                lower(NULLIF(btrim(x.local_uid_msgtext), '')),
                lower(NULLIF(btrim(x.document_id_msgtext), '')),
                lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
            ) THEN 0
            WHEN public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext)) THEN 1
            ELSE 2
        END,
        em.egmid DESC
    LIMIT 1
) m ON TRUE
WHERE r.logstate = 3
   OR COALESCE(r.msgtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%ошиб%';

CREATE OR REPLACE VIEW public.v_stg_channel_network_errors_by_document AS
SELECT *
FROM public.v_stg_channel_errors_by_document
WHERE error_top_type = 'network';

CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui AS
WITH source_rows AS (
    SELECT
        s.*,
        NULLIF((regexp_match(COALESCE(s.message, ''), 'gost-([0-9]+)', 'i'))[1], '') AS jid_from_text
    FROM public.v_stg_channel_network_errors_by_document s
)
SELECT
    s.created_at AS "Дата создания документа",
    s.exchangelog_log_id::text AS "LOGID журнала (сетевая ошибка)",
    s.journal_msgid AS "MSGID обмена",
    s.egisz_messages_egmid::text AS "EGMID сообщения (строка журнала)",
    s.document_group_key AS "Ключ документа (группировка)",
    s.relates_to_hint AS "relatesToMessage (из текста журнала)",
    s.local_uid_hint AS "localUid / DOCUMENTID (из текста)",
    s.emdr_id_hint AS "emdrId (из текста)",
    public.egisz_clean_host(s.message) AS "Хост клиники (VPN ГОСТ)",
    COALESCE(f."JID клиники", s.jid_from_text) AS "JID клиники",
    COALESCE(f."JID из журнала (gost, число)", s.jid_from_text) AS "JID из журнала (gost, число)",
    COALESCE(f."Наименование клиники", 'Клиника JID: ' || COALESCE(f."JID клиники", s.jid_from_text, '(нет JID)')) AS "Клиника (транспорт)",
    f."Медицинская организация",
    f."Тип СЭМД (код · НСИ)",
    f."Код СЭМД",
    f."Сводка ошибки" AS "Сводка ошибки регистрации",
    s.message AS "Текст сетевой ошибки",
    s.message AS "Сообщение",
    s.error_global_subcategory AS "Подтип ошибки канала",
    CASE WHEN f."Документ (ключ учёта)" IS NULL THEN 'нет' ELSE 'да' END AS "Связанный колбэк найден в аналитике",
    f."LOGID журнала EXCHANGELOG" AS "LOGID записи ответа",
    f."EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)" AS "EGMID записи ответа",
    f."Связанное сообщение" AS "Связанное сообщение (ответ РЭМД)",
    f."Идентификатор документа (localUid)",
    f."Регистрационный номер РЭМД"
FROM source_rows s
LEFT JOIN LATERAL (
    SELECT f.*
    FROM public.v_egisz_transactions_enriched_ui f
    WHERE lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), ''))
       OR lower(NULLIF(btrim(f."Рег. номер РЭМД (emdrid)"), '')) = lower(NULLIF(btrim(s.emdr_id_hint), ''))
       OR lower(NULLIF(btrim(f."Связанное сообщение"), '')) = lower(NULLIF(btrim(s.relates_to_hint), ''))
    ORDER BY
        CASE
            WHEN lower(NULLIF(btrim(f."localUid СЭМД"), '')) = lower(NULLIF(btrim(s.local_uid_hint), '')) THEN 0
            WHEN lower(NULLIF(btrim(f."Рег. номер РЭМД (emdrid)"), '')) = lower(NULLIF(btrim(s.emdr_id_hint), '')) THEN 1
            ELSE 2
        END,
        f."Обработано IPS" DESC NULLS LAST
    LIMIT 1
) f ON TRUE;

COMMENT ON VIEW public.v_rpt_network_errors_detail_ui IS
'Техническая витрина ошибок связи proxy_egisz: healthcheck/поддержка клиник, LOGSTATE=3 и строки журнала с привязкой к документу, если её удалось восстановить.';

CREATE OR REPLACE VIEW public.v_rpt_documents_no_response_ui AS
WITH messages AS (
    SELECT
        m.egmid,
        m.created_at,
        m.msgid,
        m.reply_to,
        m.document_id,
        public.egisz_normalize_semd_code(
            COALESCE(public.egisz_xml_text(r.msgtext, 'kind'),
                     public.egisz_xml_text(r.msgtext, 'KIND'))
        ) AS semd_code_resolved,
        public.egisz_clean_text_value(
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'),
                     public.egisz_xml_text(r.msgtext, 'name'),
                     public.egisz_xml_text(r.msgtext, 'documentName'))
        ) AS semd_name_payload,
        public.egisz_normalize_message_id(m.msgid) AS msgid_norm,
        lower(NULLIF(btrim(m.document_id), '')) AS document_id_norm,
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS reply_to_jid,
        public.egisz_clean_host(m.reply_to) AS reply_to_host
    FROM egisz_messages_raw m
    LEFT JOIN LATERAL (
        SELECT er.msgtext
        FROM exchangelog_raw er
        WHERE er.msgid IS NOT NULL
          AND public.egisz_normalize_message_id(er.msgid) = public.egisz_normalize_message_id(m.msgid)
        ORDER BY er.logid DESC
        LIMIT 1
    ) r ON TRUE
),
fact_message_keys AS (
    SELECT DISTINCT public.egisz_normalize_message_id(f.message_id) AS message_key
    FROM fact_egisz_transactions f
    WHERE NULLIF(public.egisz_normalize_message_id(f.message_id), '') IS NOT NULL

    UNION

    SELECT DISTINCT public.egisz_normalize_message_id(f.relates_to_id) AS message_key
    FROM fact_egisz_transactions f
    WHERE NULLIF(public.egisz_normalize_message_id(f.relates_to_id), '') IS NOT NULL
),
fact_document_keys AS (
    SELECT DISTINCT lower(NULLIF(btrim(f.local_uid_semd), '')) AS document_key
    FROM fact_egisz_transactions f
    WHERE lower(NULLIF(btrim(f.local_uid_semd), '')) IS NOT NULL
)
SELECT
    m.created_at AS "Отправлено",
    m.document_id AS "localUid СЭМД",
    m.document_id AS "Идентификатор документа (localUid)",
    m.semd_code_resolved AS "Код СЭМД",
    COALESCE(
        st.name,
        CASE
            WHEN public.egisz_clean_text_value(m.semd_name_payload) IS NOT NULL
             AND public.egisz_clean_text_value(m.semd_name_payload) !~ '^\d+$'
             AND public.egisz_clean_text_value(m.semd_name_payload) <> public.egisz_normalize_semd_code(m.semd_code_resolved)
            THEN public.egisz_clean_text_value(m.semd_name_payload)
            ELSE NULL
        END,
        CASE
            WHEN public.egisz_normalize_semd_code(m.semd_code_resolved) IS NOT NULL
            THEN 'Наименование СЭМД отсутствует в справочнике СЭМД'
            ELSE NULL
        END
    ) AS "Наименование СЭМД",
    public.egisz_semd_type_report_label(m.semd_code_resolved, m.semd_name_payload) AS "Тип СЭМД (код · НСИ)",
    COALESCE(m.reply_to_jid, l.jid)::text AS "JID клиники",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(m.reply_to_jid, l.jid)::text) AS "Наименование клиники",
    m.reply_to AS "Связанное сообщение",
    m.egmid::text AS "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    m.msgid AS "MSGID обмена"
FROM messages m
LEFT JOIN LATERAL (
    SELECT dl.*
    FROM dim_licenses dl
    WHERE (m.reply_to_jid IS NOT NULL AND dl.jid = m.reply_to_jid)
       OR (m.reply_to_host IS NOT NULL AND public.egisz_clean_host(dl.mo_domen) = m.reply_to_host)
    ORDER BY
        CASE
            WHEN m.reply_to_jid IS NOT NULL AND dl.jid = m.reply_to_jid THEN 0
            ELSE 1
        END,
        dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN dim_organizations o ON o.jid = COALESCE(m.reply_to_jid, l.jid)
LEFT JOIN dim_semd_types st ON st.code = public.egisz_normalize_semd_code(m.semd_code_resolved)
LEFT JOIN fact_message_keys fm ON fm.message_key = m.msgid_norm
LEFT JOIN fact_document_keys fd ON fd.document_key = m.document_id_norm
WHERE fm.message_key IS NULL
  AND fd.document_key IS NULL;

CREATE OR REPLACE VIEW public.v_rpt_semd_archive_ui AS
SELECT
    "Обработано IPS" AS "Дата обработки",
    "День (тренд)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "JID клиники",
    "Наименование клиники",
    "OID организации",
    "OID клиники",
    "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    "Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД",
    "Статус",
    "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    "Создание СЭМД",
    "Сводка ошибки"
FROM public.v_egisz_transactions_enriched_ui

UNION ALL

SELECT
    "Отправлено" AS "Дата обработки",
    "Отправлено"::date AS "День (тренд)",
    "Код СЭМД",
    "Наименование СЭМД",
    "Тип СЭМД (код · НСИ)",
    "JID клиники" AS "JID",
    "JID клиники",
    "Наименование клиники",
    NULL::text AS "OID организации",
    NULL::text AS "OID клиники",
    COALESCE("localUid СЭМД", "MSGID обмена", "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)") AS "Документ (ключ учёта)",
    "localUid СЭМД",
    "Связанное сообщение",
    NULL::text AS "Рег. номер РЭМД",
    'ожидание ответа' AS "Статус",
    NULL::text AS "LOGID журнала EXCHANGELOG",
    "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)",
    "MSGID обмена",
    NULL::timestamptz AS "Создание СЭМД",
    NULL::text AS "Сводка ошибки"
FROM public.v_rpt_documents_no_response_ui;

CREATE OR REPLACE VIEW public.v_rpt_clinic_connectivity_daily_ui AS
WITH success_by_day AS (
    SELECT
        "Обработано IPS"::date AS day,
        NULLIF("JID клиники", '') AS jid,
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'success')::bigint AS ok_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_remd_cnt
    FROM public.v_egisz_transactions_enriched_ui
    GROUP BY 1, 2
),
network_by_day AS (
    SELECT
        "Дата создания документа"::date AS day,
        NULLIF(COALESCE("JID клиники", "JID из журнала (gost, число)"), '') AS jid,
        MAX("Клиника (транспорт)") AS clinic_name,
        COUNT(DISTINCT "Ключ документа (группировка)")::bigint AS err_cnt
    FROM public.v_rpt_network_errors_detail_ui
    GROUP BY 1, 2
)
SELECT
    COALESCE(s.day, n.day) AS "День",
    COALESCE(s.jid, n.jid) AS "JID клиники (ключ)",
    COALESCE(s.jid, n.jid) AS "JID клиники",
    COALESCE(NULLIF(s.clinic_name, ''), NULLIF(n.clinic_name, ''), 'Клиника JID: ' || COALESCE(s.jid, n.jid)) AS "Наименование клиники",
    COALESCE(s.ok_cnt, 0)::bigint AS "Успешные ответы РЭМД (документов)",
    COALESCE(s.ok_cnt, 0)::bigint AS "Ответы РЭМД: успех (документов)",
    COALESCE(s.err_remd_cnt, 0)::bigint AS "Ответы РЭМД: отказ (документов)",
    COALESCE(n.err_cnt, 0)::bigint AS "Ошибки связи (документов)",
    ROUND(100.0 * COALESCE(s.ok_cnt, 0) / NULLIF(COALESCE(s.ok_cnt, 0) + COALESCE(n.err_cnt, 0), 0), 2) AS "Доступность транспорта (прибл.), %"
FROM success_by_day s
FULL OUTER JOIN network_by_day n ON s.day = n.day AND s.jid = n.jid;

CREATE OR REPLACE VIEW public.v_rpt_connectivity_global_daily_ui AS
SELECT
    "День",
    SUM("Успешные ответы РЭМД (документов)")::bigint AS "Успешные ответы РЭМД (документов)",
    SUM("Ошибки связи (документов)")::bigint AS "Ошибки связи (документов)",
    ROUND(100.0 * SUM("Успешные ответы РЭМД (документов)") / NULLIF(SUM("Успешные ответы РЭМД (документов)") + SUM("Ошибки связи (документов)"), 0), 2) AS "Доступность транспорта (прибл.), %"
FROM public.v_rpt_clinic_connectivity_daily_ui
GROUP BY 1;

CREATE OR REPLACE VIEW public.v_health_by_clinic_ui AS
WITH anchor AS (
    -- Use the latest observed fact as the reference point so the "last 24h" window
    -- works on stale / archival data, not only on real-time pipelines.
    SELECT COALESCE(MAX("Обработано IPS"), now()) AS ref_ts
    FROM public.v_egisz_transactions_enriched_ui
),
fact_24h AS (
    SELECT
        "JID клиники",
        MAX("Наименование клиники") AS clinic_name,
        COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS docs_cnt,
        COUNT(DISTINCT "Документ (ключ учёта)") FILTER (WHERE "Статус" = 'error')::bigint AS err_cnt
    FROM public.v_egisz_transactions_enriched_ui, anchor
    WHERE "Обработано IPS" >= anchor.ref_ts - INTERVAL '24 hours'
    GROUP BY 1
),
queue AS (
    SELECT "JID клиники", COUNT(DISTINCT "localUid СЭМД")::bigint AS queue_cnt
    FROM public.v_rpt_documents_no_response_ui
    GROUP BY 1
)
SELECT
    f."JID клиники",
    COALESCE(NULLIF(f.clinic_name, ''), 'Клиника JID: ' || f."JID клиники") AS "Наименование клиники",
    ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) AS "Доля ошибок, %",
    f.docs_cnt AS "Документов за 24ч",
    COALESCE(q.queue_cnt, 0)::bigint AS "В очереди (документов)",
    CASE
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 20 OR COALESCE(q.queue_cnt, 0) >= 100 THEN 'critical'
        WHEN ROUND(100.0 * f.err_cnt / NULLIF(f.docs_cnt, 0), 2) >= 5 OR COALESCE(q.queue_cnt, 0) >= 20 THEN 'warning'
        ELSE 'ok'
    END AS "Уровень здоровья"
FROM fact_24h f
LEFT JOIN queue q ON q."JID клиники" = f."JID клиники";

CREATE OR REPLACE VIEW public.v_health_proxy_db_ui AS
SELECT
    (SELECT COUNT(*) FROM exchangelog_raw)::bigint AS "Staging: всего строк",
    (SELECT COUNT(*) FROM egisz_messages_raw WHERE egmid IS NULL)::bigint AS "Без EGMID",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui)::bigint AS "Очередь всего",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours')::bigint AS "Очередь > 24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '24 hours' AND "Отправлено" < now() - INTERVAL '1 hour')::bigint AS "Очередь 1–24ч",
    (SELECT COUNT(DISTINCT "localUid СЭМД") FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" >= now() - INTERVAL '1 hour')::bigint AS "Очередь < 1ч",
    (SELECT MAX(egmid) FROM egisz_messages_raw) AS "Staging max EGMID",
    (SELECT MAX(created_at) FROM egisz_messages_raw) AS "Staging max Sent",
    (SELECT MAX(updated_at) FROM elt_state) AS "Последний апдейт курсора",
    (SELECT MAX(last_log_id) FROM elt_state) AS "elt_state.last_log_id",
    (SELECT MAX(last_egmid) FROM elt_state) AS "elt_state.last_egmid (курсор EGISZ_MESSAGES)",
    (SELECT MAX(logid) FROM exchangelog_raw) AS "Staging max ID",
    (SELECT COUNT(DISTINCT "Документ (ключ учёта)") FROM public.v_egisz_transactions_enriched_ui)::bigint AS "Всего документов";

CREATE OR REPLACE VIEW public.v_health_signals_ui AS
WITH anchor AS (
    SELECT MAX(log_date) AS last_fact_ts FROM fact_egisz_transactions
)
SELECT * FROM (
    VALUES
        ('raw_rows', 'Raw-строки proxy_egisz', 'green', (SELECT COUNT(*)::numeric FROM exchangelog_raw), 'строк', 'exchangelog_raw', 'Контроль поступления журнала EXCHANGELOG'),
        ('queue_24h', 'Очередь без ответа > 24ч', 'yellow', (SELECT COUNT(DISTINCT "localUid СЭМД")::numeric FROM public.v_rpt_documents_no_response_ui WHERE "Отправлено" < now() - INTERVAL '24 hours'), 'документов', 'egisz_messages_raw без callback-факта', 'Проверить клиники с зависшими документами и транспортный канал'),
        ('network_errors', 'Ошибки связи', 'yellow', (SELECT COUNT(DISTINCT "Ключ документа (группировка)")::numeric FROM public.v_rpt_network_errors_detail_ui), 'документов', 'EXCHANGELOG LOGSTATE=3 и журнал ошибок', 'Разобрать top формулировок и последние события в дашборде 02'),
        ('error_rows', 'Ошибки регистрации РЭМД', 'yellow', (SELECT COUNT(*)::numeric FROM fact_egisz_transactions WHERE status = 'error'), 'строк', 'fact_egisz_transactions.status=error', 'Проверить причины отказов ЕГИСЗ в дашбордах 04 и 05'),
        ('data_freshness',
         'Свежесть данных (последний факт)',
         CASE
             WHEN (SELECT last_fact_ts FROM anchor) IS NULL THEN 'red'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '1 hour'  THEN 'green'
             WHEN (SELECT last_fact_ts FROM anchor) >= now() - INTERVAL '24 hours' THEN 'yellow'
             ELSE 'red'
         END,
         ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT last_fact_ts FROM anchor), now()))) / 60.0, 1)::numeric,
         'минут с последнего факта',
         'fact_egisz_transactions.log_date',
         'Проверить ELT-цикл, Airflow scheduler и доступ к Firebird')
) AS v("Код сигнала", "Сигнал", "Уровень", "Значение", "Единица", "База расчёта", "Что делать");

-- Backfill error_code и error_type для уже загруженных фактов после смены
-- парсинга и таксономии.
--   1) Если error_code засорён XML-фрагментом (например, '<' в значении) —
--      повторно извлекаем код из msgtext исправленной egisz_xml_text.
--   2) Перекалькулируем error_type по новой плоской классификации
--      (см. egisz_error_classify); error_summary и error_json_text
--      перестраиваем заодно из freshly-rebuilt errors_json.
-- Идемпотентно: повторный прогон даёт тот же результат.
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

-- Backfill error_type только для строк, ещё НЕ классифицированных
-- (новые строки после миграции 2026-05-15 заполняются upsert-ом самой
-- egisz_transform_raw_to_facts; этот UPDATE — safety-net для re-init.)
-- Большие исторические пересчёты делаются отдельной миграцией с батчами,
-- чтобы не упереться в statement_timeout этого скрипта.
UPDATE public.fact_egisz_transactions f
SET error_type = CASE
        WHEN f.error_code = 'INTEGRATION_LOGSTATE_3' THEN 'Сетевая ошибка'
        ELSE public.egisz_error_classify(
            public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
        )
    END,
    error_summary = public.egisz_error_interpretation_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    ),
    error_json_text = public.egisz_error_messages_row(
        public.egisz_build_errors_json(f.status, f.error_code, f.error_message, r.msgtext)
    )
FROM public.exchangelog_raw r
WHERE r.logid = f.exchangelog_log_id
  AND f.status = 'error'
  AND f.error_type IS NULL;

-- =============================================================================
-- Analytics layer (migration 004): document registry, per-org/per-type/per-error
-- aggregates, daily/hourly timeseries, no-response queue, service health, KPI.
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.etl_run_log (
    run_ts          timestamptz NOT NULL DEFAULT now(),
    docs_processed  integer,
    errors_count    integer,
    duration_ms     integer,
    batch_min_id    bigint,
    batch_max_id    bigint,
    batch_min_egmid bigint,
    batch_max_egmid bigint,
    PRIMARY KEY (run_ts)
);

CREATE INDEX IF NOT EXISTS idx_etl_run_log_run_ts ON public.etl_run_log (run_ts DESC);

CREATE OR REPLACE FUNCTION public.egisz_doc_key(
    p_local_uid text,
    p_emdr_id text,
    p_doc_number text,
    p_message_id text,
    p_log_id bigint
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        NULLIF(btrim(p_local_uid), ''),
        NULLIF(btrim(p_emdr_id), ''),
        NULLIF(btrim(p_doc_number), ''),
        NULLIF(btrim(p_message_id), ''),
        p_log_id::text
    );
$$;

-- Сбрасываем зависимые view (порядок важен: kpi/service_health → stat_orgs/docs_no_response).
DROP VIEW IF EXISTS public.v_kpi_summary_ui      CASCADE;
DROP VIEW IF EXISTS public.v_service_health_ui   CASCADE;
DROP VIEW IF EXISTS public.v_doc_registry_ui     CASCADE;
DROP VIEW IF EXISTS public.v_doc_timeline_ui     CASCADE;
DROP VIEW IF EXISTS public.v_stat_semd_types_ui  CASCADE;
DROP VIEW IF EXISTS public.v_stat_errors_ui      CASCADE;
DROP VIEW IF EXISTS public.v_stat_orgs_ui        CASCADE;
DROP VIEW IF EXISTS public.v_stat_daily_ui       CASCADE;
DROP VIEW IF EXISTS public.v_stat_hourly_ui      CASCADE;
-- v_docs_no_response_ui used to be a regular view; the new schema makes it a matview,
-- so drop both shapes idempotently when upgrading an existing DWH.
DO $$ BEGIN DROP VIEW IF EXISTS public.v_docs_no_response_ui CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_docs_no_response_ui CASCADE;

CREATE OR REPLACE VIEW public.v_doc_registry_ui AS
WITH per_tx AS (
    SELECT
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.*
    FROM public.fact_egisz_transactions t
),
agg AS (
    SELECT
        doc_key,
        MAX(NULLIF(btrim(local_uid_semd), ''))                 AS local_uid_semd,
        MAX(NULLIF(btrim(emdr_id), ''))                        AS emdr_id,
        MAX(NULLIF(btrim(doc_number), ''))                     AS doc_number,
        MAX(public.egisz_normalize_semd_code(semd_code))       AS semd_code,
        MAX(jid)                                               AS jid,
        MIN(creation_date)                                     AS creation_date,
        MIN(log_date)                                          AS first_sent_date,
        MAX(log_date)                                          AS last_sent_date,
        COUNT(*)                                               AS attempt_count,
        bool_or(status = 'success')                            AS any_success,
        bool_or(status = 'error')                              AS any_error,
        bool_or(NULLIF(btrim(emdr_id), '') IS NOT NULL)        AS has_emdr
    FROM per_tx
    GROUP BY doc_key
),
last_tx AS (
    SELECT DISTINCT ON (doc_key)
        doc_key,
        status        AS final_status_raw,
        error_type    AS final_error_type,
        error_summary AS final_error_summary
    FROM per_tx
    ORDER BY doc_key, log_date DESC NULLS LAST, exchangelog_log_id DESC
)
SELECT
    a.doc_key                                                       AS "Идентификатор документа",
    a.local_uid_semd                                                AS "Локальный UID СЭМД",
    a.emdr_id                                                       AS "ID в РЭМД",
    a.doc_number                                                    AS "Номер документа",
    a.semd_code                                                     AS "Код СЭМД",
    COALESCE(st.name, '(нет в справочнике)')                        AS "Тип СЭМД",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || a.jid::text)    AS "Клиника",
    a.jid::text                                                     AS "JID клиники",
    a.creation_date                                                 AS "Дата создания документа",
    a.first_sent_date                                               AS "Первая отправка",
    a.last_sent_date                                                AS "Последняя отправка",
    a.attempt_count::int                                            AS "Попыток отправки",
    CASE
        WHEN a.any_success THEN 'success'
        WHEN a.any_error   THEN 'error'
        ELSE 'pending'
    END                                                              AS "Итоговый статус",
    l.final_error_type                                              AS "Тип ошибки",
    l.final_error_summary                                           AS "Описание ошибки",
    ROUND(
        EXTRACT(EPOCH FROM (COALESCE(a.last_sent_date, now()) - a.first_sent_date)) / 86400.0,
        2
    )::numeric                                                       AS "Дней в обработке",
    a.has_emdr                                                       AS "Зарегистрирован в РЭМД",
    a.last_sent_date                                                 AS "Дата последней попытки (сорт.)",
    a.jid                                                            AS "JID (число)"
FROM agg a
LEFT JOIN last_tx l ON l.doc_key = a.doc_key
LEFT JOIN public.dim_semd_types st ON st.code = a.semd_code
LEFT JOIN public.dim_organizations o ON o.jid = a.jid;

CREATE OR REPLACE VIEW public.v_doc_timeline_ui AS
SELECT
    public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS "Идентификатор документа",
    t.exchangelog_log_id                                         AS "LOGID",
    t.log_date                                                    AS "Время записи",
    t.status                                                      AS "Статус",
    t.message_id                                                  AS "MSGID обмена",
    t.relates_to_id                                               AS "Связанное сообщение",
    t.error_type                                                  AS "Тип ошибки",
    t.error_summary                                               AS "Сводка ошибки",
    t.error_json_text                                             AS "Исходный текст ошибки",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || t.jid::text) AS "Клиника",
    public.egisz_normalize_semd_code(t.semd_code)                 AS "Код СЭМД",
    COALESCE(st.name, '(нет в справочнике)')                      AS "Тип СЭМД",
    t.emdr_id                                                     AS "ID в РЭМД",
    t.local_uid_semd                                              AS "Локальный UID СЭМД"
FROM public.fact_egisz_transactions t
LEFT JOIN public.dim_organizations o ON o.jid = t.jid
LEFT JOIN public.dim_semd_types st ON st.code = public.egisz_normalize_semd_code(t.semd_code);

CREATE OR REPLACE VIEW public.v_stat_semd_types_ui AS
WITH period AS (
    SELECT (now() - INTERVAL '30 days') AS since_ts
),
filtered AS (
    SELECT
        public.egisz_normalize_semd_code(t.semd_code) AS code,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.*
    FROM public.fact_egisz_transactions t, period p
    WHERE t.log_date >= p.since_ts
),
docs AS (
    SELECT
        code, doc_key,
        COUNT(*)                              AS attempts,
        bool_or(status = 'success')           AS any_success,
        bool_or(status = 'error')             AS any_error,
        MAX(jid)                              AS jid
    FROM filtered
    GROUP BY code, doc_key
),
err_per_code AS (
    SELECT code, error_type, COUNT(*) AS cnt
    FROM filtered
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
    GROUP BY code, error_type
),
top_err AS (
    SELECT DISTINCT ON (code) code, error_type AS top_error_type, cnt AS top_error_count
    FROM err_per_code
    ORDER BY code, cnt DESC, error_type
),
agg AS (
    SELECT
        d.code,
        COUNT(*)::bigint                                                       AS unique_docs,
        SUM(d.attempts)::bigint                                                AS total_sent,
        COUNT(*) FILTER (WHERE d.any_success)::bigint                          AS success_count,
        COUNT(*) FILTER (WHERE d.any_error AND NOT d.any_success)::bigint      AS error_count,
        COUNT(*) FILTER (WHERE NOT d.any_success AND NOT d.any_error)::bigint  AS pending_count,
        AVG(d.attempts)::numeric(10,2)                                         AS avg_attempts,
        COUNT(DISTINCT d.jid)::bigint                                          AS orgs_using
    FROM docs d
    GROUP BY d.code
)
SELECT
    COALESCE(a.code, '(нет кода)')                                       AS "Код СЭМД",
    COALESCE(st.name, a.code, '(неизвестно)')                            AS "Тип СЭМД",
    a.total_sent                                                          AS "Транзакций",
    a.unique_docs                                                         AS "Уникальных документов",
    a.success_count                                                       AS "Успешных",
    a.error_count                                                         AS "С ошибкой",
    a.pending_count                                                       AS "Без ответа",
    ROUND(100.0 * a.success_count / NULLIF(a.unique_docs, 0), 1)::numeric AS "% успеха",
    a.avg_attempts                                                        AS "Среднее попыток",
    a.orgs_using                                                          AS "Клиник использует",
    t.top_error_type                                                      AS "Топ ошибки",
    t.top_error_count                                                     AS "Шт. топ ошибки",
    a.code                                                                 AS "Код СЭМД (ключ)"
FROM agg a
LEFT JOIN public.dim_semd_types st ON st.code = a.code
LEFT JOIN top_err t ON t.code = a.code
ORDER BY a.unique_docs DESC NULLS LAST;

CREATE OR REPLACE VIEW public.v_stat_errors_ui AS
WITH window_30d AS (
    SELECT
        t.error_type, t.error_summary, t.jid,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key,
        t.log_date
    FROM public.fact_egisz_transactions t
    WHERE t.status = 'error'
      AND NULLIF(btrim(t.error_type), '') IS NOT NULL
      AND t.log_date >= now() - INTERVAL '30 days'
),
trend_7d AS (
    SELECT error_type, COUNT(*)::bigint AS cnt
    FROM public.fact_egisz_transactions
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '7 days'
    GROUP BY error_type
),
trend_prev_7d AS (
    SELECT error_type, COUNT(*)::bigint AS cnt
    FROM public.fact_egisz_transactions
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '14 days'
      AND log_date <  now() - INTERVAL '7 days'
    GROUP BY error_type
),
totals AS (SELECT COUNT(*)::bigint AS total_errors FROM window_30d),
agg AS (
    SELECT
        w.error_type,
        MAX(w.error_summary)                          AS error_summary,
        COUNT(*)::bigint                              AS error_count,
        COUNT(DISTINCT w.doc_key)::bigint             AS unique_docs_affected,
        COUNT(DISTINCT w.jid)::bigint                 AS orgs_affected,
        MIN(w.log_date)                               AS first_seen,
        MAX(w.log_date)                               AS last_seen
    FROM window_30d w
    GROUP BY w.error_type
)
SELECT
    a.error_type                                                                       AS "Тип ошибки",
    COALESCE(a.error_summary, a.error_type)                                            AS "Описание ошибки",
    a.error_count                                                                       AS "Всего вхождений",
    a.unique_docs_affected                                                              AS "Уникальных документов",
    a.orgs_affected                                                                     AS "Клиник затронуто",
    ROUND(100.0 * a.error_count / NULLIF((SELECT total_errors FROM totals), 0), 1)::numeric AS "% от всех ошибок",
    a.first_seen                                                                        AS "Впервые увидели",
    a.last_seen                                                                         AS "Последнее появление",
    COALESCE(t7.cnt, 0)                                                                 AS "За последние 7 дней",
    COALESCE(tp.cnt, 0)                                                                 AS "За предыдущие 7 дней",
    COALESCE(t7.cnt, 0) - COALESCE(tp.cnt, 0)                                           AS "Дельта 7д",
    (COALESCE(t7.cnt, 0) > COALESCE(tp.cnt, 0))                                         AS "Растёт"
FROM agg a
LEFT JOIN trend_7d t7      ON t7.error_type = a.error_type
LEFT JOIN trend_prev_7d tp ON tp.error_type = a.error_type
ORDER BY a.error_count DESC;

CREATE MATERIALIZED VIEW public.v_docs_no_response_ui AS
WITH messages AS (
    SELECT
        m.egmid, m.created_at, m.msgid, m.reply_to, m.document_id,
        public.egisz_normalize_semd_code(
            COALESCE(public.egisz_xml_text(r.msgtext, 'kind'),
                     public.egisz_xml_text(r.msgtext, 'KIND'))
        ) AS semd_code_resolved,
        public.egisz_clean_text_value(
            COALESCE(public.egisz_xml_text(r.msgtext, 'documentTypeName'),
                     public.egisz_xml_text(r.msgtext, 'name'),
                     public.egisz_xml_text(r.msgtext, 'documentName'))
        ) AS semd_name_payload,
        public.egisz_normalize_message_id(m.msgid) AS msgid_norm,
        lower(NULLIF(btrim(m.document_id), '')) AS document_id_norm,
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS reply_to_jid,
        public.egisz_clean_host(m.reply_to) AS reply_to_host
    FROM public.egisz_messages_raw m
    LEFT JOIN LATERAL (
        SELECT er.msgtext
        FROM public.exchangelog_raw er
        WHERE er.msgid IS NOT NULL
          AND public.egisz_normalize_message_id(er.msgid) = public.egisz_normalize_message_id(m.msgid)
        ORDER BY er.logid DESC
        LIMIT 1
    ) r ON TRUE
),
fact_msgs AS (
    SELECT DISTINCT public.egisz_normalize_message_id(message_id) AS k FROM public.fact_egisz_transactions
        WHERE NULLIF(public.egisz_normalize_message_id(message_id), '') IS NOT NULL
    UNION
    SELECT DISTINCT public.egisz_normalize_message_id(relates_to_id) FROM public.fact_egisz_transactions
        WHERE NULLIF(public.egisz_normalize_message_id(relates_to_id), '') IS NOT NULL
),
fact_docs AS (
    SELECT DISTINCT lower(NULLIF(btrim(local_uid_semd), '')) AS k
    FROM public.fact_egisz_transactions
    WHERE lower(NULLIF(btrim(local_uid_semd), '')) IS NOT NULL
),
core AS (
    SELECT m.*, EXTRACT(EPOCH FROM (now() - m.created_at)) / 3600.0 AS wait_hours
    FROM messages m
    LEFT JOIN fact_msgs fm ON fm.k = m.msgid_norm
    LEFT JOIN fact_docs fd ON fd.k = m.document_id_norm
    WHERE fm.k IS NULL AND fd.k IS NULL
)
SELECT
    c.created_at                                                                    AS "Отправлено",
    c.document_id                                                                   AS "Локальный UID СЭМД",
    c.document_id                                                                   AS "Идентификатор документа",
    c.semd_code_resolved                                                            AS "Код СЭМД",
    COALESCE(st.name, c.semd_name_payload, '(нет в справочнике)')                   AS "Тип СЭМД",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || COALESCE(c.reply_to_jid::text, '?')) AS "Клиника",
    COALESCE(c.reply_to_jid, l.jid)::text                                           AS "JID клиники",
    c.msgid                                                                          AS "MSGID обмена",
    c.egmid                                                                          AS "EGMID",
    ROUND(c.wait_hours::numeric, 1)                                                  AS "Часов ожидания",
    CASE
        WHEN c.wait_hours > 24 THEN 'CRITICAL'
        WHEN c.wait_hours >= 4  THEN 'WARNING'
        ELSE 'PENDING'
    END                                                                              AS "Срочность",
    CASE
        WHEN c.wait_hours > 24 THEN 1
        WHEN c.wait_hours >= 4  THEN 2
        ELSE 3
    END                                                                              AS "Срочность (сорт.)"
FROM core c
LEFT JOIN LATERAL (
    SELECT dl.jid
    FROM public.dim_licenses dl
    WHERE (c.reply_to_jid IS NOT NULL AND dl.jid = c.reply_to_jid)
       OR (c.reply_to_host IS NOT NULL AND public.egisz_clean_host(dl.mo_domen) = c.reply_to_host)
    ORDER BY
        CASE WHEN c.reply_to_jid IS NOT NULL AND dl.jid = c.reply_to_jid THEN 0 ELSE 1 END,
        dl.modifydate DESC NULLS LAST, dl.id DESC
    LIMIT 1
) l ON TRUE
LEFT JOIN public.dim_organizations o ON o.jid = COALESCE(c.reply_to_jid, l.jid)
LEFT JOIN public.dim_semd_types st   ON st.code = c.semd_code_resolved;

CREATE UNIQUE INDEX IF NOT EXISTS idx_v_docs_no_response_ui_egmid
    ON public.v_docs_no_response_ui ("EGMID");
CREATE INDEX IF NOT EXISTS idx_v_docs_no_response_ui_urgency_sort
    ON public.v_docs_no_response_ui ("Срочность (сорт.)");
CREATE INDEX IF NOT EXISTS idx_v_docs_no_response_ui_wait_hours
    ON public.v_docs_no_response_ui ("Часов ожидания");
CREATE INDEX IF NOT EXISTS idx_v_docs_no_response_ui_jid
    ON public.v_docs_no_response_ui ("JID клиники");
CREATE INDEX IF NOT EXISTS idx_v_docs_no_response_ui_sent_at
    ON public.v_docs_no_response_ui ("Отправлено");

CREATE OR REPLACE VIEW public.v_stat_orgs_ui AS
WITH per_tx AS (
    SELECT t.*,
        public.egisz_doc_key(t.local_uid_semd, t.emdr_id, t.doc_number, t.message_id, t.exchangelog_log_id) AS doc_key
    FROM public.fact_egisz_transactions t
    WHERE t.jid IS NOT NULL
),
agg_30d AS (
    SELECT
        jid,
        COUNT(*)::bigint                                              AS total_sent,
        COUNT(DISTINCT doc_key)::bigint                               AS unique_docs,
        COUNT(*) FILTER (WHERE status = 'success')::bigint            AS success_count,
        COUNT(*) FILTER (WHERE status = 'error')::bigint              AS error_count,
        COUNT(DISTINCT public.egisz_normalize_semd_code(semd_code))::bigint AS distinct_semd_types
    FROM per_tx
    WHERE log_date >= now() - INTERVAL '30 days'
    GROUP BY jid
),
err_24h AS (
    SELECT jid, COUNT(*)::bigint AS cnt FROM per_tx
    WHERE status = 'error' AND log_date >= now() - INTERVAL '24 hours' GROUP BY jid
),
err_7d AS (
    SELECT jid, COUNT(*)::bigint AS cnt FROM per_tx
    WHERE status = 'error' AND log_date >= now() - INTERVAL '7 days' GROUP BY jid
),
last_success AS (SELECT jid, MAX(log_date) AS ts FROM per_tx WHERE status = 'success' GROUP BY jid),
last_error   AS (SELECT jid, MAX(log_date) AS ts FROM per_tx WHERE status = 'error'   GROUP BY jid),
err_types_30d AS (
    SELECT jid, error_type, COUNT(*) AS cnt, MAX(error_summary) AS summary
    FROM per_tx
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '30 days'
    GROUP BY jid, error_type
),
top_err AS (
    SELECT DISTINCT ON (jid) jid, error_type AS top_error_type, summary AS top_error_summary
    FROM err_types_30d
    ORDER BY jid, cnt DESC, error_type
),
no_response AS (
    SELECT
        NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '')::integer AS jid,
        COUNT(*)::bigint AS cnt
    FROM public.egisz_messages_raw m
    LEFT JOIN public.fact_egisz_transactions f
      ON public.egisz_normalize_message_id(f.message_id) = public.egisz_normalize_message_id(m.msgid)
      OR public.egisz_normalize_message_id(f.relates_to_id) = public.egisz_normalize_message_id(m.msgid)
      OR lower(NULLIF(btrim(f.local_uid_semd), '')) = lower(NULLIF(btrim(m.document_id), ''))
    WHERE f.exchangelog_log_id IS NULL
      AND NULLIF(public.egisz_extract_jid_from_endpoint(m.reply_to), '') IS NOT NULL
    GROUP BY 1
),
combined AS (
    SELECT
        a.jid, a.total_sent, a.unique_docs, a.success_count, a.error_count, a.distinct_semd_types,
        COALESCE(e24.cnt, 0)::bigint AS errors_last_24h,
        COALESCE(e7d.cnt, 0)::bigint AS errors_last_7d,
        ls.ts AS last_success_ts,
        le.ts AS last_error_ts,
        te.top_error_type, te.top_error_summary,
        COALESCE(nr.cnt, 0)::bigint  AS docs_no_response
    FROM agg_30d a
    LEFT JOIN err_24h     e24 ON e24.jid = a.jid
    LEFT JOIN err_7d      e7d ON e7d.jid = a.jid
    LEFT JOIN last_success ls ON ls.jid  = a.jid
    LEFT JOIN last_error   le ON le.jid  = a.jid
    LEFT JOIN top_err      te ON te.jid  = a.jid
    LEFT JOIN no_response  nr ON nr.jid  = a.jid
),
scored AS (
    SELECT c.*,
        ROUND(100.0 * c.success_count / NULLIF(c.total_sent, 0), 1)::numeric AS success_rate_pct,
        ROUND(100.0 * c.error_count   / NULLIF(c.total_sent, 0), 1)::numeric AS error_rate_pct,
        CASE WHEN c.last_success_ts IS NULL THEN NULL
             ELSE EXTRACT(EPOCH FROM (now() - c.last_success_ts)) / 86400.0
        END::numeric(10,2) AS days_since_last_success
    FROM combined c
)
SELECT
    s.jid::text                                                            AS "JID клиники",
    s.jid                                                                  AS "JID (число)",
    COALESCE(NULLIF(o.name, ''), 'Клиника JID: ' || s.jid::text)           AS "Клиника",
    o.inn                                                                  AS "ИНН",
    s.total_sent                                                            AS "Транзакций за 30д",
    s.unique_docs                                                           AS "Документов за 30д",
    s.success_count                                                         AS "Успешных",
    s.error_count                                                           AS "С ошибкой",
    s.success_rate_pct                                                      AS "% успеха",
    s.error_rate_pct                                                        AS "% ошибок",
    s.errors_last_24h                                                       AS "Ошибок за 24ч",
    s.errors_last_7d                                                        AS "Ошибок за 7д",
    s.distinct_semd_types                                                   AS "Разных типов СЭМД",
    s.top_error_type                                                        AS "Топ ошибки (тип)",
    s.top_error_summary                                                     AS "Топ ошибки (описание)",
    s.last_success_ts                                                       AS "Последний успех",
    s.last_error_ts                                                         AS "Последняя ошибка",
    s.days_since_last_success                                               AS "Дней с последнего успеха",
    s.docs_no_response                                                      AS "Документов без ответа",
    CASE
        WHEN COALESCE(s.error_rate_pct, 0) >= 50
          OR COALESCE(s.days_since_last_success, 999) >= 3 THEN 'CRITICAL'
        WHEN COALESCE(s.error_rate_pct, 0) >= 20
          OR s.errors_last_24h >= 10 THEN 'WARNING'
        ELSE 'OK'
    END                                                                     AS "Состояние",
    CASE
        WHEN COALESCE(s.error_rate_pct, 0) >= 50
          OR COALESCE(s.days_since_last_success, 999) >= 3 THEN 1
        WHEN COALESCE(s.error_rate_pct, 0) >= 20
          OR s.errors_last_24h >= 10 THEN 2
        ELSE 3
    END                                                                     AS "Состояние (сорт.)"
FROM scored s
LEFT JOIN public.dim_organizations o ON o.jid = s.jid
ORDER BY "Состояние (сорт.)" ASC, s.error_count DESC, s.unique_docs DESC;

CREATE OR REPLACE VIEW public.v_stat_daily_ui AS
SELECT
    date_trunc('day', log_date)::date                                          AS "День",
    COUNT(*)::bigint                                                            AS "Транзакций",
    COUNT(*) FILTER (WHERE status = 'success')::bigint                          AS "Успешных",
    COUNT(*) FILTER (WHERE status = 'error')::bigint                            AS "С ошибкой",
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success')
                  / NULLIF(COUNT(*), 0), 1)::numeric                            AS "% успеха",
    COUNT(DISTINCT CASE WHEN status = 'error' THEN error_type END)::bigint      AS "Уникальных типов ошибок",
    COUNT(DISTINCT jid)::bigint                                                 AS "Активных клиник",
    COUNT(DISTINCT public.egisz_doc_key(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id))::bigint
                                                                                AS "Уникальных документов"
FROM public.fact_egisz_transactions
WHERE log_date >= now() - INTERVAL '90 days'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW public.v_stat_hourly_ui AS
SELECT
    date_trunc('hour', log_date)                                                AS "Час",
    COUNT(*)::bigint                                                            AS "Транзакций",
    COUNT(*) FILTER (WHERE status = 'success')::bigint                          AS "Успешных",
    COUNT(*) FILTER (WHERE status = 'error')::bigint                            AS "С ошибкой",
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success')
                  / NULLIF(COUNT(*), 0), 1)::numeric                            AS "% успеха",
    COUNT(DISTINCT jid)::bigint                                                 AS "Активных клиник"
FROM public.fact_egisz_transactions
WHERE log_date >= now() - INTERVAL '48 hours'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW public.v_service_health_ui AS
WITH last_run AS (SELECT MAX(run_ts) AS ts FROM public.etl_run_log),
last_fact AS (SELECT MAX(log_date) AS ts FROM public.fact_egisz_transactions),
hour_window AS (
    SELECT
        COUNT(*)::bigint                                       AS docs_total,
        COUNT(*) FILTER (WHERE status = 'error')::bigint       AS errors_total
    FROM public.fact_egisz_transactions
    WHERE log_date >= now() - INTERVAL '1 hour'
),
critical_orgs    AS (SELECT COUNT(*)::bigint AS cnt FROM public.v_stat_orgs_ui WHERE "Состояние" = 'CRITICAL'),
no_response_crit AS (SELECT COUNT(*)::bigint AS cnt FROM public.v_docs_no_response_ui WHERE "Срочность" = 'CRITICAL'),
freshness AS (
    SELECT ROUND(EXTRACT(EPOCH FROM (now() - COALESCE((SELECT ts FROM last_run),
                                                     (SELECT ts FROM last_fact),
                                                     now()))) / 60.0, 1)::numeric AS pipeline_minutes
)
SELECT
    f.pipeline_minutes                                                              AS "Свежесть, мин",
    CASE
        WHEN f.pipeline_minutes > 60 THEN 'DEAD'
        WHEN f.pipeline_minutes > 30 THEN 'STALE'
        WHEN f.pipeline_minutes > 10 THEN 'STALE'
        ELSE 'OK'
    END                                                                              AS "Статус пайплайна",
    h.docs_total                                                                     AS "Документов за час",
    h.errors_total                                                                   AS "Ошибок за час",
    ROUND(100.0 * h.errors_total / NULLIF(h.docs_total, 0), 1)::numeric              AS "% ошибок за час",
    co.cnt                                                                            AS "Клиник в CRITICAL",
    nr.cnt                                                                            AS "Документов без ответа >24ч",
    (SELECT ts FROM last_run)                                                         AS "Последний запуск ETL",
    (SELECT ts FROM last_fact)                                                        AS "Последний факт"
FROM freshness f
CROSS JOIN hour_window h
CROSS JOIN critical_orgs co
CROSS JOIN no_response_crit nr;

CREATE OR REPLACE VIEW public.v_kpi_summary_ui AS
WITH org_summary AS (
    SELECT
        COUNT(*)::bigint                                          AS total_orgs,
        COUNT(*) FILTER (WHERE "Состояние" = 'CRITICAL')::bigint  AS orgs_critical,
        COUNT(*) FILTER (WHERE "Состояние" = 'WARNING')::bigint   AS orgs_warning,
        COUNT(*) FILTER (WHERE "Состояние" = 'OK')::bigint        AS orgs_ok
    FROM public.v_stat_orgs_ui
),
tx_30d_raw AS MATERIALIZED (
    SELECT
        status,
        public.egisz_doc_key(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id) AS doc_key
    FROM public.fact_egisz_transactions
    WHERE log_date >= now() - INTERVAL '30 days'
),
tx_30d AS (
    SELECT
        COUNT(*)::bigint                                                                       AS total_sent,
        COUNT(DISTINCT doc_key)::bigint                                                        AS total_docs,
        COUNT(*) FILTER (WHERE status = 'error')::bigint                                       AS total_errors,
        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*), 0), 1)::numeric AS success_rate_pct
    FROM tx_30d_raw
),
attempts_per_doc AS (
    SELECT AVG(cnt)::numeric(10,2) AS avg_attempts FROM (
        SELECT doc_key, COUNT(*) AS cnt FROM tx_30d_raw GROUP BY doc_key
    ) x
),
top_err AS (
    SELECT error_type, COUNT(*) AS cnt FROM public.fact_egisz_transactions
    WHERE status = 'error' AND NULLIF(btrim(error_type), '') IS NOT NULL
      AND log_date >= now() - INTERVAL '30 days'
    GROUP BY error_type ORDER BY cnt DESC LIMIT 1
),
top_semd AS (
    SELECT COALESCE(st.name, x.code, '(неизвестно)') AS name, x.cnt
    FROM (
        SELECT public.egisz_normalize_semd_code(semd_code) AS code, COUNT(*) AS cnt
        FROM public.fact_egisz_transactions WHERE log_date >= now() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    ) x
    LEFT JOIN public.dim_semd_types st ON st.code = x.code
),
top_problem_org AS (
    SELECT "Клиника" AS org_name, "% ошибок" AS error_rate
    FROM public.v_stat_orgs_ui
    WHERE "% ошибок" IS NOT NULL
    ORDER BY "% ошибок" DESC NULLS LAST LIMIT 1
),
no_resp AS (
    SELECT
        COUNT(*)::bigint                                                  AS total,
        COUNT(*) FILTER (WHERE "Срочность" = 'CRITICAL')::bigint          AS critical
    FROM public.v_docs_no_response_ui
)
SELECT
    o.total_orgs                                  AS "Всего клиник",
    o.orgs_critical                               AS "Клиник CRITICAL",
    o.orgs_warning                                AS "Клиник WARNING",
    o.orgs_ok                                     AS "Клиник OK",
    t.total_docs                                  AS "Документов за 30д",
    t.total_sent                                  AS "Транзакций за 30д",
    t.success_rate_pct                            AS "% успеха за 30д",
    t.total_errors                                AS "Ошибок за 30д",
    te.error_type                                 AS "Топ-ошибка (тип)",
    n.total                                       AS "Документов без ответа",
    n.critical                                    AS "Без ответа >24ч",
    a.avg_attempts                                AS "Среднее попыток на документ",
    ts.name                                       AS "Самый частый СЭМД",
    ts.cnt                                        AS "Самый частый СЭМД (шт)",
    tpo.org_name                                  AS "Самая проблемная клиника",
    tpo.error_rate                                AS "Её % ошибок"
FROM org_summary o
CROSS JOIN tx_30d t
CROSS JOIN attempts_per_doc a
LEFT JOIN top_err te        ON TRUE
LEFT JOIN top_semd ts       ON TRUE
LEFT JOIN top_problem_org tpo ON TRUE
CROSS JOIN no_resp n;

-- Transfer ownership of all public-schema objects to egisz so it can run DDL independently
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    LOOP
        IF r.relkind IN ('r', 'p') THEN
            EXECUTE format('ALTER TABLE public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'v' THEN
            EXECUTE format('ALTER VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW public.%I OWNER TO egisz', r.relname);
        ELSIF r.relkind = 'S' THEN
            EXECUTE format('ALTER SEQUENCE public.%I OWNER TO egisz', r.relname);
        END IF;
    END LOOP;

    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO egisz', r.sig);
    END LOOP;
END;
$$;

DO $$
DECLARE
    can_create boolean;
    can_usage  boolean;
BEGIN
    SELECT
        has_schema_privilege('egisz', 'public', 'CREATE'),
        has_schema_privilege('egisz', 'public', 'USAGE')
    INTO can_create, can_usage;

    IF NOT (can_create AND can_usage) THEN
        RAISE EXCEPTION 'egisz is still missing public schema privileges';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname = 'v_egisz_transactions_enriched_ui'
    ) THEN
        EXECUTE 'REFRESH MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname = 'v_stg_channel_errors_by_document'
    ) THEN
        EXECUTE 'REFRESH MATERIALIZED VIEW public.v_stg_channel_errors_by_document';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname = 'v_docs_no_response_ui'
    ) THEN
        EXECUTE 'REFRESH MATERIALIZED VIEW public.v_docs_no_response_ui';
    END IF;
END;
$$;

\echo 'DWH init complete: egisz owns all public-schema objects in dwh_egisz'
