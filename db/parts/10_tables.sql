-- ============================================================================
-- 10_tables.sql — Tables, dim_semd_types seed, fact + indexes
-- Source: db/dwh_init.sql, lines [27..417).
-- Loaded by db/dwh_init.sql via \i db/parts/10_tables.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE TABLE IF NOT EXISTS elt_state (
    pipeline text PRIMARY KEY,
    last_logid bigint DEFAULT 0,
    source_min_created_at timestamptz,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE elt_state ADD COLUMN IF NOT EXISTS source_min_created_at timestamptz;

INSERT INTO elt_state (pipeline, last_logid, source_min_created_at)
VALUES ('egisz', 0, timestamptz '2026-05-18 00:00:00+00')
ON CONFLICT (pipeline) DO UPDATE SET
    source_min_created_at = COALESCE(elt_state.source_min_created_at, EXCLUDED.source_min_created_at);

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

ALTER TABLE exchangelog_raw ADD COLUMN IF NOT EXISTS createdate timestamptz;

CREATE TABLE IF NOT EXISTS fact_egisz_documents (
    document_key text PRIMARY KEY,
    local_uid text,
    emdr_id text,
    semd_code text,
    status text,
    status_category text,
    message_id text,
    relates_to_id text,
    callback_log_id bigint,
    sent_at timestamptz,
    document_created_at timestamptz,
    registered_at timestamptz,
    error_type text,
    error_summary text,
    error_text text,
    patient_hash text,
    doctor_hash text,
    source_logid bigint,
    first_sent_at timestamptz,
    last_callback_at timestamptz,
    last_status text,
    jid integer,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS local_uid text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS emdr_id text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS semd_code text;
ALTER TABLE fact_egisz_documents ALTER COLUMN semd_code DROP NOT NULL;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS status_category text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS message_id text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS relates_to_id text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS callback_log_id bigint;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS sent_at timestamptz;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS document_created_at timestamptz;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS registered_at timestamptz;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS error_type text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS error_summary text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS error_text text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS patient_hash text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS doctor_hash text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS source_logid bigint;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS first_sent_at timestamptz;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS last_callback_at timestamptz;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS last_status text;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS jid integer;
ALTER TABLE fact_egisz_documents ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

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

-- Индекс EXCHANGELOG: одна строка = одно сообщение журнала (logid).
-- exchange_msgid_norm — ключ связи цепочки (relatesToMessage), не реквизит документа.
-- local_uid / emdr_id / document_key — реквизиты СЭМД, извлечённые из payload.
-- document_key канонический = lower(localUid); emdr_id — атрибут регистрации, не ключ.
CREATE TABLE IF NOT EXISTS dim_egisz_exchangelog_refs (
    logid bigint PRIMARY KEY,
    created_at timestamptz,
    exchange_msgid text,
    exchange_msgid_norm text,
    local_uid text,
    emdr_id text,
    document_key text,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS exchange_msgid text;
ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS exchange_msgid_norm text;
ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS local_uid text;
ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS emdr_id text;
ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS document_key text;
ALTER TABLE dim_egisz_exchangelog_refs ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

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

CREATE TABLE IF NOT EXISTS fact_egisz_transactions (
    exchangelog_log_id bigint PRIMARY KEY,
    document_key text,
    log_date timestamptz,
    message_id text,
    relates_to_id text,
    local_uid_semd text,
    emdr_id text,
    doc_number text,
    org_oid text,
    status text,
    message text,
    callback_url text,
    jid integer,
    semd_code text,
    semd_name text,
    error_code text,
    creation_date timestamptz,
    processed_at timestamptz DEFAULT now()
);

ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS document_key text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS creation_date timestamptz;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS error_type text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS error_summary text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS error_json_text text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS patient_name_masked text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS snils_masked text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS doctor_name text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS patient_hash text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS doctor_hash text;
ALTER TABLE fact_egisz_transactions ADD COLUMN IF NOT EXISTS message text;

-- ============================================================================
-- Range partitioning (monthly) for monotonic time-series tables.
-- PK must include the partition key: PostgreSQL enforces UNIQUE/PK only when
-- the partition column is part of the constraint. logid / exchangelog_log_id
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
            loaded_at timestamptz DEFAULT now(),
            PRIMARY KEY (logid, createdate)
        ) PARTITION BY RANGE (createdate);

        INSERT INTO public.exchangelog_raw_partitioned (
            logid, logdate, createdate, msgid, logstate, logtext, msgtext, loaded_at
        )
        SELECT
            logid,
            logdate,
            COALESCE(createdate, logdate, loaded_at, timestamptz '1970-01-01'),
            msgid,
            logstate,
            logtext,
            msgtext,
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
      AND c.relname = 'fact_egisz_transactions';

    IF relkind IS NOT NULL AND relkind <> 'p' THEN
        UPDATE public.fact_egisz_transactions
        SET log_date = COALESCE(log_date, processed_at, creation_date, now())
        WHERE log_date IS NULL;

        CREATE TABLE public.fact_egisz_transactions_partitioned (
            LIKE public.fact_egisz_transactions INCLUDING DEFAULTS
        ) PARTITION BY RANGE (log_date);

        ALTER TABLE public.fact_egisz_transactions_partitioned
            DROP CONSTRAINT IF EXISTS fact_egisz_transactions_pkey;
        ALTER TABLE public.fact_egisz_transactions_partitioned
            ADD PRIMARY KEY (exchangelog_log_id, log_date);
        ALTER TABLE public.fact_egisz_transactions_partitioned
            ALTER COLUMN log_date SET NOT NULL;

        INSERT INTO public.fact_egisz_transactions_partitioned
        SELECT *
        FROM public.fact_egisz_transactions;

        DROP TABLE public.fact_egisz_transactions;
        ALTER TABLE public.fact_egisz_transactions_partitioned RENAME TO fact_egisz_transactions;
    END IF;
END
$$;

DO $$
DECLARE
    relkind "char";
    month_offset integer;
    part_start timestamptz;
    part_end timestamptz;
    part_name text;
BEGIN
    SELECT c.relkind
    INTO relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'exchangelog_raw';

    IF relkind = 'p' THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS public.exchangelog_raw_default PARTITION OF public.exchangelog_raw DEFAULT';

        FOR month_offset IN -12..24 LOOP
            part_start := date_trunc('month', timezone('UTC', now())) + (month_offset || ' months')::interval;
            part_end := part_start + INTERVAL '1 month';
            part_name := format('exchangelog_raw_y%sm%s', to_char(part_start, 'YYYY'), to_char(part_start, 'MM'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.exchangelog_raw FOR VALUES FROM (%L) TO (%L)',
                part_name,
                part_start,
                part_end
            );
        END LOOP;
    END IF;

    SELECT c.relkind
    INTO relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'fact_egisz_transactions';

    IF relkind = 'p' THEN
        EXECUTE 'CREATE TABLE IF NOT EXISTS public.fact_egisz_transactions_default PARTITION OF public.fact_egisz_transactions DEFAULT';

        FOR month_offset IN -12..24 LOOP
            part_start := date_trunc('month', timezone('UTC', now())) + (month_offset || ' months')::interval;
            part_end := part_start + INTERVAL '1 month';
            part_name := format('fact_egisz_transactions_y%sm%s', to_char(part_start, 'YYYY'), to_char(part_start, 'MM'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.fact_egisz_transactions FOR VALUES FROM (%L) TO (%L)',
                part_name,
                part_start,
                part_end
            );
        END LOOP;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_msgid ON exchangelog_raw (msgid);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_logstate ON exchangelog_raw (logstate);
CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_createdate ON exchangelog_raw (createdate);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_semd_code ON fact_egisz_documents (semd_code);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_local_uid ON fact_egisz_documents (local_uid);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_emdr_id ON fact_egisz_documents (emdr_id);
-- Резолвинг callback→документ (egisz_transform_raw_to_facts, emdr_ref) ищет по
-- lower(btrim(emdr_id)); без функционального индекса это seq scan на каждую строку
-- батча и линейная деградация transform с ростом архива.
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_emdr_id_norm
    ON fact_egisz_documents (lower(NULLIF(btrim(emdr_id), '')))
    WHERE NULLIF(btrim(emdr_id), '') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_last_callback_at ON fact_egisz_documents (last_callback_at);
-- Инкрементальное сопровождение витрины v_egisz_documents_enriched_ui резолвит
-- затронутые в транзакции document_key через updated_at = now(); без индекса это
-- seq scan по всему архиву на каждый батч (та же O(archive)-деградация, что и emdr).
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_updated_at ON fact_egisz_documents (updated_at);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_status ON fact_egisz_documents (status);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_jid ON fact_egisz_documents (jid);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_sent_at ON fact_egisz_documents (sent_at);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_document_created_at ON fact_egisz_documents (document_created_at);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_registered_at ON fact_egisz_documents (registered_at);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_documents_callback_log_id ON fact_egisz_documents (callback_log_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_log_date ON fact_egisz_transactions (log_date);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_document_key ON fact_egisz_transactions (document_key);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_status ON fact_egisz_transactions (status);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_jid ON fact_egisz_transactions (jid);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_message_id ON fact_egisz_transactions (message_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_local_uid ON fact_egisz_transactions (local_uid_semd);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_local_uid_norm ON fact_egisz_transactions (lower(NULLIF(btrim(local_uid_semd), '')));
CREATE INDEX IF NOT EXISTS idx_fact_egisz_emdr_id ON fact_egisz_transactions (emdr_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_relates_to ON fact_egisz_transactions (relates_to_id);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_error_type ON fact_egisz_transactions (error_type);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_patient_hash ON fact_egisz_transactions (patient_hash);
CREATE INDEX IF NOT EXISTS idx_fact_egisz_doctor_hash ON fact_egisz_transactions (doctor_hash);
CREATE INDEX IF NOT EXISTS idx_dim_licenses_jid ON dim_licenses (jid);
CREATE INDEX IF NOT EXISTS idx_dim_licenses_mo_uid ON dim_licenses (mo_uid);
CREATE INDEX IF NOT EXISTS idx_dim_egisz_exchangelog_refs_exchange_msgid_norm ON dim_egisz_exchangelog_refs (exchange_msgid_norm);
CREATE INDEX IF NOT EXISTS idx_dim_egisz_exchangelog_refs_document_key ON dim_egisz_exchangelog_refs (document_key);
CREATE INDEX IF NOT EXISTS idx_dim_egisz_exchangelog_refs_local_uid_norm ON dim_egisz_exchangelog_refs (lower(NULLIF(btrim(local_uid), '')));
CREATE INDEX IF NOT EXISTS idx_dim_egisz_exchangelog_refs_emdr_id_norm ON dim_egisz_exchangelog_refs (lower(NULLIF(btrim(emdr_id), '')));
CREATE INDEX IF NOT EXISTS idx_dim_egisz_exchangelog_refs_created_at ON dim_egisz_exchangelog_refs (created_at);
