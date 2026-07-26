"""Регрессионные тесты классификатора ошибок против живого PostgreSQL.

Запуск требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_egisz);
без переменной модуль целиком скипается — как и остальной suite, не зависящий от внешних
сервисов. Фикстура идемпотентно применяет db/02_functions.sql из working tree, поэтому тесты
проверяют именно текущий код правил, а не состояние базы на момент последнего dwh_init.

Ожидаемые наименования типов — формулировки справочника ФНСИ 1.2.643.5.1.13.13.99.2.305:
расхождение теста и справочника означает, что таксономия разошлась с федеральным
классификатором, а не что тест устарел.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from conftest import load_dag_module  # noqa: E402

connect_pg = load_dag_module("egisz_etl_dag").connect_pg

DSN = os.environ.get("EGISZ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")

DB_DIR = Path(__file__).resolve().parents[1] / "db"

RESPONSIBILITY_DOMAIN = ("клиника", "МИС", "интегратор", "РЭМД", "смешанная")
CODE_NAMESPACES = ("НСИ 305", "IHE XDS", "шлюз")

# Коды-зонтики: их описание в ФНСИ («Ошибка валидации значения», «Непредвиденная ошибка»)
# не несёт диагностики, поэтому blanket-правило яруса 2 для них не заводится — причина
# читается из текста ярусами 3–4.
UMBRELLA_CODES = ("VALIDATION_ERROR", "RUNTIME_ERROR")


@pytest.fixture(scope="module")
def con():
    con = connect_pg(DSN)
    with con.cursor() as cur:
        # Функции разбора, словарь правил и классификация собраны в один модуль схемы.
        cur.execute((DB_DIR / "02_functions.sql").read_text(encoding="utf-8"))
    con.commit()
    yield con
    con.rollback()
    con.close()


def one(con, sql: str, *params):
    with con.cursor() as cur:
        cur.execute(sql, params or None)
        return cur.fetchone()[0]


# --- Корпус: (code, message, ожидаемые атомы, ожидаемые категории) ---------------------
# Сообщения — обезличенные образцы из архива callback (persist-значения заменены на […]).
CORPUS = [
    # --- Ярус 2: код закрывает разбор, трактовка — наименование из ФНСИ ---------------
    ("PATIENT_MPI_MISMATCH",
     "Указанное значение [Фамилия] [Имя] не соответствует данным ГИП [—]. Пациент найден по локальному идентификатору",
     ["Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП"],
     ["Данные пациента"]),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [111] не соответствует занимаемой им должности в организации [222] по данным ФРМР.",
     ["Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР"],
     ["Данные медработника"]),
    ("NOT_UNIQUE_PROVIDED_ID", "",
     ["Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован"],
     ["Ошибки регистрации в РЭМД"]),
    ("NO_SNILS", "СНИЛС пациента в составе сведений о пациенте обязателен для данного вида документов",
     ["Наличие СНИЛС пациента не соответствует требованиям вида документов"], ["Данные пациента"]),
    ("RESTRICT_NEW_VERSION", "Для ЭМД 230 запрещена регистрация новых версий",
     ["Для вида документа запрещено регистрировать новую версию"], ["Ошибки регистрации в РЭМД"]),
    ("WRONG_CREATION_DATE", "Дата создания документа не может быть позднее даты регистрации",
     ["Дата создания документа больше даты регистрации"], ["Ошибки регистрации в РЭМД"]),
    ("RATE_LIMIT", "Доступ к сервису временно запрещён - превышен лимит запросов",
     ["Достигнут защитный лимит, просьба повторить через минуту или позже"], ["Технические ошибки РЭМД"]),
    ("RMIS_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: Error in getDocumentFile by SOAP",
     ["Ошибка ответа от сервиса системы в getDocumentFileResponse, предоставляющей документ"],
     ["Ошибки получения файла ЭМД"]),
    # Файл получен, но не является валидным XML: код побеждает текстовый ярус
    # «файлового хранилища» — регресс подмены типа.
    ("INVALID_CONTENT", "Ошибка получения файла ЭМД из файлового хранилища: Переданный файл не является валидным XML файлом",
     ["Из предоставляющей РМИС/МИС передан документ, формат файла которого не соответствует требованиям вида документов"],
     ["Ошибки структуры и валидации"]),
    ("DOC_DATE_MISMATCH_CERT_NOT_AFTER", "Сертификат МО недействителен на дату создания документа",
     ["Сертификат ЭП недействителен на дату создания документа (документ создан позже окончания срока действия сертификата)"],
     ["Ошибки ЭП и сертификатов"]),
    ("INVALID_DOCTOR_NAME",
     "Имя [Иван] медицинского работника в запросе на регистрацию отличается от имени [Иоан] в СЭМД. СНИЛС [111]",
     ["Имя медицинского работника в запросе на регистрацию отличается от имени в СЭМД"],
     ["Данные медработника"]),

    # --- Исправленные трактовки: раньше расходились с описанием ФНСИ ------------------
    ("CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT", "Не удалось построить цепочку сертификатов",
     ["Не удалось построить цепочку сертификатов до аккредитованного удостоверяющего центра"],
     ["Ошибки ЭП и сертификатов"]),
    ("INVALID_DICTIONARY_OID", "Справочник OID [1.2.643.5.1.13.13.11.105978]. Справочник с указанным кодом отсутствует",
     ["Справочник с указанным кодом отсутствует"], ["Ошибки справочника НСИ"]),
    ("INVALID_DICTIONARY_VERSION", "Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.31] недопустима для документа вида [227].",
     ["Версия справочника недопустима для данного вида документа"], ["Ошибки справочника НСИ"]),
    ("XML_VALIDATION_ERROR", "Ошибка трансформации",
     ["Ошибка при трансформации СЭМД для проверки (Schematron)"], ["Ошибки структуры и валидации"]),
    ("SIGNATURE_VERIFICATION_ERROR", "Проверка подписи завершилась отрицательно",
     ["Подпись не верна"], ["Ошибки ЭП и сертификатов"]),
    ("OBJECT_NOT_FOUND", "Запись не найдена",
     ["Не найдена запись справочника"], ["Ошибки справочника НСИ"]),
    ("ROLE_OCCURRENCE_MISMATCH", "Роль подписанта не соответствует",
     ["Число ЭП сотрудников с требуемой ролью не соответствует требованиям вида документов"],
     ["Ошибки ЭП и сертификатов"]),
    # CA_INACCESSIBILITY и текст «Удостоверяющий центр недоступен» — одна причина,
    # раньше давали два разных типа.
    ("CA_INACCESSIBILITY", "Удостоверяющий центр сертификата недоступен: Время ожидания истекло.",
     ["Адрес OCSP-службы не указан или недоступен, CRL также недоступен"], ["Ошибки ЭП и сертификатов"]),
    ("", "Удостоверяющий центр сертификата недоступен: Время ожидания истекло.",
     ["Адрес OCSP-службы не указан или недоступен, CRL также недоступен"], ["Ошибки ЭП и сертификатов"]),

    # --- Коды, ранее не заведённые: утекали в широкие текстовые фолбэки ---------------
    ("PERSONAL_SIG_CERT_NOT_ACTUAL_ON_DOC_CREATION_DT", "",
     ["Сертификат сотрудника недействителен на дату создания документа"], ["Ошибки ЭП и сертификатов"]),
    ("DUPLICATE_PATIENT_FOUND", "",
     ["По локальному идентификатору в ГИП найдено более одной записи"], ["Данные пациента"]),
    # РЭМД отдаёт RECIPIENT_*, справочник закрепляет RECEPIENT_* — синоним разрешается
    # до сопоставления, поэтому правило одно.
    ("RECIPIENT_INFO_MISMATCH", "Получатель [111] из запроса на регистрацию сведений не найден в СЭМД",
     ["Получатель из запроса на регистрацию сведений не найден в СЭМД"], ["Данные пациента"]),
    ("RECEPIENT_INFO_MISMATCH", "",
     ["Получатель из запроса на регистрацию сведений не найден в СЭМД"], ["Данные пациента"]),
    # Синтетический код шлюза: сбой транспорта до РЭМД, вердикта нет.
    ("INTEGRATION_LOGSTATE_3", "Сетевая ошибка: Synapse TCP/IP Socket error 11001: Host not found",
     ["Сетевая ошибка"], ["Ошибки связи"]),

    # --- Ошибки схематрона: разделены по конкретной проверке (§5.8 регламента) --------
    # Раньше весь класс сводился к «Не указан адрес пациента», хотя адрес указан.
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-19. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type"
     " должен иметь не пустое значение атрибута @code. Путь: /ClinicalDocument[1]/recordTarget[1]",
     ["Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям"], ["Данные пациента"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-18. Элемент ClinicalDocument/recordTarget/patientRole/addr"
     " должен иметь 1 элемент address:Type. Путь: /ClinicalDocument[1]/recordTarget[1]",
     ["Адрес пациента: не указан тип адреса (address:Type)"], ["Данные пациента"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-17. Элемент ClinicalDocument/recordTarget/patientRole"
     " должен иметь 1 или 2 элемента addr. Путь: /ClinicalDocument[1]/recordTarget[1]",
     ["Адрес пациента: недопустимое число элементов addr"], ["Данные пациента"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-2: Элемент streetAddressLine должен содержать не пустое текстовое наполнение",
     ["Адрес пациента: составляющая адреса не заполнена"], ["Данные пациента"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-9. Элемент ClinicalDocument/recordTarget/patientRole/id[2]"
     " не должен иметь атрибут @nullFlavor. Путь: /ClinicalDocument[1]/recordTarget[1]",
     ["Идентификатор пациента: недопустимый атрибут @nullFlavor"], ["Данные пациента"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-4.1.1.1: Элемент telecom обязан содержать один атрибут @value с не пустым значением",
     ["Контактные данные: не заполнен атрибут @value элемента telecom"], ["Ошибки структуры и валидации"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: Допустимые значения для элементов functionCode[1]: CHAIRMAN, COMMISSIONER",
     ["Значение элемента не входит в перечень допустимых"], ["Ошибки структуры и валидации"]),
    ("VALIDATION_ERROR", "Ошибка валидации Schematron: экзотическое требование без известных элементов",
     ["Ошибка Schematron-валидации"], ["Ошибки структуры и валидации"]),

    # --- Валидация по XSD (§5.7 регламента) ------------------------------------------
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-complex-type.2.4.a: Invalid content was found starting with element id",
     ["XSD: недопустимый элемент или нарушен порядок элементов"], ["Ошибки структуры и валидации"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-complex-type.3.2.2: Attribute 'nullFlavor' is not allowed to appear in element 'telecom'.",
     ["XSD: недопустимый атрибут элемента"], ["Ошибки структуры и валидации"]),
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-datatype-valid.1.2.1: '5 ml' is not a valid value of union type 'real'.",
     ["XSD: значение не соответствует типу элемента"], ["Ошибки структуры и валидации"]),

    # --- Кросс-валидация запроса и СЭМД (§5.2–5.5 регламента) ------------------------
    ("", "Уникальный идентификатор документа в ЭМД [abc] отличается от уникального идентификатора документа в запросе на регистрацию сведений [def]",
     ["Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию"],
     ["Ошибки регистрации в РЭМД"]),
    ("VALIDATION_ERROR", "СНИЛС  пациента в ЭМД [111] отличается от СНИЛС пациента в запросе на регистрацию сведений [222]",
     ["СНИЛС пациента в ЭМД не совпадает с запросом на регистрацию"], ["Данные пациента"]),
    ("VALIDATION_ERROR", "Организация [ООО Клиника] не привязана к РМИС [42]",
     ["Организация не привязана к РМИС"], ["Ошибки организации / ИС"]),
    ("VALIDATION_ERROR", "Недопустимые символы в имени 'Фамилия (девичья)'",
     ["ФИО пациента содержит недопустимые символы"], ["Данные пациента"]),
    ("VALUE_MISMATCH_METADATA_AND_CERTIFICATE",
     "В ФРМР не найдена актуальная на дату создания документа карточка МР c данными из сертификата подписи МО",
     ["Подписант из сертификата не найден в ФРМР"], ["Данные медработника"]),
    ("RUNTIME_ERROR", "Не удается провести проверку ФРМР",
     ["Проверяющая подсистема РЭМД недоступна"], ["Технические ошибки РЭМД"]),

    # --- Контур ИЭМК: код из атрибута RegistryError/errorCode ------------------------
    ("XDSDictionaryValidationError", "Element representedCustodianOrganization. MO code [1.2.643] is not actual.",
     ["ИЭМК: данные не соответствуют справочнику НСИ"], ["Ошибки ИЭМК"]),
    ("XDSRepositoryError", "Internal error in repository",
     ["ИЭМК: внутренняя ошибка репозитория"], ["Ошибки ИЭМК"]),
    ("XDSDocumentUniqueIdError", "Association [RPLC] targetId with unique ID [E13B85998D5A] not found in repository",
     ["ИЭМК: заменяемый документ не найден (замена версии)"], ["Ошибки ИЭМК"]),
    ("XDSDocumentUniqueIdError", "malformed unique id",
     ["ИЭМК: некорректный идентификатор документа"], ["Ошибки ИЭМК"]),
    ("XDSRegistryBusy", "", ["ИЭМК: сервис временно недоступен"], ["Ошибки ИЭМК"]),
    ("", "[CRE-122]: PAT-001; Пациент не определен: [СНИЛС [111] не валидно контрольное число]",
     ["ИЭМК: пациент не определён"], ["Ошибки ИЭМК"]),

    # --- Остаток: формулировка отказа показывается как тип, без заглушки -------------
    ("", "совершенно нераспознаваемый текст", ["совершенно нераспознаваемый текст"], ["Прочие"]),
    ("VALIDATION_ERROR",
     "Неизвестная проверка со СНИЛС [11122233344] и OID [1.2.643.5.1.13]. Путь: /ClinicalDocument[1]/x",
     ["Неизвестная проверка со СНИЛС […] и OID […]."], ["Прочие"]),
    # Код вне классификатора и без текста — остаток, по которому строится health-сигнал.
    ("SOME_UNSEEN_CODE", "", ["Код: SOME_UNSEEN_CODE"], ["Прочие"]),
]


@pytest.mark.parametrize("code,message,expected_atoms,expected_cats", CORPUS)
def test_error_item_atoms_corpus(con, code, message, expected_atoms, expected_cats):
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)", code, message)
    assert atoms == expected_atoms
    # Категория — JOIN к dim_error_type_group; формулировки вне словаря — «Прочие».
    cats = [
        one(
            con,
            """SELECT COALESCE(
                   (SELECT g.error_category FROM dim_error_type_group g WHERE g.error_type = %s),
                   'Прочие')""",
            a,
        )
        for a in atoms
    ]
    assert cats == expected_cats


# --- Коллизии ярусов: паразитные вторые типы устранены -------------------------------
COLLISIONS = [
    # точное code-правило против текстового
    ("ASYNC_RESPONSE_TIMEOUT", "Превышен таймаут ожидания асинхронного ответа",
     "Превышено ожидание асинхронного ответа от проверяющей системы"),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [1] не соответствует данным ФРМР (автор документа)",
     "Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР"),
    ("ORG_NOT_FOUND_IN_FRMO", "Организация [ООО] не найдена в реестре организаций",
     "Организация не найдена в ФРМО"),
    ("NO_ORG_ON_DATE", "Element providerOrganization. MO code: [1.2.643] is not actual. Delete date is 2026-06-27",
     "МО недействительна на дату создания документа"),
    # соседние адресные проверки не должны срабатывать одновременно
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-21. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type"
     " должен иметь не пустое значение атрибута @codeSystemVersion. Путь: /ClinicalDocument[1]",
     "Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям"),
]


@pytest.mark.parametrize("code,message,expected_single", COLLISIONS)
def test_tiered_matching_yields_single_type(con, code, message, expected_single):
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)", code, message)
    assert atoms == [expected_single]


def test_code_rules_win_over_text_rules(con):
    """Ярус кода закрывает разбор: текстовое правило не может подменить трактовку,
    заданную классификатором ФНСИ."""
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "GET_DOCUMENT_FILE_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: Статус ответа МИС [error]")
    assert atoms == ["Ошибка при получении файла документа из предоставляющей системы"]


def test_error_classify_dedups_and_joins(con):
    result = one(con, """SELECT public.error_classify(
        '[{"code":"NO_SNILS","message":""},
          {"code":"PATIENT_MPI_MISMATCH","message":"не соответствует данным ГИП"}]'::jsonb)""")
    assert result == (
        "Наличие СНИЛС пациента не соответствует требованиям вида документов"
        " · Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП"
    )


def test_error_classify_empty_message_known_code(con):
    result = one(con, """SELECT public.error_classify(
        '[{"code":"NOT_UNIQUE_PROVIDED_ID","message":""}]'::jsonb)""")
    assert result == "Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован"


# --- Нормализация остатка --------------------------------------------------------------

def test_remd_error_type_strips_document_values(con):
    """Тип не должен нести значения конкретного документа: иначе каждый отказ становится
    отдельной строкой витрины. Полный текст остаётся в error_text."""
    label = one(con, "SELECT public.remd_error_type(%s)",
                "Ошибка валидации Schematron: У1-21. Элемент [x] со СНИЛС 11122233344"
                " и OID 1.2.643.5.1.13.13. Путь: /ClinicalDocument[1]/recordTarget[1]")
    assert "11122233344" not in label
    assert "1.2.643.5.1.13.13" not in label
    assert "Путь:" not in label
    assert "У1-21" not in label
    assert one(con, "SELECT public.remd_error_type(%s)", "") == "(без текста)"


def test_uncovered_message_surfaces_as_text(con):
    """Формулировка без правила показывается как есть, а не сводится к заглушке:
    иначе аналитик не увидит причину, ради которой открывает разбор."""
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "VALIDATION_ERROR", "Совершенно новая проверка РЭМД")
    assert atoms == ["Совершенно новая проверка РЭМД"]


# --- Парсинг payload -------------------------------------------------------------------

def test_xml_error_items_supports_namespaced_items_with_attributes(con):
    payload = (
        '<ns2:errors><ns2:item attr="x"><ns2:code>NO_SNILS</ns2:code>'
        "<ns2:message>СНИЛС отсутствует</ns2:message></ns2:item></ns2:errors>"
    )
    items = one(con, "SELECT public.xml_error_items(%s)", payload)
    assert items == [{"code": "NO_SNILS", "message": "СНИЛС отсутствует"}]


def test_xml_registry_errors_extracts_attrs_in_any_order(con):
    payload = (
        "<rs:RegistryResponse><rs:RegistryErrorList>"
        '<rs:RegistryError severity="urn:e" errorCode="XDSDictionaryValidationError"'
        ' codeContext="Значение &quot;X&quot; не найдено" location=""/>'
        '<rs:RegistryError codeContext="Internal error in repository" errorCode="XDSRepositoryError"/>'
        "</rs:RegistryErrorList></rs:RegistryResponse>"
    )
    items = one(con, "SELECT public.xml_registry_errors(%s)", payload)
    assert items == [
        {"code": "XDSDictionaryValidationError", "message": 'Значение "X" не найдено'},
        {"code": "XDSRepositoryError", "message": "Internal error in repository"},
    ]


def test_build_errors_json_falls_back_to_registry_errors(con):
    payload = (
        "<rs:RegistryResponse>"
        '<rs:RegistryError errorCode="XDSRepositoryError" codeContext="Internal error"/>'
        "</rs:RegistryResponse>"
    )
    items = one(con, "SELECT public.build_errors_json('error', NULL, NULL, %s)", payload)
    assert items == [{"code": "XDSRepositoryError", "message": "Internal error"}]
    # обычные <item> имеют приоритет над RegistryError
    both = (
        "<x><item><code>NO_SNILS</code><message>m</message></item>"
        '<rs:RegistryError errorCode="XDSRepositoryError" codeContext="c"/></x>'
    )
    items = one(con, "SELECT public.build_errors_json('error', NULL, NULL, %s)", both)
    assert items == [{"code": "NO_SNILS", "message": "m"}]


def test_parse_exchangelog_row_extracts_faultcode_last(con):
    row = one(con, "SELECT (public.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<soap:Fault><faultcode>soap:Server</faultcode><faultstring>x</faultstring></soap:Fault>")
    assert row == "SERVER"
    # <code>/<errorCode> имеют приоритет над faultcode
    row = one(con, "SELECT (public.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<r><code>VALIDATION_ERROR</code><faultcode>soap:Server</faultcode></r>")
    assert row == "VALIDATION_ERROR"


# --- Соответствие федеральному классификатору ------------------------------------------

def test_every_nsi_code_is_covered_by_a_rule(con):
    """Каждая мнемоника ФНСИ, кроме зонтичных кодов, закрыта правилом яруса 2 —
    отказ по любому коду классификатора получает наименование справочника."""
    uncovered = one(con, """
        SELECT array_agg(c.nsi_error_code ORDER BY c.nsi_error_code)
        FROM dim_nsi_error_code c
        WHERE NOT EXISTS (SELECT 1 FROM dim_error_rules r
                          WHERE r.is_active AND r.nsi_error_code = c.nsi_error_code)
    """)
    assert sorted(uncovered or []) == sorted(UMBRELLA_CODES)


def test_no_rule_invents_a_code_outside_the_dictionary(con):
    """Правило с кодом обязано объявить пространство имён, а для контура РЭМД — ссылаться
    на существующую мнемонику ФНСИ. Внешний ключ не даёт завести выдуманный код."""
    assert one(con, """
        SELECT count(*) FROM dim_error_rules
        WHERE (match_code IS NOT NULL) <> (code_namespace IS NOT NULL)
           OR (code_namespace IS NOT NULL AND code_namespace NOT IN %s)
           OR ((code_namespace IS NOT DISTINCT FROM 'НСИ 305') <> (nsi_error_code IS NOT NULL))
    """, CODE_NAMESPACES) == 0
    # для контура РЭМД сопоставляемый код и мнемоника справочника — одно и то же значение
    assert one(con, """
        SELECT count(*) FROM dim_error_rules
        WHERE code_namespace = 'НСИ 305' AND match_code IS DISTINCT FROM nsi_error_code
    """) == 0


def test_nsi_dictionary_matches_published_revision(con):
    assert one(con, "SELECT count(*) FROM dim_nsi_error_code") == 127
    assert one(con, """
        SELECT count(*) FROM dim_nsi_error_code
        WHERE oid <> '1.2.643.5.1.13.13.99.2.305' OR version <> '3.18'
    """) == 0
    # синоним обязан вести на существующую мнемонику и не совпадать с ней
    assert one(con, "SELECT count(*) FROM dim_nsi_error_code_alias WHERE alias = nsi_error_code") == 0


def test_types_carry_nsi_code_when_rule_is_code_gated(con):
    """Тип, рождённый правилом с кодом ФНСИ, обязан нести этот код: витрина показывает
    мнемонику рядом с наименованием."""
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group g
        WHERE g.nsi_error_code IS NULL
          AND EXISTS (SELECT 1 FROM dim_error_rules r
                      WHERE r.is_active AND r.interpretation = g.error_type
                        AND r.code_namespace = 'НСИ 305')
    """) == 0


# --- Инварианты словарей ---------------------------------------------------------------

def test_every_active_interpretation_is_canonical(con):
    assert one(con, """
        SELECT count(*) FROM dim_error_rules r
        WHERE r.is_active AND NOT EXISTS (
            SELECT 1 FROM dim_error_type_group g WHERE g.error_type = r.interpretation)
    """) == 0


def test_dictionary_has_no_orphan_types(con):
    """Тип, переставший порождаться правилами, снимается прунингом: иначе он остаётся
    в словаре и продолжает раздавать категорию строкам витрины."""
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group g
        WHERE g.error_type <> 'Неизвестная ошибка'
          AND NOT EXISTS (SELECT 1 FROM dim_error_rules r
                          WHERE r.is_active AND r.interpretation = g.error_type)
    """) == 0


def test_type_names_carry_no_document_values(con):
    """Наименование типа не должно содержать плейсхолдеров значений из описаний ФНСИ:
    «Справочник OID [], версия []» как имя типа нечитаемо."""
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group
        WHERE error_type LIKE '%[%' OR error_type LIKE '%]%'
    """) == 0


def test_every_type_has_responsibility_and_retryable(con):
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group
        WHERE responsibility IS NULL OR is_retryable IS NULL
           OR responsibility NOT IN %s
    """, RESPONSIBILITY_DOMAIN) == 0


def test_all_patterns_compile(con):
    # ~* форсирует компиляцию каждого регекспа; невалидный ARE уронит запрос
    assert one(con, "SELECT count(*) FROM dim_error_rules r WHERE ('' ~* r.match_pattern) IS NULL") == 0


def test_tier_matches_code_presence(con):
    assert one(con, "SELECT count(*) FROM dim_error_rules WHERE (match_tier <= 2) <> (match_code IS NOT NULL)") == 0


def test_tier2_patterns_are_catch_all(con):
    assert one(con, "SELECT count(*) FROM dim_error_rules WHERE match_tier = 2 AND match_pattern <> '(?is).*'") == 0


def test_match_codes_are_uppercase(con):
    # движок сравнивает с upper(btrim(code)); код в смешанном регистре молча не совпадёт
    assert one(con, "SELECT count(*) FROM dim_error_rules WHERE match_code IS NOT NULL AND match_code <> upper(match_code)") == 0


def test_no_duplicate_code_rules_within_tier2(con):
    # два активных code-only правила на один код дали бы недетерминированную пару типов
    assert one(con, """
        SELECT count(*) FROM (
            SELECT match_code FROM dim_error_rules
            WHERE is_active AND match_tier = 2
            GROUP BY match_code HAVING count(DISTINCT interpretation) > 1
        ) d
    """) == 0


def test_umbrella_codes_keep_text_refinements(con):
    """Документирует, почему VALIDATION_ERROR и RUNTIME_ERROR не покрыты ярусом 2:
    blanket-правило закрыло бы текстовые ярусы, которые и несут диагностику."""
    assert one(con, """
        SELECT count(*) FROM dim_error_rules
        WHERE match_tier = 2 AND match_code IN %s
    """, UMBRELLA_CODES) == 0
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "RUNTIME_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: internal_error")
    assert atoms == ["Ошибка при получении файла документа из предоставляющей системы"]


def test_iemk_interpretations_have_prefix(con):
    # Контракт наименования: все типы контура ИЭМК начинаются с «ИЭМК: » —
    # в витринах контур ошибки читается прямо из типа.
    assert one(con, """
        SELECT count(*) FROM dim_error_rules
        WHERE is_active AND error_category = 'Ошибки ИЭМК'
          AND interpretation NOT LIKE 'ИЭМК: %'
    """) == 0
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group
        WHERE error_category = 'Ошибки ИЭМК' AND error_type NOT LIKE 'ИЭМК: %'
    """) == 0


def test_no_nested_patterns_within_tier(con):
    """Эвристика на скрытые дубли: два активных правила одного яруса с одним match_code,
    где паттерн одного — подстрока паттерна другого (кроме пар с одинаковым типом —
    они легальны и дедуплицируются движком)."""
    assert one(con, """
        SELECT count(*) FROM dim_error_rules a
        JOIN dim_error_rules b ON b.is_active AND a.is_active
            AND a.rule_code < b.rule_code
            AND a.match_tier = b.match_tier
            AND a.match_code IS NOT DISTINCT FROM b.match_code
            AND a.interpretation <> b.interpretation
            AND a.match_pattern <> '(?is).*'
            AND (position(a.match_pattern IN b.match_pattern) > 0
                 OR position(b.match_pattern IN a.match_pattern) > 0)
    """) == 0
