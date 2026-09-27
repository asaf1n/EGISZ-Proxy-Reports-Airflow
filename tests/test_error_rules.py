"""Регрессионные тесты обработки ошибок против живого PostgreSQL.

Запуск требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_egisz);
без переменной модуль целиком скипается — как и остальной suite, не зависящий от внешних
сервисов. Фикстура идемпотентно применяет db/02_functions.sql из working tree поверх схемы
db/01_schema.sql, поэтому тесты проверяют текущий код правил, а не состояние базы на момент
последнего наката.

Ожидаемые наименования типов — формулировки классификатора ФНСИ 1.2.643.5.1.13.13.99.2.305:
расхождение теста и справочника означает расхождение классификации с федеральным
классификатором.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from conftest import load_dag_module  # noqa: E402

connect_pg = load_dag_module("egisz_etl_dag").connect_pg

DSN = os.environ.get("EGISZ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")

DB_DIR = Path(__file__).resolve().parents[1] / "db"
SCHEMA_SQL = (DB_DIR / "01_schema.sql").read_text(encoding="utf-8")

ASYNC = "Ошибка асинхронного ответа"
NETWORK = "Ошибка связи"

# Редакция НСИ 805, из которой взят реестр наименований справочников.
NSI_DICTIONARY_SOURCE = ("1.2.643.5.1.13.13.99.2.805", "6.19")
NSI_DICTIONARY_SIZE = 465

RESPONSIBILITY_DOMAIN = ("клиника", "МИС", "интегратор", "РЭМД", "смешанная")

# Категории — группы причин. Вид («Ошибка связи»), контур (ИЭМК) и контур НСИ (ФРЛЛО)
# категориями не являются.
CATEGORIES = (
    "Технические ошибки ЕГИСЗ",
    "Ошибки получения файла ЭМД",
    "Ошибки структуры и валидации",
    "Ошибки справочника НСИ",
    "Данные пациента",
    "Данные медработника",
    "Ошибки ЭП и сертификатов",
    "Ошибки организации / ИС",
    "Ошибки регистрации",
    "Прочие",
)

# Коды-зонтики: их описание в ФНСИ («Ошибка валидации значения», «Непредвиденная ошибка»)
# не несёт диагностики, поэтому правило яруса 2 для них не заводится — причина читается из
# текста ярусами 3–4.
UMBRELLA_CODES = ("VALIDATION_ERROR", "RUNTIME_ERROR")


@pytest.fixture(scope="module")
def con():
    con = connect_pg(DSN)
    with con.cursor() as cur:
        cur.execute((DB_DIR / "02_functions.sql").read_text(encoding="utf-8"))
    con.commit()
    yield con
    con.rollback()
    con.close()


def one(con, sql: str, *params):
    with con.cursor() as cur:
        cur.execute(sql, params or None)
        return cur.fetchone()[0]


def classify(con, code: str | None, text: str | None, kind: str = ASYNC) -> tuple[str | None, str | None]:
    with con.cursor() as cur:
        cur.execute("SELECT error_type, nsi_dictionary_oid FROM stg_egisz.classify_error(%s, %s, %s)",
                    (kind, code, text))
        return cur.fetchone()


def category(con, error_type: str | None) -> str | None:
    with con.cursor() as cur:
        cur.execute("SELECT error_category FROM mart_egisz.dim_error_type WHERE error_type = %s", (error_type,))
        row = cur.fetchone()
        return row[0] if row else None


def items(con, logstate: int | None, logtext: str | None, msgtext: str | None,
          outcome: str | None, error_code: str | None = None, error_message: str | None = None):
    with con.cursor() as cur:
        cur.execute(
            "SELECT item_no, error_kind, error_code, error_text "
            "FROM stg_egisz.error_items(%s, %s, %s, %s, %s, %s)",
            (logstate, logtext, msgtext, outcome, error_code, error_message),
        )
        return cur.fetchall()


# --- Корпус: (code, message, ожидаемый тип, ожидаемая категория) ------------------------
# Сообщения — обезличенные образцы из архива ответов (значения заменены на […]). Категория
# None — тип без правила: его заводит в справочнике разбор журнала при первом появлении.
CORPUS = [
    # --- Ярус 2: код закрывает разбор, тип — наименование из ФНСИ ------------------------
    ("PATIENT_MPI_MISMATCH",
     "Указанное значение [Фамилия] [Имя] не соответствует данным ГИП [—]. Пациент найден по локальному идентификатору",
     "Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП",
     "Данные пациента"),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [111] не соответствует занимаемой им должности в организации [222] по данным ФРМР.",
     "Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР",
     "Данные медработника"),
    ("NOT_UNIQUE_PROVIDED_ID", "",
     "Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован", "Ошибки регистрации"),
    ("NO_SNILS", "СНИЛС пациента в составе сведений о пациенте обязателен для данного вида документов",
     "Наличие СНИЛС пациента не соответствует требованиям вида документов", "Данные пациента"),
    ("RESTRICT_NEW_VERSION", "Для ЭМД 230 запрещена регистрация новых версий",
     "Для вида документа запрещено регистрировать новую версию", "Ошибки регистрации"),
    ("WRONG_CREATION_DATE", "Дата создания документа не может быть позднее даты регистрации",
     "Дата создания документа больше даты регистрации", "Ошибки регистрации"),
    ("RATE_LIMIT", "Доступ к сервису временно запрещён - превышен лимит запросов",
     "Достигнут защитный лимит, просьба повторить через минуту или позже", "Технические ошибки ЕГИСЗ"),
    ("RMIS_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: Error in getDocumentFile by SOAP",
     "Ошибка ответа от сервиса системы в getDocumentFileResponse, предоставляющей документ",
     "Ошибки получения файла ЭМД"),
    # Файл получен, но не является валидным XML: код побеждает текстовый ярус
    # «файлового хранилища».
    ("INVALID_CONTENT", "Ошибка получения файла ЭМД из файлового хранилища: Переданный файл не является валидным XML файлом",
     "Из предоставляющей РМИС/МИС передан документ, формат файла которого не соответствует требованиям вида документов",
     "Ошибки структуры и валидации"),
    ("DOC_DATE_MISMATCH_CERT_NOT_AFTER", "Сертификат МО недействителен на дату создания документа",
     "Сертификат ЭП недействителен на дату создания документа (документ создан позже окончания срока действия сертификата)",
     "Ошибки ЭП и сертификатов"),
    ("INVALID_DOCTOR_NAME",
     "Имя [Иван] медицинского работника в запросе на регистрацию отличается от имени [Иоан] в СЭМД. СНИЛС [111]",
     "Имя медицинского работника в запросе на регистрацию отличается от имени в СЭМД",
     "Данные медработника"),
    ("CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT", "Не удалось построить цепочку сертификатов",
     "Не удалось построить цепочку сертификатов до аккредитованного удостоверяющего центра",
     "Ошибки ЭП и сертификатов"),
    ("INVALID_DICTIONARY_OID", "Справочник OID [1.2.643.5.1.13.13.11.105978]. Справочник с указанным кодом отсутствует",
     "Справочник с указанным кодом отсутствует", "Ошибки справочника НСИ"),
    ("INVALID_DICTIONARY_VERSION", "Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.31] недопустима для документа вида [227].",
     "Версия справочника недопустима для данного вида документа", "Ошибки справочника НСИ"),
    ("XML_VALIDATION_ERROR", "Ошибка трансформации",
     "Ошибка при трансформации СЭМД для проверки (Schematron)", "Ошибки структуры и валидации"),
    ("SIGNATURE_VERIFICATION_ERROR", "Проверка подписи завершилась отрицательно",
     "Подпись не верна", "Ошибки ЭП и сертификатов"),
    ("OBJECT_NOT_FOUND", "Запись не найдена", "Не найдена запись справочника", "Ошибки справочника НСИ"),
    ("ROLE_OCCURRENCE_MISMATCH", "Роль подписанта не соответствует",
     "Число ЭП сотрудников с требуемой ролью не соответствует требованиям вида документов",
     "Ошибки ЭП и сертификатов"),
    # CA_INACCESSIBILITY и текст «Удостоверяющий центр недоступен» — одна причина.
    ("CA_INACCESSIBILITY", "Удостоверяющий центр сертификата недоступен: Время ожидания истекло.",
     "Адрес OCSP-службы не указан или недоступен, CRL также недоступен", "Ошибки ЭП и сертификатов"),
    ("", "Удостоверяющий центр сертификата недоступен: Время ожидания истекло.",
     "Адрес OCSP-службы не указан или недоступен, CRL также недоступен", "Ошибки ЭП и сертификатов"),
    ("PERSONAL_SIG_CERT_NOT_ACTUAL_ON_DOC_CREATION_DT", "",
     "Сертификат сотрудника недействителен на дату создания документа", "Ошибки ЭП и сертификатов"),
    ("DUPLICATE_PATIENT_FOUND", "",
     "По локальному идентификатору в ГИП найдено более одной записи", "Данные пациента"),
    # РЭМД отдаёт RECIPIENT_*, справочник закрепляет RECEPIENT_*: синоним разрешается до
    # сопоставления, поэтому правило одно.
    ("RECIPIENT_INFO_MISMATCH", "Получатель [111] из запроса на регистрацию сведений не найден в СЭМД",
     "Получатель из запроса на регистрацию сведений не найден в СЭМД", "Данные пациента"),
    ("RECEPIENT_INFO_MISMATCH", "",
     "Получатель из запроса на регистрацию сведений не найден в СЭМД", "Данные пациента"),

    # --- Схематрон: разделён по конкретной проверке (§5.8 регламента) -------------------
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-19. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type"
     " должен иметь не пустое значение атрибута @code. Путь: /ClinicalDocument[1]/recordTarget[1]",
     "Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям", "Данные пациента"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-18. Элемент ClinicalDocument/recordTarget/patientRole/addr"
     " должен иметь 1 элемент address:Type. Путь: /ClinicalDocument[1]/recordTarget[1]",
     "Адрес пациента: не указан тип адреса (address:Type)", "Данные пациента"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-17. Элемент ClinicalDocument/recordTarget/patientRole"
     " должен иметь 1 или 2 элемента addr. Путь: /ClinicalDocument[1]/recordTarget[1]",
     "Адрес пациента: недопустимое число элементов addr", "Данные пациента"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-2: Элемент streetAddressLine должен содержать не пустое текстовое наполнение",
     "Адрес пациента: составляющая адреса не заполнена", "Данные пациента"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-9. Элемент ClinicalDocument/recordTarget/patientRole/id[2]"
     " не должен иметь атрибут @nullFlavor. Путь: /ClinicalDocument[1]/recordTarget[1]",
     "Идентификатор пациента: недопустимый атрибут @nullFlavor", "Данные пациента"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-4.1.1.1: Элемент telecom обязан содержать один атрибут @value с не пустым значением",
     "Контактные данные: не заполнен атрибут @value элемента telecom", "Ошибки структуры и валидации"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: Допустимые значения для элементов functionCode[1]: CHAIRMAN, COMMISSIONER",
     "Значение элемента не входит в перечень допустимых", "Ошибки структуры и валидации"),
    ("VALIDATION_ERROR", "Ошибка валидации Schematron: экзотическое требование без известных элементов",
     "Ошибка Schematron-валидации", "Ошибки структуры и валидации"),

    # --- Валидация по XSD (§5.7 регламента) --------------------------------------------
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-complex-type.2.4.a: Invalid content was found starting with element id",
     "XSD: недопустимый элемент или нарушен порядок элементов", "Ошибки структуры и валидации"),
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-complex-type.3.2.2: Attribute 'nullFlavor' is not allowed to appear in element 'telecom'.",
     "XSD: недопустимый атрибут элемента", "Ошибки структуры и валидации"),
    ("VALIDATION_ERROR",
     "Ошибка валидации СЭМД: cvc-datatype-valid.1.2.1: '5 ml' is not a valid value of union type 'real'.",
     "XSD: значение не соответствует типу элемента", "Ошибки структуры и валидации"),

    # --- Кросс-валидация запроса и СЭМД (§5.2–5.5 регламента) --------------------------
    ("", "Уникальный идентификатор документа в ЭМД [abc] отличается от уникального идентификатора документа в запросе на регистрацию сведений [def]",
     "Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию",
     "Ошибки регистрации"),
    ("VALIDATION_ERROR", "СНИЛС  пациента в ЭМД [111] отличается от СНИЛС пациента в запросе на регистрацию сведений [222]",
     "СНИЛС пациента в ЭМД не совпадает с запросом на регистрацию", "Данные пациента"),
    ("VALIDATION_ERROR", "Организация [ООО Клиника] не привязана к РМИС [42]",
     "Организация не привязана к РМИС", "Ошибки организации / ИС"),
    ("VALIDATION_ERROR", "Недопустимые символы в имени 'Фамилия (девичья)'",
     "ФИО пациента содержит недопустимые символы", "Данные пациента"),
    ("VALUE_MISMATCH_METADATA_AND_CERTIFICATE",
     "В ФРМР не найдена актуальная на дату создания документа карточка МР c данными из сертификата подписи МО",
     "Подписант из сертификата не найден в ФРМР", "Данные медработника"),
    ("RUNTIME_ERROR", "Не удается провести проверку ФРМР",
     "Проверяющая подсистема РЭМД недоступна", "Технические ошибки ЕГИСЗ"),

    # --- Контур ИЭМК: код из атрибута RegistryError/errorCode --------------------------
    ("XDSDictionaryValidationError", "Element representedCustodianOrganization. MO code [1.2.643] is not actual.",
     "ИЭМК: данные не соответствуют справочнику НСИ", "Ошибки справочника НСИ"),
    ("XDSRepositoryError", "Internal error in repository",
     "ИЭМК: внутренняя ошибка репозитория", "Технические ошибки ЕГИСЗ"),
    ("XDSDocumentUniqueIdError", "Association [RPLC] targetId with unique ID [E13B85998D5A] not found in repository",
     "ИЭМК: заменяемый документ не найден (замена версии)", "Ошибки регистрации"),
    ("XDSDocumentUniqueIdError", "malformed unique id",
     "ИЭМК: некорректный идентификатор документа", "Ошибки регистрации"),
    ("XDSRegistryBusy", "", "ИЭМК: сервис временно недоступен", "Технические ошибки ЕГИСЗ"),
    ("", "[CRE-122]: PAT-001; Пациент не определен: [СНИЛС [111] не валидно контрольное число]",
     "ИЭМК: пациент не определён", "Данные пациента"),

    # --- Без правила: тип — текст с замаскированными значениями -------------------------
    ("", "совершенно нераспознаваемый текст", "совершенно нераспознаваемый текст", None),
    ("VALIDATION_ERROR",
     "Неизвестная проверка со СНИЛС [11122233344] и OID [1.2.643.5.1.13]. Путь: /ClinicalDocument[1]/x",
     "Неизвестная проверка со СНИЛС […] и OID […].", None),
    # Код вне классификатора и без текста типа не получает: элемент виден в контроле
    # качества, а не скрыт подставленным наименованием.
    ("SOME_UNSEEN_CODE", "", None, None),
]


@pytest.mark.parametrize("code,message,expected_type,expected_category", CORPUS)
def test_classification_corpus(con, code, message, expected_type, expected_category):
    error_type, _ = classify(con, code, message)
    assert error_type == expected_type
    assert category(con, error_type) == expected_category


# --- Коллизии ярусов: один тип на элемент -------------------------------------------------
COLLISIONS = [
    ("ASYNC_RESPONSE_TIMEOUT", "Превышен таймаут ожидания асинхронного ответа",
     "Превышено ожидание асинхронного ответа от проверяющей системы"),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [1] не соответствует данным ФРМР (автор документа)",
     "Переданная должность сотрудника не соответствует должности, зарегистрированной в ФРМР"),
    ("ORG_NOT_FOUND_IN_FRMO", "Организация [ООО] не найдена в реестре организаций",
     "Организация не найдена в ФРМО"),
    ("NO_ORG_ON_DATE", "Element providerOrganization. MO code: [1.2.643] is not actual. Delete date is 2026-06-27",
     "МО недействительна на дату создания документа"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-21. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type"
     " должен иметь не пустое значение атрибута @codeSystemVersion. Путь: /ClinicalDocument[1]",
     "Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям"),
]


@pytest.mark.parametrize("code,message,expected_single", COLLISIONS)
def test_tiered_matching_yields_single_type(con, code, message, expected_single):
    assert classify(con, code, message)[0] == expected_single


def test_code_rules_win_over_text_rules(con):
    """Ярус кода закрывает разбор: текстовое правило не подменяет тип, заданный
    классификатором ФНСИ."""
    assert classify(con, "GET_DOCUMENT_FILE_ERROR",
                    "Ошибка получения файла ЭМД из файлового хранилища: Статус ответа МИС [error]")[0] == \
        "Ошибка при получении файла документа из предоставляющей системы"


def test_rule_type_replaces_readable_message(con):
    """Тип элемента с правилом — наименование правила. Годится ли текст сообщения в тип,
    решают при пополнении правил, а не при разборе."""
    message = "СНИЛС пациента в составе сведений о пациенте обязателен для данного вида документов"
    assert classify(con, "NO_SNILS", message)[0] == \
        "Наличие СНИЛС пациента не соответствует требованиям вида документов"


# --- Маскирование текста без правила -----------------------------------------------------

@pytest.mark.parametrize("message,expected", [
    ("[CRE-013]: XYZ-001; Пациент не определен: [СНИЛС [12345678901] не валидно контрольное число 92];"
     " Patient(moId: [1.2.643.5.1.13.13.12.2.77.12345], patientId: [B1234567-B123-4C12-8A1B-1234E12DDFFA])",
     "Пациент не определен: СНИЛС […] не валидно контрольное число"),
    ("[CRE-013]: XYZ-001; Пациент не определен: [СНИЛС [12345678] не соответствует формату \\d{11}];"
     " Patient(moId: [1.2.643.5.1.13.13.12.2.77.1234], patientId: [DFD1F2A3-4EEF-5B6A-A7E8-9CC01C23BC45])",
     "Пациент не определен: СНИЛС […] не соответствует формату (11 цифр)"),
    ("Ошибки валидации в ФРМСС: [code: DUPLICATE, description: Свидетельство с номером 123456789 и серией 12"
     " уже зарегистрировано в РЭМД. Исправьте номер и/или серию документа.].",
     "Ошибки валидации в ФРМСС (DUPLICATE): Свидетельство с номером […] и серией […]"
     " уже зарегистрировано в РЭМД. Исправьте номер и/или серию документа."),
    ("Ошибки валидации в ФРМСС: [code: MSSCERT, description: Внутренняя ошибка сервиса ФРМСС,"
     " уникальный идентификатор ошибки: a1a2f3f4-d56d-78ba-bd9f-e0b12db34a56].",
     "Ошибки валидации в ФРМСС (MSSCERT): Внутренняя ошибка сервиса ФРМСС"),
])
def test_wrapped_responses_are_unwrapped(con, message, expected):
    assert classify(con, "", message)[0] == expected


def test_attribute_name_survives_bracket_masking(con):
    """«Указанное значение [Имя пациента] …» называет, что именно не совпало: реквизит
    остаётся, значения скрываются."""
    assert classify(con, "", "Указанное значение [Имя пациента] [Петрова Анна] отличается от сведений [Петрова А.]")[0] == \
        "Указанное значение [Имя пациента] […] отличается от сведений […]"


@pytest.mark.parametrize("code,message,leak", [
    ("", "ЭП МО не верна: Validation failed for the target: serial: 1a2b subject: CN=Иванова Анна Петровна,"
         " EMAILADDRESS=ivanova@example.ru", "Иванова"),
    ("", "Дата рождения сотрудника со СНИЛС [111] (1975-07-21) не соответствует данным ФРМР [222]", "1975-07-21"),
    ("", "Недопустимые символы в имени 'Петрова (сидорова)'", "Петрова"),
    ("", "Неверный формат e-mail 'Ivanov.I.I@example.ru '", "Ivanov"),
    ("", "Адрес ivanov@example.ru недоступен", "ivanov@"),
])
def test_error_type_carries_no_instance_values(con, code, message, leak):
    """Тип уходит в фильтры и сводки дашбордов, в том числе клиентских."""
    error_type, _ = classify(con, code, message)
    assert error_type and leak not in error_type


def test_masking_strips_document_values(con):
    error_type, _ = classify(con, "VALIDATION_ERROR",
                             "Проверка без правила: элемент [x] со СНИЛС 11122233344"
                             " и OID 1.2.643.5.1.13.13. Путь: /ClinicalDocument[1]/recordTarget[1]")
    assert "11122233344" not in error_type
    assert "1.2.643.5.1.13.13" not in error_type
    assert "Путь:" not in error_type


def test_network_error_type_is_masked_gateway_text(con):
    assert classify(con, "10060", "Synapse TCP/IP Socket error 10060: Connection timed out", NETWORK)[0] == \
        "Synapse TCP/IP Socket error 10060: Connection timed out"
    assert classify(con, "500", "Error while receiving data from service: https://gost-123.example.ru:9945/api"
                    " Error code: 500", NETWORK)[0] == \
        "Error while receiving data from service: <endpoint> Error code: 500"


# --- Элементы ошибки сообщения ------------------------------------------------------------

def test_delivery_failure_is_a_network_error_item(con):
    assert items(con, 3, "Synapse TCP/IP Socket error 11001: Host not found", None, None) == [
        (0, NETWORK, "11001", "Synapse TCP/IP Socket error 11001: Host not found")]
    assert items(con, 3, "Error while receiving data from service: https://x Error code: 503", None, None)[0][2] == "503"


def test_undelivered_response_keeps_both_kinds(con):
    """Сбой доставки ответа не отменяет отказа в этом же ответе."""
    payload = "<registerDocumentResult><status>error</status><item><code>NO_SNILS</code><message>м</message></item></registerDocumentResult>"
    assert items(con, 3, "Synapse TCP/IP Socket error 10060: Connection timed out", payload, "error") == [
        (0, NETWORK, "10060", "Synapse TCP/IP Socket error 10060: Connection timed out"),
        (1, ASYNC, "NO_SNILS", "м"),
    ]


def test_success_response_items_are_kept(con):
    payload = ("<registerDocumentResult><status>success</status><emdrId>1</emdrId>"
               "<item><code>VALIDATION_ERROR</code><message>м</message></item></registerDocumentResult>")
    assert items(con, 0, None, payload, "success") == [(1, ASYNC, "VALIDATION_ERROR", "м")]


def test_request_message_has_no_response_items(con):
    assert items(con, 0, None, "<item><code>X</code></item>", None) == []


def test_error_items_support_namespaced_items_with_attributes(con):
    payload = ('<ns2:errors><ns2:item attr="x"><ns2:code>NO_SNILS</ns2:code>'
               "<ns2:message>СНИЛС отсутствует</ns2:message></ns2:item></ns2:errors>")
    assert items(con, 0, None, payload, "error") == [(1, ASYNC, "NO_SNILS", "СНИЛС отсутствует")]


def test_error_items_read_registry_errors_in_any_attribute_order(con):
    payload = (
        "<rs:RegistryResponse><rs:RegistryErrorList>"
        '<rs:RegistryError severity="urn:e" errorCode="XDSDictionaryValidationError"'
        ' codeContext="Значение &quot;X&quot; не найдено" location=""/>'
        '<rs:RegistryError codeContext="Internal error in repository" errorCode="XDSRepositoryError"/>'
        "</rs:RegistryErrorList></rs:RegistryResponse>"
    )
    assert items(con, 0, None, payload, "error") == [
        (1, ASYNC, "XDSDictionaryValidationError", 'Значение "X" не найдено'),
        (2, ASYNC, "XDSRepositoryError", "Internal error in repository"),
    ]


def test_items_take_priority_over_registry_errors_and_fallback(con):
    both = ("<x><item><code>NO_SNILS</code><message>m</message></item>"
            '<rs:RegistryError errorCode="XDSRepositoryError" codeContext="c"/></x>')
    assert items(con, 0, None, both, "error") == [(1, ASYNC, "NO_SNILS", "m")]
    # Ответ об ошибке без элементов: код и текст ответа.
    assert items(con, 0, None, "<soap:Fault/>", "error", "SERVER", "текст") == [(1, ASYNC, "SERVER", "текст")]


# --- Исход асинхронного ответа ------------------------------------------------------------

@pytest.mark.parametrize("action,raw_status,document_status,fault,error_ilike,registry_status,expected", [
    ("sendRegisterDocumentResult", "success", None, False, False, None, "success"),
    ("sendRegisterDocumentResult", "error", None, False, True, None, "error"),
    ("sendRegisterDocumentResult", "", "Зарегистрировано", False, False, None, "success"),
    ("urn:ihe:iti:2007:ProvideAndRegisterDocumentSet-bAsyncResponse", "", None, False, False, "Success", "success"),
    ("urn:ihe:iti:2007:ProvideAndRegisterDocumentSet-bAsyncResponse", "", None, False, False, "Failure", "error"),
    # Запрос — не асинхронный ответ: исхода нет, сбой его доставки статус не меняет.
    ("getDocumentFile", "", None, False, False, None, None),
    ("registerDocument", "", None, True, True, None, None),
    # Нераспознанный асинхронный ответ исхода не получает и виден в контроле качества.
    ("sendRegisterDocumentResult", "processing", None, False, False, None, None),
])
def test_async_outcome(con, action, raw_status, document_status, fault, error_ilike, registry_status, expected):
    assert one(con, "SELECT stg_egisz.classify_async_status(%s, %s, %s, %s, %s, %s)",
               action, raw_status, document_status, fault, error_ilike, registry_status) == expected


def test_parse_exchangelog_row_extracts_faultcode_last(con):
    row = one(con, "SELECT (stg_egisz.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<soap:Fault><faultcode>soap:Server</faultcode><faultstring>x</faultstring></soap:Fault>")
    assert row == "SERVER"
    # <code>/<errorCode> имеют приоритет над faultcode
    row = one(con, "SELECT (stg_egisz.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<r><code>VALIDATION_ERROR</code><faultcode>soap:Server</faultcode></r>")
    assert row == "VALIDATION_ERROR"


# --- Соответствие федеральному классификатору --------------------------------------------

def test_every_nsi_code_is_covered_by_a_rule(con):
    """Каждая мнемоника ФНСИ, кроме зонтичных кодов, закрыта правилом яруса 2."""
    uncovered = one(con, """
        SELECT array_agg(c.nsi_error_code ORDER BY c.nsi_error_code)
        FROM mart_egisz.dim_nsi_error_code c
        WHERE NOT EXISTS (SELECT 1 FROM mart_egisz.dim_error_rules r
                          WHERE r.rule_kind = 'классификация' AND r.nsi_error_code = c.nsi_error_code)
    """)
    assert sorted(uncovered or []) == sorted(UMBRELLA_CODES)


def test_code_rules_reference_the_dictionary(con):
    """Правило с кодом НСИ сопоставляет ровно свою мнемонику; правило контура ИЭМК
    мнемоники НСИ не несёт."""
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE nsi_error_code IS NOT NULL AND match_code IS DISTINCT FROM nsi_error_code
    """) == 0
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE match_code LIKE 'XDS%%' AND nsi_error_code IS NOT NULL
    """) == 0


def test_nsi_dictionary_matches_published_revision(con):
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_error_code") == 127
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_error_code
        WHERE oid <> '1.2.643.5.1.13.13.99.2.305' OR version <> '3.18'
    """) == 0
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_error_code_alias WHERE alias = nsi_error_code") == 0


def test_types_carry_nsi_code_when_rule_is_code_gated(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_type t
        JOIN mart_egisz.dim_error_rules r ON r.rule_code = t.rule_code
        WHERE r.nsi_error_code IS NOT NULL AND t.nsi_error_code IS DISTINCT FROM r.nsi_error_code
    """) == 0


# --- Инварианты справочников ------------------------------------------------------------

def test_categories_are_cause_groups(con):
    assert set(one(con, """
        SELECT array_agg(error_category) FROM mart_egisz.dim_error_category
        WHERE error_kind = 'Ошибка асинхронного ответа'
    """)) == set(CATEGORIES)
    # У вида «Ошибка связи» категорий нет.
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_category
        WHERE error_kind = 'Ошибка связи' AND error_category IS NOT NULL
    """) == 0


def test_every_rule_interpretation_is_a_type_with_its_category(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'классификация' AND NOT EXISTS (
            SELECT 1 FROM mart_egisz.dim_error_type t
            WHERE t.error_type = r.interpretation AND t.error_category = r.error_category)
    """) == 0


def test_dictionary_has_no_orphan_rule_types(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_type t
        WHERE t.rule_code IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM mart_egisz.dim_error_rules r
                          WHERE r.rule_kind = 'классификация' AND r.interpretation = t.error_type)
    """) == 0


def test_rule_type_names_carry_no_document_values(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_type
        WHERE rule_code IS NOT NULL AND (error_type LIKE '%%[%%' OR error_type LIKE '%%]%%')
    """) == 0


def test_every_type_has_responsibility_and_retryable(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_type
        WHERE responsibility IS NULL OR is_retryable IS NULL OR responsibility NOT IN %s
    """, RESPONSIBILITY_DOMAIN) == 0


def test_all_patterns_compile(con):
    # ~* форсирует компиляцию каждого регекспа; невалидный ARE уронит запрос
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_rules r WHERE ('' ~* r.match_pattern) IS NULL") == 0
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'маскирование'
          AND regexp_replace('x', r.match_pattern, r.replacement, r.match_flags) IS NULL
    """) == 0


def test_masking_steps_have_distinct_order(con):
    assert one(con, """
        SELECT count(*) FROM (
            SELECT apply_order FROM mart_egisz.dim_error_rules
            WHERE rule_kind = 'маскирование' GROUP BY apply_order HAVING count(*) > 1) d
    """) == 0


def test_tier_matches_code_presence(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE rule_kind = 'классификация' AND (match_tier <= 2) <> (match_code IS NOT NULL)
    """) == 0


def test_tier2_patterns_are_catch_all(con):
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_rules WHERE match_tier = 2 AND match_pattern <> '(?is).*'") == 0


def test_match_codes_are_uppercase(con):
    # Классификация сравнивает с upper(btrim(code)).
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_rules WHERE match_code <> upper(match_code)") == 0


def test_no_duplicate_code_rules_within_tier2(con):
    assert one(con, """
        SELECT count(*) FROM (
            SELECT match_code FROM mart_egisz.dim_error_rules
            WHERE match_tier = 2
            GROUP BY match_code HAVING count(DISTINCT interpretation) > 1
        ) d
    """) == 0


def test_umbrella_codes_keep_text_refinements(con):
    """VALIDATION_ERROR и RUNTIME_ERROR не покрыты ярусом 2: сплошное правило закрыло бы
    текстовые ярусы, которые и несут диагностику."""
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_rules WHERE match_tier = 2 AND match_code IN %s",
               UMBRELLA_CODES) == 0
    assert classify(con, "RUNTIME_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: internal_error")[0] == \
        "Ошибка при получении файла документа из предоставляющей системы"


def test_iemk_interpretations_have_prefix(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE match_code LIKE 'XDS%%' AND interpretation NOT LIKE 'ИЭМК: %%'
    """) == 0


def test_no_nested_patterns_within_tier(con):
    """Эвристика на скрытые дубли: два правила одного яруса с одним match_code, где шаблон
    одного — подстрока шаблона другого (пары с одинаковым типом легальны)."""
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules a
        JOIN mart_egisz.dim_error_rules b
          ON a.rule_kind = 'классификация' AND b.rule_kind = 'классификация'
         AND a.rule_code < b.rule_code
         AND a.match_tier = b.match_tier
         AND a.match_code IS NOT DISTINCT FROM b.match_code
         AND a.interpretation <> b.interpretation
         AND a.match_pattern <> '(?is).*'
         AND (position(a.match_pattern IN b.match_pattern) > 0
              OR position(b.match_pattern IN a.match_pattern) > 0)
    """) == 0


# --- Справочник НСИ как атрибут ошибки -------------------------------------------------

DICTIONARY_MESSAGES = [
    ("Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.45] недопустима для документа"
     " вида [227]. Требуется использовать версии: [4.46]", "1.2.643.5.1.13.13.99.2.197"),
    ("Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.38] недопустима для документа"
     " вида [227]. Требуется использовать версии: [4.45]", "1.2.643.5.1.13.13.99.2.197"),
    # Захватывается справочник, а не код элемента: код принадлежит отдельному документу.
    ("Справочник OID [1.2.643.5.1.13.13.11.1005], версия [2.27]. Элемент с кодом [M51.1+] отсутствует.",
     "1.2.643.5.1.13.13.11.1005"),
    ("Справочник OID [1.2.643.5.1.13.13.11.1070]. Элемент с кодом [A04.20.001.001] отсутствует.",
     "1.2.643.5.1.13.13.11.1070"),
    ("Запись справочника [1.2.643.5.1.13.13.11.1066] с идентификатором [114] не найдена",
     "1.2.643.5.1.13.13.11.1066"),
    # Отказы вне класса: шаблон обязан молчать, иначе признак поехал бы на чужие типы.
    ("Подписант из сертификата не найден в ФРМР", None),
    ("Подразделение с идентификатором [1.2.643.5.1.13.13.12.2.36.20192.0.704432]"
     " не существовало на дату создания документа", None),
    ("Ошибка валидации Schematron: У1-21.1.5: Элемент medService:serviceCond", None),
]


@pytest.mark.parametrize("message,expected", DICTIONARY_MESSAGES)
def test_dictionary_pattern_extracts_dictionary_oid(con, message, expected):
    assert one(con, """
        SELECT (
            SELECT (regexp_match(%s, p.nsi_dictionary_pattern))[1]
            FROM (SELECT DISTINCT nsi_dictionary_pattern FROM mart_egisz.dim_error_rules
                  WHERE nsi_dictionary_pattern IS NOT NULL) p
            WHERE %s ~ p.nsi_dictionary_pattern
            LIMIT 1
        )
    """, message, message) == expected


def test_classification_returns_dictionary_of_its_own_element(con):
    oids = ["1.2.643.5.1.13.13.99.2.197", "1.2.643.5.1.13.13.11.1005"]
    for oid in oids:
        _, found = classify(con, "INVALID_DICTIONARY_VERSION",
                            f"Справочник OID [{oid}]. Версия [1] недопустима для документа вида [227].")
        assert found == oid


def test_dictionary_pattern_consistent_within_type(con):
    assert one(con, """
        SELECT count(*) FROM (
            SELECT interpretation FROM mart_egisz.dim_error_rules
            WHERE rule_kind = 'классификация'
            GROUP BY interpretation
            HAVING count(DISTINCT COALESCE(nsi_dictionary_pattern, '')) > 1
        ) x
    """) == 0


def test_dictionary_pattern_has_single_capture_group(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE nsi_dictionary_pattern IS NOT NULL
          AND length(nsi_dictionary_pattern) - length(replace(nsi_dictionary_pattern, '([', '')) <> 2
    """) == 0


def test_dictionary_pattern_declared_for_dictionary_class(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE error_category = 'Ошибки справочника НСИ' AND nsi_dictionary_pattern IS NULL
    """) == 0


# --- Текущие ошибки документа ------------------------------------------------------------

def test_current_errors_follow_last_async_response(con):
    """Ошибки текущего состояния — элементы последнего асинхронного ответа и ошибки связи
    после него; сбой доставки до ответа к текущему состоянию не относится."""
    if one(con, "SELECT to_regclass('stg_egisz.document_errors_current')") is None:
        pytest.skip("витрина текущих ошибок не построена; проверять нечего")
    doc = str(uuid.uuid4())

    def element(kind: str, code: str, text: str, item_no: int) -> dict[str, object]:
        return {"item_no": item_no, "error_kind": kind, "error_code": code, "error_text": text,
                "error_type": text, "nsi_dictionary_oid": None}

    rows = [
        (-9_000_000_001, "3 hours", None, [element(NETWORK, "10060", "до ответа", 0)]),
        (-9_000_000_002, "2 hours", "error", [element(ASYNC, "NO_SNILS", "отказ", 1)]),
        (-9_000_000_003, "1 hour", None, [element(NETWORK, "11001", "после ответа", 0)]),
    ]
    with con.cursor() as cur:
        cur.execute("SAVEPOINT current_errors")
        try:
            for logid, age, status, details in rows:
                cur.execute(
                    "INSERT INTO stg_egisz.exchange_messages (logid, log_date, dwh_id, status, error_details) "
                    "VALUES (%s, now() - %s::interval, %s, %s, %s::jsonb)",
                    (logid, age, doc, status, json.dumps(details)))
            cur.execute("SELECT pg_get_viewdef('stg_egisz.document_errors_current'::regclass, true)")
            view_sql = cur.fetchone()[0].rstrip().rstrip(";")
            cur.execute("SELECT error_text FROM (" + view_sql + ") c WHERE dwh_id = %s ORDER BY error_no", (doc,))
            assert [r[0] for r in cur.fetchall()] == ["отказ", "после ответа"]
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT current_errors")
            cur.execute("RELEASE SAVEPOINT current_errors")


# --- Реестр наименований справочников ФНСИ ---------------------------------------------

def test_nsi_dictionary_matches_published_805_revision(con):
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_dictionary") == NSI_DICTIONARY_SIZE
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionary
        WHERE source_oid <> %s OR source_version <> %s
           OR name IS NULL OR btrim(name) = ''
    """, *NSI_DICTIONARY_SOURCE) == 0


def test_nsi_dictionary_agrees_with_805_snapshot(con):
    if one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_semd_guide_dictionary") == 0:
        pytest.skip("снимок НСИ 805 не загружен; сверять нечего")
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionary d
        JOIN (SELECT DISTINCT dict_oid, dict_name FROM mart_egisz.dim_nsi_semd_guide_dictionary) g
          ON g.dict_oid = d.oid
        WHERE g.dict_name <> d.name
    """) == 0


def test_nsi_dictionary_short_name_only_shortens(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionary
        WHERE short_name IS NOT NULL
          AND (btrim(short_name) = '' OR length(short_name) >= length(name))
    """) == 0
    assert one(con, "SELECT short_name FROM mart_egisz.dim_nsi_dictionary WHERE oid = '1.2.643.5.1.13.13.11.1005'") == "МКБ-10"


def test_document_error_names_every_registered_dictionary(con):
    """Наименование справочника пусто только у OID вне 805."""
    if one(con, "SELECT to_regclass('serving_egisz.document_errors')") is None:
        pytest.skip("витрина ошибок документа не построена; проверять нечего")
    assert one(con, """
        SELECT count(*) FROM serving_egisz.document_errors e
        WHERE e.nsi_dictionary_oid IS NOT NULL
          AND e.nsi_dictionary_name IS NULL
          AND EXISTS (SELECT 1 FROM mart_egisz.dim_nsi_dictionary d WHERE d.oid = e.nsi_dictionary_oid)
    """) == 0


def test_nsi_dictionary_schema_contract() -> None:
    """Комментарий к таблице — единственное место, где записано назначение реестра и его
    потребитель."""
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_nsi_dictionary (" in SCHEMA_SQL
    assert "COMMENT ON TABLE mart_egisz.dim_nsi_dictionary IS" in SCHEMA_SQL
    assert "COMMENT ON COLUMN mart_egisz.dim_nsi_dictionary.short_name IS" in SCHEMA_SQL
    assert "rpt_error_messages" not in SCHEMA_SQL
    assert "rpt_error_breakdown" not in SCHEMA_SQL
    dictionary_ddl = SCHEMA_SQL[SCHEMA_SQL.index("CREATE TABLE IF NOT EXISTS mart_egisz.dim_nsi_dictionary ("):]
    assert "short_name text," in dictionary_ddl[:dictionary_ddl.index(");")]
    # редакция объявляется сидом, а не умолчанием колонки
    assert "SELECT v.oid, v.name, '%s'" % NSI_DICTIONARY_SOURCE[1] in SCHEMA_SQL
    assert "DELETE FROM mart_egisz.dim_nsi_dictionary WHERE source_version <> '%s';" % NSI_DICTIONARY_SOURCE[1] in SCHEMA_SQL
