"""Регрессионные тесты обработки ошибок против живого PostgreSQL.

Запуск требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_bi);
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
import re
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


UNRECOGNIZED = {
    ASYNC: "Не распознано: ошибка асинхронного ответа",
    NETWORK: "Не распознано: ошибка связи",
}


def classify(con, code: str | None, text: str | None, kind: str = ASYNC) -> tuple[str | None, str | None]:
    with con.cursor() as cur:
        cur.execute("SELECT error_type, nsi_dictionary_oid FROM stg_egisz.classify_error(%s, %s, %s)",
                    (kind, code, text))
        return cur.fetchone()


def recognized(con, code: str | None, text: str | None, kind: str = ASYNC) -> bool:
    return one(con, "SELECT is_recognized FROM stg_egisz.classify_error(%s, %s, %s)", kind, code, text)


def normalize(con, text: str | None, kind: str = ASYNC) -> str | None:
    return one(con, "SELECT stg_egisz.normalize_error_text(%s, %s)", kind, text)


def category(con, error_type: str | None) -> str | None:
    with con.cursor() as cur:
        cur.execute("SELECT error_category FROM mart_egisz.dim_error_types WHERE error_type = %s", (error_type,))
        row = cur.fetchone()
        return row[0] if row else None


def network_code(con, logtext: str | None) -> str | None:
    return one(con, "SELECT stg_egisz.network_error_code(%s)", logtext)


def remd_items(con, msgtext: str | None):
    with con.cursor() as cur:
        cur.execute("SELECT item_no, section, code, message FROM stg_egisz.remd_error_items(%s)", (msgtext,))
        return cur.fetchall()


def ihe_items(con, msgtext: str | None):
    with con.cursor() as cur:
        cur.execute("SELECT item_no, error_code, code_context, severity, location "
                    "FROM stg_egisz.ihe_error_items(%s)", (msgtext,))
        return cur.fetchall()


# --- Корпус: (code, message, ожидаемый тип, ожидаемая категория) ------------------------
# Сообщения — обезличенные образцы из архива ответов (значения заменены на […]).
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

    # --- Без правила: тип «Не распознано» -------------------------------------------------
    ("", "совершенно нераспознаваемый текст", UNRECOGNIZED[ASYNC], "Прочие"),
    ("VALIDATION_ERROR",
     "Неизвестная проверка со СНИЛС [11122233344] и OID [1.2.643.5.1.13]. Путь: /ClinicalDocument[1]/x",
     UNRECOGNIZED[ASYNC], "Прочие"),
    ("SOME_UNSEEN_CODE", "", UNRECOGNIZED[ASYNC], "Прочие"),
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


# --- Нераспознанные ошибки: тип «Не распознано», нормализованный текст — отдельно ------

def test_unrecognized_error_gets_closed_type_and_keeps_normalized_text(con):
    message = "Неизвестная проверка со СНИЛС [11122233344] и OID [1.2.643.5.1.13]. Путь: /ClinicalDocument[1]/x"
    assert classify(con, "VALIDATION_ERROR", message) == (UNRECOGNIZED[ASYNC], None)
    assert recognized(con, "VALIDATION_ERROR", message) is False
    assert normalize(con, message) == "Неизвестная проверка со СНИЛС <snils> и OID […]."
    assert recognized(con, "NO_SNILS", "любой текст") is True


def test_type_list_is_closed(con):
    """Набор типов задают правила: новый текст ошибки новый тип не заводит."""
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_types t
        WHERE t.rule_code IS NULL
    """) == len(UNRECOGNIZED)
    assert set(one(con, "SELECT array_agg(error_type) FROM mart_egisz.dim_error_types WHERE rule_code IS NULL")) == \
        set(UNRECOGNIZED.values())
    for kind, error_type in UNRECOGNIZED.items():
        assert one(con, "SELECT error_kind FROM mart_egisz.dim_error_types WHERE error_type = %s", error_type) == kind


# --- Нормализация текста нераспознанной ошибки ------------------------------------------

@pytest.mark.parametrize("message,expected", [
    ("[CRE-013]: XYZ-001; Пациент не определен: [СНИЛС [12345678901] не валидно контрольное число 92];"
     " Patient(moId: [1.2.643.5.1.13.13.12.2.77.12345], patientId: [B1234567-B123-4C12-8A1B-1234E12DDFFA])",
     "Пациент не определен: СНИЛС <snils> не валидно контрольное число"),
    ("[CRE-013]: XYZ-001; Пациент не определен: [СНИЛС [12345678] не соответствует формату \\d{11}];"
     " Patient(moId: [1.2.643.5.1.13.13.12.2.77.1234], patientId: [DFD1F2A3-4EEF-5B6A-A7E8-9CC01C23BC45])",
     "Пациент не определен: СНИЛС <snils> не соответствует формату (11 цифр)"),
    ("Ошибки валидации в ФРМСС: [code: DUPLICATE, description: Свидетельство с номером 123456789 и серией 12"
     " уже зарегистрировано в РЭМД. Исправьте номер и/или серию документа.].",
     "Ошибки валидации в ФРМСС (DUPLICATE): Свидетельство с номером […] и серией […]"
     " уже зарегистрировано в РЭМД. Исправьте номер и/или серию документа."),
    ("Ошибки валидации в ФРМСС: [code: MSSCERT, description: Внутренняя ошибка сервиса ФРМСС,"
     " уникальный идентификатор ошибки: a1a2f3f4-d56d-78ba-bd9f-e0b12db34a56].",
     "Ошибки валидации в ФРМСС (MSSCERT): Внутренняя ошибка сервиса ФРМСС"),
])
def test_wrapped_responses_are_unwrapped(con, message, expected):
    assert normalize(con, message) == expected


def test_attribute_name_survives_bracket_masking(con):
    """«Указанное значение [Имя пациента] …» называет, что именно не совпало: реквизит
    остаётся, значения скрываются."""
    assert normalize(con, "Указанное значение [Имя пациента] [Петрова Анна] отличается от сведений [Петрова А.]") == \
        "Указанное значение [Имя пациента] […] отличается от сведений […]"


@pytest.mark.parametrize("code,message,leak", [
    ("", "ЭП МО не верна: Validation failed for the target: serial: 1a2b subject: CN=Иванова Анна Петровна,"
         " EMAILADDRESS=ivanova@example.ru", "Иванова"),
    ("", "Дата рождения сотрудника со СНИЛС [111] (1975-07-21) не соответствует данным ФРМР [222]", "1975-07-21"),
    ("", "Недопустимые символы в имени 'Петрова (сидорова)'", "Петрова"),
    ("", "Неверный формат e-mail 'Ivanov.I.I@example.ru '", "Ivanov"),
    ("", "Адрес ivanov@example.ru недоступен", "ivanov@"),
])
def test_normalized_text_carries_no_instance_values(con, code, message, leak):
    """Нормализованный текст группирует нераспознанные ошибки и читается в контроле качества."""
    normalized = normalize(con, message)
    assert normalized and leak not in normalized


def test_normalization_strips_document_values(con):
    normalized = normalize(con, "Проверка без правила: элемент [x] со СНИЛС 11122233344"
                                " и OID 1.2.643.5.1.13.13. Путь: /ClinicalDocument[1]/recordTarget[1]")
    assert "11122233344" not in normalized
    assert "1.2.643.5.1.13.13" not in normalized
    assert "Путь:" not in normalized


def test_normalization_masks_personal_data_first(con):
    """Нормализация начинается со скрытия персональных данных: СНИЛС получает псевдоним
    <snils>, а не общее обозначение значения в скобках."""
    assert normalize(con, "Получатель [12345678901] из запроса на регистрацию сведений не найден в СЭМД") == \
        "Получатель <snils> из запроса на регистрацию сведений не найден в СЭМД"


# --- Ошибки связи: правила по общепринятым определениям ---------------------------------

NETWORK_TEXTS = [
    ("10054", "Synapse TCP/IP Socket error 10054: Connection reset by peer", "Соединение сброшено удалённой стороной"),
    ("10060", "Synapse TCP/IP Socket error 10060: Connection timed out", "Истекло время ожидания соединения"),
    ("10061", "Synapse TCP/IP Socket error 10061: Connection refused", "В соединении отказано"),
    ("10065", "Synapse TCP/IP Socket error 10065: No route to host", "Нет маршрута до узла"),
    ("10091", "Synapse TCP/IP Socket error 10091: ", "Сетевая подсистема недоступна"),
    ("10091", "Synapse TCP/IP Socket error 10091: Network subsystem is unusable", "Сетевая подсистема недоступна"),
    ("11001", "Synapse TCP/IP Socket error 11001: Host not found", "DNS: узел не найден"),
    ("11002", "Synapse TCP/IP Socket error 11002: Non authoritative - host not found",
     "DNS: узел не найден, ответ не окончательный"),
    ("408", "Error while receiving data from service: https://gost-123.example.ru:9945/api Error code: 408",
     "HTTP 408: истекло время ожидания запроса"),
    ("500", "Error while receiving data from service: http://gost-1234.infoclinica.lan:9945\nError code: 500",
     "HTTP 500: внутренняя ошибка сервера"),
    ("503", "Error while receiving data from service: https://10.0.0.1:443/ws Error code: 503",
     "HTTP 503: сервис недоступен"),
]


@pytest.mark.parametrize("code,text,expected", NETWORK_TEXTS)
def test_network_errors_are_recognized_by_rules(con, code, text, expected):
    assert network_code(con, text) == code
    assert classify(con, code, text, NETWORK)[0] == expected


def test_unknown_network_error_is_unrecognized(con):
    text = "Synapse TCP/IP Socket error 10013: Permission denied"
    assert classify(con, network_code(con, text), text, NETWORK)[0] == UNRECOGNIZED[NETWORK]
    assert normalize(con, "Error while receiving data from service: https://gost-1.example.ru:9945 Error code: 418",
                     NETWORK) == "Error while receiving data from service: <endpoint> Error code: 418"


def test_network_rules_carry_definition_with_source(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE rule_kind = 'классификация' AND error_kind = 'Ошибка связи'
    """) == len({row[2] for row in NETWORK_TEXTS})
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules
        WHERE rule_kind = 'классификация' AND error_kind = 'Ошибка связи'
          AND (btrim(COALESCE(definition, '')) = '' OR definition_source !~ 'https://')
    """) == 0


# --- Ошибки строки журнала обмена по источникам ----------------------------------------

def test_network_error_code_comes_from_gateway_text(con):
    assert network_code(con, "Synapse TCP/IP Socket error 11001: Host not found") == "11001"
    assert network_code(con, "Error while receiving data from service: https://x Error code: 503") == "503"


def test_remd_items_keep_response_section(con):
    """Предупреждения успешной регистрации отделены от ошибок разделом ответа."""
    payload = ("<ns2:registerDocumentResult><ns2:status>success</ns2:status><ns2:registryItem>"
               "<ns2:registrationWarnings><ns2:item><ns2:code>VALIDATION_ERROR</ns2:code>"
               "<ns2:message>м1</ns2:message></ns2:item><ns2:item><ns2:code>VALIDATION_ERROR</ns2:code>"
               "<ns2:message>м2</ns2:message></ns2:item></ns2:registrationWarnings></ns2:registryItem>"
               "</ns2:registerDocumentResult>")
    assert remd_items(con, payload) == [
        (1, "registrationWarnings", "VALIDATION_ERROR", "м1"),
        (2, "registrationWarnings", "VALIDATION_ERROR", "м2"),
    ]


def test_remd_items_support_namespaced_items_with_attributes(con):
    payload = ('<ns2:errors><ns2:item attr="x"><ns2:code>NO_SNILS</ns2:code>'
               "<ns2:message>СНИЛС отсутствует</ns2:message></ns2:item></ns2:errors>")
    assert remd_items(con, payload) == [(1, "errors", "NO_SNILS", "СНИЛС отсутствует")]


def test_ihe_items_keep_all_registry_error_attributes(con):
    payload = (
        "<rs:RegistryResponse><rs:RegistryErrorList>"
        '<rs:RegistryError severity="urn:oasis:names:tc:ebxml-regrep:ErrorSeverityType:Error"'
        ' errorCode="XDSDictionaryValidationError" codeContext="Значение &quot;X&quot; не найдено" location="doc/1"/>'
        '<rs:RegistryError codeContext="Internal error in repository" errorCode="XDSRepositoryError"/>'
        "</rs:RegistryErrorList></rs:RegistryResponse>"
    )
    assert ihe_items(con, payload) == [
        (1, "XDSDictionaryValidationError", 'Значение "X" не найдено',
         "urn:oasis:names:tc:ebxml-regrep:ErrorSeverityType:Error", "doc/1"),
        (2, "XDSRepositoryError", "Internal error in repository", None, None),
    ]


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
        FROM mart_egisz.dim_nsi_error_codes c
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
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_error_codes") == 127
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_error_codes
        WHERE source_oid <> '1.2.643.5.1.13.13.99.2.305' OR source_version <> '3.18'
    """) == 0
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_code_aliases WHERE alias = nsi_error_code") == 0


def test_types_carry_nsi_code_when_rule_is_code_gated(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_types t
        JOIN mart_egisz.dim_error_rules r ON r.rule_code = t.rule_code
        WHERE r.nsi_error_code IS NOT NULL AND t.nsi_error_code IS DISTINCT FROM r.nsi_error_code
    """) == 0


# --- Инварианты справочников ------------------------------------------------------------

def test_categories_are_cause_groups(con):
    assert set(one(con, """
        SELECT array_agg(error_category) FROM mart_egisz.dim_error_categories
        WHERE error_kind = 'Ошибка асинхронного ответа'
    """)) == set(CATEGORIES)
    # У вида «Ошибка связи» категорий нет.
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_categories
        WHERE error_kind = 'Ошибка связи' AND error_category IS NOT NULL
    """) == 0


def test_every_rule_interpretation_is_a_type_with_its_category(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'классификация' AND NOT EXISTS (
            SELECT 1 FROM mart_egisz.dim_error_types t
            WHERE t.error_type = r.interpretation
              AND t.error_category IS NOT DISTINCT FROM r.error_category)
    """) == 0


def test_dictionary_has_no_orphan_rule_types(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_types t
        WHERE t.rule_code IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM mart_egisz.dim_error_rules r
                          WHERE r.rule_kind = 'классификация' AND r.interpretation = t.error_type)
    """) == 0


def test_rule_type_names_carry_no_document_values(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_types
        WHERE rule_code IS NOT NULL AND (error_type LIKE '%%[%%' OR error_type LIKE '%%]%%')
    """) == 0


def test_every_type_has_responsibility_and_retryable(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_types
        WHERE responsibility IS NULL OR is_retryable IS NULL OR responsibility NOT IN %s
    """, RESPONSIBILITY_DOMAIN) == 0


def test_all_patterns_compile(con):
    # ~* форсирует компиляцию каждого регекспа; невалидный ARE уронит запрос
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_error_rules r WHERE ('' ~* r.match_pattern) IS NULL") == 0
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_error_rules r
        WHERE r.rule_kind = 'нормализация'
          AND regexp_replace('x', r.match_pattern, r.replacement, r.match_flags) IS NULL
    """) == 0


def test_normalization_steps_have_distinct_order(con):
    assert one(con, """
        SELECT count(*) FROM (
            SELECT apply_order FROM mart_egisz.dim_error_rules
            WHERE rule_kind = 'нормализация' GROUP BY apply_order HAVING count(*) > 1) d
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


# --- Маскирование текста для выдачи -----------------------------------------------------

def mask(con, text: str) -> str | None:
    return one(con, "SELECT mart_egisz.mask_personal_data(%s)", text)


@pytest.mark.parametrize("message,expected", [
    ("Указанное значение [Имя пациента] [Иванова Анна Петровна] не соответствует данным ГИП [Петрова Анна Петровна]."
     " Пациент найден по локальному идентификатору",
     "Указанное значение [Имя пациента] […] не соответствует данным ГИП […]. Пациент найден по локальному идентификатору"),
    ("ФИО сотрудника со СНИЛС [12345678901] не соответствуют данным ФРМР [Сидоров Иван Иванович].",
     "ФИО сотрудника со СНИЛС <snils> не соответствуют данным ФРМР […]."),
    ("Дата рождения сотрудника со СНИЛС [12345678901] ([01.02.1980]) не соответствует данным ФРМР [02.01.1980]",
     "Дата рождения сотрудника со СНИЛС <snils> ([…]) не соответствует данным ФРМР […]"),
    ("Фамилия пациента в ЭМД [Иванова] отличается от фамилии пациента в запросе на регистрацию сведений [Петрова]",
     "Фамилия пациента в ЭМД […] отличается от фамилии пациента в запросе на регистрацию сведений […]"),
    ("Несоответствие данных подписанта в запросе и в сертификате. GIVEN_NAME [АннаПетровна] в метаданных и [Анна] в сертификате",
     "Несоответствие данных подписанта в запросе и в сертификате. GIVEN_NAME […] в метаданных и […] в сертификате"),
    ("В ФРМР не найдена карточка МР c данными из сертификата подписи МО: Иванов Иван Иванович (СНИЛС: 12345678901)",
     "В ФРМР не найдена карточка МР c данными из сертификата подписи МО: […] (СНИЛС: <snils>)"),
    ("Получатель [12345678901] из запроса на регистрацию сведений не найден в СЭМД",
     "Получатель <snils> из запроса на регистрацию сведений не найден в СЭМД"),
    ("Получатель [12345678901] из запроса на регистрацию сведений не найден в СЭМД · "
     "Указанное значение [СНИЛС] [12345678901] не соответствует данным ГИП [10987654321]. Пациент найден по локальному идентификатору",
     "Получатель <snils> из запроса на регистрацию сведений не найден в СЭМД · "
     "Указанное значение [СНИЛС] <snils> не соответствует данным ГИП <snils>. Пациент найден по локальному идентификатору"),
    ("Удостоверяющий центр сертификата недоступен: 12345678901",
     "Удостоверяющий центр сертификата недоступен: <snils>"),
    ("Удостоверяющий центр сертификата недоступен: Validation failed for the target: serial: 1a2b subject: "
     "EMAILADDRESS=user@example.ru, CN=Иванов Иван, SURNAME=Иванов issuer: CN=УЦ",
     "Удостоверяющий центр сертификата недоступен: Validation failed for the target: serial: 1a2b subject: […] issuer: CN=УЦ"),
])
def test_masking_hides_personal_data(con, message, expected):
    assert mask(con, message) == expected


SNILS_CANDIDATE = re.compile(r"(?:^|[^0-9A-Za-z.:#_-])(\d{3}-\d{3}-\d{3}[ -]\d{2}|\d{11})(?![0-9A-Za-z.])")


def is_snils(value: str) -> bool:
    """СНИЛС по формату, запрету трёх одинаковых цифр подряд и контрольному числу; номера до
    001-001-998 контрольным числом не проверяются."""
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) != 11 or re.search(r"(\d)\1\1", digits[:9]):
        return False
    if int(digits[:9]) <= 1001998:
        return True
    total = sum(int(d) * (9 - i) for i, d in enumerate(digits[:9]))
    check = total if total < 100 else 0 if total in (100, 101) else (total % 101) % 100
    return check == int(digits[9:])


def test_masking_leaves_no_snils_in_rule_described_texts(con):
    """Тексты ответов, где СНИЛС стоит при реквизите из правил, после маскирования СНИЛС не
    содержат: проверка контрольным числом, а не по числу цифр."""
    with con.cursor() as cur:
        cur.execute(r"""
            SELECT DISTINCT t FROM (
                SELECT message AS t FROM stg_egisz.remd_errors
                UNION ALL SELECT code_context FROM stg_egisz.ihe_errors) s
            WHERE t ~ '\d{3}-?\d{3}-?\d{3}[ -]?\d{2}'
        """)
        texts = [row[0] for row in cur.fetchall()]
    with_snils = [t for t in texts if any(is_snils(v) for v in SNILS_CANDIDATE.findall(t))]
    if not with_snils:
        pytest.skip("в разобранных ответах нет СНИЛС; проверять нечего")
    leaked = [m for m in (mask(con, t) for t in with_snils) if any(is_snils(v) for v in SNILS_CANDIDATE.findall(m))]
    assert leaked == []


def test_masking_keeps_text_length_and_document_values(con):
    """Маскирование для выдачи не обрезает текст и не заменяет реквизиты документа:
    идентификатор документа, OID и путь в документе нужны поддержке."""
    tail = " Путь: /ClinicalDocument[1]/recordTarget[1]/patientRole[1]/addr[1]/@code" * 5
    message = ("Уникальный идентификатор документа в ЭМД [7F622F826A194F74AAC3F37BD5DEFD1D] отличается от"
               " уникального идентификатора документа в запросе на регистрацию сведений"
               " [D6C1851F-1C6F-43B4-8609-5CE7175F7127]" + tail)
    assert len(message) > 240
    assert mask(con, message) == message


def test_masking_keeps_clinic_service_address(con):
    message = "Error while receiving data from service: http://gost-1234.infoclinica.lan:9945\nError code: 500"
    assert mask(con, message) == message


def test_masking_rules_are_a_separate_dictionary(con):
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_masking_rules") > 0
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_masking_rules m
        JOIN mart_egisz.dim_error_rules r ON r.rule_code = m.rule_code
    """) == 0
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_masking_rules r
        WHERE regexp_replace('x', r.match_pattern, r.replacement, r.match_flags) IS NULL
    """) == 0


# --- Текущие ошибки документа ------------------------------------------------------------

def test_current_errors_follow_last_async_response(con):
    """Ошибки текущего состояния — элементы последнего асинхронного ответа и ошибки связи
    после него; сбой доставки до ответа к текущему состоянию не относится."""
    if one(con, "SELECT to_regclass('mart_egisz.document_errors')") is None:
        pytest.skip("витрина текущих ошибок не построена; проверять нечего")
    doc = str(uuid.uuid4())
    remd = [{"item_no": 1, "section": "errors", "code": "NO_SNILS", "message": "отказ",
             "error_type": "отказ", "nsi_dictionary_oid": None}]
    rows = [
        (-9_000_000_001, "3 hours", None, "до ответа", None),
        (-9_000_000_002, "2 hours", "error", None, remd),
        (-9_000_000_003, "1 hour", None, "после ответа", None),
    ]
    with con.cursor() as cur:
        cur.execute("SAVEPOINT current_errors")
        try:
            for logid, age, status, network_text, remd_errors in rows:
                cur.execute(
                    "INSERT INTO stg_egisz.exchange_messages "
                    "(logid, log_date, dwh_id, status, network_error_code, network_error_text, remd_errors) "
                    "VALUES (%s, now() - %s::interval, %s, %s, %s, %s, %s::jsonb)",
                    (logid, age, doc, status, network_text, network_text,
                     json.dumps(remd_errors) if remd_errors else None))
            cur.execute("SELECT pg_get_viewdef('mart_egisz.document_errors'::regclass, true)")
            view_sql = cur.fetchone()[0].rstrip().rstrip(";")
            cur.execute("SELECT error_source, error_code FROM (" + view_sql + ") c WHERE dwh_id = %s ORDER BY error_no",
                        (doc,))
            assert cur.fetchall() == [("РЭМД", "NO_SNILS"), ("связь", "после ответа")]
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT current_errors")
            cur.execute("RELEASE SAVEPOINT current_errors")


# --- Реестр наименований справочников ФНСИ ---------------------------------------------

def test_nsi_dictionary_matches_published_805_revision(con):
    assert one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_dictionaries") == NSI_DICTIONARY_SIZE
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionaries
        WHERE source_oid <> %s OR source_version <> %s
           OR name IS NULL OR btrim(name) = ''
    """, *NSI_DICTIONARY_SOURCE) == 0


def test_nsi_dictionary_agrees_with_805_dictionary(con):
    if one(con, "SELECT count(*) FROM mart_egisz.dim_nsi_semd_guide_dictionaries") == 0:
        pytest.skip("справочник НСИ 805 не загружен; сверять нечего")
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionaries d
        JOIN (SELECT DISTINCT dict_oid, dict_name FROM mart_egisz.dim_nsi_semd_guide_dictionaries) g
          ON g.dict_oid = d.oid
        WHERE g.dict_name <> d.name
    """) == 0


def test_nsi_dictionary_short_name_only_shortens(con):
    assert one(con, """
        SELECT count(*) FROM mart_egisz.dim_nsi_dictionaries
        WHERE short_name IS NOT NULL
          AND (btrim(short_name) = '' OR length(short_name) >= length(name))
    """) == 0
    assert one(con, "SELECT short_name FROM mart_egisz.dim_nsi_dictionaries WHERE oid = '1.2.643.5.1.13.13.11.1005'") == "МКБ-10"


def test_document_error_names_every_registered_dictionary(con):
    """Наименование справочника пусто только у OID вне 805."""
    if one(con, "SELECT to_regclass('serving_egisz.document_errors')") is None:
        pytest.skip("витрина ошибок документа не построена; проверять нечего")
    assert one(con, """
        SELECT count(*) FROM serving_egisz.document_errors e
        WHERE e.nsi_dictionary_oid IS NOT NULL
          AND e.nsi_dictionary_name IS NULL
          AND EXISTS (SELECT 1 FROM mart_egisz.dim_nsi_dictionaries d WHERE d.oid = e.nsi_dictionary_oid)
    """) == 0


def test_nsi_dictionary_schema_contract() -> None:
    """Комментарий к таблице — единственное место, где записано назначение реестра и его
    потребитель."""
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_nsi_dictionaries (" in SCHEMA_SQL
    assert "COMMENT ON TABLE mart_egisz.dim_nsi_dictionaries IS" in SCHEMA_SQL
    assert "COMMENT ON COLUMN mart_egisz.dim_nsi_dictionaries.short_name IS" in SCHEMA_SQL
    assert "rpt_error_messages" not in SCHEMA_SQL
    assert "rpt_error_breakdown" not in SCHEMA_SQL
    dictionary_ddl = SCHEMA_SQL[SCHEMA_SQL.index("CREATE TABLE IF NOT EXISTS mart_egisz.dim_nsi_dictionaries ("):]
    assert "short_name text," in dictionary_ddl[:dictionary_ddl.index(");")]
    # редакция объявляется сидом, а не умолчанием колонки
    assert "SELECT v.oid, v.name, '%s'" % NSI_DICTIONARY_SOURCE[1] in SCHEMA_SQL
    assert "DELETE FROM mart_egisz.dim_nsi_dictionaries WHERE source_version <> '%s';" % NSI_DICTIONARY_SOURCE[1] in SCHEMA_SQL
