"""Регрессионные тесты классификатора ошибок против живого PostgreSQL.

Запуск требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_egisz);
без переменной модуль целиком скипается — как и остальной suite, не зависящий от внешних
сервисов. Фикстура идемпотентно применяет части 20/30/40 из working tree, поэтому тесты
проверяют именно текущий код правил, а не состояние базы на момент последнего dwh_init.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

psycopg2 = pytest.importorskip("psycopg2")

from egisz_elt.common import connect_pg  # noqa: E402

DSN = os.environ.get("EGISZ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")

PARTS = Path(__file__).resolve().parents[1] / "db" / "parts"

RESPONSIBILITY_DOMAIN = ("клиника", "МИС", "интегратор", "РЭМД", "смешанная")


@pytest.fixture(scope="module")
def con():
    con = connect_pg(DSN)
    with con.cursor() as cur:
        for part in ("20_functions_parsing.sql", "30_error_rules.sql", "40_functions_errors.sql"):
            cur.execute((PARTS / part).read_text(encoding="utf-8"))
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
    # ярус 1: код + специфичный текст
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: У1-19. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type должен иметь не пустое значение атрибута @code.",
     ["Не указан адрес пациента"], ["Данные пациента"]),
    ("VALIDATION_ERROR", "Организация [ООО Клиника] не привязана к РМИС [42]",
     ["Организация не привязана к РМИС"], ["Ошибки структуры и валидации"]),
    ("VALIDATION_ERROR", "СНИЛС пациента в ЭМД [111] отличается от СНИЛС пациента в запросе на регистрацию сведений [222]",
     ["СНИЛС пациента не заполнен или некорректен"], ["Данные пациента"]),
    ("RUNTIME_ERROR", "Не удается провести проверку ФРМР",
     ["РЭМД не смог обработать запрос"], ["Технические ошибки РЭМД"]),
    ("VALUE_MISMATCH_METADATA_AND_CERTIFICATE",
     "В ФРМР не найдена актуальная на дату создания документа карточка МР c данными из сертификата подписи МО",
     ["Подписант из сертификата не найден в ФРМР"], ["Данные медработника"]),
    # ярус 2: только код (текст любой)
    ("PATIENT_MPI_MISMATCH", "Указанное значение [Фамилия] [Имя] не соответствует данным ГИП [—]. Пациент найден по локальному идентификатору",
     ["Данные пациента не соответствуют ГИП"], ["Данные пациента"]),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [111] не соответствует занимаемой им должности в организации [222] по данным ФРМР.",
     ["Должность врача не соответствует данным ФРМР"], ["Данные медработника"]),
    ("INVALID_DICTIONARY_VERSION", "Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.31] недопустима для документа вида [227].",
     ["Неактуальная версия справочника НСИ"], ["Ошибки справочника НСИ"]),
    ("XML_VALIDATION_ERROR", "Ошибка валидации СЭМД: cvc-complex-type.2.4.a: Invalid content was found starting with element id",
     ["Ошибка XSD-валидации XML"], ["Ошибки структуры и валидации"]),
    ("NO_SNILS", "СНИЛС пациента в составе сведений о пациенте обязателен для данного вида документов",
     ["СНИЛС пациента обязателен для данного вида документов"], ["Данные пациента"]),
    ("RESTRICT_NEW_VERSION", "Для ЭМД 230 запрещена регистрация новых версий",
     ["Для данного вида ЭМД запрещена регистрация новых версий"], ["Ошибки регистрации в РЭМД"]),
    ("CA_INACCESSIBILITY", "Удостоверяющий центр сертификата недоступен: Время ожидания истекло.",
     ["Недоступен сервис проверки подписи (УЦ) на стороне РЭМД"], ["Технические ошибки РЭМД"]),
    ("RMIS_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: Error in getDocumentFile by SOAP",
     ["Не удалось получить файл ЭМД из предоставляющей ИС"], ["Ошибки получения файла ЭМД"]),
    # INVALID_CONTENT — файл получен, но не является валидным XML: код побеждает
    # ярус-4 фолбэк «файлового хранилища» (регресс подмены типа).
    ("INVALID_CONTENT", "Ошибка получения файла ЭМД из файлового хранилища: Переданный файл не является валидным XML файлом",
     ["Формат файла не соответствует требованиям вида документа"], ["Ошибки структуры и валидации"]),
    ("DOC_DATE_MISMATCH_CERT_NOT_AFTER", "Сертификат МО недействителен на дату создания документа",
     ["Сертификат подписи недействителен на дату создания документа"], ["Ошибки ЭП и сертификатов"]),
    # INVALID_DOCTOR_NAME утекал в ярус-4 person_snils из-за «СНИЛС» в тексте.
    ("INVALID_DOCTOR_NAME", "Имя [Иван] медицинского работника в запросе на регистрацию отличается от имени [Иоан] в СЭМД. СНИЛС [111]",
     ["Имя врача не соответствует данным СЭМД"], ["Данные медработника"]),
    ("VALIDATION_ERROR", "Недопустимые символы в имени 'Фамилия (девичья)'",
     ["ФИО пациента не заполнено или некорректно"], ["Данные пациента"]),
    ("RUNTIME_ERROR", "Не удается произвести проверку в ГИП",
     ["РЭМД не смог обработать запрос"], ["Технические ошибки РЭМД"]),
    ("WRONG_CREATION_DATE", "Дата создания документа не может быть позднее даты регистрации",
     ["Дата создания документа позже даты регистрации"], ["Ошибки регистрации в РЭМД"]),
    ("RATE_LIMIT", "Доступ к сервису временно запрещён - для системы, соответствующей идентификатору [x], превышен лимит запросов к сервису",
     ["Превышен лимит запросов к РЭМД"], ["Технические ошибки РЭМД"]),
    # ярус 3: специфичный текст без кода
    ("", "Уникальный идентификатор документа в ЭМД [abc] отличается от уникального идентификатора документа в запросе на регистрацию сведений [def]",
     ["Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию"], ["Ошибки регистрации в РЭМД"]),
    ("", "Дата рождения пациента не заполнена",
     ["Дата рождения пациента не заполнена или некорректна"], ["Данные пациента"]),
    # ярус 4: широкие фолбэки
    ("", "СНИЛС [123] запрещен к передаче",
     ["СНИЛС не найден или не соответствует данным пациента/медработника"], ["Данные пациента"]),
    ("", "Организация не прошла проверку",
     ["Ошибки организации"], ["Ошибки организации / ИС"]),
    # ИЭМК (XDS): код из атрибута RegistryError/errorCode
    ("XDSDictionaryValidationError", "Element representedCustodianOrganization. MO code [1.2.643] is not actual.",
     ["ИЭМК: данные не соответствуют справочнику НСИ"], ["Ошибки ИЭМК"]),
    ("XDSRepositoryError", "Internal error in repository",
     ["ИЭМК: внутренняя ошибка репозитория"], ["Ошибки ИЭМК"]),
    # CRE-126: RPLC-текст уточняет генерик кода (замена версии, документ не найден).
    ("XDSDocumentUniqueIdError", "Association [RPLC] targetId with unique ID [E13B85998D5A] not found in repository",
     ["ИЭМК: заменяемый документ не найден (замена версии)"], ["Ошибки ИЭМК"]),
    ("XDSDocumentUniqueIdError", "malformed unique id",
     ["ИЭМК: некорректный идентификатор документа"], ["Ошибки ИЭМК"]),
    ("XDSRegistryBusy", "", ["ИЭМК: сервис временно недоступен"], ["Ошибки ИЭМК"]),
    # Внутренний код платформы в codeContext, RegistryError без errorCode.
    ("", "[CRE-122]: PAT-001; Пациент не определен: [СНИЛС [111] не валидно контрольное число]",
     ["ИЭМК: пациент не определён"], ["Ошибки ИЭМК"]),
    # фолбэки движка
    ("NOT_UNIQUE_PROVIDED_ID", "", ["Документ уже зарегистрирован в РЭМД"], ["Ошибки регистрации в РЭМД"]),
    ("SOME_UNSEEN_CODE", "", ["Код: SOME_UNSEEN_CODE"], ["Прочие"]),
    ("RUNTIME_ERROR", "", ["Техническая ошибка на стороне РЭМД"], ["Технические ошибки РЭМД"]),
    ("INTERNAL_ERROR", "", ["Техническая ошибка на стороне РЭМД"], ["Технические ошибки РЭМД"]),
    ("TIMEOUT", "", ["Таймаут асинхронной обработки на стороне РЭМД"], ["Технические ошибки РЭМД"]),
    ("VALIDATION_ERROR", "Ошибка валидации Schematron: экзотическое требование без известных элементов",
     ["Ошибка Schematron-валидации"], ["Ошибки структуры и валидации"]),
    ("", "совершенно нераспознаваемый текст", ["Неизвестная ошибка"], ["Прочие"]),
]


@pytest.mark.parametrize("code,message,expected_atoms,expected_cats", CORPUS)
def test_error_item_atoms_corpus(con, code, message, expected_atoms, expected_cats):
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)", code, message)
    assert atoms == expected_atoms
    # Категория — JOIN к dim_error_type_group; атомы вне словаря («Код: X») — «Прочие».
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
    # точное code-правило против широкого текстового
    ("ASYNC_RESPONSE_TIMEOUT", "Превышен таймаут ожидания асинхронного ответа",
     "Таймаут асинхронной обработки на стороне РЭМД"),
    ("INVALID_SNILS", "СНИЛС указан неверно: контрольное число не сходится",
     "Неверный формат или контрольная сумма СНИЛС"),
    ("PERSON_POST_IN_FRMR_MISMATCH",
     "Указанная должность сотрудника со СНИЛС [1] не соответствует данным ФРМР (автор документа)",
     "Должность врача не соответствует данным ФРМР"),
    ("ORGANIZATION_NOT_FOUND", "Организация [ООО] не найдена в реестре организаций",
     "Организация не найдена в реестре РЭМД"),
    ("VALIDATION_ERROR",
     "Ошибка валидации Schematron: элемент assignedAuthor должен содержать СНИЛС автора",
     "СНИЛС автора (врача) не заполнен или некорректен"),
]


@pytest.mark.parametrize("code,message,expected_single", COLLISIONS)
def test_tiered_matching_yields_single_type(con, code, message, expected_single):
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)", code, message)
    assert atoms == [expected_single]


def test_schematron_code_gated_rules_do_not_fire_on_other_codes(con):
    """Однословные schematron-паттерны (custodian, legalAuthenticator) жёстко привязаны
    к match_code = VALIDATION_ERROR и не срабатывают на произвольном тексте с другим кодом."""
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "SOME_OTHER_CODE", "поле custodian не заполнено")
    assert "Данные хранителя документа не заполнены" not in atoms
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "SOME_OTHER_CODE", "ошибка в legalAuthenticator")
    assert "Данные заверителя документа не заполнены или некорректны" not in atoms


def test_error_classify_dedups_and_joins(con):
    result = one(con, """SELECT public.error_classify(
        '[{"code":"INVALID_SNILS","message":"СНИЛС неверен"},
          {"code":"PATIENT_MPI_MISMATCH","message":"не соответствует данным ГИП"}]'::jsonb)""")
    assert result == (
        "Неверный формат или контрольная сумма СНИЛС · Данные пациента не соответствуют ГИП"
    )


def test_error_classify_empty_message_known_code(con):
    result = one(con, """SELECT public.error_classify(
        '[{"code":"NOT_UNIQUE_PROVIDED_ID","message":""}]'::jsonb)""")
    assert result == "Документ уже зарегистрирован в РЭМД"


# --- Парсинг payload -------------------------------------------------------------------

def test_xml_error_items_supports_namespaced_items_with_attributes(con):
    payload = (
        '<ns2:errors><ns2:item attr="x"><ns2:code>INVALID_SNILS</ns2:code>'
        "<ns2:message>СНИЛС неверен</ns2:message></ns2:item></ns2:errors>"
    )
    items = one(con, "SELECT public.xml_error_items(%s)", payload)
    assert items == [{"code": "INVALID_SNILS", "message": "СНИЛС неверен"}]


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
        "<x><item><code>INVALID_SNILS</code><message>m</message></item>"
        '<rs:RegistryError errorCode="XDSRepositoryError" codeContext="c"/></x>'
    )
    items = one(con, "SELECT public.build_errors_json('error', NULL, NULL, %s)", both)
    assert items == [{"code": "INVALID_SNILS", "message": "m"}]


def test_parse_exchangelog_row_extracts_faultcode_last(con):
    row = one(con, "SELECT (public.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<soap:Fault><faultcode>soap:Server</faultcode><faultstring>x</faultstring></soap:Fault>")
    assert row == "SERVER"
    # <code>/<errorCode> имеют приоритет над faultcode
    row = one(con, "SELECT (public.parse_exchangelog_row(%s, NULL, NULL)).error_code",
              "<r><code>VALIDATION_ERROR</code><faultcode>soap:Server</faultcode></r>")
    assert row == "VALIDATION_ERROR"


# --- Инварианты словарей ---------------------------------------------------------------

def test_every_active_interpretation_is_canonical(con):
    assert one(con, """
        SELECT count(*) FROM dim_error_rules r
        WHERE r.is_active AND NOT EXISTS (
            SELECT 1 FROM dim_error_type_group g WHERE g.error_type = r.interpretation)
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


def test_runtime_error_keeps_tier4_refinements(con):
    """Документирует, почему для RUNTIME_ERROR нет blanket-правила яруса 2: живые тексты
    кода уточняются текстовыми ярусами (здесь — сбой получения файла ЭМД, ярус 4)."""
    atoms = one(con, "SELECT public.error_item_atoms(%s, %s)",
                "RUNTIME_ERROR", "Ошибка получения файла ЭМД из файлового хранилища: internal_error")
    assert "Не удалось получить файл ЭМД из предоставляющей ИС" in atoms


def test_no_fallback_suffix_variants_in_dictionary(con):
    # Фолбэк-ветки error_item_atoms выровнены на канонические типы: вариант-строки
    # с суффиксом расщепляли один логический тип на две строки витрины.
    assert one(con, """
        SELECT count(*) FROM dim_error_type_group
        WHERE error_type LIKE '%повторите отправку позже%'
    """) == 0


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
