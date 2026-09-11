from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from conftest import load_script_module, sql_section


SCHEMA_SQL = Path("db/01_schema.sql").read_text(encoding="utf-8")
FUNCTIONS_SQL = Path("db/02_functions.sql").read_text(encoding="utf-8")
VIEWS_SQL = Path("db/04_views.sql").read_text(encoding="utf-8")
LOADER_PATH = Path("scripts/load_nsi_semd_guides.py")

GUIDE_RECORD = {
    "OID": "1.2.643.5.1.13.13.15.1.2",
    "OID_SYNONYM": "1.2.643.5.1.13.13.14.37.9.2",
    "SEMD_ID": "1",
    "FULL_NAME": "Руководство по реализации CDA (Release 2) уровень 3 Льготный рецепт Редакция 2",
    "RELEASE": "2",
    "GIT_PUB_DATE": "28.03.2022",
    "GIT_LINK": "https://git.minzdrav.gov.ru/semd/1.2.643.5.1.13.13.15.1/-/tree/1.2.643.5.1.13.13.15.1.2",
    "FORMAT": "CDA R2",
}

# Протокол информационного взаимодействия ВИМИС: в реестре руководств он есть, но вида
# медицинской документации за ним не стоит, поэтому SEMD_ID и RELEASE пусты.
PROTOCOL_RECORD = {
    "OID": "1.2.643.5.1.13.13.16.1.2.1",
    "FULL_NAME": 'Протокол информационного взаимодействия ВИМИС "Профилактика"',
}

DICTIONARY_RECORD = {
    "ID": "SEMD17R5/99.2.983",
    "IG_OID": "1.2.643.5.1.13.13.15.17.5",
    "IG_NAME": "Протокол инструментального исследования",
    "IG_VERSION": "Редакция 5",
    "IG_OID_SYNONYM": "1.2.643.5.1.13.13.14.99.9.5",
    "DICT_OID": "1.2.643.5.1.13.13.99.2.983",
    "DICT_NAME": "Режимы работы аппарата УЗИ",
    "DICT_VERSION": "*",
    "DICT_IDS_SYSTEMNAME": "ID",
    "COLLECTION": [{"DICT_IDS": "1"}, {"DICT_IDS": "2"}],
}


@pytest.fixture(scope="module")
def loader():
    return load_script_module("load_nsi_semd_guides")


def test_guide_row_maps_source_fields(loader) -> None:
    row = loader.guide_row(GUIDE_RECORD, "7.74")

    assert row[0] == "1.2.643.5.1.13.13.15.1.2"
    assert row[1] == 1
    assert row[3] == 2
    assert row[4] == "CDA R2"
    assert row[5] == date(2022, 3, 28)
    assert row[7] == loader.GUIDE_SOURCE_OID
    assert row[8] == "7.74"


def test_guide_row_keeps_missing_semd_id(loader) -> None:
    row = loader.guide_row(PROTOCOL_RECORD, "7.74")

    assert row[0] == "1.2.643.5.1.13.13.16.1.2.1"
    assert row[1] is None
    assert row[3] is None
    assert row[5] is None


def test_alias_rows_split_multi_value_synonym(loader) -> None:
    record = dict(GUIDE_RECORD, OID_SYNONYM="1.2.643.5.1.13.13.14.5.9.3; 1.2.643.5.1.13.2.7.5.1.5.9.3")

    assert loader.alias_rows(record) == [
        ("1.2.643.5.1.13.13.14.5.9.3", GUIDE_RECORD["OID"]),
        ("1.2.643.5.1.13.2.7.5.1.5.9.3", GUIDE_RECORD["OID"]),
    ]
    assert loader.alias_rows(dict(GUIDE_RECORD, OID_SYNONYM=None)) == []
    assert loader.alias_rows(dict(GUIDE_RECORD, OID_SYNONYM="  ")) == []


def test_dictionary_row_omits_guide_attributes(loader) -> None:
    """Наименование, редакция и синонимы OID руководства выводятся из guide_oid и живут
    в реестре руководств: повтор в связующей таблице разошёлся бы с ним при первом же выпуске."""
    row = loader.dictionary_row(DICTIONARY_RECORD, "6.19")

    assert row[0] == "1.2.643.5.1.13.13.15.17.5"
    assert row[1] == "1.2.643.5.1.13.13.99.2.983"
    assert row[2] == "SEMD17R5/99.2.983"
    assert DICTIONARY_RECORD["IG_NAME"] not in row
    assert DICTIONARY_RECORD["IG_VERSION"] not in row
    assert DICTIONARY_RECORD["IG_OID_SYNONYM"] not in row


def test_dictionary_version_star_is_kept_verbatim(loader) -> None:
    """«*» — это «любая версия». Обнуление превратило бы требование в «версия неизвестна»."""
    assert loader.dictionary_row(DICTIONARY_RECORD, "6.19")[4] == "*"


def test_source_version_is_taken_from_json_member_name(loader) -> None:
    assert loader.source_version_from_name("1.2.643.5.1.13.13.99.2.638_7.74.json") == "7.74"
    assert loader.source_version_from_name("1.2.643.5.1.13.13.99.2.805_6.19.json") == "6.19"

    # Имя опубликованного архива оканчивается на _json: разбор контейнера вместо вложенного
    # файла подставил бы в source_version слово «json».
    with pytest.raises(ValueError):
        loader.source_version_from_name("1.2.643.5.1.13.13.99.2.638_7.74_json.zip")


def test_source_oid_mismatch_is_rejected(loader) -> None:
    loader.assert_source_oid("1.2.643.5.1.13.13.99.2.638_7.74.json", loader.GUIDE_SOURCE_OID)

    with pytest.raises(ValueError):
        loader.assert_source_oid(
            "1.2.643.5.1.13.13.99.2.805_6.19.json", loader.GUIDE_SOURCE_OID
        )


def test_validate_references_rejects_orphan_dictionaries(loader) -> None:
    loader.validate_references([GUIDE_RECORD], [dict(DICTIONARY_RECORD, IG_OID=GUIDE_RECORD["OID"])])

    with pytest.raises(ValueError):
        loader.validate_references([GUIDE_RECORD], [DICTIONARY_RECORD])


def test_validate_references_rejects_duplicate_guide_oid(loader) -> None:
    with pytest.raises(ValueError):
        loader.validate_references([GUIDE_RECORD, dict(GUIDE_RECORD)], [])


def test_validate_references_rejects_alias_colliding_with_active_oid(loader) -> None:
    other = dict(GUIDE_RECORD, OID="1.2.643.5.1.13.13.15.1.3", OID_SYNONYM=GUIDE_RECORD["OID"])

    with pytest.raises(ValueError):
        loader.validate_references([GUIDE_RECORD, other], [])


def test_semd_guide_schema_contract() -> None:
    assert "CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide (" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide_alias (" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS dim_nsi_semd_guide_dictionary (" in SCHEMA_SQL
    assert "PRIMARY KEY (guide_oid, dict_oid)" in SCHEMA_SQL
    assert SCHEMA_SQL.count("REFERENCES dim_nsi_semd_guide (oid) ON DELETE CASCADE") == 2
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_nsi_semd_guide_dictionary_source_id" in SCHEMA_SQL
    assert "CREATE INDEX IF NOT EXISTS idx_dim_nsi_semd_guide_dictionary_dict_oid" in SCHEMA_SQL
    assert "CREATE INDEX IF NOT EXISTS idx_dim_nsi_semd_guide_alias_guide" in SCHEMA_SQL

    for table in ("dim_nsi_semd_guide", "dim_nsi_semd_guide_alias", "dim_nsi_semd_guide_dictionary"):
        assert f"COMMENT ON TABLE {table} IS" in SCHEMA_SQL


def test_guide_oid_rename_is_idempotent() -> None:
    """Колонка названа по содержанию, а переименование должно выдерживать повторный накат."""
    assert "RENAME COLUMN git_link TO ig_oid" in SCHEMA_SQL
    assert "AND column_name = 'ig_oid'" in SCHEMA_SQL
    assert (
        "INSERT INTO dim_semd_types (code, type_code, name, level, format_code, "
        "start_date, end_date, implementation_guide, ig_oid)" in SCHEMA_SQL
    )


def test_field_swap_and_branch_number_are_documented() -> None:
    """Эти два комментария — вся память о том, как устроена связь: без них колонку ig_oid
    «чинят» обратно в git_link, а semd_id принимают за код вида документации."""
    assert "COMMENT ON COLUMN dim_semd_types.ig_oid IS" in SCHEMA_SQL
    assert "COMMENT ON COLUMN dim_semd_types.implementation_guide IS" in SCHEMA_SQL
    assert "COMMENT ON COLUMN dim_nsi_semd_guide.semd_id IS" in SCHEMA_SQL

    swap_comment = SCHEMA_SQL[SCHEMA_SQL.index("COMMENT ON COLUMN dim_semd_types.ig_oid IS"):]
    assert "GIT_LINK" in swap_comment[:400]


def test_semd_dictionaries_view_contract() -> None:
    assert "CREATE OR REPLACE VIEW public.dim_semd_guide_oid AS" in FUNCTIONS_SQL

    assert "DROP VIEW IF EXISTS public.rpt_semd_dictionaries CASCADE;" in sql_section(
        VIEWS_SQL, "drop_dependents"
    )
    assert "DROP VIEW IF EXISTS public.rpt_semd_guides CASCADE;" in sql_section(
        VIEWS_SQL, "drop_dependents"
    )

    section = sql_section(VIEWS_SQL, "semd_guides")
    assert "CREATE OR REPLACE VIEW public.rpt_semd_guides AS" in section
    assert "CREATE OR REPLACE VIEW public.rpt_semd_dictionaries AS" in section
    assert "LEFT JOIN public.dim_semd_guide_oid r ON r.published_oid" in section
    assert "FROM public.rpt_semd_guides s" in section
    assert "COMMENT ON VIEW public.rpt_semd_dictionaries IS" in section


def test_loader_contains_no_ddl() -> None:
    """DDL живёт только в db/01_schema.sql: scripts/ не входит в переносимый комплект,
    а второй экземпляр описания расходится с первым (так вышло со справочником 1461)."""
    source = LOADER_PATH.read_text(encoding="utf-8")

    for statement in ("CREATE TABLE", "CREATE INDEX", "CREATE OR REPLACE VIEW", "COMMENT ON"):
        assert statement not in source
