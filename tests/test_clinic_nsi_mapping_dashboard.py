from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from conftest import load_script_module


DASHBOARD = Path("metabase_dashboards/09_clinic_nsi_mapping.json")
EXPECTED_COLUMNS = ["JID", "Наименование CASH", "Наименование НСИ", "ИНН", "OID"]


def test_clinic_nsi_mapping_view_contract() -> None:
    schema_sql = Path("db/01_schema.sql").read_text(encoding="utf-8")
    views_sql = Path("db/04_views.sql").read_text(encoding="utf-8")

    assert "ALTER TABLE dim_organizations ADD COLUMN IF NOT EXISTS nsi_name text;" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS dim_nsi_organization" in schema_sql
    assert "source_oid text NOT NULL DEFAULT '1.2.643.5.1.13.13.11.1461'" in schema_sql
    assert "parent_id text" in schema_sql
    assert "raw_json jsonb NOT NULL" in schema_sql
    assert "idx_dim_nsi_organization_active_mo" in schema_sql
    assert "CREATE OR REPLACE VIEW public.rpt_clinic_nsi_mapping AS" in views_sql
    assert "o.name AS cash_name" in views_sql
    assert "o.nsi_name" in views_sql
    assert "LEFT JOIN public.dim_nsi_organization n ON n.oid = public.clean_text_value(o.fir_oid)" in views_sql
    assert "NULLIF(btrim(n.name_short), '')" in views_sql
    assert "public.clean_text_value(o.fir_oid) AS oid" in views_sql
    assert "AS is_mapped" in views_sql


def test_load_nsi_organization_1461_maps_source_fields() -> None:
    loader = load_script_module("load_nsi_organization_1461")
    record = {
        "id": 1,
        "oid": "1.2.643.5.1.13.13.12.2.77.1",
        "nameFull": "Полное имя",
        "nameShort": "Короткое имя",
        "medicalSubjectId": 1,
        "medicalSubjectName": "Организация здравоохранения",
        "inn": "1234567890",
        "kpp": "123456789",
        "ogrn": "1234567890123",
        "regionId": 77,
        "regionName": "Москва",
        "organizationType": 1,
        "moDeptId": 100,
        "moDeptName": "Департамент",
        "deleteDate": "",
        "createDate": "01.02.2024",
        "modifyDate": "03.04.2025",
        "moAgencyKindId": 20,
        "postIndex": "101000",
        "latitude": "55,75",
        "longtitude": "37.61",
        "parentId": "1.2.643.5.1.13.13.12.2.77.0",
    }

    row = loader.record_to_row(record, "6.1891")

    assert row[0] == 1
    assert row[1] == "1.2.643.5.1.13.13.12.2.77.1"
    assert row[2] == "1.2.643.5.1.13.13.11.1461"
    assert row[3] == "6.1891"
    assert row[4] == "Полное имя"
    assert row[5] == "Короткое имя"
    assert row[16] is None
    assert row[18] == date(2024, 2, 1)
    assert row[19] == date(2025, 4, 3)
    assert row[36] == "55.75"
    assert row[37] == "37.61"
    assert row[43] == "1.2.643.5.1.13.13.12.2.77.0"


def test_clinic_nsi_mapping_dashboard_has_two_tables() -> None:
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))

    assert dashboard["name"] == "Сопоставление клиник с НСИ"
    assert dashboard["width"] == "full"
    assert dashboard["parameters"] == []
    assert [card["name"] for card in dashboard["cards"]] == [
        "Клиники с сопоставлением НСИ",
        "Клиники без сопоставления НСИ",
    ]

    for card, mapping_predicate in zip(dashboard["cards"], ("WHERE is_mapped", "WHERE NOT is_mapped"), strict=True):
        assert card["display"] == "table"
        query = card["dataset_query"]["native"]["query"]
        assert "public.rpt_clinic_nsi_mapping" in query
        assert mapping_predicate in query
        assert all(f'AS "{column}"' in query for column in EXPECTED_COLUMNS)
        assert [
            column["name"]
            for column in card["visualization_settings"]["table.columns"]
            if column.get("enabled", True)
        ] == EXPECTED_COLUMNS
