#!/usr/bin/env python3
"""Load NSI 1461 medical organization dictionary into DWH.

The loader replaces the permanent NSI snapshot in mart_egisz.dim_nsi_organizations and
refreshes mart_egisz.dim_organizations.nsi_name for already matched OIDs. OID backfill for
CASH/JPERSONS rows is optional and uses only active parent .12.2. records with a
single OID per INN. Tables, indexes and views are declared in db/ only.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import json
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2


SOURCE_OID = "1.2.643.5.1.13.13.11.1461"
DEFAULT_PAGE_SIZE = 1000

COPY_SQL = """
COPY mart_egisz.dim_nsi_organizations (
    nsi_id, oid, source_oid, source_version, name_full, name_short,
    medical_subject_id, medical_subject_name, inn, kpp, ogrn, region_id,
    region_name, organization_type, mo_dept_id, mo_dept_name, delete_date,
    delete_reason, create_date, modify_date, mo_level, mo_agency_kind_id,
    mo_agency_kind, post_index, aoid_area, aoid_street, houseid,
    addr_region_id, addr_region_name, area_name, prefix_area, street_name,
    prefix_street, house, building, struct, latitude, longitude, founder,
    profile_agency_kind_id, profile_agency_kind, cadastral_number, old_oid,
    parent_id
) FROM STDIN WITH (FORMAT csv)
"""

REFRESH_ORG_NAMES_SQL = """
UPDATE mart_egisz.dim_organizations o
SET
    nsi_name = COALESCE(NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), '')),
    updated_at = now()
FROM mart_egisz.dim_nsi_organizations n
WHERE n.oid = stg_egisz.clean_text_value(o.fir_oid)
  AND o.nsi_name IS DISTINCT FROM COALESCE(NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), ''));
"""

BACKFILL_ORG_OIDS_SQL = """
WITH active_mo AS (
    SELECT
        NULLIF(btrim(inn), '') AS inn,
        oid,
        COALESCE(NULLIF(btrim(name_short), ''), NULLIF(btrim(name_full), '')) AS nsi_name
    FROM mart_egisz.dim_nsi_organizations
    WHERE delete_date IS NULL
      AND parent_id IS NULL
      AND oid LIKE '1.2.643.5.1.13.13.12.2.%'
      AND NULLIF(btrim(inn), '') IS NOT NULL
),
unique_by_inn AS (
    SELECT
        inn,
        MIN(oid) AS oid,
        MIN(nsi_name) AS nsi_name
    FROM active_mo
    GROUP BY inn
    HAVING COUNT(DISTINCT oid) = 1
)
UPDATE mart_egisz.dim_organizations o
SET
    fir_oid = u.oid,
    nsi_name = u.nsi_name,
    updated_at = now()
FROM unique_by_inn u
WHERE NULLIF(btrim(o.inn), '') = u.inn
  AND (
      stg_egisz.clean_text_value(o.fir_oid) IS DISTINCT FROM u.oid
   OR o.nsi_name IS DISTINCT FROM u.nsi_name
  );
"""


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def int_value(value: Any) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    return int(text)


def numeric_value(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    return text.replace(",", ".")


def parse_nsi_date(value: Any) -> date | None:
    text = clean_text(value)
    if text is None:
        return None
    day, month, year = text.split(".")
    return date(int(year), int(month), int(day))


def version_from_path(path: Path) -> str:
    stem = path.stem
    if "_" not in stem:
        return ""
    return stem.rsplit("_", 1)[-1]


def load_records(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as fh:
        payload = json.load(fh)
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        raise ValueError(f"{path} must contain a top-level 'records' list")
    return records


def record_to_row(record: dict[str, Any], source_version: str) -> tuple[Any, ...]:
    return (
        int_value(record.get("id")),
        clean_text(record.get("oid")),
        SOURCE_OID,
        source_version,
        clean_text(record.get("nameFull")),
        clean_text(record.get("nameShort")),
        int_value(record.get("medicalSubjectId")),
        clean_text(record.get("medicalSubjectName")),
        clean_text(record.get("inn")),
        clean_text(record.get("kpp")),
        clean_text(record.get("ogrn")),
        int_value(record.get("regionId")),
        clean_text(record.get("regionName")),
        int_value(record.get("organizationType")),
        int_value(record.get("moDeptId")),
        clean_text(record.get("moDeptName")),
        parse_nsi_date(record.get("deleteDate")),
        clean_text(record.get("deleteReason")),
        parse_nsi_date(record.get("createDate")),
        parse_nsi_date(record.get("modifyDate")),
        clean_text(record.get("moLevel")),
        int_value(record.get("moAgencyKindId")),
        clean_text(record.get("moAgencyKind")),
        clean_text(record.get("postIndex")),
        clean_text(record.get("aoidArea")),
        clean_text(record.get("aoidStreet")),
        clean_text(record.get("houseid")),
        int_value(record.get("addrRegionId")),
        clean_text(record.get("addrRegionName")),
        clean_text(record.get("areaName")),
        clean_text(record.get("prefixArea")),
        clean_text(record.get("streetName")),
        clean_text(record.get("prefixStreet")),
        clean_text(record.get("house")),
        clean_text(record.get("building")),
        clean_text(record.get("struct")),
        numeric_value(record.get("latitude")),
        numeric_value(record.get("longtitude")),
        clean_text(record.get("founder")),
        int_value(record.get("profileAgencyKindId")),
        clean_text(record.get("profileAgencyKind")),
        clean_text(record.get("cadastralNumber")),
        clean_text(record.get("oldOid")),
        clean_text(record.get("parentId")),
    )


def connect(args: argparse.Namespace):
    if args.dsn:
        return psycopg2.connect(args.dsn)
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password or os.environ.get("PGPASSWORD"),
    )


def write_copy_file(records: list[dict[str, Any]], source_version: str) -> Path:
    temp = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        prefix="dim_nsi_organizations_",
        suffix=".csv",
        delete=False,
    )
    try:
        with temp:
            writer = csv.writer(temp, lineterminator="\n")
            for record in records:
                writer.writerow(record_to_row(record, source_version))
        return Path(temp.name)
    except BaseException:
        with contextlib.suppress(OSError):
            Path(temp.name).unlink()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("json_file", type=Path)
    parser.add_argument("--dsn")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "dwh_bi_old"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "egisz"))
    parser.add_argument("--password", default=None)
    parser.add_argument("--source-version", default=None)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--backfill-fir-oid", action="store_true")
    args = parser.parse_args()

    records = load_records(args.json_file)
    source_version = args.source_version or version_from_path(args.json_file)
    copy_path = write_copy_file(records, source_version)

    try:
        with connect(args) as con:
            with con.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = %s", ("15s",))
                cur.execute("SET LOCAL statement_timeout = %s", ("30min",))
                cur.execute("TRUNCATE mart_egisz.dim_nsi_organizations")
                with copy_path.open("r", encoding="utf-8", newline="") as fh:
                    cur.copy_expert(COPY_SQL, fh)
                cur.execute(REFRESH_ORG_NAMES_SQL)
                refreshed_names = cur.rowcount
                backfilled_oids = 0
                if args.backfill_fir_oid:
                    cur.execute(BACKFILL_ORG_OIDS_SQL)
                    backfilled_oids = cur.rowcount
                cur.execute("ANALYZE mart_egisz.dim_nsi_organizations")
                cur.execute("ANALYZE mart_egisz.dim_organizations")
    finally:
        with contextlib.suppress(OSError):
            copy_path.unlink()

    print(f"loaded_records={len(records)}")
    print(f"refreshed_dim_organizations_nsi_name={refreshed_names}")
    print(f"backfilled_dim_organizations_fir_oid={backfilled_oids}")


if __name__ == "__main__":
    main()
