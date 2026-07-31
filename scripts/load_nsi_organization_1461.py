#!/usr/bin/env python3
"""Load NSI 1461 medical organization dictionary into DWH.

The loader replaces the permanent NSI snapshot in dim_nsi_organization and
refreshes dim_organizations.nsi_name for already matched OIDs. OID backfill for
CASH/JPERSONS rows is optional and uses only active parent .12.2. records with a
single OID per INN.
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

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.dim_nsi_organization (
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
ALTER TABLE public.dim_organizations ADD COLUMN IF NOT EXISTS nsi_name text;
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_inn
    ON public.dim_nsi_organization (inn)
    WHERE inn IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_ogrn
    ON public.dim_nsi_organization (ogrn)
    WHERE ogrn IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_nsi_organization_active_mo
    ON public.dim_nsi_organization (inn, oid)
    WHERE delete_date IS NULL
      AND parent_id IS NULL
      AND oid LIKE '1.2.643.5.1.13.13.12.2.%';
"""

COMMENT_DDL = """
COMMENT ON TABLE public.dim_nsi_organization IS
'НСИ 1.2.643.5.1.13.13.11.1461 «ФРМО. Справочник медицинских организаций»; полный снимок версии источника.';
COMMENT ON COLUMN public.dim_nsi_organization.parent_id IS
'parentId из НСИ: OID родительской записи, а не внутренний nsi_id.';
COMMENT ON COLUMN public.dim_organizations.name IS
'Наименование организации из CASH/JPERSONS.';
COMMENT ON COLUMN public.dim_organizations.fir_oid IS
'OID медицинской организации из НСИ; sync_dictionaries не заполняет это поле из JPERSONS.';
COMMENT ON COLUMN public.dim_organizations.nsi_name IS
'Наименование медицинской организации из НСИ для аудита сопоставления с CASH.';
COMMENT ON VIEW public.rpt_clinic_nsi_mapping IS
'Аудит сопоставления клиник CASH/JPERSONS с НСИ 1461: JID, наименование CASH, наименование НСИ, ИНН, OID и признак сопоставления.';
"""

VIEW_SQL = """
CREATE OR REPLACE VIEW public.rpt_clinic_nsi_mapping AS
SELECT
    o.jid,
    o.name AS cash_name,
    COALESCE(NULLIF(btrim(o.nsi_name), ''), NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), '')) AS nsi_name,
    o.inn,
    public.clean_text_value(o.fir_oid) AS oid,
    (NULLIF(btrim(o.fir_oid), '') IS NOT NULL) AS is_mapped
FROM public.dim_organizations o
LEFT JOIN public.dim_nsi_organization n ON n.oid = public.clean_text_value(o.fir_oid)
WHERE o.jid IS NOT NULL;
"""

COPY_SQL = """
COPY public.dim_nsi_organization (
    nsi_id, oid, source_oid, source_version, name_full, name_short,
    medical_subject_id, medical_subject_name, inn, kpp, ogrn, region_id,
    region_name, organization_type, mo_dept_id, mo_dept_name, delete_date,
    delete_reason, create_date, modify_date, mo_level, mo_agency_kind_id,
    mo_agency_kind, post_index, aoid_area, aoid_street, houseid,
    addr_region_id, addr_region_name, area_name, prefix_area, street_name,
    prefix_street, house, building, struct, latitude, longitude, founder,
    profile_agency_kind_id, profile_agency_kind, cadastral_number, old_oid,
    parent_id, raw_json
) FROM STDIN WITH (FORMAT csv)
"""

REFRESH_ORG_NAMES_SQL = """
UPDATE public.dim_organizations o
SET
    nsi_name = COALESCE(NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), '')),
    updated_at = now()
FROM public.dim_nsi_organization n
WHERE n.oid = public.clean_text_value(o.fir_oid)
  AND o.nsi_name IS DISTINCT FROM COALESCE(NULLIF(btrim(n.name_short), ''), NULLIF(btrim(n.name_full), ''));
"""

BACKFILL_ORG_OIDS_SQL = """
WITH active_mo AS (
    SELECT
        NULLIF(btrim(inn), '') AS inn,
        oid,
        COALESCE(NULLIF(btrim(name_short), ''), NULLIF(btrim(name_full), '')) AS nsi_name
    FROM public.dim_nsi_organization
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
UPDATE public.dim_organizations o
SET
    fir_oid = u.oid,
    nsi_name = u.nsi_name,
    updated_at = now()
FROM unique_by_inn u
WHERE NULLIF(btrim(o.inn), '') = u.inn
  AND (
      public.clean_text_value(o.fir_oid) IS DISTINCT FROM u.oid
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
        json.dumps(record, ensure_ascii=False, separators=(",", ":")),
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
        prefix="dim_nsi_organization_",
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
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "dwh_egisz"))
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
                cur.execute(TABLE_DDL)
                cur.execute("TRUNCATE public.dim_nsi_organization")
                with copy_path.open("r", encoding="utf-8", newline="") as fh:
                    cur.copy_expert(COPY_SQL, fh)
                cur.execute(INDEX_DDL)
                cur.execute(REFRESH_ORG_NAMES_SQL)
                refreshed_names = cur.rowcount
                backfilled_oids = 0
                if args.backfill_fir_oid:
                    cur.execute(BACKFILL_ORG_OIDS_SQL)
                    backfilled_oids = cur.rowcount
                cur.execute(VIEW_SQL)
                cur.execute(COMMENT_DDL)
                cur.execute("ANALYZE public.dim_nsi_organization")
                cur.execute("ANALYZE public.dim_organizations")
    finally:
        with contextlib.suppress(OSError):
            copy_path.unlink()

    print(f"loaded_records={len(records)}")
    print(f"refreshed_dim_organizations_nsi_name={refreshed_names}")
    print(f"backfilled_dim_organizations_fir_oid={backfilled_oids}")


if __name__ == "__main__":
    main()
