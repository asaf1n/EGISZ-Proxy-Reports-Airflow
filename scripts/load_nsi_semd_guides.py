#!/usr/bin/env python3
"""Load the NSI registries that bind a document kind to its required dictionaries.

Sources are two ФНСИ exports, either as the published .zip or as an extracted .json:
1.2.643.5.1.13.13.99.2.638 (implementation guides, with their OID synonyms) and
1.2.643.5.1.13.13.99.2.805 (dictionaries prescribed by each guide). The anchor —
НСИ 1520 — is a seed in db/01_schema.sql and is not touched here.

DDL lives in db/01_schema.sql only; apply the schema before running this loader.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import json
import os
import re
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import psycopg2


GUIDE_SOURCE_OID = "1.2.643.5.1.13.13.99.2.638"
DICTIONARY_SOURCE_OID = "1.2.643.5.1.13.13.99.2.805"

VERSION_PATTERN = re.compile(r"^\d+(?:\.\d+)+$")
SYNONYM_SEPARATOR = ";"

REQUIRED_TABLES = (
    "public.dim_nsi_semd_guide",
    "public.dim_nsi_semd_guide_alias",
    "public.dim_nsi_semd_guide_dictionary",
)

TRUNCATE_SQL = """
TRUNCATE public.dim_nsi_semd_guide_dictionary,
         public.dim_nsi_semd_guide_alias,
         public.dim_nsi_semd_guide
"""

GUIDE_COPY_SQL = """
COPY public.dim_nsi_semd_guide (
    oid, semd_id, full_name, release_number, format, git_pub_date, git_link,
    source_oid, source_version, raw_json
) FROM STDIN WITH (FORMAT csv)
"""

ALIAS_COPY_SQL = """
COPY public.dim_nsi_semd_guide_alias (alias_oid, guide_oid) FROM STDIN WITH (FORMAT csv)
"""

DICTIONARY_COPY_SQL = """
COPY public.dim_nsi_semd_guide_dictionary (
    guide_oid, dict_oid, source_id, dict_name, dict_version, dict_ids_systemname,
    source_oid, source_version, raw_json
) FROM STDIN WITH (FORMAT csv)
"""


class SourcePayload:
    """Records of one ФНСИ export together with the name that carries its version."""

    def __init__(self, records: list[dict[str, Any]], json_name: str) -> None:
        self.records = records
        self.json_name = json_name


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


def parse_nsi_date(value: Any) -> date | None:
    text = clean_text(value)
    if text is None:
        return None
    day, month, year = text.split(".")
    return date(int(year), int(month), int(day))


def read_records(path: Path) -> SourcePayload:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".json")]
            if len(names) != 1:
                raise ValueError(
                    f"{path}: ожидается ровно один JSON в архиве, найдено {len(names)}"
                )
            payload = json.loads(archive.read(names[0]).decode("utf-8-sig"))
            json_name = names[0]
    else:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        json_name = path.name

    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        raise ValueError(f"{path}: ожидается объект с полем 'records'")
    return SourcePayload(records, json_name)


def source_version_from_name(name: str) -> str:
    """Version comes from the JSON file name, never from the archive that wraps it.

    The published archive stem ends in `_json`, so the container name yields the word
    `json` where a version is expected and the whole snapshot loads mislabelled.
    """
    stem = Path(name).stem
    candidate = stem.rsplit("_", 1)[-1] if "_" in stem else ""
    if not VERSION_PATTERN.match(candidate):
        raise ValueError(f"{name}: версия источника не выводится из имени файла")
    return candidate


def assert_source_oid(name: str, expected_oid: str) -> None:
    """Both exports share the same shape, so swapping the two options fails only at the
    foreign key — far from its cause. The OID prefix in the file name settles it up front.
    """
    if not Path(name).name.startswith(expected_oid + "_"):
        raise ValueError(f"{name}: ожидалась выгрузка НСИ {expected_oid}")


def alias_oids(record: dict[str, Any]) -> list[str]:
    raw = clean_text(record.get("OID_SYNONYM"))
    if raw is None:
        return []
    return [part for part in (item.strip() for item in raw.split(SYNONYM_SEPARATOR)) if part]


def guide_row(record: dict[str, Any], source_version: str) -> tuple[Any, ...]:
    return (
        clean_text(record.get("OID")),
        int_value(record.get("SEMD_ID")),
        clean_text(record.get("FULL_NAME")),
        int_value(record.get("RELEASE")),
        clean_text(record.get("FORMAT")),
        parse_nsi_date(record.get("GIT_PUB_DATE")),
        clean_text(record.get("GIT_LINK")),
        GUIDE_SOURCE_OID,
        source_version,
        json.dumps(record, ensure_ascii=False),
    )


def alias_rows(record: dict[str, Any]) -> list[tuple[str, str]]:
    guide_oid = clean_text(record.get("OID"))
    if guide_oid is None:
        return []
    return [(alias, guide_oid) for alias in alias_oids(record)]


def dictionary_row(record: dict[str, Any], source_version: str) -> tuple[Any, ...]:
    """IG_NAME, IG_VERSION and IG_OID_SYNONYM are left out on purpose: in the source they
    are functionally dependent on IG_OID and byte-identical to the guide registry.
    """
    return (
        clean_text(record.get("IG_OID")),
        clean_text(record.get("DICT_OID")),
        clean_text(record.get("ID")),
        clean_text(record.get("DICT_NAME")),
        clean_text(record.get("DICT_VERSION")),
        clean_text(record.get("DICT_IDS_SYSTEMNAME")),
        DICTIONARY_SOURCE_OID,
        source_version,
        json.dumps(record, ensure_ascii=False),
    )


def validate_references(
    guides: list[dict[str, Any]], dictionaries: list[dict[str, Any]]
) -> None:
    """Runs before the transaction opens: a foreign-key violation after TRUNCATE is both
    unreadable and alarming, even though it rolls back.
    """
    guide_oids = [clean_text(record.get("OID")) for record in guides]
    if None in guide_oids:
        raise ValueError("НСИ 638: запись без OID")
    if len(set(guide_oids)) != len(guide_oids):
        raise ValueError("НСИ 638: OID руководства не уникален")

    aliases = [alias for record in guides for alias in alias_oids(record)]
    if len(set(aliases)) != len(aliases):
        raise ValueError("НСИ 638: синоним OID встречается дважды")
    collisions = sorted(set(aliases) & set(guide_oids))
    if collisions:
        raise ValueError(
            f"НСИ 638: OID одновременно основной и синоним: {collisions[:5]}"
        )

    known = set(guide_oids)
    orphans = sorted(
        {
            oid
            for record in dictionaries
            if (oid := clean_text(record.get("IG_OID"))) is not None and oid not in known
        }
    )
    if orphans:
        raise ValueError(f"НСИ 805: руководство отсутствует в реестре 638: {orphans[:5]}")

    pairs = [
        (clean_text(record.get("IG_OID")), clean_text(record.get("DICT_OID")))
        for record in dictionaries
    ]
    if len(set(pairs)) != len(pairs):
        raise ValueError("НСИ 805: пара (руководство, справочник) не уникальна")

    source_ids = [clean_text(record.get("ID")) for record in dictionaries]
    if len(set(source_ids)) != len(source_ids):
        raise ValueError("НСИ 805: идентификатор записи источника не уникален")


def write_copy_file(rows: Iterable[tuple[Any, ...]], prefix: str) -> Path:
    temp = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", prefix=prefix, suffix=".csv", delete=False
    )
    try:
        with temp:
            writer = csv.writer(temp, lineterminator="\n")
            for row in rows:
                writer.writerow(row)
        return Path(temp.name)
    except BaseException:
        with contextlib.suppress(OSError):
            Path(temp.name).unlink()
        raise


def require_tables(cur: Any) -> None:
    for table in REQUIRED_TABLES:
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is None:
            raise RuntimeError(f"{table} отсутствует: примените db/dwh_init.sql")


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guides", type=Path, required=True)
    parser.add_argument("--dictionaries", type=Path, required=True)
    parser.add_argument("--dsn")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "dwh_egisz"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "egisz"))
    parser.add_argument("--password", default=None)
    parser.add_argument("--guides-version", default=None)
    parser.add_argument("--dictionaries-version", default=None)
    args = parser.parse_args()

    guides = read_records(args.guides)
    dictionaries = read_records(args.dictionaries)
    assert_source_oid(guides.json_name, GUIDE_SOURCE_OID)
    assert_source_oid(dictionaries.json_name, DICTIONARY_SOURCE_OID)

    guides_version = args.guides_version or source_version_from_name(guides.json_name)
    dictionaries_version = args.dictionaries_version or source_version_from_name(
        dictionaries.json_name
    )
    validate_references(guides.records, dictionaries.records)

    guide_path = write_copy_file(
        (guide_row(record, guides_version) for record in guides.records),
        "dim_nsi_semd_guide_",
    )
    alias_path = write_copy_file(
        (row for record in guides.records for row in alias_rows(record)),
        "dim_nsi_semd_guide_alias_",
    )
    dictionary_path = write_copy_file(
        (dictionary_row(record, dictionaries_version) for record in dictionaries.records),
        "dim_nsi_semd_guide_dictionary_",
    )
    copy_plan = (
        (guide_path, GUIDE_COPY_SQL),
        (alias_path, ALIAS_COPY_SQL),
        (dictionary_path, DICTIONARY_COPY_SQL),
    )

    try:
        with connect(args) as con:
            with con.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = %s", ("15s",))
                cur.execute("SET LOCAL statement_timeout = %s", ("30min",))
                require_tables(cur)
                cur.execute(TRUNCATE_SQL)
                for path, statement in copy_plan:
                    with path.open("r", encoding="utf-8", newline="") as fh:
                        cur.copy_expert(statement, fh)
                for table in REQUIRED_TABLES:
                    cur.execute(f"ANALYZE {table}")
                cur.execute("SELECT count(*) FROM public.dim_nsi_semd_guide_alias")
                loaded_aliases = cur.fetchone()[0]
    finally:
        for path, _ in copy_plan:
            with contextlib.suppress(OSError):
                path.unlink()

    print(f"loaded_guides={len(guides.records)} version={guides_version}")
    print(f"loaded_aliases={loaded_aliases}")
    print(f"loaded_dictionaries={len(dictionaries.records)} version={dictionaries_version}")


if __name__ == "__main__":
    main()
