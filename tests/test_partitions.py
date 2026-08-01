"""Тесты месячной сетки партиций против живого PostgreSQL.

Запуск требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_egisz);
без переменной живая часть модуля скипается. Фикстура идемпотентно применяет
ensure_time_partitions из db/01_schema.sql, поэтому проверяется текущий код функции,
а не состояние базы на момент последнего наката схемы.

Инвариант сетки: границы соседних месяцев совпадают (без разрывов и перекрытий) и
привязаны к полуночи UTC. Разрыв делает вставку строки невозможной, перекрытие валит
CREATE следующей партиции — оба исхода наступают только при смене якоря сетки.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from conftest import load_dag_module  # noqa: E402

connect_pg = load_dag_module("egisz_etl_dag").connect_pg

DSN = os.environ.get("EGISZ_TEST_PG_DSN")

DB_DIR = Path(__file__).resolve().parents[1] / "db"
SCHEMA_SQL = (DB_DIR / "01_schema.sql").read_text(encoding="utf-8")

PARTITIONED_TABLES = ("exchangelog_raw", "transactions")

BOUNDS_SQL = """
SELECT parent.relname,
       child.relname,
       (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'FROM \\(''([^'']+)''\\)'))[1]::timestamptz,
       (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'TO \\(''([^'']+)''\\)'))[1]::timestamptz
FROM pg_inherits i
JOIN pg_class parent ON parent.oid = i.inhparent
JOIN pg_class child ON child.oid = i.inhrelid
WHERE parent.relname = ANY(%s)
ORDER BY 1, 3
"""


def function_source() -> str:
    """Текст ensure_time_partitions из модуля схемы — без ручного дублирования.

    Слайс начинается с DROP: переименование параметров несовместимо с CREATE OR REPLACE.
    """
    start = SCHEMA_SQL.index("DROP FUNCTION IF EXISTS public.ensure_time_partitions")
    end = SCHEMA_SQL.index("\n$$;", start) + len("\n$$;")
    return SCHEMA_SQL[start:end]


def test_partition_grid_is_anchored_in_naive_utc() -> None:
    """Сетка считается наивным UTC: неявное приведение взяло бы часовой пояс сессии."""
    source = function_source()
    declarations = source[source.index("DECLARE") : source.index("BEGIN")]

    for variable in ("window_start", "window_end", "data_start", "data_end", "part_start", "part_end"):
        assert re.search(rf"{variable} timestamp;", declarations), variable
    assert "timestamptz" not in declarations
    assert source.count("part_start AT TIME ZONE 'UTC'") == 1
    assert source.count("part_end AT TIME ZONE 'UTC'") == 1
    assert "timezone(''UTC'', min(%I))" in source
    assert "timezone(''UTC'', max(%I))" in source


def test_maintained_tables_come_from_the_catalogue() -> None:
    """Второй перечень партиционированных таблиц разошёлся бы с каталогом молча."""
    source = function_source()

    assert "pg_partitioned_table" in source
    assert "'exchangelog_raw'" not in source
    assert "'transactions'" not in source


live_pg = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")


@pytest.fixture(scope="module")
def con():
    con = connect_pg(DSN)
    with con.cursor() as cur:
        cur.execute(function_source())
    con.commit()
    yield con
    con.rollback()
    con.close()


def read_bounds(con) -> list[tuple[str, str, object, object]]:
    with con.cursor() as cur:
        cur.execute(BOUNDS_SQL, (list(PARTITIONED_TABLES),))
        return cur.fetchall()


@live_pg
def test_partition_grid_has_no_gaps_or_overlaps(con) -> None:
    previous: dict[str, tuple[str, object]] = {}
    for parent, part, low, high in read_bounds(con):
        if parent in previous:
            prior_name, prior_high = previous[parent]
            assert low == prior_high, f"{prior_name} → {part}: {prior_high} против {low}"
        previous[parent] = (part, high)


@live_pg
def test_partition_bounds_sit_on_utc_month_start(con) -> None:
    with con.cursor() as cur:
        cur.execute("SET LOCAL TIME ZONE 'UTC'")
        offenders = [
            (part, str(low))
            for _parent, part, low, _high in read_bounds(con)
            if low != low.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        ]
    con.rollback()
    assert not offenders, f"границы вне полуночи UTC первого числа: {offenders[:5]}"


@live_pg
@pytest.mark.parametrize("timezone_pair", [("Europe/Moscow", "Asia/Irkutsk")])
def test_created_bounds_do_not_follow_session_timezone(con, timezone_pair) -> None:
    """Одна и та же функция в разных поясах сессии обязана дать те же границы."""
    grids = []
    for zone in timezone_pair:
        with con.cursor() as cur:
            cur.execute(f"SET LOCAL TIME ZONE '{zone}'")
            # Горизонт шире рабочего: месяцы за краем текущей сетки создаются заново.
            cur.execute("SELECT public.ensure_time_partitions(12, 36)")
            grids.append(read_bounds(con))
        con.rollback()

    assert grids[0] == grids[1]


@live_pg
def test_new_partitioned_table_is_picked_up_without_editing_the_function(con) -> None:
    """Перечень таблиц из каталога: сетка достраивается любой RANGE-таблице по времени."""
    probe = "partition_probe"
    with con.cursor() as cur:
        cur.execute(
            f"CREATE TABLE public.{probe} (id bigint, observed_at timestamptz NOT NULL) "
            "PARTITION BY RANGE (observed_at)"
        )
        cur.execute("SELECT public.ensure_time_partitions(1, 1)")
        cur.execute(BOUNDS_SQL, ([probe],))
        bounds = cur.fetchall()
        cur.execute("SET LOCAL TIME ZONE 'UTC'")
        cur.execute(BOUNDS_SQL, ([probe],))
        bounds_utc = cur.fetchall()
    con.rollback()

    # Окно (1, 1) — предыдущий, текущий и следующий месяцы.
    assert len(bounds) == 3
    for _parent, _part, low, _high in bounds_utc:
        assert (low.day, low.hour, low.minute) == (1, 0, 0), low
