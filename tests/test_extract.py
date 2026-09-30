from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

from conftest import load_dag_module

extract_dag = load_dag_module("egisz_etl_dag")

extract_exchangelog_batch = extract_dag.extract_exchangelog_batch
extract_message_registry_batch = extract_dag.extract_message_registry_batch
fetch_depth_floor = extract_dag.fetch_depth_floor
normalize_registry_key = extract_dag.normalize_registry_key
transform_exchangelog_batch = extract_dag.transform_exchangelog_batch
run_analyze = extract_dag.run_analyze


@pytest.fixture
def pg_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def fb_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture(autouse=True)
def cursor_outside_window():
    """По умолчанию отметка стоит перед окном: первая строка окна открывает участок."""
    with patch("egisz_etl_dag.is_cursor_in_window", return_value=False) as probe:
        yield probe


def _raw_row(logid: int, created: datetime | None = None) -> dict[str, object]:
    return {
        "logid": logid,
        "logdate": None,
        "createdate": created.isoformat() if created is not None else None,
        "msgid": None,
        "logstate": None,
        "logtext": None,
        "msgtext": None,
        "uri": "/emdr/callback",
    }


def test_extract_cursor_counts_the_proxy_not_raw(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Отметка выгрузки считается по журналу шлюза отдельно от отметки разбора."""
    rows = [_raw_row(101)]

    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100, transform=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=rows) as fetch,
        patch("egisz_etl_dag.load_raw_logs") as load_raw,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw") as analyze_raw,
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=2000, raw_rounds=3, depth_days=0
        )

    fetch.assert_called_once_with(fb_conn, after_logid=100, limit=2000)
    load_raw.assert_called_once_with(pg_conn, rows)
    analyze_raw.assert_called_once_with(pg_conn)
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=101)
    assert result == {"count": 1, "extract_logid_cursor": 101}


def test_extract_holds_cursor_before_gap_inside_window(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Разрыв LOGID в окне удерживает отметку, но страница загружается целиком."""
    page = [_raw_row(101), _raw_row(102), _raw_row(105), _raw_row(106)]
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.is_cursor_in_window", return_value=True),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=page) as fetch,
        patch("egisz_etl_dag.load_raw_logs") as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw"),
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=4, raw_rounds=3, depth_days=30
        )

    fetch.assert_called_once_with(fb_conn, after_logid=100, limit=4)
    load.assert_called_once_with(pg_conn, page)
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=102)
    assert result == {"count": 4, "extract_logid_cursor": 102}


def test_extract_holds_cursor_when_first_row_is_after_gap_inside_window(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.is_cursor_in_window", return_value=True),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=[_raw_row(105)]),
        patch("egisz_etl_dag.load_raw_logs"),
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw"),
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=10, raw_rounds=3, depth_days=30
        )

    update.assert_not_called()
    assert result == {"count": 1, "extract_logid_cursor": 100}


def test_extract_passes_gaps_among_rows_older_than_window(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Разрывы среди строк старше окна не удерживают отметку; окно открывает участок."""
    now = datetime.now()
    pages = [
        [_raw_row(500, now - timedelta(days=90)), _raw_row(900, now - timedelta(days=80))],
        [_raw_row(2_000, now - timedelta(days=2)), _raw_row(2_001, now - timedelta(days=1))],
        [],
    ]
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", side_effect=pages) as fetch,
        patch("egisz_etl_dag.load_raw_logs") as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw") as analyze,
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=2, raw_rounds=3, depth_days=30
        )

    assert [c.kwargs["after_logid"] for c in fetch.call_args_list] == [100, 900, 2_001]
    load.assert_called_once_with(pg_conn, pages[1])
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=2_001)
    assert result == {"count": 2, "extract_logid_cursor": 2_001}


def test_contiguous_prefix_end_bridges_old_rows_and_stops_at_gap_in_window() -> None:
    since = datetime(2026, 9, 1)
    old, fresh = datetime(2026, 8, 1), datetime(2026, 9, 15)
    rows = [_raw_row(10, old), _raw_row(50, old), _raw_row(60, fresh), _raw_row(61, fresh), _raw_row(70, fresh)]

    end, in_window = extract_dag.contiguous_prefix_end(
        rows, after=0, since=since, after_in_window=False
    )

    assert (end, in_window) == (61, True)


def test_contiguous_prefix_end_without_window_treats_first_row_as_start() -> None:
    rows = [_raw_row(7), _raw_row(8), _raw_row(10)]

    assert extract_dag.contiguous_prefix_end(rows, after=0, since=None, after_in_window=False) == (8, True)


def test_extract_window_is_applied_to_fetched_rows_and_cursor_passes_old_ones(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Строки старше окна в raw не попадают, но курсор проходит через них."""
    now = datetime.now()
    old = _raw_row(101, now - timedelta(days=90))
    fresh = _raw_row(102, now - timedelta(days=1))
    undated = _raw_row(103)
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", side_effect=[[old, fresh, undated], []]),
        patch("egisz_etl_dag.load_raw_logs") as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw"),
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=3, raw_rounds=3, depth_days=30
        )

    load.assert_called_once_with(pg_conn, [fresh, undated])
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=103)
    assert result == {"count": 2, "extract_logid_cursor": 103}


def test_extract_page_entirely_below_window_advances_cursor_without_load(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    old = [_raw_row(101, datetime.now() - timedelta(days=90)), _raw_row(102, datetime.now() - timedelta(days=89))]
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=old),
        patch("egisz_etl_dag.load_raw_logs") as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw") as analyze,
    ):
        result = extract_exchangelog_batch(
            pg_conn, fb_conn, raw_rows=10, raw_rounds=3, depth_days=30
        )

    load.assert_not_called()
    analyze.assert_not_called()
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=102)
    assert result == {"count": 0, "extract_logid_cursor": 102}


def test_extract_empty_window_retries_same_cursor_on_next_run(
    pg_conn: MagicMock, fb_conn: MagicMock,
) -> None:
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", side_effect=[[], [_raw_row(200)]]) as fetch,
        patch("egisz_etl_dag.load_raw_logs") as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_raw") as analyze,
    ):
        first = extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=10, raw_rounds=3, depth_days=30)
        update.assert_not_called()
        load.assert_not_called()
        analyze.assert_not_called()
        second = extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=10, raw_rounds=3, depth_days=30)

    assert first == {"count": 0, "extract_logid_cursor": 100}
    assert second == {"count": 1, "extract_logid_cursor": 200}
    assert [c.kwargs["after_logid"] for c in fetch.call_args_list] == [100, 100]
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_logid=200)


def test_extract_load_failure_does_not_advance_cursor(
    pg_conn: MagicMock, fb_conn: MagicMock,
) -> None:
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=100)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=[_raw_row(200)]),
        patch("egisz_etl_dag.load_raw_logs", side_effect=RuntimeError("load failed")),
        patch("egisz_etl_dag.update_cursors") as update,
        pytest.raises(RuntimeError, match="load failed"),
    ):
        extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=10, raw_rounds=3, depth_days=30)
    update.assert_not_called()


def _cursors(*, extract: int = 0, transform: int = 0, egmid: int = 0) -> dict[str, int]:
    return {
        "extract_logid_cursor": extract,
        "transform_logid_cursor": transform,
        "extract_egmid_cursor": egmid,
    }


def test_transform_exchangelog_runs_multiple_iterations(pg_conn: MagicMock) -> None:
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=500, transform=100)),
        patch("egisz_etl_dag.bounded_transform_to_logid", side_effect=[200, 300, 300]),
        patch(
            "egisz_etl_dag.transform_raw_to_facts",
            side_effect=[
                {"transformed": 100, "unlinked": 2, "sends_without_clinic": 1},
                {"transformed": 50, "unlinked": 0, "sends_without_clinic": 0},
            ],
        ) as transform,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag._analyze_exchangelog_documents") as analyze_docs,
    ):
        result = transform_exchangelog_batch(
            pg_conn,
            transform_rows=5000,
            transform_rounds=6,
        )

    assert transform.call_count == 2
    assert update.call_count == 2
    analyze_docs.assert_called_once_with(pg_conn)
    assert result["transformed"] == 150
    assert result["unlinked"] == 2
    assert result["sends_without_clinic"] == 1
    assert result["transform_logid_cursor"] == 300


def test_transform_is_bounded_by_the_extract_cursor(pg_conn: MagicMock) -> None:
    """Разбор ограничен отметкой последней успешно загруженной строки журнала."""
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(extract=102, transform=102)),
        patch("egisz_etl_dag.transform_raw_to_facts") as transform,
        patch("egisz_etl_dag.update_cursors") as update,
    ):
        result = transform_exchangelog_batch(
            pg_conn,
            transform_rows=5000,
            transform_rounds=6,
        )

    transform.assert_not_called()
    update.assert_not_called()
    assert result["transformed"] == 0
    assert result["transform_logid_cursor"] == 102


def test_bounded_transform_to_logid_stops_at_the_extract_cursor() -> None:
    con = MagicMock()
    assert extract_dag.bounded_transform_to_logid(
        con, from_logid=100, to_logid=100, raw_rows=5000) == 100
    assert extract_dag.bounded_transform_to_logid(
        con, from_logid=100, to_logid=500, raw_rows=0) == 100
    con.cursor.assert_not_called()


def test_normalize_registry_key_matches_sql_canonical_form() -> None:
    """Ключ реестра приводится к одному виду на обеих сторонах: без дефисов,
    без префикса urn:uuid: и угловых скобок, в верхнем регистре."""
    expected = "A07167955FA149D1BF532EFAD47EFA46"
    assert normalize_registry_key("a0716795-5fa1-49d1-bf53-2efad47efa46") == expected
    assert normalize_registry_key("urn:uuid:A0716795-5FA1-49D1-BF53-2EFAD47EFA46") == expected
    assert normalize_registry_key("<A07167955FA149D1BF532EFAD47EFA46>") == expected
    assert normalize_registry_key(None) is None
    assert normalize_registry_key("  ") is None


def test_load_message_registry_keeps_source_rows_by_egmid(pg_conn: MagicMock) -> None:
    """EGISZ_MESSAGES хранится как реестр по EGMID, без раннего отбора по DOCUMENTID."""
    assert extract_dag.is_iemk_reply_to("http://gost-2.lan:9921")
    assert not extract_dag.is_iemk_reply_to("http://gost-1.lan:9945")

    rows = [
        (1, "a0716795-5fa1-49d1-bf53-2efad47efa46", "http://gost-1.lan:9945", "UID-OLD", None),
        (2, "urn:uuid:A0716795-5FA1-49D1-BF53-2EFAD47EFA46", "http://gost-2.lan:9921", "IEMK-UID", None),
        (3, None, "http://gost-3.lan:9945", "UID-ONLY", None),
        (4, None, None, None, None),
    ]

    with patch("egisz_etl_dag.execute_values") as execute_values:
        loaded = extract_dag.load_message_registry(pg_conn, rows)

    values = execute_values.call_args.args[2]
    assert loaded == 4
    assert values == [
        (1, "A07167955FA149D1BF532EFAD47EFA46", "uid-old", "http://gost-1.lan:9945", None),
        (2, "A07167955FA149D1BF532EFAD47EFA46", None, "http://gost-2.lan:9921", None),
        (3, None, "uid-only", "http://gost-3.lan:9945", None),
        (4, None, None, None, None),
    ]


def test_extract_message_registry_advances_its_own_cursor(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Реестр подач читается keyset-курсором по EGMID и двигает собственную отметку."""
    rows = [(7, "MSG-1", "http://gost-1.lan:9945", "UID-1", None)]

    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(egmid=5)),
        patch("egisz_etl_dag.fetch_message_registry_after_cursor", side_effect=[rows, []]) as fetch,
        patch("egisz_etl_dag.load_message_registry", return_value=1) as load,
        patch("egisz_etl_dag.update_cursors") as update,
        patch("egisz_etl_dag.run_analyze"),
    ):
        loaded = extract_message_registry_batch(
            pg_conn,
            fb_conn,
            registry_rows=5000,
            registry_rounds=3,
            depth_days=0,
        )

    assert loaded == 1
    fetch.assert_called_once_with(fb_conn, after_egmid=5, limit=5000)
    load.assert_called_once_with(pg_conn, rows)
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, extract_egmid=7)


def test_depth_floor_skips_source_prefix_outside_window(fb_conn: MagicMock) -> None:
    """Глубина отдаёт отметку ПЕРЕД первой строкой окна: keyset читает её включительно."""
    cursor = fb_conn.cursor.return_value
    # Проба: строка за отметкой вне окна → считаем границу.
    cursor.fetchone.side_effect = [(datetime.now() - timedelta(days=400),), (10_500_000,)]

    floor = fetch_depth_floor(fb_conn, source="message_registry", depth_days=30, after_id=5)

    assert floor == 10_499_999
    probe_stmt, floor_stmt = [call.args[0] for call in cursor.execute.call_args_list]
    assert probe_stmt == extract_dag.DEPTH_FLOOR_SQL["message_registry"]["probe"]
    assert floor_stmt == extract_dag.DEPTH_FLOOR_SQL["message_registry"]["floor"]


def test_depth_floor_skips_range_scan_when_cursor_is_inside_window(fb_conn: MagicMock) -> None:
    """Отметка уже в окне — тяжёлый MIN(...) по диапазону дат не выполняется.

    На прод-объёме этот скан стоит около трёх минут; в установившемся режиме он был бы
    чистыми накладными расходами на каждом запуске пятиминутного DAG.
    """
    cursor = fb_conn.cursor.return_value
    cursor.fetchone.return_value = (datetime.now() - timedelta(hours=1),)

    assert fetch_depth_floor(fb_conn, source="message_registry", depth_days=30, after_id=42) == 0

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements == [extract_dag.DEPTH_FLOOR_SQL["message_registry"]["probe"]]


def test_depth_floor_is_disabled_by_zero_and_does_not_query_source(fb_conn: MagicMock) -> None:
    assert fetch_depth_floor(fb_conn, source="message_registry", depth_days=0, after_id=0) == 0
    fb_conn.cursor.assert_not_called()


def test_depth_floor_keeps_cursor_when_source_tail_is_exhausted(fb_conn: MagicMock) -> None:
    """За отметкой строк нет — отметку не трогаем, иначе приём укатился бы назад."""
    fb_conn.cursor.return_value.fetchone.return_value = (None,)

    assert fetch_depth_floor(fb_conn, source="message_registry", depth_days=30, after_id=7) == 0


def test_depth_floor_keeps_cursor_when_window_is_empty(fb_conn: MagicMock) -> None:
    """В окне нет строк (источник молчит месяц) — отметка остаётся на месте."""
    cursor = fb_conn.cursor.return_value
    cursor.fetchone.side_effect = [(datetime.now() - timedelta(days=400),), (None,)]

    assert fetch_depth_floor(fb_conn, source="message_registry", depth_days=30, after_id=7) == 0


def test_extract_message_registry_lifts_cursor_to_depth_floor(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Отметка ниже окна поднимается к его границе."""
    rows = [(10_500_100, "MSG-1", "http://gost-1.lan:9945", "UID-1", None)]

    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(egmid=5)),
        patch("egisz_etl_dag.fetch_depth_floor", return_value=10_499_999),
        patch("egisz_etl_dag.fetch_message_registry_after_cursor", side_effect=[rows, []]) as fetch,
        patch("egisz_etl_dag.load_message_registry", return_value=1),
        patch("egisz_etl_dag.update_cursors"),
        patch("egisz_etl_dag.run_analyze"),
    ):
        extract_message_registry_batch(
            pg_conn,
            fb_conn,
            registry_rows=5000,
            registry_rounds=3,
            depth_days=30,
        )

    fetch.assert_called_once_with(fb_conn, after_egmid=10_499_999, limit=5000)


def test_extract_message_registry_keeps_cursor_ahead_of_depth_floor(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Отметка выше границы окна не откатывается: курсоры только растут."""
    with (
        patch("egisz_etl_dag.get_cursors", return_value=_cursors(egmid=10_600_000)),
        patch("egisz_etl_dag.fetch_depth_floor", return_value=10_499_999),
        patch("egisz_etl_dag.fetch_message_registry_after_cursor", return_value=[]) as fetch,
        patch("egisz_etl_dag.run_analyze"),
    ):
        extract_message_registry_batch(
            pg_conn,
            fb_conn,
            registry_rows=5000,
            registry_rounds=3,
            depth_days=30,
        )

    fetch.assert_called_once_with(fb_conn, after_egmid=10_600_000, limit=5000)


def test_run_analyze_commits_before_switching_autocommit(pg_conn: MagicMock) -> None:
    pg_conn.autocommit = False
    cursor = MagicMock()
    pg_conn.cursor.return_value.__enter__.return_value = cursor

    run_analyze(pg_conn, "ANALYZE public.documents", "ANALYZE public.transactions")

    pg_conn.commit.assert_called_once()
    pg_conn.set_session.assert_any_call(autocommit=True)
    pg_conn.set_session.assert_any_call(autocommit=False)
    assert cursor.execute.call_count == 2
