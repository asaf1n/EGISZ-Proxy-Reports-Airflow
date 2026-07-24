from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from conftest import load_dag_module

extract_dag = load_dag_module("egisz_extract_dag")

extract_exchangelog_batch = extract_dag.extract_exchangelog_batch
transform_exchangelog_batch = extract_dag.transform_exchangelog_batch
run_analyze = extract_dag.run_analyze


@pytest.fixture
def pg_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def fb_conn() -> MagicMock:
    return MagicMock()


def test_extract_exchangelog_defers_fetch_when_raw_tail_exists(pg_conn: MagicMock, fb_conn: MagicMock) -> None:
    with (
        patch("egisz_extract_dag.get_cursors", return_value={"last_logid": 100}),
        patch("egisz_extract_dag.pending_transform_tail", return_value=(50, 200)),
        patch("egisz_extract_dag.fetch_exchangelog_after_cursor") as fetch,
    ):
        result = extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=2000, raw_rounds=3)

    fetch.assert_not_called()
    assert result == {"count": 0, "last_logid": 100, "cursor_logid": 200}


def test_extract_exchangelog_loads_from_source_when_raw_is_current(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    rows = [{"logid": 101, "logdate": None, "createdate": None, "msgid": None, "logstate": None, "logtext": None, "msgtext": None}]

    with (
        patch("egisz_extract_dag.get_cursors", return_value={"last_logid": 100}),
        patch("egisz_extract_dag.pending_transform_tail", side_effect=[(0, 100), (0, 100)]),
        patch("egisz_extract_dag.fetch_exchangelog_after_cursor", return_value=rows) as fetch,
        patch("egisz_extract_dag.load_raw_logs") as load_raw,
        patch("egisz_extract_dag._analyze_exchangelog_raw") as analyze_raw,
    ):
        result = extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=2000, raw_rounds=3)

    fetch.assert_called_once_with(fb_conn, after_logid=100, limit=2000)
    load_raw.assert_called_once_with(pg_conn, rows)
    analyze_raw.assert_called_once_with(pg_conn)
    assert result["count"] == 1
    assert result["last_logid"] == 100
    assert result["cursor_logid"] == 101


def test_transform_exchangelog_runs_multiple_iterations(pg_conn: MagicMock) -> None:
    load_info = {"count": 0, "last_logid": 100, "cursor_logid": 500}
    pending_side_effects = [
        (10, 500),
        (5, 500),
        (5, 500),
        (0, 300),
        (0, 300),
    ]

    with (
        patch("egisz_extract_dag.pending_transform_tail", side_effect=pending_side_effects),
        patch("egisz_extract_dag.bounded_transform_to_logid", side_effect=[200, 300]),
        patch("egisz_extract_dag.transform_raw_to_facts", side_effect=[100, 50]) as transform,
        patch("egisz_extract_dag.update_cursors") as update,
        patch("egisz_extract_dag._analyze_exchangelog_documents") as analyze_docs,
        patch("egisz_extract_dag.refresh_error_breakdown"),
    ):
        result = transform_exchangelog_batch(
            pg_conn,
            load_info,
            transform_rows=5000,
            transform_rounds=6,
        )

    assert transform.call_count == 2
    assert update.call_count == 2
    analyze_docs.assert_called_once_with(pg_conn)
    assert result["transformed"] == 150
    assert result["last_logid"] == 300


def test_transform_exchangelog_noop_when_tail_equals_watermark(pg_conn: MagicMock) -> None:
    load_info = {"count": 0, "last_logid": 100, "cursor_logid": 100}

    with patch("egisz_extract_dag.transform_raw_to_facts") as transform:
        result = transform_exchangelog_batch(pg_conn, load_info, transform_rows=5000, transform_rounds=6)

    transform.assert_not_called()
    assert result["transformed"] == 0


def test_run_analyze_commits_before_switching_autocommit(pg_conn: MagicMock) -> None:
    pg_conn.autocommit = False
    cursor = MagicMock()
    pg_conn.cursor.return_value.__enter__.return_value = cursor

    run_analyze(pg_conn, "ANALYZE public.documents", "ANALYZE public.transactions")

    pg_conn.commit.assert_called_once()
    pg_conn.set_session.assert_any_call(autocommit=True)
    pg_conn.set_session.assert_any_call(autocommit=False)
    assert cursor.execute.call_count == 2
