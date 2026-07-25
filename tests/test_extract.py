from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from conftest import load_dag_module

extract_dag = load_dag_module("egisz_etl_dag")

extract_exchangelog_batch = extract_dag.extract_exchangelog_batch
extract_message_registry_batch = extract_dag.extract_message_registry_batch
normalize_registry_key = extract_dag.normalize_registry_key
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
        patch("egisz_etl_dag.get_cursors", return_value={"last_logid": 100}),
        patch("egisz_etl_dag.pending_transform_tail", return_value=(50, 200)),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor") as fetch,
    ):
        result = extract_exchangelog_batch(pg_conn, fb_conn, raw_rows=2000, raw_rounds=3)

    fetch.assert_not_called()
    assert result == {"count": 0, "last_logid": 100, "cursor_logid": 200}


def test_extract_exchangelog_loads_from_source_when_raw_is_current(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    rows = [
        {
            "logid": 101,
            "logdate": None,
            "createdate": None,
            "msgid": None,
            "logstate": None,
            "logtext": None,
            "msgtext": None,
            "uri": "/emdr/callback",
        }
    ]

    with (
        patch("egisz_etl_dag.get_cursors", return_value={"last_logid": 100}),
        patch("egisz_etl_dag.pending_transform_tail", side_effect=[(0, 100), (0, 100)]),
        patch("egisz_etl_dag.fetch_exchangelog_after_cursor", return_value=rows) as fetch,
        patch("egisz_etl_dag.load_raw_logs") as load_raw,
        patch("egisz_etl_dag._analyze_exchangelog_raw") as analyze_raw,
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
    # Хвост опрашивается один раз на итерацию; третий опрос возвращает пустой остаток.
    pending_side_effects = [
        (10, 500),
        (5, 500),
        (0, 300),
    ]

    with (
        patch("egisz_etl_dag.pending_transform_tail", side_effect=pending_side_effects),
        patch("egisz_etl_dag.bounded_transform_to_logid", side_effect=[200, 300]),
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
            load_info,
            transform_rows=5000,
            transform_rounds=6,
            lag_logids=0,
        )

    assert transform.call_count == 2
    assert update.call_count == 2
    analyze_docs.assert_called_once_with(pg_conn)
    assert result["transformed"] == 150
    assert result["unlinked"] == 2
    assert result["sends_without_clinic"] == 1
    assert result["last_logid"] == 300


def test_transform_exchangelog_noop_when_tail_equals_watermark(pg_conn: MagicMock) -> None:
    load_info = {"count": 0, "last_logid": 100, "cursor_logid": 100}

    with patch("egisz_etl_dag.transform_raw_to_facts") as transform:
        result = transform_exchangelog_batch(
            pg_conn,
            load_info,
            transform_rows=5000,
            transform_rounds=6,
            lag_logids=0,
        )

    transform.assert_not_called()
    assert result["transformed"] == 0


def test_transform_holds_watermark_back_by_safety_lag(pg_conn: MagicMock) -> None:
    """Хвост журнала в пределах защитного запаса не разбирается: строка, опоздавшая
    на несколько позиций LOGID, иначе осталась бы ниже отметки навсегда."""
    load_info = {"count": 0, "last_logid": 100, "cursor_logid": 150}

    with patch("egisz_etl_dag.transform_raw_to_facts") as transform:
        result = transform_exchangelog_batch(
            pg_conn,
            load_info,
            transform_rows=5000,
            transform_rounds=6,
            lag_logids=1000,
        )

    transform.assert_not_called()
    assert result["transformed"] == 0
    assert result["last_logid"] == 100


def test_safe_transform_ceiling_keeps_lag_below_journal_tail() -> None:
    assert extract_dag.safe_transform_ceiling(watermark=100, tail_logid=5000, lag_logids=1000) == 4000
    # Запас шире доступного хвоста — разбирать нечего, отметка остаётся на месте.
    assert extract_dag.safe_transform_ceiling(watermark=100, tail_logid=500, lag_logids=1000) == 100
    assert extract_dag.safe_transform_ceiling(watermark=100, tail_logid=5000, lag_logids=0) == 5000


def test_normalize_registry_key_matches_sql_canonical_form() -> None:
    """Ключ реестра приводится к одному виду на обеих сторонах: без дефисов,
    без префикса urn:uuid: и угловых скобок, в верхнем регистре."""
    expected = "A07167955FA149D1BF532EFAD47EFA46"
    assert normalize_registry_key("a0716795-5fa1-49d1-bf53-2efad47efa46") == expected
    assert normalize_registry_key("urn:uuid:A0716795-5FA1-49D1-BF53-2EFAD47EFA46") == expected
    assert normalize_registry_key("<A07167955FA149D1BF532EFAD47EFA46>") == expected
    assert normalize_registry_key(None) is None
    assert normalize_registry_key("  ") is None


def test_extract_message_registry_advances_its_own_cursor(
    pg_conn: MagicMock,
    fb_conn: MagicMock,
) -> None:
    """Реестр подач читается keyset-курсором по EGMID и двигает собственную отметку."""
    rows = [(7, "MSG-1", "http://gost-1.lan:9945", "UID-1", None)]

    with (
        patch("egisz_etl_dag.get_cursors", return_value={"last_logid": 0, "last_egmid": 5}),
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
        )

    assert loaded == 1
    fetch.assert_called_once_with(fb_conn, after_egmid=5, limit=5000)
    load.assert_called_once_with(pg_conn, rows)
    update.assert_called_once_with(pg_conn, extract_dag.PIPELINE, egmid=7)


def test_run_analyze_commits_before_switching_autocommit(pg_conn: MagicMock) -> None:
    pg_conn.autocommit = False
    cursor = MagicMock()
    pg_conn.cursor.return_value.__enter__.return_value = cursor

    run_analyze(pg_conn, "ANALYZE public.documents", "ANALYZE public.transactions")

    pg_conn.commit.assert_called_once()
    pg_conn.set_session.assert_any_call(autocommit=True)
    pg_conn.set_session.assert_any_call(autocommit=False)
    assert cursor.execute.call_count == 2
