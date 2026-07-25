from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from conftest import load_dag_module

maintenance_dag = load_dag_module("egisz_maintenance_dag")

reconcile_window_since = maintenance_dag.reconcile_window_since
reconcile_journal_window = maintenance_dag.reconcile_journal_window
coalesce_logid_windows = maintenance_dag.coalesce_logid_windows
transform_missing_windows = maintenance_dag.transform_missing_windows


def test_reconcile_window_since_uses_lookback_days() -> None:
    now = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)

    since = reconcile_window_since(30, now=now)

    assert since == datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)


def test_reconcile_walks_window_in_chunks_without_loading_it_whole() -> None:
    """Множества сравниваются постранично: за раз запрашивается только шаг по LOGID."""
    pg_conn = MagicMock()
    fb_conn = MagicMock()

    with (
        patch("egisz_maintenance_dag.source_logid_bounds", return_value=(1, 25)),
        patch("egisz_maintenance_dag.fetch_source_logids_range", return_value=set()) as source,
        patch("egisz_maintenance_dag.fetch_raw_logids_range", return_value=set()) as raw,
        patch("egisz_maintenance_dag.load_raw_logs") as load_raw,
    ):
        totals = reconcile_journal_window(
            pg_conn,
            fb_conn,
            lookback_days=2,
            chunk_logids=10,
        )

    assert totals["missing"] == 0
    load_raw.assert_not_called()
    assert [call.kwargs for call in source.call_args_list] == [
        {"low": 1, "high": 10},
        {"low": 11, "high": 20},
        {"low": 21, "high": 25},
    ]
    assert [call.kwargs for call in raw.call_args_list] == [
        {"low": 1, "high": 10},
        {"low": 11, "high": 20},
        {"low": 21, "high": 25},
    ]


def test_reconcile_loads_and_transforms_only_missing_rows() -> None:
    pg_conn = MagicMock()
    fb_conn = MagicMock()
    late_rows = [{"logid": 7}]

    with (
        patch("egisz_maintenance_dag.source_logid_bounds", return_value=(1, 10)),
        patch("egisz_maintenance_dag.fetch_source_logids_range", return_value={5, 6, 7}),
        patch("egisz_maintenance_dag.fetch_raw_logids_range", return_value={5, 6}),
        patch(
            "egisz_maintenance_dag.fetch_exchangelog_by_logids",
            return_value=late_rows,
        ) as fetch_rows,
        patch("egisz_maintenance_dag.load_raw_logs") as load_raw,
        patch("egisz_maintenance_dag.run_analyze"),
        patch(
            "egisz_maintenance_dag.transform_missing_windows",
            return_value={"transformed": 3, "unlinked": 0, "sends_without_clinic": 0},
        ) as transform,
    ):
        totals = reconcile_journal_window(
            pg_conn,
            fb_conn,
            lookback_days=2,
            chunk_logids=100,
        )

    fetch_rows.assert_called_once_with(fb_conn, [7])
    load_raw.assert_called_once_with(pg_conn, late_rows)
    transform.assert_called_once_with(pg_conn, [7])
    assert totals == {
        "missing": 1,
        "transformed": 3,
        "unlinked": 0,
        "sends_without_clinic": 0,
    }


def test_reconcile_skips_empty_source_window() -> None:
    pg_conn = MagicMock()
    fb_conn = MagicMock()

    with (
        patch("egisz_maintenance_dag.source_logid_bounds", return_value=(0, 0)),
        patch("egisz_maintenance_dag.fetch_source_logids_range") as source,
    ):
        totals = reconcile_journal_window(pg_conn, fb_conn, lookback_days=2, chunk_logids=10)

    source.assert_not_called()
    assert totals["missing"] == 0


def test_coalesce_logid_windows_merges_only_consecutive_runs() -> None:
    assert coalesce_logid_windows([5, 6, 7, 20, 21, 40]) == [(5, 7), (20, 21), (40, 40)]


def test_transform_missing_windows_runs_one_transform_per_dense_run() -> None:
    con = MagicMock()

    with patch(
        "egisz_maintenance_dag.transform_raw_to_facts",
        return_value={"transformed": 2, "unlinked": 1, "sends_without_clinic": 0},
    ) as transform:
        totals = transform_missing_windows(con, [5, 6, 20])

    assert [call.kwargs for call in transform.call_args_list] == [
        {"from_logid": 4, "to_logid": 6},
        {"from_logid": 19, "to_logid": 20},
    ]
    assert totals == {"transformed": 4, "unlinked": 2, "sends_without_clinic": 0}
