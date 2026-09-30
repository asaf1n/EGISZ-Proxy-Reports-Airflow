from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowSkipException

from conftest import load_dag_module

maintenance_dag = load_dag_module("egisz_maintenance_dag")

check_journal_window = maintenance_dag.check_journal_window
coalesce_logid_windows = maintenance_dag.coalesce_logid_windows
transform_missing_windows = maintenance_dag.transform_missing_windows

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def _cursors(extract_logid: int) -> dict[str, int]:
    return {
        "extract_logid_cursor": extract_logid,
        "transform_logid_cursor": extract_logid,
        "extract_egmid_cursor": 0,
    }


def test_check_skips_when_every_source_row_is_in_raw() -> None:
    pg_conn = MagicMock()
    fb_conn = MagicMock()

    with (
        patch("egisz_maintenance_dag.get_cursors", return_value=_cursors(1000)),
        patch("egisz_maintenance_dag.raw_window_low", return_value=100) as low,
        patch("egisz_maintenance_dag.fetch_source_logids_range", return_value={100, 101}),
        patch("egisz_maintenance_dag.fetch_raw_logids_range", return_value={100, 101}),
        patch("egisz_maintenance_dag.load_raw_logs") as load_raw,
        pytest.raises(AirflowSkipException),
    ):
        check_journal_window(pg_conn, fb_conn, lookback_days=7, now=NOW)

    assert low.call_args.kwargs["since"] == datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    load_raw.assert_not_called()


def test_check_loads_and_transforms_only_missing_rows() -> None:
    pg_conn = MagicMock()
    fb_conn = MagicMock()
    late_rows = [{"logid": 7}]

    with (
        patch("egisz_maintenance_dag.get_cursors", return_value=_cursors(10)),
        patch("egisz_maintenance_dag.raw_window_low", return_value=1),
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
        totals = check_journal_window(pg_conn, fb_conn, lookback_days=7, now=NOW)

    fetch_rows.assert_called_once_with(fb_conn, [7])
    load_raw.assert_called_once_with(pg_conn, late_rows)
    transform.assert_called_once_with(pg_conn, [7])
    assert totals == {
        "missing": 1,
        "transformed": 3,
        "unlinked": 0,
        "sends_without_clinic": 0,
    }


def test_check_skips_when_the_extract_cursor_has_not_moved() -> None:
    with (
        patch("egisz_maintenance_dag.get_cursors", return_value=_cursors(0)),
        patch("egisz_maintenance_dag.raw_window_low") as low,
        pytest.raises(AirflowSkipException),
    ):
        check_journal_window(MagicMock(), MagicMock(), lookback_days=7, now=NOW)

    low.assert_not_called()


def test_check_skips_when_window_lies_above_the_extract_cursor() -> None:
    """Окно целиком выше отметки — сравнивать нечего, недостачи это не означает."""
    with (
        patch("egisz_maintenance_dag.get_cursors", return_value=_cursors(100)),
        patch("egisz_maintenance_dag.raw_window_low", return_value=500),
        patch("egisz_maintenance_dag.fetch_source_logids_range") as source,
        pytest.raises(AirflowSkipException),
    ):
        check_journal_window(MagicMock(), MagicMock(), lookback_days=7, now=NOW)

    source.assert_not_called()


def test_check_skips_when_raw_has_rows_the_source_no_longer_has() -> None:
    """В raw есть лишнее, а недостающих нет — строки удалены на стороне шлюза."""
    with (
        patch("egisz_maintenance_dag.get_cursors", return_value=_cursors(10)),
        patch("egisz_maintenance_dag.raw_window_low", return_value=1),
        patch("egisz_maintenance_dag.fetch_source_logids_range", return_value={5, 6}),
        patch("egisz_maintenance_dag.fetch_raw_logids_range", return_value={5, 6, 7}),
        patch("egisz_maintenance_dag.load_raw_logs") as load_raw,
        pytest.raises(AirflowSkipException),
    ):
        check_journal_window(MagicMock(), MagicMock(), lookback_days=7, now=NOW)

    load_raw.assert_not_called()


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
