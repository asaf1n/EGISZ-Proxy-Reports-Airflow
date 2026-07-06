from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from egisz_elt.reconcile import (
    ReconcileWindowVolumeError,
    fetch_reconcile_window_sets,
    reconcile_window_since,
)


def test_reconcile_window_since_uses_lookback_days() -> None:
    now = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)

    since = reconcile_window_since(30, now=now)

    assert since == datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)


def test_fetch_reconcile_window_sets_applies_max_logids_within_lookback_window() -> None:
    """Guard counts only rows inside lookback_days, not the whole journal."""
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    since = reconcile_window_since(7, now=now)
    pg_conn = MagicMock()
    fb_conn = MagicMock()

    with (
        patch("egisz_elt.reconcile.count_exchangelog_rows", return_value=50) as count_rows,
        patch(
            "egisz_elt.reconcile.fetch_exchangelog_logids",
            return_value={101, 102},
        ) as fetch_source,
        patch(
            "egisz_elt.reconcile.get_all_raw_logids",
            return_value={101},
        ) as fetch_raw,
    ):
        result_since, source, raw, source_count = fetch_reconcile_window_sets(
            pg_conn,
            fb_conn,
            lookback_days=7,
            max_logids=100,
            now=now,
        )

    assert result_since == since
    assert source == {101, 102}
    assert raw == {101}
    assert source_count == 50
    count_rows.assert_called_once_with(fb_conn, since=since)
    fetch_source.assert_called_once_with(fb_conn, since=since)
    fetch_raw.assert_called_once_with(pg_conn, since=since)


def test_fetch_reconcile_window_sets_raises_when_window_exceeds_max_logids() -> None:
    pg_conn = MagicMock()
    fb_conn = MagicMock()
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)

    with (
        patch("egisz_elt.reconcile.count_exchangelog_rows", return_value=150) as count_rows,
        patch("egisz_elt.reconcile.fetch_exchangelog_logids") as fetch_source,
        patch("egisz_elt.reconcile.get_all_raw_logids") as fetch_raw,
    ):
        with pytest.raises(ReconcileWindowVolumeError, match="150 LOGID\\(s\\) in the 7-day window"):
            fetch_reconcile_window_sets(
                pg_conn,
                fb_conn,
                lookback_days=7,
                max_logids=100,
                now=now,
            )

    count_rows.assert_called_once()
    fetch_source.assert_not_called()
    fetch_raw.assert_not_called()


def test_large_journal_outside_window_does_not_trigger_guard() -> None:
    """A huge total journal is fine when the lookback window stays under max_logids."""
    pg_conn = MagicMock()
    fb_conn = MagicMock()

    with (
        patch("egisz_elt.reconcile.count_exchangelog_rows", return_value=100) as count_rows,
        patch("egisz_elt.reconcile.fetch_exchangelog_logids", return_value=set()) as fetch_source,
        patch("egisz_elt.reconcile.get_all_raw_logids", return_value=set()) as fetch_raw,
    ):
        _, source, raw, source_count = fetch_reconcile_window_sets(
            pg_conn,
            fb_conn,
            lookback_days=30,
            max_logids=100,
        )

    assert source_count == 100
    assert source == set()
    assert raw == set()
    assert count_rows.call_args.kwargs["since"] is not None
    assert fetch_source.call_args.kwargs["since"] == count_rows.call_args.kwargs["since"]
    assert fetch_raw.call_args.kwargs["since"] == count_rows.call_args.kwargs["since"]
