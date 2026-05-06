from __future__ import annotations

import argparse
import sys

from proxy_reports_etl.config import ConfigError, load_config_from_env
from proxy_reports_etl.fb_client import connect_fb, ping_fb
from proxy_reports_etl.pg_client import connect_pg, ping_pg
from proxy_reports_etl.etl import run_sync
from proxy_reports_etl.locks import PipelineLockBusyError


def _cmd_test_connections() -> int:
    cfg = load_config_from_env()
    fb = connect_fb(cfg.firebird)
    try:
        ping_fb(fb)
    finally:
        fb.close()

    pg = connect_pg(cfg.postgres)
    try:
        ping_pg(pg)
        pg.commit()
    finally:
        pg.close()
    print("ok")
    return 0


def _cmd_sync() -> int:
    cfg = load_config_from_env()
    fb = connect_fb(cfg.firebird)
    pg = connect_pg(cfg.postgres)
    try:
        try:
            stats = run_sync(cfg=cfg, fb_con=fb, pg_con=pg, log=print)
            print(
                f"stats fetched={stats.fetched} upserted={stats.upserted} "
                f"cursor_before={stats.last_cursor_before!r} cursor_after={stats.last_cursor_after!r}"
            )
            return 0
        except PipelineLockBusyError as e:
            print(f"skipped_lock: {e}", file=sys.stderr)
            return 75
    finally:
        try:
            fb.close()
        finally:
            pg.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="proxy-reports-etl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("test-connections", help="Check Firebird and Postgres connectivity.")
    sub.add_parser("sync", help="Run one ETL batch (incremental).")
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    p = build_parser()
    args = p.parse_args(argv)
    try:
        if args.cmd == "test-connections":
            return _cmd_test_connections()
        if args.cmd == "sync":
            return _cmd_sync()
        raise AssertionError(f"Unhandled cmd: {args.cmd}")
    except ConfigError as e:
        print(str(e), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
