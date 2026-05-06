from __future__ import annotations

from dataclasses import dataclass
import os


class ConfigError(ValueError):
    pass


def _env(name: str, *, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _req(name: str) -> str:
    v = _env(name)
    if not v:
        raise ConfigError(f"Missing required env var: {name}")
    return v


def _int(name: str, *, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"Invalid int for {name}: {raw!r}") from e


@dataclass(frozen=True)
class FirebirdConfig:
    dsn: str
    user: str
    password: str
    charset: str = "UTF8"


@dataclass(frozen=True)
class PostgresConfig:
    dsn: str


@dataclass(frozen=True)
class EtlConfig:
    pipeline: str
    source_sql: str
    cursor_column: str
    batch_size: int
    target_table: str


@dataclass(frozen=True)
class AppConfig:
    firebird: FirebirdConfig
    postgres: PostgresConfig
    etl: EtlConfig


def load_config_from_env() -> AppConfig:
    fb = FirebirdConfig(
        dsn=_req("FB_DSN"),
        user=_req("FB_USER"),
        password=_req("FB_PASSWORD"),
        charset=_env("FB_CHARSET", default="UTF8") or "UTF8",
    )
    pg = PostgresConfig(dsn=_req("PG_DSN"))
    etl = EtlConfig(
        pipeline=_env("ETL_PIPELINE", default="proxy_reports") or "proxy_reports",
        source_sql=_req("FB_SOURCE_SQL"),
        cursor_column=_env("ETL_CURSOR_COLUMN", default="LOGID") or "LOGID",
        batch_size=_int("ETL_BATCH_SIZE", default=500),
        target_table=_env("PG_TARGET_TABLE", default="proxy_reports_raw") or "proxy_reports_raw",
    )
    if etl.batch_size <= 0:
        raise ConfigError("ETL_BATCH_SIZE must be > 0")
    return AppConfig(firebird=fb, postgres=pg, etl=etl)
