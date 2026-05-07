from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class FirebirdConfig:
    """Firebird connection configuration."""
    dsn: str
    user: str
    password: str
    charset: str

    @staticmethod
    def from_env() -> FirebirdConfig:
        return FirebirdConfig(
            dsn=os.getenv("FB_DSN", "host.docker.internal/3050:proxy_egisz"),
            user=os.getenv("FB_USER", "sysdba"),
            password=os.getenv("FB_PASSWORD", "masterkey"),
            charset=os.getenv("FB_CHARSET", "WIN1251"),
        )


@dataclass
class PostgresConfig:
    """PostgreSQL DWH connection configuration."""
    dsn: str

    @staticmethod
    def from_env() -> PostgresConfig:
        return PostgresConfig(
            dsn=os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/dwh_egisz"),
        )


@dataclass
class ELTConfig:
    """ELT pipeline configuration with dual cursor support."""
    pipeline: str
    batch_size: int
    log_cursor_column: str  # EXCHANGELOG cursor (usually LOGID)
    msg_cursor_column: str  # EGISZ_MESSAGES cursor (usually EGMID)
    log_source_table: str
    msg_source_table: str
    log_target_table: str
    fact_target_table: str

    @staticmethod
    def from_env() -> ELTConfig:
        return ELTConfig(
            pipeline=os.getenv("ELT_PIPELINE", "egisz"),
            batch_size=int(os.getenv("ELT_BATCH_SIZE", "500")),
            log_cursor_column=os.getenv("LOG_CURSOR_COLUMN", "LOGID"),
            msg_cursor_column=os.getenv("MSG_CURSOR_COLUMN", "EGMID"),
            log_source_table=os.getenv("LOG_SOURCE_TABLE", "EXCHANGELOG"),
            msg_source_table=os.getenv("MSG_SOURCE_TABLE", "EGISZ_MESSAGES"),
            log_target_table=os.getenv("LOG_TARGET_TABLE", "egisz_raw"),
            fact_target_table=os.getenv("FACT_TARGET_TABLE", "fact_egisz_transactions"),
        )
