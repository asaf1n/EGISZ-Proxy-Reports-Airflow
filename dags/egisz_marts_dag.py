"""Самодостаточный DAG: единственное место обновления материализованных витрин.

Запускается публикацией активов, а не расписанием: обновление витрин — следствие того,
что изменились факты, а не того, кто именно их изменил. Производители (приём фактов
и суточное обслуживание) публикуют активы, этот DAG их потребляет.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, курсоры, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

import psycopg2
from airflow.sdk import Asset, Connection, dag, task

log = logging.getLogger(__name__)

DWH_CONN_ID = "dwh_egisz_pg"
DWH_POOL = "dwh_postgres"

# Активы связывают производителей фактов с DAG обновления витрин: REFRESH выполняется
# в одном месте (egisz_marts_dag), которое запускается по публикации актива.
ASSET_FACTS = Asset("egisz://facts")
ASSET_DICTIONARIES = Asset("egisz://dictionaries")

# Витрины отчётного слоя динамики (вкладки «Динамика по неделям/месяцам»).
REPORT_MARTS = (
    "public.rpt_documents_weekly",
    "public.rpt_error_breakdown_weekly",
    "public.rpt_documents_monthly",
    "public.rpt_error_breakdown_monthly",
)

# Дефолты настроек DAG; переопределяются переменной окружения EGISZ_<KEY> (env, не Airflow Variables).
DEFAULTS: dict[str, str | int] = {
    # Недельные и месячные срезы не нуждаются в пятиминутной свежести: полный пересчёт
    # агрегата по всему архиву на каждом батче приёма — цена без выгоды.
    "period_marts_interval_minutes": 60,
}


def _setting(key: str) -> str:
    """Настройка DAG из переменной окружения EGISZ_<KEY>, иначе из DEFAULTS.

    Читается и при парсинге (расписание), и внутри задач — без обращения к метабазе Airflow.
    Значения фиксированы в DEFAULTS и переопределяются переменной окружения процессов Airflow
    (см. deploy/README.md). Airflow Variables (метабаза) не используются —
    на Airflow 3 их чтение при парсинге в воркере подвешивало DAG.
    """
    return os.environ.get("EGISZ_" + key.upper(), str(DEFAULTS[key]))


def get_int(key: str) -> int:
    return int(_setting(key))


def connect_pg(conn_params: Any) -> psycopg2.extensions.connection:
    try:
        if isinstance(conn_params, str):
            return psycopg2.connect(conn_params)
        return psycopg2.connect(
            host=conn_params.host,
            port=conn_params.port,
            user=conn_params.login,
            password=conn_params.password,
            database=conn_params.schema,
        )
    except UnicodeDecodeError as exc:
        # Русифицированный PostgreSQL на Windows отвечает на отказ подключения текстом
        # в кодировке сервера (cp1251), а psycopg2 ждёт UTF-8 — реальная причина отказа
        # (неверный пароль/база, правило pg_hba) прячется за UnicodeDecodeError.
        detail = bytes(exc.object).decode("cp1251", errors="replace")
        raise psycopg2.OperationalError(
            f"PostgreSQL rejected the connection; server message: {detail}"
        ) from exc


def should_run_now(
    con: psycopg2.extensions.connection,
    job: str,
    min_interval_minutes: int,
) -> bool:
    """True, если с прошлого выполнения ``job`` прошло не меньше указанного интервала.

    Гейт каденции для задач, которые живут внутри более частого DAG: справочники
    и витрины динамики не нуждаются в пятиминутной свежести.
    """
    if min_interval_minutes <= 0:
        return True
    with con.cursor() as cur:
        cur.execute(
            "SELECT ran_at <= now() - make_interval(mins => %s) FROM etl_job_runs WHERE job = %s",
            (min_interval_minutes, job),
        )
        row = cur.fetchone()
    return True if row is None else bool(row[0])


def mark_job_run(con: psycopg2.extensions.connection, job: str) -> None:
    """Записать момент выполнения задачи с собственной каденцией."""
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_job_runs (job, ran_at)
            VALUES (%s, now())
            ON CONFLICT (job) DO UPDATE SET ran_at = now();
            """,
            (job,),
        )
    con.commit()


def _refresh_matview(con: psycopg2.extensions.connection, qualified_name: str) -> None:
    """Refresh a materialized view after facts change.

    CONCURRENTLY (needs the unique index + a populated matview) keeps dashboard reads
    unblocked during the ~seconds-long rebuild; falls back to a plain refresh if the
    matview was never populated. Runs in autocommit — REFRESH CONCURRENTLY cannot run
    inside a transaction block.
    """
    con.commit()
    previous_autocommit = con.autocommit
    con.set_session(autocommit=True)
    try:
        with con.cursor() as cur:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified_name}")
            except psycopg2.Error as exc:
                log.warning(
                    "CONCURRENTLY refresh of %s failed (%s); falling back to plain refresh",
                    qualified_name,
                    exc,
                )
                cur.execute(f"REFRESH MATERIALIZED VIEW {qualified_name}")
    finally:
        con.set_session(autocommit=previous_autocommit)


def refresh_error_breakdown(con: psycopg2.extensions.connection) -> None:
    """Refresh the rpt_error_breakdown materialized view after facts change."""
    _refresh_matview(con, "public.rpt_error_breakdown")


def refresh_report_marts(con: psycopg2.extensions.connection) -> None:
    """Refresh the periodic reporting marts.

    rpt_error_breakdown_* читают rpt_error_breakdown — вызывать ПОСЛЕ
    refresh_error_breakdown().
    """
    for matview in REPORT_MARTS:
        _refresh_matview(con, matview)


def run_analyze(con: psycopg2.extensions.connection, *statements: str) -> None:
    """Run ANALYZE outside a transaction (PostgreSQL forbids ANALYZE inside one).

    Read-only SELECTs leave psycopg2 in an open transaction; commit first so
    set_session(autocommit=True) is legal.
    """
    if not statements:
        return
    con.commit()
    previous_autocommit = con.autocommit
    con.set_session(autocommit=True)
    try:
        with con.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
    finally:
        con.set_session(autocommit=previous_autocommit)


def _dwh_connection():
    return connect_pg(Connection.get(DWH_CONN_ID))


@dag(
    dag_id="egisz_marts_dag",
    schedule=[ASSET_FACTS, ASSET_DICTIONARIES],
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "marts"],
)
def egisz_marts_pipeline() -> None:
    # Ретраи гасят транзиентный DeadlockDetected: обновление витрин пересекается
    # с приёмом фактов по блокировкам; REFRESH идемпотентен — повтор безопасен.
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def refresh_fact_marts() -> None:
        """Витрина разбивки ошибок — свежесть та же, что у фактов."""
        pg_conn = _dwh_connection()
        try:
            refresh_error_breakdown(pg_conn)
            run_analyze(pg_conn, "ANALYZE public.rpt_error_breakdown")
        finally:
            pg_conn.close()

    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def refresh_period_marts() -> bool:
        """Витрины динамики — полный агрегат по архиву, поэтому со своей каденцией."""
        pg_conn = _dwh_connection()
        try:
            interval_minutes = get_int("period_marts_interval_minutes")
            if not should_run_now(pg_conn, "refresh_period_marts", interval_minutes):
                log.info(
                    "Period marts refreshed less than %s minute(s) ago; skipping.",
                    interval_minutes,
                )
                return False
            refresh_report_marts(pg_conn)
            run_analyze(pg_conn, *(f"ANALYZE {matview}" for matview in REPORT_MARTS))
            mark_job_run(pg_conn, "refresh_period_marts")
            return True
        finally:
            pg_conn.close()

    # Порядок обязателен: недельные и месячные витрины читают rpt_error_breakdown.
    refresh_fact_marts() >> refresh_period_marts()


egisz_marts_pipeline()
