from __future__ import annotations

import re
from pathlib import Path

from conftest import load_dag_module

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
REPO_ROOT = DAGS_DIR.parents[1]
PARTS_DIR = REPO_ROOT / "db" / "parts"
DWH_POOL = "dwh_postgres"


def _read(dag_file: str) -> str:
    return (DAGS_DIR / dag_file).read_text(encoding="utf-8")


DAG_STEMS = ("egisz_extract_dag", "egisz_dimensions_dag", "egisz_reconcile_dag")


def test_dag_setting_keys_do_not_collide() -> None:
    """Каждый DAG объявляет только свои настройки — ключи не пересекаются между файлами."""
    seen: dict[str, str] = {}
    for stem in DAG_STEMS:
        for key in load_dag_module(stem).DEFAULTS:
            assert key not in seen, f"настройка {key!r} объявлена и в {seen[key]}, и в {stem}"
            seen[key] = stem


def test_settings_resolve_from_env_or_defaults_without_metabase() -> None:
    """Все настройки (расписания и параметры задач) — из env EGISZ_<KEY> или DEFAULTS.

    Airflow Variables не используются: их top-level чтение при парсинге DAG в воркере
    Airflow 3 уходило в supervisor RPC и подвешивало DAG. Значения обязаны резолвиться
    без обращения к метабазе.
    """
    import os

    for stem in DAG_STEMS:
        module = load_dag_module(stem)
        assert not hasattr(module, "Variable"), f"{stem}: Variable не должен импортироваться"

        for key in module.DEFAULTS:
            assert module._setting(key) == str(module.DEFAULTS[key]), (stem, key)
            env_name = f"EGISZ_{key.upper()}"
            os.environ[env_name] = "override"
            try:
                assert module._setting(key) == "override", (stem, key)
            finally:
                del os.environ[env_name]

        # get_int парсит целочисленные параметры задач поверх _setting.
        if hasattr(module, "get_int"):
            int_key = next(k for k in module.DEFAULTS if not k.endswith("_schedule"))
            assert module.get_int(int_key) == int(module.DEFAULTS[int_key]), stem


def test_extract_dag_uses_entity_named_tasks_and_metadata_only_xcom() -> None:
    src = _read("egisz_extract_dag.py")

    assert 'dag_id="egisz_extract_dag"' in src
    assert "def extract_exchangelog" in src
    assert "def transform_exchangelog" in src
    assert "def load_exchangelog_batch" not in src
    assert "def process_exchangelog_batch" not in src
    assert "def has_new_exchangelog_rows" not in src
    assert "def build_document_facts" not in src
    assert "def refresh_materialized_views" not in src
    assert "def advance_logid_watermark" not in src
    assert "def extract_and_load_batch" not in src
    assert "def analyze_staging" not in src
    assert "def transform_data" not in src
    assert "def update_watermark" not in src

    # Таск-обёртки тонкие: вся работа в модульных функциях того же файла.
    assert "extract_exchangelog_batch(" in src
    assert "transform_exchangelog_batch(" in src

    assert '"rows":' not in src
    assert '_setting("extract_schedule")' in src
    assert 'get_int("extract_raw_rows")' in src
    assert 'get_int("extract_raw_rounds")' in src
    assert 'get_int("transform_rows")' in src
    assert 'get_int("transform_rounds")' in src
    assert 'pool="dwh_postgres"' in src or "pool=DWH_POOL" in src
    # Транзиентный DeadlockDetected (maintenance-прогон схемы поверх 5-минутного батча)
    # не должен красить ран: transform идемпотентен, повтор безопасен.
    assert "retries=2" in src
    assert "retry_delay=timedelta(minutes=1)" in src
    assert "BATCH_SIZE = 5000" not in src
    assert "@task.short_circuit" not in src

    assert "transform_exchangelog(extracted)" in src
    assert "extracted >> transformed" not in src
    assert "get_current_context" not in src

    # Витрины динамики: отдельная задача после transform, гейт по метаданным батча.
    assert 'task_id="refresh_report_marts"' in src
    assert "refresh_report_marts_task(transformed)" in src


def test_dimensions_dag_owns_dimension_sync_and_mart_maintenance() -> None:
    src = _read("egisz_dimensions_dag.py")

    assert 'dag_id="egisz_dimensions_dag"' in src
    assert "def sync_dimensions" in src
    assert "def dimensions_changed" not in src
    assert "def maintain_enriched_ui" not in src
    assert "sync_directories" in src
    assert "reconcile_document_attributes_ui" in src
    assert '_setting("dimensions_schedule")' in src
    assert "@task.short_circuit" not in src
    assert "pool=DWH_POOL" in src or 'pool="dwh_postgres"' in src
    # Тот же риск DeadlockDetected, что у transform: reconcile идемпотентен.
    assert "retries=2" in src


def test_reconcile_dag_does_full_constancy_check_without_moving_watermark() -> None:
    src = _read("egisz_reconcile_dag.py")

    assert 'dag_id="egisz_reconcile_dag"' in src
    assert "def reconcile_proxy_raw" in src
    assert "def reconcile_late_arrivals" not in src

    assert "fetch_reconcile_window_sets(" in src
    assert "source_logids - raw_logids" in src
    assert "lookback_days=lookback_days" in src
    assert "max_logids=max_logids" in src
    assert 'get_int("reconcile_lookback_days")' in src
    assert "fetch_exchangelog_logids_in_band" not in src
    assert "get_raw_logids_in_band" not in src
    assert "RECONCILE_WATERMARK_LOOKBACK_LOGIDS" not in src

    # Watermark двигает только extract: reconcile не несёт update_cursors.
    assert "def update_cursors" not in src
    assert "update_cursors(" not in src

    assert 'get_int("reconcile_max_logids")' in src

    assert '_setting("reconcile_schedule")' in src
    assert "pending_transform_tail" in src
    assert "AirflowSkipException" in src
    assert "backfill_semd_codes" not in src
    assert "retries=2" in src


def test_dag_files_are_self_contained_units() -> None:
    """DAG-файл разворачивается на целевой Airflow как есть: ни пакета, ни PYTHONPATH."""
    for path in sorted(DAGS_DIR.glob("egisz_*.py")):
        src = path.read_text(encoding="utf-8")
        assert "egisz_elt" not in src, path.name
        assert "_install_embedded_egisz_elt" not in src, path.name
        # Настройки читаются при импорте — метабаза Airflow может быть недоступна.
        assert "def _setting" in src, path.name
        # Airflow 3: Task SDK вместо снятых путей airflow.decorators / airflow.models.
        assert "from airflow.sdk import" in src, path.name
        assert "from airflow.decorators import" not in src, path.name
        assert "from airflow.hooks.base import" not in src, path.name
        assert "from airflow.models import" not in src, path.name


def test_report_marts_refresh_matches_sql_layer() -> None:
    """Список обновляемых витрин в DAG-ах совпадает с матвью недельного и месячного слоёв."""
    weekly_sql = (PARTS_DIR / "85_views_weekly.sql").read_text(encoding="utf-8")
    monthly_sql = (PARTS_DIR / "86_views_monthly.sql").read_text(encoding="utf-8")
    declared = set(
        re.findall(
            r"CREATE MATERIALIZED VIEW (public\.\w+)",
            weekly_sql + monthly_sql,
        )
    )

    for stem in ("egisz_extract_dag", "egisz_dimensions_dag", "egisz_reconcile_dag"):
        assert set(load_dag_module(stem).REPORT_MARTS) == declared, stem

    # Идемпотентность каркаса: DROP в 60, REFRESH + ANALYZE в 90, подключение части в init.
    drops = (PARTS_DIR / "60_drop_dependents.sql").read_text(encoding="utf-8")
    finalize = (PARTS_DIR / "90_views_health_and_finalize.sql").read_text(encoding="utf-8")
    init = (PARTS_DIR.parent / "dwh_init.sql").read_text(encoding="utf-8")

    assert "\\i db/parts/86_views_monthly.sql" in init
    for matview in declared:
        assert f"DROP MATERIALIZED VIEW IF EXISTS {matview} CASCADE" in drops, matview
        assert f"REFRESH MATERIALIZED VIEW {matview}" in finalize, matview
        assert f"ANALYZE {matview}" in finalize, matview

    # REFRESH CONCURRENTLY в DAG-ах требует уникального индекса на каждой витрине.
    for matview in declared:
        table = matview.split(".", 1)[1]
        assert re.search(rf"CREATE UNIQUE INDEX[^;]+ON {matview}\b", weekly_sql + monthly_sql), table


def test_all_dag_files_compile() -> None:
    import py_compile

    for path in sorted(DAGS_DIR.glob("egisz_*.py")):
        py_compile.compile(str(path), doraise=True)


def test_up_ps1_provisions_airflow_pool_and_connections() -> None:
    src = Path(__file__).resolve().parents[1].joinpath("up.ps1").read_text(encoding="utf-8")
    assert "Restore-AirflowStatefulSetsAfterStop" in src
    assert "Ensure-AirflowStatefulSetReplicas" in src
    assert "airflow-redis" in src
    assert "Sync-AirflowWorkerReplicas" not in src
    assert "Initialize-AirflowDwhPool" in src
    assert "pools', 'set', $DwhPoolName" in src or "pools set" in src
    assert "dwh_postgres" in src
    # Airflow Variables больше не провижинятся: настройки читаются из env/DEFAULTS.
    assert "Initialize-AirflowEgiszVariables" not in src
    assert "egisz-variables" not in src
    # Подключения хранятся в метабазе Airflow, а не подмешиваются секретом в env.
    assert "Initialize-AirflowEgiszConnections" in src
    assert "k8s\\airflow\\egisz-connections.json" in src
    assert "'connections', 'add'" in src
    assert "Test-AirflowConnectionsFromSecret" not in src
    assert "AIRFLOW_CONN_DWH_EGISZ_PG" not in src
    # Airflow 3: api-server и dag-processor вместо webserver, чарт закреплён.
    assert "component=api-server,release=airflow" in src
    assert "component=dag-processor,release=airflow" in src
    assert "component=webserver" not in src
    assert "airflow-webserver" not in src
    assert "/api/v2/monitor/health" in src
    assert "--version $AirflowChartVersion" in src
    assert "Initialize-EgiszDags" not in src
    assert "dags', 'unpause" not in src
    assert "egisz-airflow-worker:latest" in src
    assert "egisz-metabase:latest" in src
    assert "Get-DashboardsManifestHash" in src
    assert 'egisz-metabase:${metabaseTag}' in src
    assert "Get-LoadBalancerPortForwardConflict" in src
    assert "Sync-MetabaseDashboardArtifacts" in src
    assert "Test-MetabaseIntegrationDashboard" in src
    assert "verify_metabase_integration.py" in src
    assert "Test-MetabaseManifestUnchanged" in src
    assert "metabase-deployed-manifest" in src


def test_airflow_stack_targets_one_version() -> None:
    """Пин зависимости, базовый образ и airflowVersion чарта не должны расходиться."""
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dockerfile = (REPO_ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    values = (REPO_ROOT / "k8s" / "airflow" / "values.yaml").read_text(encoding="utf-8")

    pinned = re.search(r'"apache-airflow==([\d.]+)"', pyproject)
    assert pinned, "версия apache-airflow не закреплена в pyproject.toml"
    version = pinned.group(1)
    assert version.startswith("3."), "прод-контур работает на Airflow 3"

    assert f"FROM apache/airflow:{version}-python3.11" in dockerfile
    assert f'airflowVersion: "{version}"' in values

    # Подключения не подмешиваются секретом в окружение подов.
    assert "extraEnvFrom" not in values
    assert "secretRef" not in values
    # Airflow 3: параметры парсинга живут в [dag_processor], а не в [scheduler].
    assert "dag_processor:" in values
    assert "min_file_process_interval" in values
    assert "webserver:" not in values
    assert "apiServer:" in values


def test_dags_expose_expected_tasks_and_dependencies() -> None:
    """DAG-объекты собираются из файлов через Task SDK — без метабазы Airflow.

    Декоратор @dag возвращает готовый DAG, поэтому граф задач проверяется тем же
    вызовом, который выполняет сам файл при парсинге.
    """
    extract = load_dag_module("egisz_extract_dag").egisz_extract_pipeline()
    dimensions = load_dag_module("egisz_dimensions_dag").egisz_dimensions_pipeline()
    reconcile = load_dag_module("egisz_reconcile_dag").egisz_reconcile_pipeline()

    assert extract.dag_id == "egisz_extract_dag"
    assert dimensions.dag_id == "egisz_dimensions_dag"
    assert reconcile.dag_id == "egisz_reconcile_dag"

    # Пул провижинится отдельно (up.ps1 / внешняя инструкция) — задачи обязаны его требовать.
    pooled = {
        task.task_id
        for dag in (extract, dimensions, reconcile)
        for task in dag.tasks
        if task.pool == DWH_POOL
    }
    assert pooled == {
        "transform_exchangelog",
        "refresh_report_marts",
        "sync_dimensions",
        "reconcile_proxy_raw",
    }

    assert {t.task_id for t in extract.tasks} == {
        "extract_exchangelog",
        "transform_exchangelog",
        "refresh_report_marts",
    }
    assert {t.task_id for t in dimensions.tasks} == {"sync_dimensions"}
    assert {t.task_id for t in reconcile.tasks} == {"reconcile_proxy_raw"}

    assert extract.task_dict["extract_exchangelog"].downstream_task_ids == {
        "transform_exchangelog"
    }
    assert extract.task_dict["transform_exchangelog"].downstream_task_ids == {
        "refresh_report_marts"
    }
    assert reconcile.task_dict["reconcile_proxy_raw"].downstream_task_ids == set()
