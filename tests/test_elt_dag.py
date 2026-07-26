from __future__ import annotations

import re
from pathlib import Path

from conftest import load_dag_module, sql_section

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
REPO_ROOT = DAGS_DIR.parent
PARTS_DIR = REPO_ROOT / "db"
DWH_POOL = "dwh_postgres"


def _read(dag_file: str) -> str:
    return (DAGS_DIR / dag_file).read_text(encoding="utf-8")


DAG_STEMS = ("egisz_etl_dag", "egisz_marts_dag", "egisz_reconcile_maintenance_dag")


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
    src = _read("egisz_etl_dag.py")

    assert 'dag_id="egisz_etl_dag"' in src
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
    assert '_setting("etl_schedule")' in src
    assert 'get_int("extract_raw_rows")' in src
    assert 'get_int("extract_raw_rounds")' in src
    assert 'get_int("transform_rows")' in src
    assert 'get_int("transform_rounds")' in src
    assert 'pool="dwh_postgres"' in src or "pool=DWH_POOL" in src
    # Транзиентный DeadlockDetected (суточное обслуживание поверх 5-минутного батча)
    # не должен красить ран: transform идемпотентен, повтор безопасен.
    assert "retries=2" in src
    assert "retry_delay=timedelta(minutes=1)" in src
    assert "BATCH_SIZE = 5000" not in src
    assert "@task.short_circuit" not in src

    assert "transform_exchangelog(extracted)" in src
    assert "get_current_context" not in src

    # Реестр подач читается до transform: без него ответ ЕГИСЗ не с чем связать.
    assert "def extract_message_registry" in src
    assert 'get_int("registry_rows")' in src
    assert "extracted >> registry >> dictionaries >> transformed" in src

    # Справочники живут здесь, но со своей каденцией; проход по архиву — в обслуживании.
    assert "def sync_dictionaries" in src
    assert "sync_directories" in src
    assert 'should_run_now(pg_conn, "sync_dictionaries"' in src
    assert "reconcile_document_attributes_ui" not in src

    # Отметка держится ниже хвоста журнала на защитный запас.
    assert 'get_int("etl_lag_logids")' in src
    assert "safe_transform_ceiling(" in src

    # Витрины обновляет отдельный DAG-потребитель: здесь только публикация актива.
    assert "outlets=[ASSET_FACTS]" in src
    assert "REFRESH MATERIALIZED VIEW" not in src
    assert "REPORT_MARTS" not in src


def test_marts_dag_is_the_only_place_that_refreshes_matviews() -> None:
    marts = _read("egisz_marts_dag.py")
    etl = _read("egisz_etl_dag.py")
    maintenance = _read("egisz_reconcile_maintenance_dag.py")

    assert 'dag_id="egisz_marts_dag"' in marts
    # Запускается публикацией актива, а не расписанием: обновление витрин — следствие
    # изменения фактов, а не того, кто именно их изменил.
    assert "schedule=[ASSET_FACTS, ASSET_DICTIONARIES]" in marts
    assert "def refresh_fact_marts" in marts
    assert "def refresh_period_marts" in marts
    assert "refresh_fact_marts() >> refresh_period_marts()" in marts
    assert 'should_run_now(pg_conn, "refresh_period_marts"' in marts

    # Ни один другой DAG матвью не обновляет и не несёт их список.
    for src in (etl, maintenance):
        assert "refresh_report_marts" not in src
        assert "_refresh_matview" not in src
        assert "REFRESH MATERIALIZED VIEW" not in src


def test_maintenance_dag_corrects_journal_without_moving_watermark() -> None:
    src = _read("egisz_reconcile_maintenance_dag.py")

    assert 'dag_id="egisz_reconcile_maintenance_dag"' in src
    assert "def reconcile_journal_tail" in src
    assert "def reconcile_archive_attributes" in src
    assert "def maintain_partitions" in src

    # Сверка идёт шагами по LOGID: окно целиком в память воркера не поднимается.
    assert "reconcile_journal_window(" in src
    assert "source_logids - raw_logids" in src
    assert 'get_int("reconcile_chunk_logids")' in src
    # Штатное окно — узкое; широкое доступно ручным прогоном (params.deep).
    module = load_dag_module("egisz_reconcile_maintenance_dag")
    assert module.DEFAULTS["reconcile_lookback_days"] < module.DEFAULTS["reconcile_deep_lookback_days"]
    assert 'params={"deep": False}' in src
    assert "fetch_exchangelog_logids_in_band" not in src
    assert "ReconcileWindowVolumeError" not in src

    # Отметку двигает только приём: обслуживание её не трогает.
    assert "def update_cursors" not in src
    assert "update_cursors(" not in src

    assert '_setting("maintenance_schedule")' in src
    assert "reconcile_document_attributes_ui" in src
    assert "recompute_document_versions" in src
    assert "ensure_time_partitions" in src
    # error_text принадлежит последнему ответу и пишется в transform: сверка по архиву
    # возвращала текст отказа на документы, прошедшие со второй попытки.
    assert "repair_document_error_text" not in src
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
    views_sql = (PARTS_DIR / "04_views.sql").read_text(encoding="utf-8")
    # REPORT_MARTS — только периодический слой: rpt_error_breakdown обновляется
    # отдельной задачей (refresh_fact_marts) сразу за фактами, до периодических витрин.
    periodic_sql = sql_section(views_sql, "weekly") + sql_section(views_sql, "monthly")
    declared = set(re.findall(r"CREATE MATERIALIZED VIEW (public\.\w+)", periodic_sql))

    # Список витрин живёт только в DAG, который их обновляет.
    assert set(load_dag_module("egisz_marts_dag").REPORT_MARTS) == declared

    # Идемпотентность каркаса: DROP, CREATE и первичное наполнение — в одном модуле схемы.
    drops = views_sql
    finalize = views_sql
    init = (PARTS_DIR / "dwh_init.sql").read_text(encoding="utf-8")

    assert "\\i db/04_views.sql" in init
    for matview in declared:
        assert f"DROP MATERIALIZED VIEW IF EXISTS {matview} CASCADE" in drops, matview
        assert f"REFRESH MATERIALIZED VIEW {matview}" in finalize, matview
        assert f"ANALYZE {matview}" in finalize, matview

    # Пересчёты и обновление витрин выполняются накатом только при пустом отчётном слое:
    # полные проходы в теле наката пересекались по блокировкам с приёмом фактов.
    assert "IF EXISTS (SELECT 1 FROM public.documents)" in finalize
    assert "AND NOT EXISTS (SELECT 1 FROM public.document_attributes)" in finalize

    # REFRESH CONCURRENTLY в DAG-ах требует уникального индекса на каждой витрине.
    for matview in declared:
        table = matview.split(".", 1)[1]
        assert re.search(rf"CREATE UNIQUE INDEX[^;]+ON {matview}\b", periodic_sql), table


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
    # Провижининг значениями не владеет: файл наполняет только пустую метабазу.
    # delete + add затирал бы реквизиты, заведённые в UI, значениями из шаблона.
    assert "'connections', 'delete'" not in src
    assert "Test-AirflowConnectionExists" in src
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
    dockerfile = (REPO_ROOT / "k8s" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
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
    etl = load_dag_module("egisz_etl_dag").egisz_etl_pipeline()
    marts = load_dag_module("egisz_marts_dag").egisz_marts_pipeline()
    maintenance = load_dag_module("egisz_reconcile_maintenance_dag").egisz_reconcile_maintenance_pipeline()

    assert etl.dag_id == "egisz_etl_dag"
    assert marts.dag_id == "egisz_marts_dag"
    assert maintenance.dag_id == "egisz_reconcile_maintenance_dag"

    # Пул провижинится отдельно (up.ps1 / внешняя инструкция) — задачи обязаны его требовать.
    pooled = {
        task.task_id
        for dag in (etl, marts, maintenance)
        for task in dag.tasks
        if task.pool == DWH_POOL
    }
    assert pooled == {
        "extract_message_registry",
        "sync_dictionaries",
        "transform_exchangelog",
        "refresh_fact_marts",
        "refresh_period_marts",
        "reconcile_journal_tail",
        "reconcile_archive_attributes",
        "maintain_partitions",
    }

    assert {t.task_id for t in etl.tasks} == {
        "extract_exchangelog",
        "extract_message_registry",
        "sync_dictionaries",
        "transform_exchangelog",
    }
    assert {t.task_id for t in marts.tasks} == {"refresh_fact_marts", "refresh_period_marts"}
    assert {t.task_id for t in maintenance.tasks} == {
        "reconcile_journal_tail",
        "reconcile_archive_attributes",
        "maintain_partitions",
    }

    # Реестр подач наполняется до transform, иначе ответ не с чем связать.
    assert etl.task_dict["extract_exchangelog"].downstream_task_ids == {
        "extract_message_registry",
        "transform_exchangelog",
    }
    assert etl.task_dict["extract_message_registry"].downstream_task_ids == {"sync_dictionaries"}
    assert etl.task_dict["sync_dictionaries"].downstream_task_ids == {"transform_exchangelog"}
    assert etl.task_dict["transform_exchangelog"].downstream_task_ids == set()

    # Разбивка ошибок обновляется раньше периодических витрин, которые её читают.
    assert marts.task_dict["refresh_fact_marts"].downstream_task_ids == {"refresh_period_marts"}

    # Производители фактов публикуют активы, на которые подписан DAG витрин.
    def _asset_names(task: object) -> set[str]:
        return {asset.name for asset in (getattr(task, "outlets", None) or [])}

    assert _asset_names(etl.task_dict["transform_exchangelog"]) == {"egisz://facts"}
    assert _asset_names(maintenance.task_dict["reconcile_journal_tail"]) == {"egisz://facts"}
    assert _asset_names(maintenance.task_dict["reconcile_archive_attributes"]) == {
        "egisz://dictionaries"
    }
