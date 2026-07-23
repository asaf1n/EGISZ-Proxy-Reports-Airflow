from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATOR = REPO_ROOT / "scripts" / "build_standalone_dags.py"
DAG_FILES = (
    "egisz_dimensions_dag.py",
    "egisz_extract_dag.py",
    "egisz_reconcile_dag.py",
)

# DagBag парсит standalone-файлы в чистом интерпретаторе: cwd вне репозитория и без
# PYTHONPATH, чтобы канонический пакет src/egisz_elt был гарантированно недоступен.
_DAGBAG_CHECK = """\
import json
import sys

from airflow.utils.db import initdb

initdb()

from airflow.models import DagBag

dagbag = DagBag(dag_folder=sys.argv[1], include_examples=False)
payload = {
    "import_errors": {key: str(value) for key, value in dagbag.import_errors.items()},
    "tasks": {
        dag_id: sorted(task.task_id for task in dag.tasks)
        for dag_id, dag in sorted(dagbag.dags.items())
    },
}
print(json.dumps(payload))
"""


def _build(output_dir: Path) -> None:
    # stdin=DEVNULL: наследование невалидного родительского stdin на Windows
    # роняет запуск подпроцесса (WinError 6 при дублировании дескриптора).
    subprocess.run(
        [sys.executable, str(GENERATOR), "--output", str(output_dir)],
        check=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        text=True,
    )


def test_generator_output_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    _build(first)
    _build(second)

    assert sorted(p.name for p in first.glob("*.py")) == list(DAG_FILES)
    for name in DAG_FILES:
        assert (first / name).read_bytes() == (second / name).read_bytes()


def test_generator_embeds_whole_package_in_each_file(tmp_path: Path) -> None:
    # Пакет встраивается целиком: DagBag/CLI парсят все DAG-файлы в одном процессе,
    # а регистрацию в sys.modules выполняет только первый из них.
    _build(tmp_path)

    package_modules = sorted(
        "egisz_elt" if path.name == "__init__.py" else f"egisz_elt.{path.stem}"
        for path in (REPO_ROOT / "src" / "egisz_elt").glob("*.py")
    )
    assert len(package_modules) >= 5

    for name in DAG_FILES:
        source = (tmp_path / name).read_text(encoding="utf-8")
        assert source.startswith("# Автосгенерировано scripts/build_standalone_dags.py")
        assert "_install_embedded_egisz_elt()" in source
        for module in package_modules:
            assert f'"{module}": r' in source, (name, module)


def test_standalone_dags_load_without_package_on_path(tmp_path: Path) -> None:
    dags_dir = tmp_path / "dags"
    _build(dags_dir)

    check_script = tmp_path / "dagbag_check.py"
    check_script.write_text(_DAGBAG_CHECK, encoding="utf-8")

    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    env["AIRFLOW_HOME"] = str(tmp_path / "airflow_home")
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{(tmp_path / 'airflow.db').as_posix()}"

    result = subprocess.run(
        [sys.executable, str(check_script), str(dags_dir)],
        check=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        cwd=tmp_path,
        env=env,
        timeout=300,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])

    assert payload["import_errors"] == {}, payload["import_errors"]
    assert payload["tasks"] == {
        "egisz_dimensions_dag": ["sync_dimensions"],
        "egisz_extract_dag": [
            "extract_exchangelog",
            "refresh_weekly_reports",
            "transform_exchangelog",
        ],
        "egisz_reconcile_dag": ["reconcile_proxy_raw"],
    }
