"""Контракт самодостаточных DAG-файлов.

Общий блок (подключения, watermark, обновление витрин) продублирован в трёх файлах
сознательно: каждый разворачивается на целевой Airflow как есть. Цена дублирования —
риск дрейфа копий, который и снимают эти тесты.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("egisz_*.py"))

# Целевой Airflow ставит только зависимости из pyproject.toml; ничего сверх них
# и стандартной библиотеки DAG-файл импортировать не должен.
ALLOWED_IMPORT_ROOTS = {
    "airflow",
    "firebird",
    "psycopg2",
    "datetime",
    "logging",
    "os",
    "time",
    "typing",
    "__future__",
}


def _module_ast(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _top_level_definitions(tree: ast.Module) -> dict[str, ast.AST]:
    """Map top-level function/class/constant name → its AST node."""
    definitions: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            definitions[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    definitions[target.id] = node
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            definitions[node.target.id] = node
    return definitions


def test_dag_files_exist() -> None:
    assert {path.name for path in DAG_FILES} == {
        "egisz_extract_dag.py",
        "egisz_dimensions_dag.py",
        "egisz_reconcile_dag.py",
    }


# DEFAULTS — настройки конкретного DAG (свои ключи в каждом файле), а не общий блок.
PER_DAG_DEFINITIONS = {"DEFAULTS"}


def test_shared_definitions_are_identical_across_dag_files() -> None:
    """Одноимённые определения в разных DAG-файлах обязаны совпадать до AST."""
    per_file = {path.name: _top_level_definitions(_module_ast(path)) for path in DAG_FILES}

    names = [name for defs in per_file.values() for name in defs if name not in PER_DAG_DEFINITIONS]
    shared = {name for name in names if sum(name in defs for defs in per_file.values()) > 1}
    assert shared, "общий блок пуст — проверьте, что файлы не разошлись структурно"

    for name in sorted(shared):
        dumps = {
            file_name: ast.dump(defs[name])
            for file_name, defs in per_file.items()
            if name in defs
        }
        canonical_file, canonical = next(iter(dumps.items()))
        for file_name, dump in dumps.items():
            assert dump == canonical, (
                f"определение {name!r} разошлось между {canonical_file} и {file_name}: "
                "правки общего блока вносятся синхронно во все DAG-файлы"
            )


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda path: path.name)
def test_dag_file_imports_only_runtime_dependencies(path: Path) -> None:
    roots: set[str] = set()
    for node in ast.walk(_module_ast(path)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".", 1)[0])

    assert "egisz_elt" not in roots
    unexpected = roots - ALLOWED_IMPORT_ROOTS
    assert not unexpected, f"{path.name}: неожиданные зависимости {sorted(unexpected)}"
