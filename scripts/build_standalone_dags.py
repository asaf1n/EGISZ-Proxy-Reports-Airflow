"""Сборка самодостаточных DAG-файлов для внешнего Airflow.

Каждый канонический DAG из ``airflow/dags/`` превращается в один файл: пакет
``egisz_elt`` встраивается целиком как исходные тексты и регистрируется в
``sys.modules`` до импорта кода DAG. Целевому Airflow не нужны ни ``PYTHONPATH``,
ни ``pip install`` пакета — только сам DAG-файл и зависимости из requirements.txt
(см. deploy/external-airflow/README.md).

Вызывается из scripts/build_external_bundle.ps1; выходные файлы детерминированы —
повторная сборка из тех же исходников даёт байт-в-байт тот же результат.
"""

from __future__ import annotations

import argparse
import ast
from graphlib import TopologicalSorter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
PACKAGE_DIR = REPO_ROOT / "src" / "egisz_elt"
PACKAGE = "egisz_elt"
FUTURE_IMPORT = "from __future__ import annotations"
EMBED_DELIMITER = "'''"

GENERATED_HEADER = """\
# Автосгенерировано scripts/build_standalone_dags.py — самодостаточный DAG-файл
# для внешнего Airflow: пакет egisz_elt встроен ниже как исходные тексты.
# Не редактировать вручную — правки вносить в airflow/dags/ и src/egisz_elt/,
# затем пересобрать бандл (scripts/build_external_bundle.ps1).
"""

INSTALLER_SOURCE = '''\
def _install_embedded_egisz_elt() -> None:
    """Регистрирует встроенные модули egisz_elt в sys.modules до импорта кода DAG.

    Повторная регистрация (другой самодостаточный DAG в том же интерпретаторе)
    пропускается: все файлы одной сборки несут идентичные исходники пакета.
    """
    if "egisz_elt" in sys.modules:
        return
    for name, source in _EGISZ_ELT_SOURCES.items():
        spec = importlib.util.spec_from_loader(name, loader=None, is_package=(name == "egisz_elt"))
        module = importlib.util.module_from_spec(spec)
        exec(compile(source, f"<embedded {name}>", "exec"), module.__dict__)
        sys.modules[name] = module
        if "." in name:
            parent_name, _, attribute = name.rpartition(".")
            setattr(sys.modules[parent_name], attribute, module)


_install_embedded_egisz_elt()
'''


def read_package_sources() -> dict[str, str]:
    """Прочитать модули пакета egisz_elt: имя модуля → исходный текст."""
    sources: dict[str, str] = {}
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        name = PACKAGE if path.name == "__init__.py" else f"{PACKAGE}.{path.stem}"
        sources[name] = path.read_text(encoding="utf-8")
    if PACKAGE not in sources:
        raise FileNotFoundError(f"{PACKAGE_DIR / '__init__.py'} not found")
    return sources


def imported_package_modules(source: str, known: set[str]) -> set[str]:
    """Собрать импортируемые модули egisz_elt из исходного текста (AST, без выполнения)."""
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == PACKAGE:
                # from egisz_elt import common, extract — имена-подмодули.
                imported.update(
                    f"{PACKAGE}.{alias.name}"
                    for alias in node.names
                    if f"{PACKAGE}.{alias.name}" in known
                )
            elif node.module.startswith(f"{PACKAGE}."):
                imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(
                alias.name
                for alias in node.names
                if alias.name == PACKAGE or alias.name.startswith(f"{PACKAGE}.")
            )
    return {name for name in imported if name in known}


def resolve_embedded_order(sources: dict[str, str]) -> list[str]:
    """Порядок регистрации модулей: «зависимости раньше зависимых».

    Пакет встраивается целиком в каждый DAG-файл: DagBag/CLI парсят все файлы в одном
    процессе, а установка выполняется только первым из них — частичный набор модулей
    ломал бы импорт соседних DAG.
    """
    known = set(sources)
    graph = {
        name: imported_package_modules(module_source, known) - {name, PACKAGE}
        for name, module_source in sources.items()
        if name != PACKAGE
    }
    ordered = list(TopologicalSorter(graph).static_order())
    # Пакет-родитель регистрируется первым: подмодули пришиваются к нему атрибутами.
    return [PACKAGE] + ordered


def embed_module_source(name: str, source: str) -> str:
    """Обернуть исходник модуля в raw-строку; тройная одинарная кавычка — разделитель."""
    if EMBED_DELIMITER in source:
        raise ValueError(
            f"{name}: source contains {EMBED_DELIMITER!r}; embedding delimiter would break"
        )
    if not source.endswith("\n"):
        source += "\n"
    return f'    "{name}": r{EMBED_DELIMITER}\n{source}{EMBED_DELIMITER},\n'


def strip_future_import(dag_source: str) -> str:
    """Убрать __future__-импорт из тела DAG: в склеенном файле он уже объявлен первым."""
    lines = dag_source.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.strip() == FUTURE_IMPORT:
            del lines[index]
            # Схлопнуть оставшуюся пустую строку, чтобы не плодить вертикальные зазоры.
            if index < len(lines) and lines[index].strip() == "":
                del lines[index]
            break
    return "".join(lines)


def render_standalone_dag(dag_path: Path, sources: dict[str, str]) -> str:
    """Собрать текст самодостаточного DAG-файла из канонических исходников."""
    dag_source = dag_path.read_text(encoding="utf-8")
    embedded = resolve_embedded_order(sources)

    parts: list[str] = [
        GENERATED_HEADER,
        "\n",
        f"{FUTURE_IMPORT}\n",
        "\n",
        "import importlib.util\n",
        "import sys\n",
        "\n",
        "_EGISZ_ELT_SOURCES: dict[str, str] = {\n",
        *(embed_module_source(name, sources[name]) for name in embedded),
        "}\n",
        "\n",
        "\n",
        INSTALLER_SOURCE,
        "\n",
        f"# ==== Исходный DAG: airflow/dags/{dag_path.name} ====\n",
        strip_future_import(dag_source),
    ]
    return "".join(parts)


def build_standalone_dags(output_dir: Path) -> list[Path]:
    """Сгенерировать самодостаточные DAG-файлы в ``output_dir``; вернуть их пути."""
    sources = read_package_sources()
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for dag_path in sorted(DAGS_DIR.glob("egisz_*_dag.py")):
        target = output_dir / dag_path.name
        target.write_text(render_standalone_dag(dag_path, sources), encoding="utf-8", newline="\n")
        written.append(target)
    if not written:
        raise FileNotFoundError(f"no egisz_*_dag.py found in {DAGS_DIR}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="каталог для самодостаточных DAG-файлов (например dist/external/airflow/dags)",
    )
    args = parser.parse_args()
    for target in build_standalone_dags(args.output):
        print(f"[standalone] {target}")


if __name__ == "__main__":
    main()
