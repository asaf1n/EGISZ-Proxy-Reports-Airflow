"""Загрузка самодостаточных DAG-файлов как модулей.

Пакета egisz_elt больше нет: канонический исходник — dags/*.py. Тесты берут
функции напрямую из файла, который разворачивается на целевые контуры, поэтому проверяют
именно поставляемый код. Импорт DAG-файла не должен требовать ни метабазы Airflow, ни
Connections — настройки при импорте падают на DEFAULTS (см. _variable_or_default).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"
SCRIPTS_DIR = REPO_ROOT / "scripts"

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


def load_script_module(stem: str) -> ModuleType:
    """Import ``scripts/<stem>.py`` as a module (каталог не пакет, отсюда загрузка по пути)."""
    cached = sys.modules.get(stem)
    if cached is not None:
        return cached

    path = SCRIPTS_DIR / f"{stem}.py"
    spec = importlib.util.spec_from_file_location(stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load script module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[stem] = module
    spec.loader.exec_module(module)
    return module


SQL_SECTION_MARKER = "-- ---------------------------------------------------------------- section: "


def sql_section(text: str, name: str) -> str:
    """Именованная секция склеенного модуля схемы (`db/*.sql`).

    Модули собраны из секций по зонам ответственности. Тест, проверяющий контракт
    одного слоя, обязан читать его секцию, а не весь файл.
    """
    start = text.index(SQL_SECTION_MARKER + name) + len(SQL_SECTION_MARKER + name)
    rest = text[start:]
    nxt = rest.find("\n" + SQL_SECTION_MARKER)
    return rest if nxt < 0 else rest[:nxt]


def load_dag_module(stem: str) -> ModuleType:
    """Import ``dags/<stem>.py`` under its own name (cached in sys.modules).

    Имя модуля совпадает с именем файла, поэтому patch-цели в тестах пишутся как
    ``egisz_etl_dag.<функция>``.
    """
    cached = sys.modules.get(stem)
    if cached is not None:
        return cached

    path = DAGS_DIR / f"{stem}.py"
    spec = importlib.util.spec_from_file_location(stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load DAG module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[stem] = module
    spec.loader.exec_module(module)
    return module
