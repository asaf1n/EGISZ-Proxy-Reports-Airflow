#!/usr/bin/env python3
"""
CLI: выгрузка дашбордов Metabase в JSON (каталог как у setup-dashboards.sh).

Реализация: `egisz_monitor_corp.metabase_export` (тот же код, что Config UI и образ Metabase).

Переменные окружения: METABASE_URL, METABASE_ADMIN_EMAIL, METABASE_ADMIN_PASSWORD;
опционально METABASE_EXPORT_DIR — каталог для *.json (иначе repo/metabase_dashboards или /app/metabase_dashboards в образе).

Запуск из корня репозитория:  py -3 metabase/export_dashboards_from_api.py

В поде Metabase:  PYTHONPATH=/app python3 -m egisz_monitor_corp.metabase_export

Не перезаписывайте репозиторий «вслепую» после правок только в UI: для native field filters сверяйте блоки `metabase-field-filters` у карточек в `metabase_dashboards/*.json` (краткая памятка — `field_filter_defaults.yaml`).
"""
from __future__ import annotations

from egisz_monitor_corp.metabase_export import main_cli

if __name__ == "__main__":
    raise SystemExit(main_cli())
