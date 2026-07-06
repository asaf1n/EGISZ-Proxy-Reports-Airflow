from __future__ import annotations

from airflow.models import Variable

# Keep in sync with k8s/airflow/egisz-variables.json (UI import / up.ps1 provisioning).
DEFAULTS: dict[str, str | int] = {
    "extract_schedule": "*/5 * * * *",
    "extract_raw_rows": 2000,
    "extract_raw_rounds": 3,
    "transform_rows": 5000,
    "transform_rounds": 6,
    "dimensions_schedule": "@hourly",
    "reconcile_schedule": "@daily",
    "reconcile_lookback_days": 30,
    "reconcile_max_logids": 20000000,
}


def get_str(key: str) -> str:
    default = DEFAULTS[key]
    return str(Variable.get(key, default_var=default))


def get_int(key: str) -> int:
    default = DEFAULTS[key]
    return int(Variable.get(key, default_var=default))
