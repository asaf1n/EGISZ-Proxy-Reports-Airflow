# EGISZ Proxy Reports ETL (Firebird → Postgres)

Минимальный ETL-проект для периодической выгрузки данных из Firebird в Postgres под управлением Airflow (Kubernetes).

## Принципы

- **Минимальная поверхность**: только ETL-код, Airflow DAG и k8s примеры.
- **Конфиг через env**: Airflow/K8s Secrets, без UI и без сложной конфигурации.
- **Идемпотентность**: повторный запуск безопасен; прогресс хранится как watermark в `etl_state`.
- **Защита от параллельного запуска**: `pg_try_advisory_lock` по имени пайплайна.

## Конфигурация (env)

Обязательные:

- `FB_DSN`, `FB_USER`, `FB_PASSWORD`
- `PG_DSN` (например `postgresql://user:pass@host:5432/db`)
- `FB_SOURCE_SQL` (SQL `SELECT ...` **обязательно** включает курсор-колонку)

Рекомендуемые:

- `ETL_CURSOR_COLUMN` (по умолчанию `LOGID`)
- `ETL_BATCH_SIZE` (по умолчанию `500`)
- `ETL_PIPELINE` (по умолчанию `proxy_reports`)
- `PG_TARGET_TABLE` (по умолчанию `proxy_reports_raw`)

Пример Secret: [`k8s/postgres/dwh-credentials-secret.example.yaml`](k8s/postgres/dwh-credentials-secret.example.yaml)

## Локальный запуск (вне Airflow)

После установки зависимостей:

```bash
python -m proxy_reports_etl.cli test-connections
python -m proxy_reports_etl.cli sync
```

## Airflow (Kubernetes)

1. Соберите и опубликуйте образ воркера из [`k8s/airflow/Dockerfile`](k8s/airflow/Dockerfile).
2. Создайте секрет `proxy-reports-etl-env` с env для ETL (см. пример выше).
3. Установите Airflow Helm chart и подключите values: [`k8s/airflow/values.example.yaml`](k8s/airflow/values.example.yaml).

DAG: [`airflow/dags/proxy_reports_etl_dag.py`](airflow/dags/proxy_reports_etl_dag.py)

## Metabase (опционально)

Пример манифеста: [`k8s/metabase/metabase.yaml`](k8s/metabase/metabase.yaml) — только приложение Metabase с внешней Postgres app DB.
