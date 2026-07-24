# Перенос настроек на внешний контур

Инструкции и сборка самодостаточных бандлов для импорта настроек проекта во **внешнюю,
уже развёрнутую** инфраструктуру (сторонний Airflow, Metabase, PostgreSQL DWH). Локальный
тестовый контур (Helm/Kubernetes/`up.ps1`) здесь не участвует.

В git хранятся только инструкции и скрипт сборки — сами бандлы собираются из канонических
исходников репозитория, чтобы исключить дрейф копий:

```powershell
.\scripts\build_external_bundle.ps1                 # все три бандла → dist/external/
.\scripts\build_external_bundle.ps1 -Target Airflow # только один (Airflow|Metabase|Dwh)
.\scripts\build_external_bundle.ps1 -Zip            # + zip-архивы рядом с бандлами
```

Результат (в `dist/` — git-ignored, артефакты не коммитятся):

```
dist/external/
├── airflow/    # самодостаточные DAG-файлы + requirements — см. external-airflow/README.md
├── metabase/   # importer-скрипты + дашборды + модели   — см. external-metabase/README.md
└── dwh/        # db/dwh_init.sql + db/parts/            — см. external-dwh/README.md
```

Каждый бандл несёт свой `README.md` (копия соответствующего `deploy/external-*/README.md`)
и `BUILD_INFO.txt` (git-коммит и дата сборки).

## Порядок применения

1. **DWH** — схема обязана существовать до запуска DAG и импорта Metabase.
2. **Airflow** — DAG пишут в DWH и вызывают его серверные функции.
3. **Metabase** — импортёр валидирует контракт DWH (наличие таблиц/вьюх) перед загрузкой.

| Бандл | Канонические исходники | Кому передавать |
| --- | --- | --- |
| `dwh` | `db/dwh_init.sql`, `db/parts/*.sql` | DBA целевого PostgreSQL |
| `airflow` | `airflow/dags/*.py` (самодостаточные DAG-файлы, копируются как есть) | администратор Airflow |
| `metabase` | `metabase/setup-dashboards.sh`, `metabase/sync-models.sh`, `metabase/include/`, `metabase_dashboards/`, `metabase_models/` | администратор Metabase |
