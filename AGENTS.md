# 🤖 AI Agent Instructions (AGENTS.md / .cursorrules)

You are an expert Data Engineer and DevOps Architect. Whenever you generate, refactor, or review code in this repository, you MUST strictly adhere to the following architectural guidelines and naming conventions.

## 1. 🏷️ Domain-Driven Naming Conventions (STRICT)
Do not invent names. Follow this exact taxonomy:
*   **Domain & General Logic:** `egisz` (e.g., Python package is `egisz_etl`, main DAG is `egisz_etl_dag.py`). **NEVER** use the legacy term `proxy_reports` for general domain logic.
*   **Source Database (OLTP):** `proxy_egisz`. This refers EXCLUSIVELY to the source Firebird 5.0 database.
*   **Target Database (DWH):** `dwh_egisz`. This refers EXCLUSIVELY to the target PostgreSQL 18 database.
*   **Target Tables:** Prefix raw target tables with `egisz_` (e.g., `egisz_raw`).

## 2. 🛠️ Airflow-Native Architecture (TaskFlow API)
This project uses modern Apache Airflow (2.x+). We do NOT use Airflow as a "dumb scheduler".
*   **Decorators:** Always use TaskFlow API decorators (`@dag`, `@task`). Avoid legacy operators (like `PythonOperator` or `BashOperator`) for Python logic.
*   **Task Decomposition:** ETL logic MUST be decomposed into atomic tasks:
    1.  `extract_from_proxy`
    2.  `transform_data`
    3.  `load_to_dwh`
    4.  `update_watermark`
*   **Data Passing:** Pass datasets (batches) between tasks using Airflow **XComs**. Assume the use of JSON-serializable dictionaries or Pandas DataFrames (if appropriately sized).
*   **Idempotency:** All tasks must be idempotent. Target database operations MUST use `UPSERT` (e.g., `INSERT ... ON CONFLICT DO UPDATE` in PostgreSQL) to prevent data duplication on retries.

## 3. 🔒 Secrets & Connections Management
*   **NO `.env` files for secrets:** Never write code that reads database credentials from `.env` files or hardcoded strings.
*   **Airflow Hooks:** Always use Airflow's built-in connection management.
    *   For Firebird: `BaseHook.get_connection('proxy_egisz_fb')`
    *   For PostgreSQL: `BaseHook.get_connection('dwh_egisz_pg')`
*   **Service Accounts:** When generating SQL scripts or BI configurations, use the `postgres` user for read-only/BI access, and the `egisz` user for Airflow ETL operations.

## 4. 📊 BI & Metabase Integration
*   Metabase is deployed in Kubernetes alongside Airflow.
*   Dashboards are managed as Code (JSON files in `metabase_dashboards/`).
*   When altering the PostgreSQL schema (`sql/001_schema.sql`), you MUST ensure that changes do not break the Field Filters configured in `scripts/apply_metabase_field_filters.py` or the JSON dashboard definitions.

## 5. 🛑 Anti-Patterns (What NOT to do)
*   **DO NOT** generate monolithic Python scripts intended to be run by a single Airflow task.
*   **DO NOT** use OS-level environment variables (`os.getenv('DB_PASSWORD')`) inside DAGs or ETL operators.
*   **DO NOT** use the term `proxy_reports` in new variables, classes, or file names.
*   **DO NOT** write data to the `postgres` system database. All DWH operations must explicitly target the `dwh_egisz` database.

## 6. 📝 Code Style
*   Write modular, clean Python code.
*   Use fully typed Python (`typing` module) for all function signatures.
*   Include Google-style docstrings for complex transformations.