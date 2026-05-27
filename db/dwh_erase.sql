-- ============================================================================
-- dwh_erase.sql — полная очистка аналитической БД dwh_egisz.
--
-- Сметает ВСЕ объекты схемы public: таблицы, materialized views, views,
-- функции, последовательности, расширения и роль egisz. После прогона
-- база возвращается в состояние до первого запуска dwh_init.sql.
--
-- Использование (PowerShell):
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_erase.sql
--
-- ⚠️  ВНИМАНИЕ: операция деструктивная и НЕ откатываемая. Перед запуском:
--   1. Останови Airflow DAG egisz_elt_dag (иначе DROP упрётся в lock).
--   2. Закрой все Metabase-сессии к dwh_egisz (или сними нагрузку).
--   3. Сделай бэкап если нужно: pg_dump -U postgres dwh_egisz > backup.sql
--
-- Для пересоздания после очистки:
--   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
-- ============================================================================

\echo 'Erasing dwh_egisz public schema and role egisz...'

-- Прерываем все активные соединения к dwh_egisz, кроме нашего собственного,
-- чтобы DROP SCHEMA / DROP OWNED не упёрся в locks от Metabase/Airflow.
DO $$
BEGIN
    PERFORM pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid();
END
$$;

-- DROP SCHEMA public CASCADE — самый простой и надёжный способ снести
-- все user-объекты: таблицы (включая elt_state, exchangelog_raw,
-- fact_egisz_transactions, fact_egisz_messages, fact_egisz_documents, dim_*), materialized views
-- (v_egisz_transactions_enriched_ui, v_stg_channel_errors_by_document),
-- обычные views (v_rpt_*, v_health_*), функции (egisz_*) и индексы.
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- Возвращаем стандартные права на восстановленную схему public.
GRANT USAGE, CREATE ON SCHEMA public TO PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;
COMMENT ON SCHEMA public IS 'standard public schema';

-- Снимаем зависимости роли egisz (если она существует) и удаляем её.
-- REASSIGN/DROP OWNED нужны на случай, если в БД остались объекты вне
-- схемы public, принадлежащие egisz (sequences, etc.).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'egisz') THEN
        EXECUTE 'REASSIGN OWNED BY egisz TO postgres';
        EXECUTE 'DROP OWNED BY egisz CASCADE';
        BEGIN
            EXECUTE 'DROP ROLE egisz';
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE NOTICE 'Role egisz has dependencies outside %, keeping the role and reusing it on dwh_init.sql.', current_database();
        END;
    END IF;
END
$$;

\echo 'DWH erase complete. To re-create the schema:'
\echo '  psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql'
