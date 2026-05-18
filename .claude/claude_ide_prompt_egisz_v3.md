# Claude Code Task: Полная аналитическая система отдела интеграции ЕГИСЗ

## КОНТЕКСТ ПРОЕКТА

Репозиторий `EGISZ-Proxy-Reports-Airflow`. ELT-пайплайн и BI-витрина для отдела интеграции МИС с РЭМД ЕГИСЗ.

**Стек:**
- Airflow 2.11.2, TaskFlow API, DAG `egisz_elt_dag`, `*/5 * * * *`
- PostgreSQL DWH `dwh_egisz` — DDL в `db/dwh_init.sql`
- Metabase v0.60.2.5, дашборды как JSON в `metabase_dashboards/`, импорт через `metabase/setup-dashboards.sh`
- Firebird 5 `proxy_egisz` — источник журнала обмена

**Правила (AGENTS.md — строго):**
- Вся трансформация — в PostgreSQL-функциях, Python только вызывает
- Только TaskFlow API (`@dag`, `@task`), никаких `PythonOperator`
- Секреты — только `BaseHook.get_connection()`, никаких `os.getenv`
- Никогда не использовать `proxy_reports` в идентификаторах
- Обратная совместимость не требуется — объекты можно пересоздавать

**Перед написанием кода обязательно прочитай:**
- `db/dwh_init.sql` — текущая схема, точные имена колонок и типы
- `metabase_dashboards/01_operational.json` — формат JSON дашбордов для точного воспроизведения
- `metabase/setup-dashboards.sh` — паттерн импорта для добавления новых дашбордов
- `src/egisz_elt/sql/` — существующие представления, чтобы не дублировать логику

---

## БИЗНЕС-КОНТЕКСТ

**Заказчик:** Компания предоставляет сервис интеграции МИС нескольких клиник с ЕГИСЗ через прокси-сервис `proxy_egisz`.

**Пользователи системы:** Сотрудники отдела интеграции — специалисты сопровождения и руководитель отдела. НЕ сотрудники клиник.

**Ключевая сущность — Электронный Медицинский Документ (ЭМД/СЭМД).** Всё строится вокруг него: сколько отправлено, какие типы, статус регистрации, ошибки, история конкретного документа.

**Полный спектр аналитических задач отдела:**
1. Общая картина работы сервиса — сколько документов отправляется, успешность, динамика
2. Разбивка по типам СЭМД — какие виды документов регистрируются и как
3. Разбивка по ошибкам — что идёт не так, паттерны, частота
4. Разбивка по клиникам — у кого проблемы и с чем
5. Документы без ответа — зависшие документы, требующие внимания
6. Поиск и история конкретного документа — архив для расследования инцидентов
7. Здоровье канала и сервиса — работает ли пайплайн, нет ли деградации

---

## ЗАДАЧА

Создать или пересоздать полную аналитическую систему: SQL-представления, дашборды Metabase, скрипты деплоя. Существующие дашборды 01–06 могут быть пересмотрены и улучшены если они не покрывают нужды.

**Артефакты:**
1. `migrations/004_full_analytics.sql` — все новые и пересозданные SQL-объекты
2. `db/dwh_init.sql` — синхронизировать те же объекты
3. `metabase_dashboards/` — новые и обновлённые JSON дашборды
4. `metabase/setup-dashboards.sh` — полный скрипт импорта всех дашбордов
5. `airflow/dags/egisz_elt_dag.py` — если потребуется обновление refresh-задачи

---

## ЧАСТЬ 1 — SQL-ПРЕДСТАВЛЕНИЯ

### Ключевые принципы для всех представлений:
- Основная единица — ЭМД, идентифицируется через `COALESCE(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id::text)`
- Все представления с суффиксом `_ui` — для Metabase
- Числовые поля для сортировки там где Metabase не умеет сортировать по строке
- `NULLIF` везде где возможно деление

---

### 1.1 `v_doc_registry_ui` — Реестр документов (архив)

**Назначение:** Поиск и просмотр истории конкретного документа. Одна строка на ЭМД (не на транзакцию).

```sql
-- Логика: группировать транзакции по doc_key, взять последнее состояние
-- doc_key = COALESCE(local_uid_semd, emdr_id, doc_number, message_id, exchangelog_log_id::text)
SELECT
  doc_key                          AS "Идентификатор документа",
  local_uid_semd                   AS "Локальный UID СЭМД",
  emdr_id                          AS "ID в РЭМД",
  doc_number                       AS "Номер документа",
  semd_code                        AS "Код СЭМД",
  semd_name                        AS "Тип СЭМД",           -- из dim_semd_types
  org_name                         AS "Клиника",             -- из dim_organizations
  creation_date                    AS "Дата создания документа",
  first_sent_date                  AS "Первая отправка",
  last_sent_date                   AS "Последняя отправка",
  attempt_count                    AS "Попыток отправки",
  final_status                     AS "Итоговый статус",     -- success/error/pending
  final_error_type                 AS "Тип ошибки",
  final_error_summary              AS "Описание ошибки",
  days_in_processing               AS "Дней в обработке",
  is_registered                    AS "Зарегистрирован в РЭМД"  -- bool: есть emdr_id
```

Параметры Metabase для этого представления:
- `{{doc_id}}` — поиск по любому из идентификаторов (ILIKE по local_uid_semd, emdr_id, doc_number)
- `{{org_name}}` — фильтр по клинике
- `{{semd_code}}` — фильтр по типу СЭМД
- `{{status}}` — фильтр по статусу
- `{{date_from}}` / `{{date_to}}` — период по дате создания документа

---

### 1.2 `v_doc_timeline_ui` — История транзакций документа

**Назначение:** Для drill-down по конкретному документу — все попытки отправки в хронологии.

```sql
-- Все транзакции для одного doc_key, отсортированные по времени
SELECT
  exchangelog_log_id,
  log_date,
  status,
  message_id,
  relates_to_id,
  error_type,
  error_summary,
  error_json_text,  -- полный текст ошибки для диагностики
  org_name,
  semd_code,
  semd_name
FROM fact_egisz_transactions f
JOIN dim_organizations o ON ...
JOIN dim_semd_types s ON ...
WHERE doc_key = {{doc_key}}   -- параметр Metabase
ORDER BY log_date ASC
```

---

### 1.3 `v_stat_semd_types_ui` — Статистика по типам СЭМД

**Назначение:** Разбивка всех отправок по видам документов.

Поля:
- `semd_code`, `semd_name` (из `dim_semd_types`, fallback на `semd_code` если нет в справочнике)
- `total_sent` — транзакций за период
- `unique_docs` — уникальных документов (по `doc_key`)
- `success_count`, `error_count`, `pending_count`
- `success_rate_pct`
- `avg_attempts` — среднее число попыток на документ
- `top_error_type` — самая частая ошибка для этого типа СЭМД
- `orgs_using` — сколько клиник отправляют этот тип

Параметр: `{{period_days}}` — количество дней (7/30/90, default 30)

---

### 1.4 `v_stat_errors_ui` — Статистика по ошибкам

**Назначение:** Полная картина ошибочных регистраций — паттерны, частота, тренд.

Поля:
- `error_type` — канонический тип из `egisz_error_interpretation_rules`
- `error_summary` — человекочитаемое описание
- `error_count` — количество транзакций с этой ошибкой
- `unique_docs_affected` — уникальных документов с этой ошибкой
- `orgs_affected` — сколько клиник столкнулись
- `pct_of_all_errors` — доля от всех ошибок за период
- `first_seen` — когда впервые появилась
- `last_seen` — последнее появление
- `trend_7d` vs `trend_prev_7d` — сравнение двух семидневок для выявления роста
- `is_growing` — bool: `trend_7d > trend_prev_7d`

Параметры: `{{period_days}}`, `{{org_name}}` (опционально — ошибки конкретной клиники)

---

### 1.5 `v_stat_orgs_ui` — Статистика по клиникам

**Назначение:** Сравнительная таблица клиник по всем ключевым метрикам.

Поля:
- `jid`, `org_name`, `inn`
- `total_sent`, `unique_docs`, `success_count`, `error_count`
- `success_rate_pct`, `error_rate_pct`
- `errors_last_24h`, `errors_last_7d`
- `distinct_semd_types` — ширина интеграции (сколько видов СЭМД)
- `top_error_type`, `top_error_summary`
- `last_success_date`, `last_error_date`
- `days_since_last_success`
- `docs_no_response` — документов без ответа (anti-join с callback)
- `org_health` — `'CRITICAL'` / `'WARNING'` / `'OK'` (логика: CRITICAL если error_rate>=50% OR days_since_last_success>=3; WARNING если error_rate>=20% OR errors_last_24h>=10)
- `org_health_order` — 1/2/3

---

### 1.6 `v_stat_daily_ui` — Дневная динамика сервиса

**Назначение:** Трендовые графики — как меняется объём и качество во времени.

```sql
SELECT
  DATE_TRUNC('day', log_date)  AS day,
  COUNT(*)                      AS total_sent,
  COUNT(*) FILTER (WHERE status = 'success') AS success_count,
  COUNT(*) FILTER (WHERE status = 'error')   AS error_count,
  ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*),0), 1) AS success_rate_pct,
  COUNT(DISTINCT CASE WHEN status = 'error' THEN error_type END) AS distinct_error_types,
  COUNT(DISTINCT jid)           AS active_orgs
FROM fact_egisz_transactions
WHERE log_date > NOW() - INTERVAL '90 days'
GROUP BY 1
ORDER BY 1
```

---

### 1.7 `v_stat_hourly_ui` — Часовая динамика (оперативный мониторинг)

Аналогично `v_stat_daily_ui` но по часам за последние 48 часов. Для графика «что происходит прямо сейчас».

---

### 1.8 `v_docs_no_response_ui` — Документы без ответа (пересоздать)

Пересоздать существующее представление, добавив:
- `wait_hours` — часов ожидания
- `urgency` — `'CRITICAL'` (>24ч) / `'WARNING'` (4–24ч) / `'PENDING'` (<4ч)
- `urgency_order` — 1/2/3
- `org_name` из `dim_organizations`
- `semd_name` из `dim_semd_types`

---

### 1.9 `v_service_health_ui` — Здоровье сервиса (пересоздать)

Пересоздать `v_health_signals_ui`, расширив:
- `pipeline_freshness` — минут с последнего успешного запуска ELT
- `pipeline_status` — `'OK'` / `'STALE'` / `'DEAD'` (>10мин / >30мин / >60мин)
- `docs_processed_last_hour` — документов обработано за последний час
- `error_rate_last_hour` — процент ошибок за последний час
- `orgs_critical_count` — клиник в CRITICAL прямо сейчас
- `docs_no_response_critical` — зависших документов >24ч

---

### 1.10 `v_kpi_summary_ui` — Сводные KPI для executive-плитки

Одна строка. Все ключевые цифры сервиса за 30 дней:
```
total_orgs, orgs_critical, orgs_warning, orgs_ok,
total_docs_30d, total_sent_30d, success_rate_30d_pct,
total_errors_30d, top_error_type_30d,
docs_no_response_count, docs_no_response_critical,
avg_attempts_per_doc,
most_used_semd_name, most_used_semd_count,
most_problematic_org_name, most_problematic_org_error_rate
```

---

## ЧАСТЬ 2 — ДАШБОРДЫ METABASE

Изучи формат из `metabase_dashboards/01_operational.json` перед написанием JSON.
Все дашборды — в коллекцию «Интеграция с ЕГИСЗ».

---

### Дашборд A: «Общая картина сервиса» (пересоздать 01 + 05)

**Аудитория:** Руководитель отдела, быстрый обзор состояния.

**Строка 1 — KPI-плитки из `v_kpi_summary_ui`:**
- Всего клиник / Проблемных (CRITICAL) / С предупреждениями
- Документов за 30 дней / Успешность % / Документов без ответа

**Строка 2 — Линейный график «Динамика успешности» из `v_stat_daily_ui`:**
- X = day, Y = success_rate_pct, secondary Y = total_sent
- За 30 дней

**Строка 3 — Два Bar chart рядом:**
- «Топ типов СЭМД по объёму» из `v_stat_semd_types_ui` (топ 10 по total_sent)
- «Клиники по объёму» из `v_stat_orgs_ui` (топ 10 по total_sent)

**Строка 4 — Таблица клиник с сводкой** из `v_stat_orgs_ui`:
org_name, org_health, total_sent, success_rate_pct, errors_last_24h, top_error_summary, days_since_last_success

---

### Дашборд B: «Ошибки и качество» (пересоздать 04)

**Аудитория:** Специалист сопровождения — разбирается в причинах ошибок.

**Строка 1 — Плитки:**
- Всего ошибок за период / Уникальных типов ошибок / Клиник с ошибками / Растущих ошибок (is_growing=true)

**Строка 2 — Bar chart «Топ-15 ошибок»** из `v_stat_errors_ui`:
- X = error_summary (обрезать до 50 символов), Y = error_count
- Цвет: красный если `is_growing`, серый иначе

**Строка 3 — Таблица ошибок** из `v_stat_errors_ui`:
error_summary, error_count, unique_docs_affected, orgs_affected, pct_of_all_errors, last_seen, is_growing

**Строка 4 — Таблица «Ошибки по клиникам × типу»** из `v_stat_orgs_ui` + drill-down:
Матрица: строки = клиники, колонки = топ типов ошибок (или просто таблица с org_name, top_error_type, error_count)

**Параметр дашборда:** `{{period_days}}` (7 / 30 / 90 дней), `{{org_name}}` опционально

---

### Дашборд C: «Клиники» (новый, заменяет фрагменты из разных дашбордов)

**Аудитория:** Аккаунт-менеджер и специалист сопровождения.

**Строка 1 — «Светофор» клиник:**
- Три счётчика: CRITICAL / WARNING / OK из `v_kpi_summary_ui`
- Bar chart горизонтальный «Топ-15 клиник по % ошибок» из `v_stat_orgs_ui`

**Строка 2 — Полная таблица клиник** из `v_stat_orgs_ui`:
org_name, org_health, total_sent, success_rate_pct, error_rate_pct, errors_last_24h,
distinct_semd_types, docs_no_response, top_error_summary, days_since_last_success, last_success_date

Field Filter: `{{org_health}}` → `org_health` (category filter)

**Строка 3 — Детализация по выбранной клинике** (параметр `{{org_name}}`):

3a. Линейный график «Динамика клиники» — аналог `v_stat_daily_ui` с WHERE jid = выбранная клиника

3b. Таблица «Ошибки клиники» из `v_stat_errors_ui` WHERE org_name = `{{org_name}}`

3c. Таблица «Типы СЭМД клиники» из `v_stat_semd_types_ui` WHERE org_name = `{{org_name}}`

---

### Дашборд D: «Типы СЭМД» (новый)

**Аудитория:** Специалист — анализ покрытия и проблем по видам документов.

**Строка 1 — Plитки:**
- Всего типов СЭМД в обороте / Типов с ошибками / Наиболее используемый тип

**Строка 2 — Bar chart «Объём по типам СЭМД»** (топ 15, цвет по success_rate)

**Строка 3 — Таблица типов СЭМД** из `v_stat_semd_types_ui`:
semd_name, total_sent, unique_docs, success_rate_pct, avg_attempts, orgs_using, top_error_type, last_sent

Field Filter: `{{semd_code}}`

**Строка 4 — Детализация** (параметр `{{semd_name}}`):
- Таблица клиник, использующих этот тип: org_name, sent_count, success_rate_pct, top_error_type
- Топ ошибок для этого типа СЭМД

---

### Дашборд E: «Архив СЭМД» (новый, заменяет 06)

**Аудитория:** Специалист — расследование инцидентов, поиск конкретного документа.

**Строка 1 — Поисковые параметры:**
```
{{doc_id}}     — Поиск по ID документа (local_uid_semd / emdr_id / doc_number)
{{org_name}}   — Клиника
{{semd_code}}  — Тип СЭМД
{{status}}     — Статус (success / error / pending)
{{date_from}}  — С даты
{{date_to}}    — По дату
```

**Строка 2 — Таблица документов** из `v_doc_registry_ui`:
doc_key (обрезать), semd_name, org_name, creation_date, attempt_count, final_status,
final_error_summary, days_in_processing, is_registered

Сортировка по умолчанию: last_sent_date DESC

**Строка 3 — История документа** (появляется при выборе `{{doc_key}}`):
Таблица из `v_doc_timeline_ui` — все попытки отправки с деталями ошибок

---

### Дашборд F: «Оперативный мониторинг» (пересоздать 01 + 02)

**Аудитория:** Дежурный специалист — что происходит прямо сейчас.

**Строка 1 — Статус сервиса** из `v_service_health_ui`:
- Плитка: Pipeline status (OK / STALE / DEAD) с цветом
- Плитка: Документов за последний час
- Плитка: % ошибок за последний час
- Плитка: Клиник в CRITICAL
- Плитка: Зависших документов >24ч

**Строка 2 — Часовой график** из `v_stat_hourly_ui` за 48 часов:
- total_sent и error_count на одном графике

**Строка 3 — Документы без ответа** из `v_docs_no_response_ui`:
Таблица, отсортированная по urgency_order ASC, wait_hours DESC
org_name, semd_name, doc_key, wait_hours, urgency, first_sent_date

Field Filter: `{{urgency}}`

---

## ЧАСТЬ 3 — setup-dashboards.sh

Обнови `metabase/setup-dashboards.sh`:

1. Добавить проверку всех новых представлений перед импортом
2. Импортировать все дашборды A–F в правильном порядке
3. Добавить параметризацию через переменные окружения:
   ```bash
   METABASE_URL=${METABASE_URL:-"http://localhost:3000"}
   METABASE_USER=${METABASE_USER:-"admin@example.com"}
   METABASE_PASSWORD=${METABASE_PASSWORD:-""}
   DWH_HOST=${DWH_HOST:-"localhost"}
   ```
4. Функция проверки что представление существует:
   ```bash
   check_view_exists() {
     local view_name=$1
     # psql -c "SELECT 1 FROM $view_name LIMIT 1" и проверить exit code
   }
   ```

---

## ЧАСТЬ 4 — Airflow DAG

Если `v_doc_registry_ui`, `v_stat_semd_types_ui`, `v_stat_orgs_ui`, `v_stat_errors_ui` будут материализованными view (реши сам исходя из объёма данных в схеме) — добавить их в список refresh в `refresh_materialized_views` task.

Также добавить в XCom payload статистику текущего батча для `v_stat_hourly_ui`:
```python
# В update_watermark task — записывать в отдельную таблицу etl_run_log:
# (run_ts, docs_processed, errors_count, duration_ms)
# Это позволит строить график производительности пайплайна
```

Создать таблицу `etl_run_log` в миграции:
```sql
CREATE TABLE IF NOT EXISTS etl_run_log (
  run_ts       timestamptz PRIMARY KEY DEFAULT NOW(),
  docs_processed int,
  errors_count   int,
  duration_ms    int,
  batch_min_id   bigint,
  batch_max_id   bigint
);
```

---

## ФИНАЛЬНАЯ ПРОВЕРКА (в конце migrations/004_full_analytics.sql)

```sql
DO $$
DECLARE v_count int;
BEGIN
  -- Проверить все представления
  PERFORM 1 FROM v_doc_registry_ui LIMIT 1;
  PERFORM 1 FROM v_stat_semd_types_ui LIMIT 1;
  PERFORM 1 FROM v_stat_errors_ui LIMIT 1;
  PERFORM 1 FROM v_stat_orgs_ui LIMIT 1;
  PERFORM 1 FROM v_stat_daily_ui LIMIT 1;
  PERFORM 1 FROM v_stat_hourly_ui LIMIT 1;
  PERFORM 1 FROM v_docs_no_response_ui LIMIT 1;
  PERFORM 1 FROM v_service_health_ui LIMIT 1;
  PERFORM 1 FROM v_kpi_summary_ui LIMIT 1;
  PERFORM 1 FROM etl_run_log LIMIT 0;

  -- Проверить что в dim_semd_types есть данные (нужны для JOIN)
  SELECT COUNT(*) INTO v_count FROM dim_semd_types;
  ASSERT v_count > 0, 'dim_semd_types пуст — загрузи справочник СЭМД';

  RAISE NOTICE 'Migration 004 verified OK — % views ready', 9;
END $$;
```

---

## ПОРЯДОК ВЫПОЛНЕНИЯ

1. Прочитай `db/dwh_init.sql` — точная схема таблиц (типы колонок, имена, ключи)
2. Прочитай `metabase_dashboards/01_operational.json` — формат JSON для дашбордов
3. Прочитай `metabase/setup-dashboards.sh` — паттерн импорта
4. Создай `migrations/004_full_analytics.sql` со всеми представлениями и `etl_run_log`
5. Синхронизируй в `db/dwh_init.sql`
6. Создай JSON для дашбордов A–F в `metabase_dashboards/`
7. Обнови `metabase/setup-dashboards.sh`
8. Обнови `airflow/dags/egisz_elt_dag.py` если нужен refresh или `etl_run_log`
9. Обнови README — разделы «DWH-модель», «Metabase и дашборды»

## ЧЕГО НЕ ДЕЛАТЬ

- Не трогать `egisz_transform_raw_to_facts()` — ядро пайплайна
- Не менять XCom-контракт между задачами
- Не добавлять трансформацию в Python
- Не хардкодить имена клиник, пороги, OID
