# Сервис эксплуатационной аналитики обмена с ЕГИСЗ

Сервис собирает данные обмена СЭМД с ЕГИСЗ из прокси-БД интегратора, преобразует их в аналитическую модель PostgreSQL и публикует эксплуатационные дашборды в Metabase.

Основной сценарий: каждые 5 минут Airflow забирает новые строки журналов Firebird, загружает raw-слой как временный staging, разбирает его в persistent DWH-таблицы, инкрементально сопровождает аналитические витрины и продвигает watermark. Пользователь видит состояние отправок, callback'ов, ошибок, очередей ожидания и здоровья ETL-контура по разложенным fact-таблицам, а не по raw.

| Интерфейс | Адрес | Логин | Пароль |
|---|---|---|---|
| Metabase | `https://10.20.0.76:3000` | `admin@egisz.local` | `egisz` |
| Airflow | `https://10.20.0.76:8080` | `admin` | `admin` |

## Содержание

- [Назначение](#назначение)
- [Предметная область](#предметная-область)
- [Архитектура](#архитектура)
- [Источник данных](#источник-данных)
- [Airflow ELT](#airflow-elt)
- [DWH-модель](#dwh-модель)
- [Парсинг и классификация](#парсинг-и-классификация)
- [Дашборды Metabase](#дашборды-metabase)
- [Эксплуатация](#эксплуатация)
- [Структура репозитория](#структура-репозитория)
- [Глоссарий](#глоссарий)

## Назначение

Сервис предназначен для оперативного контроля обмена структурированными электронными медицинскими документами между МИС клиник и ЕГИСЗ.

Он отвечает на вопросы:

| Направление контроля | Что показывает сервис |
|---|---|
| Отправка документов | Количество сообщений и документов по времени, клиникам, типам СЭМД и статусам. |
| Обратная связь РЭМД | Наличие callback'ов, регистрационный номер `emdrId`, документы в обработке и просроченные ожидания. |
| Ошибки регистрации | Человекочитаемую классификацию отказов РЭМД, сетевых ошибок и технических сбоев. |
| Полнота данных | Документы без ответа, пустые или противоречивые идентификаторы, нераспознанные callback'и. |
| Здоровье ETL | Свежесть данных, движение watermark, объём staging/fact-слоёв, актуальность витрин. |

Рабочее окно данных стенда начинается с `2026-05-18`. Нижняя отсечка хранится в `elt_state.source_min_created_at` (seed при init) и фильтрует выборку `EXCHANGELOG` по `LOGDATE`/`CREATEDATE`. Значение `NULL` означает отсутствие нижней границы.

## Предметная область

**ЕГИСЗ** — Единая государственная информационная система в сфере здравоохранения.

**РЭМД** — реестр электронных медицинских документов, куда медицинские организации передают СЭМД.

**СЭМД** — структурированный электронный медицинский документ: выписной эпикриз, протокол, направление, заключение и другие типы медицинских документов.

Обмен с РЭМД асинхронный. МИС формирует CDA-документ, подписывает его, отправляет через интеграционный шлюз и получает технический ответ о приёме. Аналитические статусы строятся по итоговым записям: документ зарегистрирован и получил `emdrId`, отклонён с ошибками регистрации или не дошёл из-за ошибки связи.

Одна бизнес-операция обычно представлена несколькими техническими сообщениями. Поэтому сервис восстанавливает цепочку обмена по нескольким идентификаторам: `messageId`, `relatesTo`, `relatesToMessage`, `localUid`, `DOCUMENTID`, `emdrId`, `MSGID`.

## Архитектура

Проект построен как ELT-сервис. Python отвечает за оркестрацию и загрузку raw staging. Предметная трансформация выполняется в PostgreSQL функциями PL/pgSQL и сохраняет результат в persistent fact-таблицы.

| Компонент | Технология | Ответственность |
|---|---|---|
| Источник | Firebird 5, база `proxy_egisz` | Журнал обмена шлюза и справочники клиник. |
| Оркестратор | Apache Airflow 2.11.2 | Инкрементальная загрузка, запуск SQL-трансформации, refresh витрин, watermark. |
| DWH | PostgreSQL 16+, база `dwh_egisz` | Staging-слой, справочники, persistent facts, функции парсинга, аналитические view. |
| BI | Metabase v0.60.2.5 | 8 дашбордов, 89 native SQL-карточек. |
| Деплой | Kubernetes + Docker Desktop | Airflow через Helm, Metabase через манифест, запуск через `up.ps1`. |

Локальные порты: Airflow `localhost:8080`, Metabase `localhost:3000`.

Служебные базы разделены:

| База | Назначение |
|---|---|
| `dwh_egisz` | Аналитический DWH. |
| `airflow_db` | Метаданные Airflow. |
| `metabase_app` | Внутреннее состояние Metabase. |

## Источник данных

Firebird-база `proxy_egisz` содержит журнал сообщений шлюза и справочники организаций. Сервис читает источник инкрементально, без записи в Firebird.

| Таблица источника | Назначение | Основные поля |
|---|---|---|
| `EXCHANGELOG` | Основной журнал обмена: исходящие сообщения, callback'и, технические события. | `LOGID`, `LOGDATE`, `CREATEDATE`, `MSGID`, `LOGSTATE`, `LOGTEXT`, `MSGTEXT` |
| `JPERSONS` | Справочник организаций. | `JID`, `JNAME`, `JINN`, `JADDR` |
| `EGISZ_LICENSES` | Привязка клиник, OID, JID и доменов МИС. | `ID`, `SERVICE_TYPE`, `JID`, `MO_UID`, `MO_DOMEN`, `BDATE`, `FDATE`, `KIND`, `MODIFYDATE` |

Firebird-запросы используют keyset pagination:

```sql
WHERE <cursor_column> > ?
ORDER BY <cursor_column>
ROWS ?
```

`LIMIT/OFFSET` в Firebird-диалекте не используется.

## Airflow ELT

DAG: `airflow/dags/egisz_elt_dag.py`

Параметры:

| Параметр | Значение |
|---|---|
| `dag_id` | `egisz_elt_dag` |
| Расписание | `*/5 * * * *` |
| `max_active_runs` | `1` |
| `BATCH_SIZE` | `3000` |
| Pipeline key | `egisz` |
| Firebird connection | `proxy_egisz_fb` |
| PostgreSQL connection | `dwh_egisz_pg` |

Доступы к источнику и DWH берутся из Airflow Connections (`proxy_egisz_fb`, `dwh_egisz_pg`) через `BaseHook.get_connection`; учётные данные в коде и переменных окружения не хранятся. Нижняя отсечка источника (`source_min_created_at`) — конфигурация в `elt_state`, не константа DAG. Расписание `*/5 * * * *` — polling; идемпотентность обеспечивает watermark, а не `data_interval`. `catchup=False`, поэтому пайплайн идёт только вперёд по watermark.

Последовательность задач:

```text
sync_dimensions
  -> extract_and_load_batch
  -> analyze_staging
  -> transform_data
  -> refresh_materialized_views
  -> update_watermark
```

| Задача | Результат |
|---|---|
| `sync_dimensions` | Обновляет `dim_organizations` и `dim_licenses` из Firebird. |
| `extract_and_load_batch` | Keyset-выборка `EXCHANGELOG` по `LOGID` из Firebird и сразу UPSERT в `exchangelog_raw` в одном таске (payload не уходит в XCom). |
| `analyze_staging` | Обновляет статистику PostgreSQL для `exchangelog_raw` после загрузки. |
| `transform_data` | Вызывает `public.egisz_transform_raw_to_facts(...)`, заполняет центральный факт `fact_egisz_documents`, lineage-таблицу `fact_egisz_transactions` и инкрементально сопровождает витрину `v_egisz_documents_enriched_ui` по затронутым `document_key`. |
| `refresh_materialized_views` | Обновляет дневной rollup `v_egisz_documents_daily_ui`, затем запускает `ANALYZE`. Обогащённая витрина `v_egisz_documents_enriched_ui` сопровождается инкрементально в `transform_data`, а не полным REFRESH. |
| `update_watermark` | Продвигает `last_logid` в `elt_state` через `GREATEST(current, new)`. |

XCom-контракт между задачами (только метаданные батча, без строк журнала):

```python
{
    "count": int,
    "last_logid": int,
    "cursor_logid": int,
}
```

Опционально после `transform_data`: `"transformed": int`. Полезная нагрузка `EXCHANGELOG` (включая `logtext`/`msgtext`) остаётся в памяти таска `extract_and_load_batch` и не сериализуется в metadata-БД Airflow.

## DWH-модель

Схема DWH описана в `db/dwh_init.sql` и модулях `db/parts/*.sql`. Скрипты идемпотентны и рассчитаны на повторный запуск.

Запуск:

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Модули схемы:

| Файл | Назначение |
|---|---|
| `00_bootstrap.sql` | Роль `egisz`, права на схему `public`. |
| `10_tables.sql` | Таблицы, индексы, seed `dim_semd_types`. |
| `20_functions_parsing.sql` | XML-парсинг, нормализация идентификаторов, host/JID helpers. |
| `30_error_rules.sql` | Декларативные правила интерпретации ошибок. |
| `40_functions_errors.sql` | Функции классификации и подготовки error JSON. |
| `50_transform.sql` | Основная функция `egisz_transform_raw_to_facts`. |
| `60_drop_dependents.sql` | Идемпотентная пересборка зависимых view. |
| `70_views_core.sql` | Источник витрины `v_egisz_documents_enriched_src`, persistent-витрина `v_egisz_documents_enriched_ui`, дневной rollup `v_egisz_documents_daily_ui`, `v_rpt_error_interpretations_ui`. |
| `75_views_stg.sql` | Stage-view сетевых ошибок (`v_stg_channel_errors_by_document`) поверх `fact_egisz_documents` со `status = network_error` для дашбордов 02/04. |
| `80_views_rpt.sql` | Отчётные `v_rpt_*_ui`. |
| `90_views_health_and_finalize.sql` | Healthcheck-view, ownership, первичное наполнение витрины, refresh дневного rollup и `ANALYZE`. |

Основные слои:

| Слой | Объекты | Содержание |
|---|---|---|
| Raw staging | `exchangelog_raw` | Временный входной слой для парсинга SOAP/XML журнала. Партиционирована по `createdate` (RANGE, помесячно + DEFAULT). Production может очищать старые партиции raw после разложения в facts. |
| ELT state | `elt_state` | Watermark загрузки (`last_logid`) и конфигурация отсечки источника (`source_min_created_at`). |
| Dimensions | `dim_organizations`, `dim_licenses`, `dim_semd_types`, `dim_egisz_exchangelog_refs` | Организации, лицензии, типы СЭМД, индекс сообщений EXCHANGELOG для связи callback с документом. |
| Fact | `fact_egisz_documents`, `fact_egisz_transactions` | Центральный документный факт СЭМД и lineage callback/ошибок (включая `network_error` и `async_error`). `fact_egisz_transactions` партиционирована по `log_date` (RANGE, помесячно + DEFAULT). |
| Витрины | `v_egisz_documents_enriched_ui` (persistent-таблица + источник `v_egisz_documents_enriched_src`), `v_egisz_documents_daily_ui` (materialized view) | Быстрые витрины Metabase поверх document-grain facts. `enriched_ui` сопровождается инкрементально в `egisz_transform_raw_to_facts` по затронутым `document_key`, без полного REFRESH; дневной rollup `daily_ui` остаётся materialized view. |
| Reporting views | `v_rpt_*_ui`, `v_health_*_ui` | Готовые SQL-интерфейсы для карточек. |

`_raw`-таблицы не являются источником отчётности. Представления и дашборды должны читать только persistent DWH-слой (`fact_egisz_*`, dimensions, витрины `v_egisz_documents_*`). Это важно для production, где raw staging периодически очищается.

### Партиционирование и ретеншн

`exchangelog_raw` и `fact_egisz_transactions` — монотонно растущие time-series таблицы. При init (`db/parts/10_tables.sql`) они идемпотентно конвертируются в RANGE-партиции по времени (`createdate` и `log_date` соответственно): помесячные партиции на 12 месяцев назад и 24 вперёд от текущего UTC-месяца, плюс DEFAULT-партиция для строк вне диапазона. Повторный `dwh_init.sql` пропускает конвертацию, если таблица уже `relkind = 'p'`, и только досоздаёт недостающие партиции.

Первичный ключ включает ключ партиционирования (`logid, createdate` и `exchangelog_log_id, log_date`), потому что PostgreSQL требует этого для UNIQUE/PK на партиционированных таблицах; `logid` / `exchangelog_log_id` по-прежнему уникальны в данных источника, UPSERT-таргеты в `load_raw_logs` и `egisz_transform_raw_to_facts` обновлены соответственно.

Ретеншн: удаление старых партиций (`DROP TABLE ..._y2024m01`) — операционная процедура вне init; BI не читает raw, поэтому срок хранения staging/fact-transactions настраивается независимо от витрин.

Статусы фактов. Машинный код хранится в `fact_egisz_documents.status` и пробрасывается в витрины как `«Статус (код)»`; единая русская нотификация — в колонке `«Статус»`.

| Статус (код) | Нотификация в колонке «Статус» | Значение |
|---|---|---|
| `success` | **Успешно зарегистрирован** | По документу получен `<ns2:status>` = success. |
| `async_error` | **Ошибка асинхронного ответа РЭМД** | По документу получен `<ns2:status>` = error — финальный отказ РЭМД в callback (`sendRegisterDocumentResult`). |
| `network_error` | **Ошибка связи** | Последняя запись по идентифицируемому документу с `LOGSTATE = 3` (транспортная/сетевая ошибка канала). |
| `waiting` | **В обработке** | Документ отправлен из МО в ЕГИСЗ и идентифицирован (клиника, тип СЭМД), но ответа или ошибки по нему ещё не было. |

Единая нотификация. Колонка **«Статус»** во всех таблицах и графиках содержит один и тот же адаптированный русский текст (4 значения выше); машинный код для фильтров и агрегаций — **«Статус (код)»**. Статус пересчитывается при каждом обновлении документа в `egisz_transform_raw_to_facts` (последний callback выбирается через `GREATEST(last_callback_at)`), поэтому после новых данных доли и счётчики меняются согласованно.

Единый документный универсум. Все карточки, чья бизнес-логика — количество документов, считают одно и то же общее число — `COUNT(DISTINCT «Документ (ключ учёта)»)` по всем 4 статусам — и показывают его в разных срезах; статусные диаграммы суммируются к этому итогу (включая **«В обработке»**). Доли успеха/ошибок (%) считаются по финализированным документам (`success` + `async_error` + `network_error`), без «В обработке». Карточки по ошибкам используют отдельный универсум (`async_error` + `network_error`) — одинаковый между всеми срезами по ошибкам. Очередь «В обработке» дополнительно детализируется по возрасту ожидания в дашборде `03_documents_no_response.json`.

Запись об ЭМД появляется в `fact_egisz_documents` при наличии минимального набора реквизитов — **localUid + JID + KIND**. Поля могут приходить разными сообщениями одного документа: трансформация собирает их по `document_key` из getDocumentFile-сообщений (окно батча и lookback назад), запись создаётся при полном наборе. Сетевые ошибки (`LOGSTATE=3`) фиксируются по наличию `localUid` независимо от KIND.

Связь callback с документом восстанавливается по цепочке сообщений в `EXCHANGELOG`:

| Объект | Назначение |
|---|---|
| `dim_egisz_exchangelog_refs` | Одна строка на `LOGID`: ключ сообщения (`exchange_msgid_norm`) и реквизиты СЭМД (`local_uid`, `document_id`, `emdr_id`, `document_key`). |
| `fact_egisz_documents` | Актуальное состояние документа; ключ учёта — `document_key` (обычно `lower(localUid)`). |
| `fact_egisz_transactions` | Lineage событий: одна строка на callback/ошибку в журнале; поле `message` — сырой текст события. |

Колбэк без `localUid` сопоставляется с документом через `relatesToMessage` → sibling-сообщение в EXCHANGELOG с тем же `exchange_msgid_norm`, затем по `emdrId` или `DOCUMENTID`.

## Парсинг и классификация

Парсинг SOAP/XML выполняется в PostgreSQL. Python не содержит предметной логики разбора payload.

| Функция | Назначение |
|---|---|
| `egisz_xml_text(payload, tag_name)` | Извлекает текст из XML-тега без привязки к namespace prefix. |
| `egisz_normalize_message_id(value)` | Приводит `<urn:uuid:...>`, `urn:uuid:...` и bare UUID к единой форме. |
| `safe_cast_timestamptz(text)` | Безопасно приводит текст к `timestamptz`. |
| `egisz_clean_host(text)` | Нормализует endpoint/host callback. |
| `egisz_extract_jid_from_endpoint(text)` | Извлекает JID из GOST endpoint. |
| `egisz_clean_text_value(text)` | Нормализует пробелы и служебные символы. |
| `egisz_normalize_semd_code(text)` | Приводит код СЭМД к канонической форме. |

Ошибки РЭМД приводятся к каноническим категориям через таблицу `egisz_error_interpretation_rules` и функции из `40_functions_errors.sql`.

| Группа ошибок | Примеры |
|---|---|
| Структура СЭМД | XSD, Schematron, некорректный XML. |
| Справочники НСИ | Код отсутствует, версия справочника недействительна, значение не соответствует НСИ. |
| Пациент и медработник | СНИЛС, ФРМР, ГИП, должность, подразделение. |
| Организация | ФРМО, OID, лицензия, привязка к РМИС. |
| Доступ и регистрация ИС | `ACCESS_DENIED`, `DISABLED_RMIS`, `NO_RMIS`. |
| ЭП и сертификаты | Истёкший или отозванный сертификат, недоступный CRL/OCSP. |
| Документооборот | Дубликат, аннулирование, документ не найден. |
| Получение файла ЭМД | Ошибки `getDocumentFile` и недоступность МИС. |
| Технические ошибки РЭМД | `INTERNAL_ERROR`, `RUNTIME_ERROR`, `ASYNC_RESPONSE_TIMEOUT`. |
| Сетевая ошибка | Таймауты, обрывы связи, недоступность endpoint. |
| Неизвестная ошибка | Callback или текст ошибки не попал ни под одно правило. |

Результат классификации хранится в колонках lineage и документного факта:

| Колонка | Таблица | Назначение |
|---|---|---|
| `message` | `fact_egisz_transactions` | Сырой текст события из XML или `LOGTEXT`. |
| `error_type` | `fact_egisz_transactions`, `fact_egisz_documents` | Короткий тип ошибки для группировки. |
| `error_summary` | `fact_egisz_transactions`, `fact_egisz_documents` | Пользовательская интерпретация. |
| `error_json_text` | `fact_egisz_transactions` | Нормализованный JSON ошибок для drill-down. |
| `error_text` | `fact_egisz_documents` | Исходный текст ошибки для отчётов (`COALESCE(error_json_text, message)`). |

## Дашборды Metabase

JSON-описания дашбордов находятся в `metabase_dashboards/`. Импорт выполняет `metabase/setup-dashboards.sh` при старте контейнера. Field-фильтры настраиваются скриптом `scripts/apply_metabase_field_filters.py` по правилам `metabase_dashboards/field_filter_defaults.yaml`.

| Дашборд | Назначение | Карточек | Основные источники | Фильтры |
|---|---|---|---|---|
| `01_operational.json` | Операционный поток документов, статусы, ошибки, динамика. | 9 | `v_egisz_documents_enriched_ui`, `v_rpt_error_category_breakdown_ui` | Период, код СЭМД, JID, идентификаторы |
| `02_service.json` | Healthcheck ETL, канала и прокси-БД. | 15 | `v_health_*_ui`, `v_stg_channel_errors_by_document` | Период, код ошибки, JID |
| `03_documents_no_response.json` | Очередь документов без финального callback. | 5 | `v_rpt_documents_no_response_ui` | Период, JID, тип СЭМД |
| `04_quality_and_errors.json` | Качество данных и детализация отказов РЭМД. | 22 | `v_rpt_error_category_breakdown_ui`, `v_rpt_error_interpretations_ui` | Период, категория, тип ошибки |
| `05_executive.json` | Управленческие KPI: активные JID, объёмы, статусы, MRR/ARR по фикс-тарифу. | 13 | `v_rpt_documents_ui`, `v_egisz_documents_enriched_ui` | Период |
| `06_semd_archive.json` | Архив документов и поиск по идентификаторам. | 6 | `v_rpt_semd_archive_ui` | Период, JID, код СЭМД, `localUid`, `emdrId`, `LOGID`, связанное сообщение, статус |
| `07_client_service.json` | Клиентский мониторинг по одному JID. | 8 | `v_rpt_client_documents_ui` | JID, период, тип документа |
| `08_client_bianalytic.json` | Клиентская BI-аналитика без раскрытия ПДн. | 11 | `v_rpt_client_documents_ui` | JID, период, тип документа |

Финансовые карточки используют модель `10 000 ₽ / JID / месяц`. MRR считается как количество активных JID за период, умноженное на фиксированный тариф.

## Эксплуатация

Все команды выполняются из корня репозитория.

### Предварительные требования

| Компонент | Назначение |
|---|---|
| Docker Desktop | Linux engine, сборка образов Airflow и Metabase. |
| Kubernetes в Docker Desktop | Namespace `egisz-bi`, workload Airflow и Metabase. |
| `kubectl`, `helm` | Деплой и управление кластером. |
| PostgreSQL 16+ | DWH `dwh_egisz` на хосте (`host.docker.internal:5432` для pod'ов). |
| Firebird 5 | Источник `proxy_egisz` на хосте (`host.docker.internal:3050` для pod'ов). |
| Python 3.11+ | Тесты и вспомогательные скрипты (`py -m pytest`). |

Порядок первичного развёртывания:

1. В Docker Desktop включить Kubernetes и дождаться статуса *Running*.
2. Проверить контекст: `kubectl config get-contexts`. При необходимости: `kubectl config use-context docker-desktop`.
3. Создать роль и базу DWH (один раз на чистом PostgreSQL):

```sql
CREATE ROLE egisz LOGIN PASSWORD 'egisz';
CREATE DATABASE dwh_egisz OWNER postgres;
```

4. Развернуть схему DWH:

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

5. Запустить стенд: `.\up.ps1`.
6. В Airflow снять паузу с DAG `egisz_elt_dag`.

Скрипт `up.ps1` проверяет Docker Linux engine, доступность Kubernetes и при отсутствии текущего контекста пытается выбрать `docker-desktop` или единственный доступный контекст.

### Запуск стенда

```powershell
.\up.ps1                         # Airflow + Metabase
.\up.ps1 -Action Airflow         # только Airflow
.\up.ps1 -Action Metabase        # только Metabase
.\up.ps1 -Action Stop            # остановка без удаления PVC
.\up.ps1 -Action Stop-Airflow    # остановить Airflow
.\up.ps1 -Action Stop-Metabase   # остановить Metabase
```

Stop-команды масштабируют workload до нуля и сохраняют PVC.

После `Install-Airflow` скрипт напоминает выполнить `db/dwh_init.sql`, если схема ещё не развёрнута, и снять паузу с `egisz_elt_dag`.

### DWH

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Полная очистка DWH:

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -v CONFIRM_DWH_ERASE=1 -f db/dwh_erase.sql
```

`dwh_erase.sql` очищает объекты `public` только при явном флаге подтверждения; после очистки нужно повторно выполнить `db/dwh_init.sql`.

### Тесты

```powershell
py -m pytest -q
py -m pytest tests/test_dashboards.py -v
py -m pytest tests/test_pg_client.py -k normalize
```

### Проверка статистики DWH

```sql
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    n_mod_since_analyze,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE relname IN (
    'exchangelog_raw',
    'fact_egisz_documents',
    'fact_egisz_transactions',
    'v_egisz_documents_enriched_ui',
    'v_egisz_documents_daily_ui'
)
ORDER BY relname;
```

### Ручной refresh витрин

`v_egisz_documents_enriched_ui` — persistent-таблица: штатно её точечно сопровождает
`egisz_transform_raw_to_facts`. Полная пересборка (например, после ручного исправления
fact-слоя) — через источник; дневной rollup остаётся materialized view.

```sql
-- Полная пересборка обогащённой витрины из текущего fact-слоя:
TRUNCATE public.v_egisz_documents_enriched_ui;
INSERT INTO public.v_egisz_documents_enriched_ui
SELECT * FROM public.v_egisz_documents_enriched_src;

REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_daily_ui;
ANALYZE public.v_egisz_documents_enriched_ui;
ANALYZE public.v_egisz_documents_daily_ui;
```

## Структура репозитория

```text
airflow/dags/egisz_elt_dag.py      DAG Airflow
src/egisz_elt/fb_client.py         чтение Firebird
src/egisz_elt/pg_client.py         загрузка PostgreSQL и watermark
db/dwh_init.sql                    сборка DWH-схемы
db/dwh_erase.sql                   полная очистка DWH-схемы
db/parts/*.sql                     модульная DWH-схема
metabase_dashboards/*.json         дашборды Metabase
metabase/setup-dashboards.sh       импорт дашбордов
scripts/apply_metabase_field_filters.py
tests/                             pytest-тесты
k8s/                               манифесты Airflow и Metabase
up.ps1                             локальный запуск Kubernetes-стенда
```

## Глоссарий

| Термин | Значение |
|---|---|
| ЕГИСЗ | Единая государственная информационная система в сфере здравоохранения. |
| РЭМД | Реестр электронных медицинских документов. |
| СЭМД | Структурированный электронный медицинский документ. |
| МИС | Медицинская информационная система клиники. |
| МО | Медицинская организация. |
| JID | Внутренний идентификатор организации в прокси-системе. |
| OID | Идентификатор организации в ФРМО/ЕГИСЗ. |
| `localUid` | Идентификатор документа на стороне МИС. |
| `emdrId` | Регистрационный номер документа в РЭМД. |
| `messageId` | Идентификатор SOAP/Ws-Addressing сообщения. |
| `relatesTo` / `relatesToMessage` | Ссылка callback на исходное сообщение в цепочке EXCHANGELOG. |
| `exchange_msgid_norm` | Нормализованный идентификатор сообщения в `dim_egisz_exchangelog_refs`. |
| `document_key` | Ключ учёта документа в DWH (обычно `lower(localUid)`). |
| Watermark | Текущий курсор пайплайна по `LOGID` (`elt_state.last_logid`, продвигается через `GREATEST`). |
| Raw layer | Временные staging-таблицы DWH с данными источника до предметной трансформации. |
| Fact table | Таблица нормализованных транзакций обмена СЭМД. |
| Materialized view | Предрасчитанная витрина PostgreSQL для быстрых BI-запросов. |
