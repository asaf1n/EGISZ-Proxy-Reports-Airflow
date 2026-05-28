# Сервис эксплуатационной аналитики обмена с ЕГИСЗ

Сервис собирает данные обмена СЭМД с ЕГИСЗ из прокси-БД интегратора, преобразует их в аналитическую модель PostgreSQL и публикует эксплуатационные дашборды в Metabase.

Основной сценарий: каждые 5 минут Airflow забирает новые строки журналов Firebird, загружает raw-слой как временный staging, разбирает его в persistent DWH-таблицы, обновляет материализованные витрины и продвигает watermark. Пользователь видит состояние отправок, callback'ов, ошибок, очередей ожидания и здоровья ETL-контура по разложенным fact-таблицам, а не по raw.

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
| Здоровье ETL | Свежесть данных, движение watermark, объём staging/fact-слоёв, состояние материализованных витрин. |

Текущее рабочее окно данных стенда начинается с `2026-05-18`. Ограничение закреплено в DAG через `SOURCE_MIN_CREATED_AT` и применяется к основным и связанным строкам Firebird.

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
| BI | Metabase v0.60.2.5 | 8 дашбордов и около 100 native SQL-карточек. |
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
| `EGISZ_MESSAGES` | Метаданные сообщений и callback'ов. | `EGMID`, `CREATEDATE`, `MSGID`, `REPLYTO`, `DOCUMENTID` |
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

Последовательность задач:

```text
sync_dimensions
  -> extract_cursor_batches
  -> load_to_dwh
  -> analyze_staging
  -> transform_data
  -> refresh_materialized_views
  -> update_watermark
```

| Задача | Результат |
|---|---|
| `sync_dimensions` | Обновляет `dim_organizations` и `dim_licenses` из Firebird. |
| `extract_cursor_batches` | Забирает только курсорные партии `EXCHANGELOG` и `EGISZ_MESSAGES`. |
| `load_to_dwh` | Выполняет UPSERT журнальных payload'ов в `exchangelog_raw` и напрямую сохраняет структурированные `EGISZ_MESSAGES` в `stg_egisz_messages`. |
| `analyze_staging` | Обновляет статистику PostgreSQL для staging-таблиц и `stg_egisz_messages` после загрузки. |
| `resolve_related_refs_from_dwh` | Просит PostgreSQL разобрать загруженный `exchangelog_raw` и вернуть связанные `MSGID` / идентификаторы документов. |
| `load_related_messages` | Догружает связанные старые `EGISZ_MESSAGES` по идентификаторам из DWH-парсинга и пишет их в `stg_egisz_messages`. |
| `transform_data` | Вызывает `public.egisz_transform_raw_to_facts(...)`, заполняет центральный факт `fact_egisz_documents`, ошибки канала и внутреннюю lineage-таблицу `fact_egisz_transactions`. |
| `refresh_materialized_views` | Обновляет `v_egisz_documents_enriched_ui`, `v_egisz_documents_daily_ui` и `v_stg_channel_errors_by_document`, затем запускает `ANALYZE`. |
| `update_watermark` | Продвигает `elt_state` через `GREATEST(current, new)`. |

XCom-контракт между задачами:

```python
{
    "count": int,
    "message_count": int,
    "cursor_message_count": int,
    "last_log_id": int,
    "last_egmid": int,
    "cursor_logid": int,
    "cursor_egmid": int,
    "rows": list[dict],
    "message_rows": list[dict],
}
```

Watermark обновляется только после успешной загрузки, трансформации и refresh витрин.

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
| `70_views_core.sql` | `v_egisz_documents_enriched_ui`, `v_rpt_error_interpretations_ui`. |
| `75_views_stg.sql` | `v_stg_channel_errors_by_document`, alias сетевых ошибок. |
| `80_views_rpt.sql` | Отчётные `v_rpt_*_ui`. |
| `90_views_health_and_finalize.sql` | Healthcheck-view, ownership, refresh и `ANALYZE`. |

Основные слои:

| Слой | Объекты | Содержание |
|---|---|---|
| Raw staging | `exchangelog_raw` | Временный входной слой для парсинга SOAP/XML журнала. Production может очищать эту таблицу после разложения данных в DWH facts. `EGISZ_MESSAGES` уже структурирована и грузится напрямую в `stg_egisz_messages`. |
| ELT state | `elt_state` | Курсоры загрузки `last_log_id` и `last_egmid`. |
| Dimensions | `dim_organizations`, `dim_licenses`, `dim_semd_types` | Организации, лицензии, типы СЭМД. |
| Fact | `fact_egisz_documents`, `fact_egisz_channel_errors`, `fact_egisz_transactions` | Центральный документный факт СЭМД, ошибки канала и внутренняя callback/event lineage-таблица. |
| Staging | `stg_egisz_messages` | Структурированный входной слой `EGISZ_MESSAGES`; не является источником отчётности. |
| Materialized views | `v_egisz_documents_enriched_ui`, `v_egisz_documents_daily_ui`, `v_stg_channel_errors_by_document` | Быстрые витрины для Metabase и отчётных view; строятся поверх document-grain facts. |
| Reporting views | `v_rpt_*_ui`, `v_health_*_ui` | Готовые SQL-интерфейсы для карточек. |

`_raw`-таблицы не являются источником отчётности. Представления и дашборды должны читать только persistent DWH-слой (`fact_egisz_*`, dimensions, materialized views). Это важно для production, где raw staging периодически очищается.

Статусы фактов:

| Статус | Значение |
|---|---|
| `success` | Успешный ответ ЕГИСЗ/РЭМД по документу. |
| `error` | РЭМД вернул финальный отказ или зафиксирована сетевая ошибка. |
| `pending` | Промежуточный или не финальный callback; не используется в основных аналитических дашбордах. |
| `unknown` | Callback получен, но статус не распознан правилами; не используется в основных аналитических дашбордах. |

Пользовательская разбивка статусов ЭМД в аналитике: **Успешный ответ**, **Ошибка регистрации**, **Ошибка связи**. Документы без связанного callback'а показываются только в дашборде `03_documents_no_response.json`. `error_type` заполняется только для `status = 'error'`; для `success`, `pending` и `unknown` значение остаётся `NULL`.

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

Результат классификации хранится в трёх колонках:

| Колонка | Назначение |
|---|---|
| `error_type` | Короткий тип ошибки для группировки. |
| `error_summary` | Пользовательская интерпретация. |
| `error_json_text` | Полный нормализованный JSON для drill-down. |

## Дашборды Metabase

JSON-описания дашбордов находятся в `metabase_dashboards/`. Импорт выполняет `metabase/setup-dashboards.sh` при старте контейнера. Field-фильтры настраиваются скриптом `scripts/apply_metabase_field_filters.py` по правилам `metabase_dashboards/field_filter_defaults.yaml`.

| Дашборд | Назначение | Основные источники | Фильтры |
|---|---|---|---|
| `01_operational.json` | Операционный поток документов, статусы, ошибки, динамика. | `v_egisz_documents_enriched_ui`, `v_rpt_error_category_breakdown_ui` | Период, код СЭМД, JID, идентификаторы |
| `02_service.json` | Healthcheck ETL, канала и прокси-БД. | `v_health_*_ui`, `v_stg_channel_errors_by_document` | Период, код ошибки, JID |
| `03_documents_no_response.json` | Очередь документов без финального callback. | `v_rpt_documents_no_response_ui` | Период, JID, тип СЭМД |
| `04_quality_and_errors.json` | Качество данных и детализация отказов РЭМД. | `v_rpt_error_category_breakdown_ui`, `v_rpt_error_interpretations_ui` | Период, категория, тип ошибки |
| `05_executive.json` | Управленческие KPI: активные JID, объёмы, статусы, MRR/ARR по фикс-тарифу. | `v_rpt_documents_ui`, `v_egisz_documents_enriched_ui` | Период |
| `06_semd_archive.json` | Архив документов и поиск по идентификаторам. | `v_rpt_semd_archive_ui` | Период, JID, код СЭМД, `localUid`, `emdrId`, `LOGID`, `EGMID` |
| `07_client_service.json` | Клиентский мониторинг по одному JID. | `v_rpt_client_documents_ui` | JID, период, тип документа |
| `08_client_bianalytic.json` | Клиентская BI-аналитика без раскрытия ПДн. | `v_rpt_client_documents_ui` | JID, период, тип документа |

Финансовые карточки используют модель `10 000 ₽ / JID / месяц`. MRR считается как количество активных JID за период, умноженное на фиксированный тариф.

## Эксплуатация

Все команды выполняются из корня репозитория.

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

### DWH

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
```

Полная очистка DWH:

```powershell
psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_erase.sql
```

`dwh_erase.sql` удаляет объекты схемы `public` и роль `egisz`; после него нужно повторно выполнить `db/dwh_init.sql`.

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
    'stg_egisz_messages',
    'fact_egisz_documents',
    'fact_egisz_channel_errors',
    'fact_egisz_transactions',
    'v_egisz_documents_enriched_ui',
    'v_stg_channel_errors_by_document'
)
ORDER BY relname;
```

### Ручной refresh витрин

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_enriched_ui;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_stg_channel_errors_by_document;
ANALYZE public.v_egisz_documents_enriched_ui;
ANALYZE public.v_stg_channel_errors_by_document;
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
| `relatesTo` / `relatesToMessage` | Ссылка callback на исходное сообщение. |
| Watermark | Последний успешно обработанный `LOGID` и `EGMID`. |
| Raw layer | Временные staging-таблицы DWH с данными источника до предметной трансформации. |
| Fact table | Таблица нормализованных транзакций обмена СЭМД. |
| Materialized view | Предрасчитанная витрина PostgreSQL для быстрых BI-запросов. |
