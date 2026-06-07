# Сервис эксплуатационной аналитики обмена с ЕГИСЗ

Сервис собирает данные обмена СЭМД с ЕГИСЗ из прокси-БД интегратора, преобразует их в аналитическую модель PostgreSQL и публикует эксплуатационные дашборды в Metabase.

Основной сценарий: каждые 5 минут Airflow забирает новые строки журналов Firebird, загружает raw-слой как временный staging, разбирает его в persistent DWH-таблицы, инкрементально сопровождает аналитические витрины и продвигает watermark. Пользователь видит состояние отправок, callback'ов, ошибок, очередей ожидания и здоровья ETL-контура по разложенным fact-таблицам, а не по raw.

Единый источник описания проекта — этот файл (`README.md`): предметная область, архитектура, DWH, парсинг, дашборды и эксплуатация. При разработке и автоматизации (в том числе с ИИ-агентами) ориентироваться на README и общие инструкции репозитория; доменные детали в код и комментарии не дублировать — ссылаться на соответствующий раздел README.

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
| Здоровье ETL | Свежесть данных, движение watermark, дозагрузку опоздавших строк журнала, объём staging/fact-слоёв, актуальность витрин. |

Нижней отсечки по дате нет: forward-выборка `EXCHANGELOG` идёт только по LOGID-курсору (`LOGID > last_logid`), а дозагрузка опоздавших строк сравнивает полосу `LOGID` под watermark с `exchangelog_raw` — тоже без даты (см. «Дозагрузка опоздавших строк»).

## Предметная область

**ЕГИСЗ** — Единая государственная информационная система в сфере здравоохранения.

**РЭМД** — реестр электронных медицинских документов, куда медицинские организации передают СЭМД.

**СЭМД** — структурированный электронный медицинский документ: выписной эпикриз, протокол, направление, заключение и другие типы медицинских документов.

Обмен с РЭМД асинхронный. МИС формирует CDA-документ, подписывает его, отправляет через интеграционный шлюз методом `RegisterDocument`. На запрос приходит **синхронный** SOAP-ответ `RegisterDocumentResponse` со `<status>success</status>` — он подтверждает только приём запроса РЭМД (валидный синтаксис), но **не регистрацию документа**. Регистрация подтверждается отдельным **асинхронным** callback'ом `registerDocumentResult` со `<documentStatus>Зарегистрировано</documentStatus>` (или `<status>OK</status>`) и присвоенным `emdrId`. Аналитические статусы строятся по итоговым записям: документ зарегистрирован и получил `emdrId`, отклонён с ошибками регистрации или не дошёл из-за ошибки связи; синхронный приём остаётся в статусе «в обработке» до асинхронного ответа.

Одна бизнес-операция обычно представлена несколькими техническими сообщениями. Поэтому сервис восстанавливает цепочку обмена по нескольким идентификаторам: `messageId`, `relatesTo`, `relatesToMessage`, `localUid`, `emdrId`, `MSGID`. Канонический ключ учёта документа — всегда `lower(localUid)`; `emdrId` (рег. номер РЭМД) и `OID` (код типа в справочнике НСИ / OID организации) — атрибуты, а не идентификатор экземпляра, и ключом документа быть не могут.

## Архитектура

Проект построен как ELT-сервис. Python отвечает за оркестрацию и загрузку raw staging. Предметная трансформация выполняется в PostgreSQL функциями PL/pgSQL и сохраняет результат в persistent fact-таблицы.

| Компонент | Технология | Ответственность |
|---|---|---|
| Источник | Firebird 5, база `proxy_egisz` | Журнал обмена шлюза и справочники клиник. |
| Оркестратор | Apache Airflow 2.11.2 | Инкрементальная загрузка, запуск SQL-трансформации, refresh витрин, watermark, дозагрузка опоздавших строк журнала. |
| DWH | PostgreSQL 16+, база `dwh_egisz` | Staging-слой, справочники, persistent facts, функции парсинга, аналитические view. |
| BI | Metabase v0.60.2.5 | 8 дашбордов, 106 SQL-карточек. |
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
| `BATCH_SIZE` | `5000` |
| Pipeline key | `egisz` |
| Firebird connection | `proxy_egisz_fb` |
| PostgreSQL connection | `dwh_egisz_pg` |

Доступы к источнику и DWH берутся из Airflow Connections (`proxy_egisz_fb`, `dwh_egisz_pg`) через `BaseHook.get_connection`; учётные данные в коде и переменных окружения не хранятся. Расписание `*/5 * * * *` — polling; идемпотентность обеспечивает watermark, а не `data_interval`. `catchup=False`, поэтому пайплайн идёт только вперёд по watermark.

Последовательность задач:

```text
  -> sync_dimensions
  -> extract_and_load_batch
  -> analyze_staging
  -> transform_data
  -> refresh_materialized_views
  -> update_watermark
  -> reconcile_late_arrivals
```

| Задача | Результат |
|---|---|
| `sync_dimensions` | Обновляет `dim_organizations` и `dim_licenses` из Firebird. |
| `extract_and_load_batch` | Keyset-выборка `EXCHANGELOG` по `LOGID` из Firebird и сразу UPSERT в `exchangelog_raw` в одном таске (payload не уходит в XCom). |
| `analyze_staging` | Обновляет статистику PostgreSQL для `exchangelog_raw` после загрузки. |
| `transform_data` | Вызывает `public.egisz_transform_raw_to_facts(...)`: разлагает payload батча в `dim_egisz_exchangelog_refs` (один проход XML на `LOGID`), заполняет `fact_egisz_documents`, lineage-таблицу `fact_egisz_transactions` и инкрементально сопровождает витрину `v_egisz_documents_enriched_ui` по затронутым `document_key`. |
| `refresh_materialized_views` | Обновляет дневной rollup `v_egisz_documents_daily_ui`, затем запускает `ANALYZE`. Обогащённая витрина `v_egisz_documents_enriched_ui` сопровождается инкрементально в `transform_data`, а не полным REFRESH. |
| `update_watermark` | Продвигает `last_logid` в `elt_state` через `GREATEST(current, new)`. |
| `reconcile_late_arrivals` | Сетка безопасности от внеочередного наполнения журнала. Сравнивает полосу `LOGID` прокси непосредственно под watermark с `exchangelog_raw` и догружает+трансформирует недостающее, **не двигая** `last_logid`. В установившемся режиме — no-op. |

#### Дозагрузка опоздавших строк

Прокси-журнал материализует строки **не по порядку `LOGID`**: асинхронные callback'и СЭМД и массовые backfill'ы шлюза появляются ниже уже продвинутого watermark. Форвардный курсор `LOGID > last_logid` такие строки не видит и теряет их навсегда.

Поэтому `reconcile_late_arrivals` каждый цикл сравнивает множество `LOGID` прокси в полосе непосредственно под watermark — `(last_logid - RECONCILE_WATERMARK_LOOKBACK_LOGIDS, last_logid]` — с `exchangelog_raw` и догружает недостающее, не сдвигая `last_logid` назад. Единственным писателем курсора остаётся `GREATEST(current, new)` в `update_watermark`. Восстановленные `LOGID` трансформируются плотными окнами, а не одним диапазоном `min..max`.

Полоса ограничена по `LOGID`, а не по дате: скан дешёвый и не зависит от размера журнала. Ширина полосы (`RECONCILE_WATERMARK_LOOKBACK_LOGIDS` в DAG, по умолчанию `200000`) должна с запасом перекрывать типичный разброс внеочередного наполнения (инцидент 2026-06-01 — ~68k ниже watermark); шире — надёжнее ловит совсем старые строки, но дороже скан. Связь восстановленного callback'а с родительским документом происходит в transform по уже разложенному `relates_to_id` → `dim_egisz_exchangelog_refs.exchange_msgid_norm`, без повторного сканирования `MSGTEXT`.

XCom-контракт между задачами (только метаданные батча, без строк журнала):

```python
{
    "count": int,
    "last_logid": int,
    "cursor_logid": int,
}
```

Опционально после `transform_data`: `"transformed": int`. Задача `reconcile_late_arrivals` не принимает XCom-вход и читает своё состояние из `elt_state`, возвращая число дозагруженных строк. Полезная нагрузка `EXCHANGELOG` (включая `logtext`/`msgtext`) остаётся в памяти таска `extract_and_load_batch` и не сериализуется в metadata-БД Airflow.

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
| `20_functions_parsing.sql` | XML-парсинг (`egisz_xml_text`, `egisz_parse_exchangelog_row`), нормализация идентификаторов, классификация статуса, host/JID helpers. |
| `30_error_rules.sql` | Декларативные правила интерпретации ошибок. |
| `40_functions_errors.sql` | Функции классификации и подготовки error JSON. |
| `50_transform.sql` | Основная функция `egisz_transform_raw_to_facts`: разложение в dim, сборка facts и инкрементальное сопровождение `v_egisz_documents_enriched_ui`. |
| `60_drop_dependents.sql` | Идемпотентная пересборка зависимых view. |
| `70_views_core.sql` | Источник витрины `v_egisz_documents_enriched_src`, persistent-витрина `v_egisz_documents_enriched_ui`, дневной rollup `v_egisz_documents_daily_ui`, `v_rpt_error_interpretations_ui`. |
| `75_views_stg.sql` | View сетевых ошибок (`v_stg_channel_errors_by_document`, alias `v_stg_channel_network_errors_by_document`) поверх `fact_egisz_documents` со `status = network_error` для дашбордов 02/04. Отдельной таблицы `fact_egisz_channel_errors` нет. |
| `80_views_rpt.sql` | Отчётные `v_rpt_*_ui`. |
| `90_views_health_and_finalize.sql` | Healthcheck-view, ownership, первичное наполнение витрины, refresh дневного rollup и `ANALYZE`. |

Основные слои:

| Слой | Объекты | Содержание |
|---|---|---|
| Raw staging | `exchangelog_raw` | Временный входной слой для парсинга SOAP/XML журнала. Партиционирована по `createdate` (RANGE, помесячно + DEFAULT). Production может очищать старые партиции raw после разложения в facts. |
| ELT state | `elt_state` | Watermark загрузки (`last_logid`) на конвейер. |
| Dimensions | `dim_organizations`, `dim_licenses`, `dim_semd_types`, `dim_egisz_exchangelog_refs` | Организации, лицензии, типы СЭМД, persistent-слой разложенного payload EXCHANGELOG (одна строка на `LOGID`) для связи callback с документом и всех последующих шагов transform. |
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
| `success` | **Успешно зарегистрирован** | Получен **асинхронный** callback регистрации: `<documentStatus>Зарегистрировано</documentStatus>` либо `<status>OK/success</status>` в `registerDocumentResult`. Синхронный `RegisterDocumentResponse` со `<status>success</status>` сюда **не** относится — он остаётся `waiting`. |
| `async_error` | **Ошибка асинхронного ответа РЭМД** | По документу получен `<ns2:status>` = error — финальный отказ РЭМД в callback (`sendRegisterDocumentResult`). |
| `network_error` | **Ошибка связи** | Последняя запись по идентифицируемому документу с `LOGSTATE = 3` (транспортная/сетевая ошибка канала). |
| `waiting` | **В обработке** | Документ отправлен из МО в ЕГИСЗ и идентифицирован (клиника, тип СЭМД), но ответа или ошибки по нему ещё не было. |

Единая нотификация. Колонка **«Статус»** во всех таблицах и графиках содержит один и тот же адаптированный русский текст (4 значения выше); машинный код для фильтров и агрегаций — **«Статус (код)»**. Статус пересчитывается при каждом обновлении документа в `egisz_transform_raw_to_facts` (последний callback выбирается через `GREATEST(last_callback_at)`), поэтому после новых данных доли и счётчики меняются согласованно.

Единый документный универсум. Все карточки, чья бизнес-логика — количество документов, считают одно и то же общее число — `COUNT(DISTINCT «Документ (ключ учёта)»)` по всем 4 статусам — и показывают его в разных срезах; статусные диаграммы суммируются к этому итогу (включая **«В обработке»**). Доли успеха/ошибок (%) считаются по финализированным документам (`success` + `async_error` + `network_error`), без «В обработке». Карточки по ошибкам используют отдельный универсум (`async_error` + `network_error`) — одинаковый между всеми срезами по ошибкам. Очередь «В обработке» дополнительно детализируется по возрасту ожидания в дашборде `03_documents_no_response.json`.

Запись об ЭМД появляется в `fact_egisz_documents` при наличии минимального набора реквизитов — **localUid + JID + KIND**. Поля могут приходить разными сообщениями одного документа: трансформация собирает их по `document_key` из getDocumentFile-сообщений (окно батча и lookback назад), запись создаётся при полном наборе. Сетевые ошибки (`LOGSTATE=3`) фиксируются по наличию `localUid` независимо от KIND.

Связь callback с документом восстанавливается по уже разложенным данным `dim_egisz_exchangelog_refs`, а не повторным парсингом `MSGTEXT`:

| Объект | Назначение |
|---|---|
| `dim_egisz_exchangelog_refs` | Одна строка на `LOGID`: результат единственного прохода `egisz_parse_exchangelog_row` — ключи связи (`exchange_msgid_norm`, `relates_to_id`), реквизиты СЭМД (`local_uid`, `emdr_id`, `document_key`, `kind_xml`, `action`, …), поля статуса/ошибки и BI-хэшируемые атрибуты. |
| `fact_egisz_documents` | Актуальное состояние документа; ключ учёта — `document_key` (всегда `lower(localUid)`). Колбэк без `localUid` не создаёт новый ключ, а обновляет существующую строку (резолв по `relates_to_id` / `emdrId` / предыдущему getDocumentFile). |
| `fact_egisz_transactions` | Lineage событий: одна строка на callback/ошибку в журнале; поле `message` — сырой текст события. |

Колбэк без `localUid` сопоставляется с документом через `relates_to_id` → строка dim с тем же `exchange_msgid_norm`, затем по `emdr_id`, затем по предыдущему getDocumentFile той же клиники (`jid_from_payload`). OID при этом не используется как ключ ни при каких условиях.

## Парсинг и классификация

Парсинг SOAP/XML выполняется в PostgreSQL. Python не содержит предметной логики разбора payload — он только загружает raw и вызывает `egisz_transform_raw_to_facts`.

### Поток разложения (один проход на LOGID)

Каждый XML-тег и regex-маркер статуса извлекаются **ровно один раз** на строку журнала. Повторного вызова `egisz_xml_text` по `MSGTEXT` внутри transform нет.

```text
exchangelog_raw
    → egisz_parse_exchangelog_row(msgtext, msgid, logtext)   # единственный проход regex/XML
    → dim_egisz_exchangelog_refs                             # persistent разложение
    → document_attributes / gdf_events / callback matching / fact_egisz_*
```

Первый шаг `egisz_transform_raw_to_facts`:

1. Разлагает все `LOGID` текущего батча `(from_logid, to_logid]` через `egisz_parse_exchangelog_row` и UPSERT в `dim_egisz_exchangelog_refs`.
2. Догружает lookback до 500 `LOGID` назад только для строк, ещё отсутствующих в dim (нужно для сборки getDocumentFile-реквизитов).
3. Все последующие CTE (сборка `localUid`+`JID`+`KIND`, `gdf_events`, фильтрация callback'ов, JOIN по `relates_to_id` / `emdr_id`) читают **только** `dim_egisz_exchangelog_refs` плюс не-XML поля из `exchangelog_raw` (`logstate`, `logtext`, `logdate`, `createdate`).

Исключение: разбор `<item>` внутри error-payload (`egisz_xml_error_items`) вызывается только для строк с `final_status = 'error'` при построении `error_json_text`.

### Утилитные функции

| Функция | Назначение |
|---|---|
| `egisz_xml_text(payload, tag_name)` | Низкоуровневое извлечение текста из XML-тега без привязки к namespace prefix. Вызывается только внутри `egisz_parse_exchangelog_row` и `egisz_xml_error_items`, не в transform напрямую. |
| `egisz_parse_exchangelog_row(msgtext, msgid, logtext)` | Полное разложение одной строки EXCHANGELOG: все теги, нормализованные идентификаторы, `jid_from_payload`, BI-поля и boolean-маркеры для `egisz_classify_async_status`. |
| `egisz_normalize_message_id(value)` | Приводит `<urn:uuid:...>`, `urn:uuid:...` и bare UUID к единой форме. |
| `egisz_document_key(local_uid)` | Канонический ключ документа — `lower(localUid)`. |
| `safe_cast_timestamptz(text)` | Безопасно приводит текст к `timestamptz`, возвращает `NULL` вместо исключения (даты приходят в нескольких форматах). |
| `egisz_clean_host(text)` | Нормализует endpoint/host callback для JOIN со справочником `dim_licenses`. |
| `egisz_extract_jid_from_endpoint(text)` | Извлекает JID из GOST endpoint вида `gost-1234.<домен>` → `1234`. |
| `egisz_clean_text_value(text)` | Нормализует пробелы, удаляет BOM и неразрывные пробелы. |
| `egisz_normalize_semd_code(text)` | Приводит код СЭМД к канонической форме (разные шаблоны используют разные префиксы/суффиксы). |
| `egisz_classify_async_status(...)` | Финальный статус callback'а по разложенным полям (`raw_status`, `document_status`, boolean-маркеры payload). Не перечитывает `MSGTEXT`. |
| `egisz_network_error_type(text)` | Сворачивает текст LOGSTATE=3 в канонический тип: URL, `gost-*` endpoint, UUID, IP и значения в `[…]` заменяются плейсхолдерами для топов на дашбордах 02/04. |

### Слой `dim_egisz_exchangelog_refs`

| Группа колонок | Примеры | Назначение |
|---|---|---|
| Ключи связи | `exchange_msgid_norm`, `relates_to_id` | JOIN callback → исходное сообщение / документ. |
| Реквизиты СЭМД | `local_uid`, `emdr_id`, `document_key`, `kind_xml`, `action` | Идентификация документа и сборка getDocumentFile. |
| Статус и ошибка | `raw_status`, `document_status`, `error_code`, `xml_message`, `has_*_marker` | Классификация и построение fact-строк. |
| Контекст | `jid_from_payload`, `creation_date`, `org_oid`, `doc_number` | Обогащение и fallback-связка по клинике. |
| BI (хэшируемые) | `raw_patient_name`, `raw_snils`, `raw_doctor_name` | Маскирование и `patient_hash` / `doctor_hash` без повторного парсинга. |

Индексы: `exchange_msgid_norm`, `relates_to_id`, `document_key`, `local_uid`, `emdr_id`, `(action, logid DESC)` для getDocumentFile.

### Нормализация идентификаторов

Один и тот же идентификатор сообщения встречается в трёх формах; все приводятся к bare UUID при разложении в dim. Связка callback с документом идёт по `dim_egisz_exchangelog_refs` (`relates_to_id` → `exchange_msgid_norm`, `emdr_id`, `document_key`); fact-таблицы `fact_egisz_transactions` индексируются по `message_id` / `relates_to_id` (`idx_fact_egisz_message_id_norm`, `idx_fact_egisz_relates_to_norm`). Функциональные индексы на `exchangelog_raw` остаются для совместимости и не используются в transform.

| Форма | Пример |
|---|---|
| Голый UUID | `dd73fc79-e2e6-479c-a285-2a470fc4f04e` |
| С префиксом | `urn:uuid:dd73fc79-e2e6-479c-a285-2a470fc4f04e` |
| В угловых скобках | `<urn:uuid:dd73fc79-e2e6-479c-a285-2a470fc4f04e>` |

### Классификация ошибок

Ошибки РЭМД приходят в нескольких формах: машинный код в SOAP-faultcode (`VALIDATION_ERROR`, `RUNTIME_ERROR`, `ASYNC_RESPONSE_TIMEOUT` и др.), свободный текст, до десятка `<item>` внутри ответного XML, Schematron-фрагменты в человекочитаемом тексте. Все они приводятся к каноническому типу через декларативную таблицу `egisz_error_interpretation_rules` (`db/parts/30_error_rules.sql`) и функции из `40_functions_errors.sql`.

Каждое правило — это `(rule_code, priority, match_code, match_pattern, interpretation, error_category)`. Применение: для каждого `<item>` функция `egisz_error_interpretation_type(code, message)` выбирает правило с наименьшим `priority`, у которого `match_code` совпал (или `NULL`) и `match_pattern` сматчился на текст. Несколько типов в одном callback дедуплицируются и склеиваются через ` · ` (`egisz_error_classify`). Если ни одно правило не сработало — тип `Неизвестная ошибка`. Категория (~10 групп) выводится из `interpretation` функцией `egisz_error_category`.

Seed содержит **86 активных правил**, сводящихся к ~74 каноническим типам и **10 категориям** `error_category` (плюс `Прочие` для нераспознанного). Источник истины — `db/parts/30_error_rules.sql`; новое правило добавляется через `INSERT ... ON CONFLICT (rule_code) DO UPDATE`.

Часть сообщений РЭМД встраивает в текст конкретные значения в `[квадратных скобках]` (id ЭМД и запроса, дата/время, структурное подразделение) — без нормализации каждое такое сообщение становится отдельным `error_type` и раздувает кардинальность (одна семья «Уникальный идентификатор документа в ЭМД […] отличается…» давала >12 тыс. псевдотипов). Правила кросс-валидации `RegisterHealthDocument` сворачивают эти семьи в канонический тип по текстовому паттерну (`\[.*?\]` поглощает изменчивое значение), как это уже сделано для ФИО и пола пациента.

#### Категории

| Категория (`error_category`) | Содержание | Типичные триггеры |
|---|---|---|
| Данные пациента | Адрес, ФИО, дата рождения, СНИЛС, ГИП, ДУЛ, пол, получатель. | `PATIENT_MPI_MISMATCH`, `INVALID_SNILS`, `RECIPIENT_INFO_MISMATCH`, Schematron `patientRole/*` |
| Данные медработника | ФРМР, должность, специальность, СНИЛС автора, отчество, организация автора. | `PERSON_NOT_FOUND`, `PERSON_POST_IN_FRMR_MISMATCH`, `VALUE_MISMATCH_METADATA_AND_FRMR`, `INVALID_DOCTOR_PATRONYMIC` |
| Ошибки структуры и валидации | XSD/Schematron, разбор XML, телефон, заверитель, хранитель, дата создания, привязка к РМИС. | `VALIDATION_ERROR`, `cvc-*`, `SAXParseException` |
| Ошибки справочника НСИ | Версия справочника, отсутствующий код, неверное наименование. | `INVALID_DICTIONARY_OID`, `INVALID_ELEMENT_VALUE_CODE`, `INVALID_ELEMENT_VALUE_NAME` |
| Ошибки регистрации в РЭМД | Документ уже/не зарегистрирован, аннулирован, дубликат, метаописание, доступ; кросс-валидация `RegisterHealthDocument` (id/дата/СП документа в СЭМД ≠ в запросе на регистрацию). | `NOT_UNIQUE_PROVIDED_ID`, `DOCUMENT_NOT_FOUND`, `ATTRIBUTE_MISMATCH`, `ACCESS_DENIED`, `DUPLICATE_REQUEST`, текст `...в ЭМД [...] отличается от ... в запросе на регистрацию` |
| Ошибки организации / ИС | ФРМО/ОГРН, лицензия, регистрация и активность ИС в РЭМД. | `ORGANIZATION_NOT_FOUND`, `ORGANIZATION_NOT_REGISTERED`, `DISABLED_RMIS`, `NO_RMIS` |
| Ошибки получения файла ЭМД | `getDocumentFile`: МИС недоступна, файл не передан, запись не найдена. | `MIS_NOT_AVAILABLE`, `REGISTRY_ITEM_NOT_FOUND`, `FILE_WAS_NOT_SENT`, `RMIS_ERROR` |
| Ошибки ЭП и сертификатов | Сертификат истёк/отозван/недействителен, CRL/OCSP, проверка подписи, роль. | `SIGNATURE_VERIFICATION_ERROR`, `ROLE_OCCURRENCE_MISMATCH`, `CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT` |
| Технические ошибки РЭМД | Внутренняя ошибка, таймаут асинхронного ответа, недоступность УЦ. | `RUNTIME_ERROR`, `INTERNAL_ERROR`, `ASYNC_RESPONSE_TIMEOUT`, `CA_UNAVAILABLE`, `CA_INACCESSIBILITY` |
| Ошибки связи | Сетевые таймауты, обрывы канала между шлюзом и РЭМД/клиникой. | текст `network/connection/timeout/таймаут` |
| Прочие | Callback или текст не попал ни под одно правило → `Неизвестная ошибка`. | финальный fallback |

#### Канонические типы ошибок

Сводный перечень типов (`error_type`), на которые сворачиваются правила. SOAP-код — `match_code` правила (`—` означает срабатывание по текстовому паттерну `match_pattern`).

| № | Категория | Тип ошибки (`error_type`) | SOAP-код РЭМД | Триггер (код / паттерн) |
|---|---|---|---|---|
| 1 | Данные пациента | Не указан адрес пациента | `VALIDATION_ERROR` | Schematron `patientRole/addr/address:Type` |
| 2 | Данные пациента | ФИО пациента не заполнено или некорректно | `VALIDATION_ERROR` | `patientRole/(name\|given\|family)` |
| 3 | Данные пациента | Дата рождения пациента не заполнена или некорректна | `VALIDATION_ERROR` | `patientRole...birthTime` |
| 4 | Данные пациента | СНИЛС пациента не заполнен или некорректен | `VALIDATION_ERROR` | `patientRole...(SNILS\|СНИЛС)` |
| 5 | Данные пациента | Данные пациента не соответствуют ГИП | `PATIENT_MPI_MISMATCH` | код / текст `ГИП...пациент` |
| 6 | Данные пациента | ФИО пациента в ЭМД не соответствует данным ЕГИСЗ | — | `(Имя\|Фамилия\|Отчество) пациента в ЭМД [...] отличается` |
| 7 | Данные пациента | Пол пациента в ЭМД не соответствует данным ЕГИСЗ | — | `Пол пациента в ЭМД [...] отличается` |
| 8 | Данные пациента | СНИЛС не найден или не соответствует данным | — | `СНИЛС\|SNILS` |
| 9 | Данные пациента | Неверный формат или контрольная сумма СНИЛС | `INVALID_SNILS` | код / текст `СНИЛС...неверн/контрольн` |
| 10 | Данные пациента | Получатель из запроса не найден в СЭМД | `RECIPIENT_INFO_MISMATCH` | код / текст `Получатель...не найден` |
| 11 | Данные пациента | Документ, удостоверяющий личность: некорректные реквизиты | — | `ДУЛ\|реквизит...удостоверени` |
| 12 | Данные медработника | Специальность врача не соответствует справочнику НСИ | `VALIDATION_ERROR` | `assignedAuthor...(specialit\|code/codeSystem)` |
| 13 | Данные медработника | СНИЛС автора (врача) не заполнен или некорректен | `VALIDATION_ERROR` | `assignedAuthor...(SNILS\|СНИЛС)` |
| 14 | Данные медработника | Данные организации автора документа не заполнены | `VALIDATION_ERROR` | `assignedAuthor...representedOrganization` |
| 15 | Данные медработника | Должность врача не соответствует данным ФРМР | `PERSON_POST_IN_FRMR_MISMATCH` | код / текст `ФРМР...должность` |
| 16 | Данные медработника | Медработник не найден в ФРМР | `PERSON_NOT_FOUND` | — |
| 17 | Данные медработника | Данные медработника не соответствуют ФРМР | `VALUE_MISMATCH_METADATA_AND_FRMR` | код / текст `ФРМР\|автор` |
| 18 | Данные медработника | Подписант из сертификата не найден в ФРМР | `VALUE_MISMATCH_METADATA_AND_CERTIFICATE` | `не найдена актуальная...карточка МР` |
| 19 | Данные медработника | Отчество врача не соответствует данным СЭМД | `INVALID_DOCTOR_PATRONYMIC` | — |
| 20 | Структура и валидация | Ошибка XSD-валидации XML | — | `cvc-*\|XML_VALIDATION_ERROR\|xsd` |
| 21 | Структура и валидация | Ошибка разбора XML-структуры документа | — | `SAXParseException\|ParseError\|org.xml` |
| 22 | Структура и валидация | Код типа документа не соответствует справочнику НСИ | `VALIDATION_ERROR` | `ClinicalDocument/code` |
| 23 | Структура и валидация | Дата/время создания документа не заполнены или некорректны | `VALIDATION_ERROR` | `creationTime...(не заполнен/некорректн/обязател)` |
| 24 | Структура и валидация | Данные заверителя документа не заполнены или некорректны | `VALIDATION_ERROR` | `legalAuthenticator` |
| 25 | Структура и валидация | Данные хранителя документа не заполнены | `VALIDATION_ERROR` | `custodian\|representedCustodianOrganization` |
| 26 | Структура и валидация | Некорректно заполнен телефон | `VALIDATION_ERROR` | `telecom...(не пустым\|@value)` |
| 27 | Структура и валидация | Организация не привязана к РМИС | `VALIDATION_ERROR` | `не привязана к РМИС` |
| 28 | Справочники НСИ | Неактуальная версия справочника НСИ | `INVALID_DICTIONARY_OID` | — |
| 29 | Справочники НСИ | Код отсутствует в справочнике НСИ | `INVALID_ELEMENT_VALUE_CODE` | — |
| 30 | Справочники НСИ | Наименование не соответствует справочнику НСИ | `INVALID_ELEMENT_VALUE_NAME` | — |
| 31 | Справочники НСИ | Ошибка справочника НСИ | — | `Справочник OID\|codeSystem\|версия...справочник` |
| 32 | Регистрация в РЭМД | Документ уже зарегистрирован в РЭМД | `NOT_UNIQUE_PROVIDED_ID` | — |
| 33 | Регистрация в РЭМД | Документ не найден в РЭМД | `DOCUMENT_NOT_FOUND` | — |
| 34 | Регистрация в РЭМД | Неверный идентификатор документа РЭМД | `INVALID_EMDR_ID` | — |
| 35 | Регистрация в РЭМД | Метаописание документа не соответствует зарегистрированному | `ATTRIBUTE_MISMATCH` / `ATTRIBUTE_NOT_FOUND` | — |
| 36 | Регистрация в РЭМД | Вид документа не актуален на дату создания | `NO_DOCUMENT_KIND_ON_DATE` | — |
| 37 | Регистрация в РЭМД | Подразделение или запись справочника не найдены на дату | `OBJECT_NOT_FOUND` | код / текст `Подразделение...не найден` |
| 38 | Регистрация в РЭМД | Доступ к операции запрещён в РЭМД | `ACCESS_DENIED` | — |
| 39 | Регистрация в РЭМД | Дублирующий запрос | `DUPLICATE_REQUEST` | — |
| 40 | Регистрация в РЭМД | Неподдерживаемый тип СЭМД в РЭМД | `UNSUPPORTED_DOCUMENT_TYPE` | — |
| 41 | Регистрация в РЭМД | Неверный формат запроса | `INVALID_REQUEST_FORMAT` | — |
| 42 | Регистрация в РЭМД | Документ аннулирован | — | `аннулирован...документ` |
| 43 | Регистрация в РЭМД | Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию | — | `Уникальный идентификатор документа в ЭМД [...] отличается` |
| 44 | Регистрация в РЭМД | Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию | — | `Дата создания документа в ЭМД [...] отличается` |
| 45 | Регистрация в РЭМД | Дата подписи МО позже даты поступления запроса на регистрацию | — | `Дата и время создания подписи МО [...] не может быть позже` |
| 46 | Регистрация в РЭМД | Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию | — | `не совпадает с СП providerOrganization` |
| 47 | Регистрация в РЭМД | Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию | — | `не совпадает с СП representedOrganization` |
| 48 | Регистрация в РЭМД | Для данного вида ЭМД запрещена регистрация новых версий | — | `запрещена регистрация новых версий` |
| 49 | Организация / ИС | Организация не найдена в реестре РЭМД | `ORGANIZATION_NOT_FOUND` | — |
| 50 | Организация / ИС | Организация не зарегистрирована в РЭМД | `ORGANIZATION_NOT_REGISTERED` | — |
| 51 | Организация / ИС | Лицензия организации не найдена | `ORGANIZATION_LICENSE_NOT_FOUND` | — |
| 52 | Организация / ИС | Несоответствие данных организации в ФРМО | — | `(ОГРН\|ОКПО\|КПП\|ИНН)...(СЭМД\|ФРМО)...не совпадает` |
| 53 | Организация / ИС | ИС зарегистрирована в РЭМД, но не активна | `DISABLED_RMIS` | — |
| 54 | Организация / ИС | ИС не зарегистрирована в РЭМД | `NO_RMIS` | — |
| 55 | Организация / ИС | Ошибки организации (generic fallback) | — | `организаци\|ОГРН\|ФРМО\|лицензи` |
| 56 | Получение файла ЭМД | Сервис предоставляющей ИС недоступен | `MIS_NOT_AVAILABLE` | — |
| 57 | Получение файла ЭМД | Запись ЭМД не найдена в предоставляющей ИС | `REGISTRY_ITEM_NOT_FOUND` | — |
| 58 | Получение файла ЭМД | ИС не передала файл ЭМД в ответе getDocumentFile | `FILE_WAS_NOT_SENT` | — |
| 59 | Получение файла ЭМД | Не удалось получить файл ЭМД из предоставляющей ИС | `RMIS_ERROR` / `GET_DOCUMENT_FILE_ERROR` | код / текст `getDocumentFile` |
| 60 | ЭП и сертификаты | Сертификат ЭП истёк | — | `сертификат...истёк\|certificate...expired` |
| 61 | ЭП и сертификаты | Сертификат ЭП отозван | — | `сертификат...отозван\|certificate...revoked` |
| 62 | ЭП и сертификаты | Недействительный сертификат подписи | — | `CANT_BUILD_CERT_CHAIN\|цепочк...сертификат` |
| 63 | ЭП и сертификаты | Сертификат недействителен на дату создания документа | — | `DOC_DATE_MISMATCH_CERT_NOT_BEFORE` |
| 64 | ЭП и сертификаты | Срок действия сертификата организации истёк | `CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT` | — |
| 65 | ЭП и сертификаты | Не удалось проверить электронную подпись | `SIGNATURE_VERIFICATION_ERROR` | — |
| 66 | ЭП и сертификаты | Недоступен сервис проверки статуса сертификата (CRL/OCSP) | — | `CRL\|OCSP\|сервис...проверк...сертификат` |
| 67 | ЭП и сертификаты | Данные подписи не соответствуют данным документа | `VALUE_MISMATCH_METADATA_AND_CERTIFICATE` | — |
| 68 | ЭП и сертификаты | Подпись роли не соответствует требованиям РЭМД | `ROLE_OCCURRENCE_MISMATCH` | — |
| 69 | Технические ошибки РЭМД | РЭМД не смог обработать запрос | `RUNTIME_ERROR` | `Невозможно обработать запрос` |
| 70 | Технические ошибки РЭМД | Техническая ошибка на стороне РЭМД | `INTERNAL_ERROR` / `RUNTIME_ERROR` | код / текст `внутренн...ошиб` |
| 71 | Технические ошибки РЭМД | Таймаут асинхронной обработки на стороне РЭМД | `ASYNC_RESPONSE_TIMEOUT` | — |
| 72 | Технические ошибки РЭМД | Недоступен сервис проверки подписи (УЦ) на стороне РЭМД | `CA_UNAVAILABLE` / `CA_INACCESSIBILITY` | — |
| 73 | Ошибки связи | Сетевая ошибка | — | `network\|connection\|timeout\|таймаут\|соединени` |
| 74 | Прочие | Неизвестная ошибка | — | финальный fallback (ни одно правило не сработало) |

View `v_rpt_error_interpretations_ui` раскрывает интерпретации построчно (для аудита правил), `v_rpt_error_category_breakdown_ui` агрегирует по категориям для дашбордов 01/04.

#### Хранение результата классификации

| Колонка | Таблица | Назначение |
|---|---|---|
| `message` | `fact_egisz_transactions` | Сырой текст события из XML или `LOGTEXT`. |
| `error_type` | `fact_egisz_transactions`, `fact_egisz_documents` | Канонический тип ошибки для группировки. |
| `error_summary` | `fact_egisz_transactions`, `fact_egisz_documents` | Пользовательская интерпретация для карточек. |
| `error_json_text` | `fact_egisz_transactions` | Нормализованный JSON всех `<item>` (`{code, message}`) для drill-down. |
| `error_text` | `fact_egisz_documents` | Исходный текст ошибки для отчётов (`COALESCE(error_json_text, message)`). |

## Дашборды Metabase

JSON-описания дашбордов находятся в `metabase_dashboards/`. Импорт выполняет `metabase/setup-dashboards.sh` при старте контейнера и через `kubectl exec` в `up.ps1 -Action Metabase`. Field-фильтры настраиваются скриптом `scripts/apply_metabase_field_filters.py` по правилам `metabase_dashboards/field_filter_defaults.yaml`.

У основных дашбордов (01–06) период по умолчанию — `past30days~`; на клиентских (07/08) — `past7days~`/`past30days~` и примерный `JID` для локального стенда. Суффикс `~` означает «включая текущий день»: без него относительный фильтр Metabase обрезает окно по концу прошлых суток, и документы за сегодня в выборку не попадают. Это также предотвращает ситуацию, когда в URL остаются пустые query-параметры (`?dwh_date_filter=`) и Metabase с `auto_apply_filters` не отрисовывает привязанные карточки. Переменная `METABASE_AUTO_APPLY_FILTERS` (по умолчанию `true`) задаётся в `setup-dashboards.sh`. Проверка импорта и выполнения SQL-карточек: `py scripts/audit_metabase_dashboards.py` (нужен запущенный Metabase на `localhost:3000`).

**Единый принцип фильтрации.** Фильтр дашборда привязывается к каждой карточке, чья витрина-источник физически содержит соответствующую колонку (dimension field filter из `field_filter_defaults.yaml`). Где колонки нет, фильтр к карточке намеренно не привязывается — карточка сохраняет свой собственный охват, а не показывает молча нефильтрованный результат: например, `v_rpt_error_category_breakdown_ui` не содержит `Статус`/идентификаторов, поэтому «Ошибки по типу/виду» реагируют только на период/код СЭМД/JID. Снимки «сейчас» (healthcheck, сводка прокси-БД, окна 24ч/72ч) и метрики trailing-30d (`MRR`/`ARR`) от периода намеренно не зависят и помечены в названии. На `01`/`02`/`06` идентификаторы (`localUid`, `relatesToMessage`, `emdrId`, `LOGID`) и статус протянуты во все документные карточки, чтобы точечный поиск менял весь дашборд согласованно.

**Период графика = фильтру.** Почасовые тренды на `02` используют единый `dwh_date_filter` по **«Обработано IPS»** (ось X совпадает с выбранным периодом). На `04` отдельный `parse_created_filter` по **«Дата создания документа»** для блока транспортных ошибок.

**Единая палитра статусов.** Четыре статуса окрашены одинаково во всех диаграммах (пироги, stacked-бары): Успешно зарегистрирован — зелёный `#84BB4C`, Ошибка асинхронного ответа РЭМД — фиолетовый `#A989C5`, Ошибка связи — оранжевый `#F2994A`, В обработке — синий `#509EE3`. Цвета зашиты в `visualization_settings` соответствующих карточек (`pie.rows` / `series_settings`).

#### Компоновка `02_service` (сервис интеграции)

| Зона | Карточки | Зависимость от периода |
|---|---|---|
| Верх | Почасовые тренды сетевых и async-ошибок; пирог «async vs network»; пирог уровней healthcheck | Тренды и пирог ошибок — да; healthcheck — снимок «сейчас» (в пироге те же сигналы, что в таблице, без очереди callback) |
| Середина | Heatmap клиника × день; топ клиник по error rate (24ч); объём по клиникам | Heatmap и объём — да; 24ч — фиксированное окно |
| Healthcheck | Таблица сигналов ELT (`queue_24h` / `pending_backlog_24h` скрыты — детализация на **03**) | Нет |
| Низ | KPI за период (документов, % ошибок, успешно, в обработке); сводка прокси-БД и очереди; доля ошибок по дням; топ клиник за период; топ формулировок связи; последние 50 транспортных сбоев | KPI и динамика — да; «В обработке» и сводка прокси — снимок; детальный разбор отказов РЭМД — **04** |

| Дашборд | Назначение | Карточек | Основные источники | Фильтры |
|---|---|---|---|---|
| `01_operational.json` | Операционный поток документов, статусы, ошибки, динамика. | 9 | `v_rpt_documents_ui`, `v_rpt_error_category_breakdown_ui` | Период, код СЭМД, JID, `localUid`/`relatesTo`/`emdrId`/`LOGID`, статус |
| `02_service.json` | Healthcheck ETL, качество потока и транспортные сбои. | 17 | `v_health_*_ui`, `v_rpt_documents_ui`, `v_rpt_network_errors_detail_ui` | Период по «Обработано IPS», код СЭМД, JID, `localUid`/`relatesTo`/`emdrId`/`LOGID` |
| `03_documents_no_response.json` | Очередь документов без финального callback. | 5 | `v_rpt_documents_no_response_ui` | Период, код СЭМД, JID, `localUid` |
| `04_quality_and_errors.json` | Качество данных и детализация отказов РЭМД. | 22 | `v_rpt_documents_ui`, `v_rpt_error_category_breakdown_ui`, `v_rpt_network_errors_detail_ui` | Период: колбэки (IPS) / транспорт / доступность |
| `05_executive.json` | Управленческие KPI: активные JID, объёмы, статусы, MRR/ARR по фикс-тарифу. | 20 (15 SQL) | `v_rpt_documents_ui` | Период (`past30days~`) |
| `06_semd_archive.json` | Архив документов и поиск по идентификаторам. | 6 | `v_rpt_semd_archive_ui` | Период, JID, код СЭМД, `localUid`, `emdrId`, `LOGID`, связанное сообщение, статус |
| `07_client_service.json` | Клиентский мониторинг по одному JID. | 9 (8 SQL) | `v_rpt_client_documents_ui` | JID (обязателен), период, тип документа |
| `08_client_bianalytic.json` | Клиентская BI-аналитика без раскрытия ПДн. | 18 (14 SQL) | `v_rpt_client_documents_ui` | JID (обязателен), период, тип документа |

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
scripts/apply_metabase_field_filters.py  настройка field-фильтров дашбордов
scripts/audit_metabase_dashboards.py  проверка карточек и запросов Metabase
scripts/export_dashboard.py        выгрузка дашборда из Metabase в JSON
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
| `exchange_msgid_norm` | Нормализованный идентификатор сообщения в `dim_egisz_exchangelog_refs` (ключ связи цепочки). |
| `relates_to_id` | Нормализованный `relatesToMessage` / `relatesTo` из разложенного payload; используется для JOIN callback → родительское сообщение. |
| `dim_egisz_exchangelog_refs` | Persistent-слой разложенного EXCHANGELOG: один проход XML на `LOGID`, источник для всей связки документов в transform. |
| `document_key` | Ключ учёта документа в DWH — всегда `lower(localUid)`; `emdrId` / `OID` ключом не являются. |
| Watermark | Текущий курсор пайплайна по `LOGID` (`elt_state.last_logid`, продвигается через `GREATEST`). |
| Raw layer | Временные staging-таблицы DWH с данными источника до предметной трансформации. |
| Fact table | Таблица нормализованных транзакций обмена СЭМД. |
| Materialized view | Предрасчитанная витрина PostgreSQL для быстрых BI-запросов (`v_egisz_documents_daily_ui`). Обогащённая витрина `v_egisz_documents_enriched_ui` — persistent-таблица, сопровождаемая инкрементально в transform. |
