# Раскладка объектов ЕГИСЗ-DWH по схемам

Документ задаёт целевую структуру хранения ЕГИСЗ-DWH в общей BI-базе: какие объекты в какую
схему переходят и под какими именами. Сейчас большинство объектов лежит в `public`, объекты
обработки ошибок — в `stg_egisz`, `mart_egisz` и `mart_egisz_selfservice`.

На этом этапе меняется только структура хранения. Права доступа и перевод потребителей на
чтение только `serving_egisz` — отдельные задачи. До них Metabase читает любой слой.

## Слои и правила имён

```text
raw_egisz → stg_egisz → mart_egisz → serving_egisz
                            └──────→ mart_egisz_admin
служебное состояние конвейера: etl_meta (объекты egisz_*)
```

| Схема | Содержимое | Правило имён |
|---|---|---|
| `etl_meta` (общая) | служебное состояние конвейера | префикс `egisz_` |
| `raw_egisz` | копия Firebird | имена источника; служебные поля — `_loaded_at` |
| `stg_egisz` | разбор журнала и реестра подач | сущность во множественном числе |
| `mart_egisz` | документы и справочники | `dim_` — справочники |
| `serving_egisz` | представления и агрегаты для потребителей | без префиксов; уточнение — суффиксом (`_current`, `_sent`, `_weekly`, `_monthly`) |
| `mart_egisz_admin` | эксплуатационные представления | `health_*`, диагностика |

В `serving_egisz` переносится то, что однозначно служит выдаче. Базовые таблицы и справочники
остаются в `mart_egisz`, пока их выдача не проанализирована. Объекты нижних слоёв на
`serving_egisz` не ссылаются.

## Раскладка объектов

### etl_meta

| Сейчас | Цель |
|---|---|
| `etl_state` | `egisz_etl_state` |
| `exchangelog_parse_attempts` | `egisz_exchangelog_parse_attempts` |

### raw_egisz

| Сейчас | Цель | Примечание |
|---|---|---|
| `exchangelog_raw` | `exchangelog` | разделы `exchangelog_yYYYYmMM`; `loaded_at` → `_loaded_at` |
| `dim_message_document` | `egisz_messages` | колонки источника `egmid`, `msgid`, `replyto`, `documentid`, `createdate` и `_loaded_at`; триггер снимается |

Строки реестра, загруженные до переноса, уже нормализованы триггером: localUid приведён к
нижнему регистру, у ИЭМК значение обнулено. Дословное значение даст только повторная выгрузка
`EGISZ_MESSAGES`. На результат разбора это не влияет: правило нормализации идемпотентно.

### stg_egisz

| Сейчас | Цель | Примечание |
|---|---|---|
| `transactions` | `exchange_messages` | строка журнала — сообщение обмена; имя отличает её от реестра `egisz_messages`; разделы `exchange_messages_yYYYYmMM` |
| `stg_egisz.message_error` | `network_errors`, `remd_errors`, `ihe_errors` | представления по источникам с исходным текстом; общая форма без текста — `mart_egisz.exchangelog_errors` |
| `stg_egisz.document_error_current` | `mart_egisz.document_errors` | ошибки текущего состояния документа в общей форме, выше stage |
| — | `message_registry` (представление) | реестр подач: правило ИЭМК и нормализация localUid вместо триггера |

### mart_egisz

Имена не меняются, меняется только схема.

| Группа | Объекты |
|---|---|
| документы | `documents`, `document_attributes` — выдача базовых таблиц требует отдельного анализа |
| справочники шлюза | `dim_organizations`, `dim_licenses` |
| справочники НСИ | `dim_nsi_organizations`, `dim_nsi_dictionaries`, `dim_nsi_semd_guides`, `dim_nsi_semd_guide_aliases`, `dim_nsi_semd_guide_dictionaries` |
| справочники состояний и классификаторы | `dim_nsi_semd_types`, `dim_document_statuses`, `dim_pending_segments`, `dim_sent_states`, `dim_control_chart_phases` |
| представления над справочниками | `dim_clinic_oids`, `dim_clinic_hosts`, `dim_semd_guide_oids` |
| справочники ошибок (уже в схеме) | `dim_error_rules`, `dim_masking_rules`, `dim_error_categories`, `dim_error_types`, `dim_nsi_error_codes`, `dim_error_code_aliases` |

### serving_egisz

| Сейчас | Цель | Примечание |
|---|---|---|
| `rpt_documents`, `rpt_document_versions` | `document_versions`, `documents_current` | все версии; состояние к выдаче — `documents_current` |
| `rpt_documents_sent` | `documents_sent` | |
| `rpt_documents_weekly` | `documents_weekly` | материализованное |
| `rpt_documents_monthly` | `documents_monthly` | материализованное |
| `mart_egisz_selfservice.document_error` | `document_errors` | материализованное |
| `mart_egisz.agg_document_error_weekly` | `document_errors_weekly` | материализованное |
| `mart_egisz.agg_document_error_monthly` | `document_errors_monthly` | материализованное |
| `mart_egisz_selfservice.network_error` | `network_errors` | |
| `rpt_document_file_request` | `document_file_requests` | |
| `rpt_clinic_nsi_mapping` | `clinic_nsi_mapping` | |
| `rpt_clinic_semd_activity` | `clinic_semd_activity` | |
| `rpt_semd_guides` | `semd_guides` | |
| `rpt_semd_dictionaries` | `semd_dictionaries` | |

Схема `mart_egisz_selfservice` снимается.

### mart_egisz_admin

| Сейчас | Цель |
|---|---|
| `rpt_health_signals` | `health_signals` |
| `rpt_health_versions` | `health_versions` |
| `rpt_health_sync` | `health_sync` |
| `rpt_health_by_clinic` | `health_by_clinic` |
| `rpt_health_message_registry_no_document` | `health_message_registry_no_document` |
| `rpt_document_lineage` | `document_lineage` |

## Функции: размещение и сокращение

Сейчас функций 30, после переноса останется 26.

| Схема | Функции |
|---|---|
| `etl_meta` | `egisz_ensure_time_partitions` (было `ensure_time_partitions`) |
| `stg_egisz` | `xml_text`, `parse_exchangelog_row`, `classify_async_status`, `normalize_message_id`, `message_registry_key`, `clean_text_value`, `clean_host`, `extract_gost_endpoint`, `normalize_semd_code`, `dwh_id`, `egisz_subsystem`, `network_error_code`, `remd_error_items`, `ihe_error_items`, `xml_attribute`, `normalize_error_text`, `classify_error`, `parse_exchangelog_errors`, `reclassify_errors` |
| `mart_egisz` | `transform_raw_to_facts`, `recompute_document_versions`, `recompute_document_attributes`, `recompute_document_jids`, `resolve_document_jid`, `document_status_final`, `document_status_nonfinal`, `recompute_document_error_texts`, `mask_personal_data` |
| `serving_egisz` | `report_timezone`, `is_pending_at`, `pending_segment_at`, `pending_segment_code_at`, `refresh_report_marts` |

`report_timezone`, `is_pending_at`, `pending_segment_at` и `pending_segment_code_at` вызывают только объекты
`serving_egisz` и карточки Metabase.

Снимаются:

- `dim_message_document_guard` — правило переходит в `stg_egisz.message_registry`.
- `safe_cast_timestamptz` — у неё один вызов. Функция заменяется выражением
  `NULLIF(btrim(x), '')::timestamptz`. Название обещает защиту от ошибок приведения, которой в
  теле нет.
- `jid_from_mo_uid`, `jid_from_host` — их вызывает только `resolve_document_jid`, логика
  переходит в неё.

Порядок обновления витрин сейчас задан в трёх местах: в `refresh_report_marts`, в
`REPORT_MARTS` обоих DAG-ов и в `$ReportMarts` сценария `deploy/apply-dwh-schema.ps1`.
Остаётся одно определение — функция `serving_egisz.refresh_report_marts()`; DAG-и и сценарий
вызывают её.

`transform_raw_to_facts` (около 800 строк) при переносе не меняется. Разбиение — отдельная
задача.

## Что меняется вместе со структурой

- **Модули `db/`.**
  - Все имена квалифицируются схемой.
  - Схема `etl_meta` создаётся, если её нет (локальная копия).
  - `egisz_ensure_time_partitions` и сигнал сетки разделов в `health_signals` ищут разделы в
    новых схемах, а не в `public`.
  - Смена владельца в финальном блоке `04_views.sql` обходит схемы ЕГИСЗ.
  - В начале `dwh_init.sql` проверяется, что в `public` не осталось объектов `egisz`. Без этой
    проверки модули, применённые до переноса данных, создали бы пустые двойники таблиц.
- **DAG-и и загрузчики.**
  - DAG-и переходят на новые имена таблиц и колонок.
  - `scripts/load_nsi_organization_1461.py` перестаёт создавать таблицу и представление в
    `public`: их определения остаются только в `db/`.
- **Metabase.**
  - Провижининг адресует объекты ссылкой «схема.объект» по фиксированному списку схем
    `DWH_SCHEMAS_REGEX` в `metabase/setup-dashboards.sh`. Сейчас в списке `public`,
    `stg_egisz`, `mart_egisz_selfservice`, `mart_egisz`. Объекты схем, которых нет в списке,
    провижининг не находит.
  - В список добавляются `serving_egisz` и `mart_egisz_admin`, `public` и `mart_egisz_selfservice`
    из него убираются.
  - Генераторы, JSON дашбордов и модели получают новые схемы и имена. Источники карточек по
    смыслу не меняются.
- **Сценарии.** `deploy/apply-dwh-schema.ps1`, `scripts/export_dashboard.py`,
  `scripts/verify_metabase_cards.py`, разовые `scripts/*.sql`.
- **Задача по доступу.** В `etl_meta` для роли `egisz` заданы права по умолчанию в пользу ролей
  Redmine: созданные в ней таблицы `egisz_*` получат их права.

## Порядок перехода

1. Разовый сценарий вне репозитория выполняется одной транзакцией:
   - создаёт схемы;
   - переносит таблицы с данными командами `ALTER ... SET SCHEMA`, `RENAME` и `RENAME COLUMN`
     — меняется только каталог, данные не копируются;
   - переносит разделы секционированных таблиц по одному: перенос родителя их не затрагивает;
   - переименовывает первичные ключи и индексы под новые имена таблиц;
   - переносит функции с зависимыми объектами командой `ALTER FUNCTION ... SET SCHEMA`;
   - снимает триггер реестра подач;
   - удаляет представления и функции в `public`.
2. Модули `db/` применяются дважды: второй прогон ничего не меняет.
3. Выкладываются DAG-и, затем Metabase переимпортируется, карточки проверяются с параметрами.

Прод переводится отдельно, после репетиции на локальной копии.
