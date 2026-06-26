# Сервис эксплуатационной аналитики обмена с ЕГИСЗ

Эксплуатационная аналитика проксирующего шлюза между МИС клиник и РЭМД: журнал обмена → документная витрина в PostgreSQL → четыре дашборда Metabase.

**Для кого:** дежурная поддержка, аналитики, руководство, клиенты-клиники.

---

## Содержание

- [Контекст: ЕГИСЗ, СЭМД, обмен и где в нём наш контур](#контекст-егисз-сэмд-обмен-и-где-в-нём-наш-контур)
- [Состав аналитики](#состав-аналитики)
- [Архитектура и поток данных](#архитектура-и-поток-данных)
- [Источник данных](#источник-данных)
- [ELT-конвейер](#elt-конвейер)
- [Парсинг и нормализация](#парсинг-и-нормализация)
- [Сборка документа](#сборка-документа)
- [Классификация ошибок](#классификация-ошибок)
- [DWH-модель](#dwh-модель)
- [Дашборды Metabase](#дашборды-metabase)
- [Запуск контуров](#запуск-контуров)
- [Глоссарий](#глоссарий)
- [Приложение: типы ошибок](#приложение-типы-ошибок)
  - [Справочник активных правил классификации](#справочник-активных-правил-классификации)

---

## Контекст: ЕГИСЗ, СЭМД, обмен и где в нём наш контур

**ЕГИСЗ** — единая государственная информационная система в сфере здравоохранения. Для клиники ключевое взаимодействие — передача **СЭМД** (структурированных электронных медицинских документов) в **РЭМД** (реестр электронных медицинских документов).

Обмен асинхронный: МИС отправляет CDA в SOAP-конверте, получает синхронное «принято к обработке», затем — callback с результатом (`emdrId` или список ошибок). Одна отправка СЭМД — **как минимум два сообщения** в журнале: исходящее и callback.

Между МИС и ЕГИСЗ стоит проксирующий шлюз. Он пишет все сообщения в журнальную БД **`proxy_egisz`** (Firebird). Сервис строит на её основе документную витрину для BI и оперативного контроля.
---

## Состав аналитики

Пять срезов эксплуатационной аналитики; каждый опирается на витрины DWH и вкладки дашборда «Интеграция с ЕГИСЗ» (или отдельные клиентские дашборды).

| Срез | Содержание | Витрина / представление |
| ---- | ---------- | ----------------------- |
| Отправка и очередь | Объёмы, статусы, динамика по клиникам и типам СЭМД | `rpt_documents` |
| Обратная связь | Документы без callback, корреляция исходящего сообщения и ответа | `rpt_documents_waiting`, `rpt_documents` |
| Ошибки | Канонический `error_type`, разбивка по категориям | `rpt_error_breakdown`, `rpt_network_errors` |
| Полнота | Атрибуты документа, `emdrId`, расхождения JID | `rpt_documents`, `document_attributes` |
| Контур загрузки | Курсор журнала, хвост raw, health-сигналы | `rpt_health_*`, `elt_state` |

---

## Архитектура и поток данных

Схема **ELT**: Airflow извлекает и загружает; парсинг, нормализация, сборка документа и классификация ошибок — в PostgreSQL (`db/parts/`). Metabase читает представления `rpt_*`.

```
proxy_egisz (EXCHANGELOG, JPERSONS, EGISZ_LICENSES)
        │  extract / reconcile
        ▼
exchangelog_raw  ──transform──►  transactions  ──►  documents
        │                              │                │
        │                              └──── document_attributes
        ▼
   elt_state.last_logid          dim_* (справочники)
                                        │
                                        ▼
                                   rpt_*  ──►  Metabase
```

| Компонент | Роль |
| --------- | ---- |
| `proxy_egisz` (Firebird) | Журнал обмена и справочники — источник |
| Airflow | Три DAG: документы, справочники, сверка |
| `dwh_egisz` (PostgreSQL) | Raw, факты, справочники, правила ошибок, `rpt_*` |
| Metabase | Четыре дашборда, четыре модели поверх `rpt_*` |

Служебные БД Airflow и Metabase отделены от `dwh_egisz`.

---

## Источник данных

Инкрементальная выборка журнала — по монотонному `EXCHANGELOG.LOGID` (`LOGID > last_logid`).

| Таблица | Grain | Поля |
| ------- | ----- | ---- |
| `EXCHANGELOG` | 1 строка = 1 SOAP-сообщение | `LOGID`, `LOGDATE`, `CREATEDATE`, `MSGID`, `LOGSTATE`, `LOGTEXT`, `MSGTEXT` |
| `JPERSONS` | Организация | `JID`, `JNAME`, `JINN`, `FIR_OID` |
| `EGISZ_LICENSES` | Лицензия МО | `mo_uid`, `JID` |

`MSGTEXT` — полный SOAP/XML (BLOB в источнике, `text` в DWH). Бизнес-поля извлекаются при transform; исходный журнал не меняется. `LOGDATE` — служебная метка строки журнала; дата документа — из payload (`creationDateTime`).

---

## ELT-конвейер

Три независимых DAG; у каждого `max_active_runs = 1`.

### Основной поток документов

```
extract_exchangelog ▸ transform_exchangelog
```

| Задача | Действие |
| ------ | -------- |
| `extract_exchangelog` | Читает `elt_state.last_logid`. Пока в `exchangelog_raw` есть строки с `logid` выше watermark — источник не читается. Иначе fetch `EXCHANGELOG` → UPSERT в `exchangelog_raw`, `ANALYZE`. |
| `transform_exchangelog` | Цикл: `transform_raw_to_facts(from, to)` — парсинг raw в `transactions`, сборка `documents`, сдвиг watermark через `GREATEST`. |

При сбое watermark не откатывается: следующий прогон перечитывает тот же диапазон. Дублей документов нет (UPSERT по `dwh_id`).

### Справочники

| Задача | Действие |
| ------ | -------- |
| `sync_dimensions` | UPSERT `dim_organizations` ← `JPERSONS`, `dim_licenses` ← `EGISZ_LICENSES`. При изменениях — `reconcile_document_attributes_ui()` (имена клиник, JID в `document_attributes`). |

### Сверка полноты журнала

| Задача | Действие |
| ------ | -------- |
| `reconcile_proxy_raw` | Set-diff всех `LOGID` источника и `exchangelog_raw`; догрузка missing → transform с prefix-lookback. Watermark не меняет. Skip при raw-хвосте extract. |

Callback'и и backfill шлюза могут появиться с `LOGID` ниже watermark. Основной поток их не видит (`LOGID > last_logid`); сверка закрывает пропуски.

### Периодичность

| Поток | Интервал |
| ----- | -------- |
| Документы | каждые 5 мин |
| Справочники | каждый час |
| Сверка | раз в сутки |

---

## Парсинг и нормализация

Каждая строка `exchangelog_raw` парсится **один раз** функцией `parse_exchangelog_row(msgtext, msgid, logtext)`; результат пишется в `transactions` (`xml_*`). Дальнейшая сборка документа читает только `transactions`.

Утилиты (`db/parts/20_functions_parsing.sql`):

| Функция | Назначение |
| ------- | ---------- |
| `xml_text` | Текст тега XML без привязки к namespace |
| `normalize_message_id` | `urn:uuid:` → единый формат |
| `dwh_id` | Ключ документа: `lower(localUid)` |
| `resolve_document_jid` | JID: `mo_uid` из XML → fallback по host/gost-endpoint |
| `normalize_semd_code` | Код СЭМД из payload |
| `classify_async_status` | Статус сообщения до уровня документа |

Извлекаемые поля:

| Поле | Источник | Правило |
| ---- | -------- | ------- |
| ID сообщения | `<messageId>` / `MSGID` | Приоритет XML |
| Связанное сообщение | `<relatesToMessage>` / `<relatesTo>` | Корреляция callback |
| Локальный ID | `<localUid>` / `<DOCUMENTID>` | Приоритет `localUid` |
| ID в РЭМД | `<emdrId>` | |
| OID организации | `<organization>` | → `dim_licenses.mo_uid` → JID |
| Код / название СЭМД | `<kind>`, `<name>` | Название также из `dim_semd_types` (НСИ `1.2.643.5.1.13.13.11.1520`) |
| Ошибка | `<code>`, `<message>`; при `LOGSTATE = 3` — `LOGTEXT` | Вход классификатора |
| Дата создания | `<creationDateTime>` | `safe_cast_timestamptz` |

**Статус сообщения** (`classify_async_status`): синхронный `RegisterDocumentResponse` с `<status>success</status>` — только приём запроса (`pending`), не регистрация. Регистрация — асинхронный callback с `registerDocumentResult` / `<documentStatus>Зарегистрировано</documentStatus>`. `LOGSTATE = 3` — транспортная ошибка.

**Статус документа** в `documents`: `success`, `async_error` (отказ РЭМД), `network_error` (`LOGSTATE = 3`), `waiting` (callback не пришёл). Подписи — `dim_document_status`; в Metabase Models — поле «Статус».

**JID:** `resolve_document_jid(org_oid, endpoint)`. Флаг `clinic_jid_mismatch` — расхождение OID из XML, `JPERSONS.fir_oid`, `EGISZ_LICENSES.mo_uid`.

---

## Сборка документа

Два слоя:

1. **Сообщение** — `transform_raw_to_facts` парсит новые строки `exchangelog_raw` в `transactions` (поля `xml_*`, классификация callback на уровне строки). Повторный парсинг той же строки пропускается (`xml_parsed_at`).

2. **Документ** — UPSERT в `documents` (PK `dwh_id`). Несколько строк журнала одной отправки сходятся в одну запись: ключ `lower(localUid)`; callback привязывается по `relatesTo` / `emdrId` (`normalize_message_id`). Накапливаются `first_sent_at`, `last_callback_at`, `registered_at`, итоговый `status`, `error_type`, `jid`.

Lookback при transform: extract использует окно по ширине батча; reconcile передаёт prefix журнала, чтобы поздний callback связался с ранним `getDocumentFile`.

`document_attributes` (1:1 к документу): lineage OID, host, endpoint, BI-маски; обновляется после transform и `sync_dimensions`.

---

## Классификация ошибок

РЭМД отдаёт ошибки как SOAP-faultcode, массив `<item>` (`code` + `message`), фрагменты Schematron. Правила — `dim_error_rules` (`db/parts/30_error_rules.sql`); логика — `db/parts/40_functions_errors.sql`.

На каждый `<item>`: все активные правила проверяются независимо (`match_code` + regex `match_pattern`); сработавшие `interpretation` дедуплицируются и склеиваются. Иначе — code-fallback, эвристики Schematron, «Неизвестная ошибка».

В `documents`:

| Поле | Содержание |
| ---- | ---------- |
| `error_type` | Канонические **типы** для группировки и фильтра; несколько атомов через `·` |
| `error_summary` | **Сводка ошибки** — интерпретируемый текст (одна или несколько ошибок через `·`) |
| `error_text` | Исходный текст из `<message>` **без редактирования** |

`rpt_error_breakdown` раскладывает `error_type` на атомарные строки (split по `·` и legacy ` - `).

Перечень типов — [приложение](#приложение-типы-ошибок). Условия правил — [справочник](#справочник-активных-правил-классификации).

---

## DWH-модель

БД `dwh_egisz`. Схема — идемпотентный прогон `db/dwh_init.sql` (модули `db/parts/`).

| Слой | Объект | Grain / назначение |
| ---- | ------ | ------------------ |
| Состояние | `elt_state` | `last_logid` — курсор инкрементальной обработки |
| Raw | `exchangelog_raw` | Строка журнала как в источнике |
| Транзакции | `transactions` | Строка журнала + `xml_*` (parse-once) + `error_type` на callback |
| Факт | `documents` | Один СЭМД — одна строка |
| Атрибуты | `document_attributes` | Lineage клиники, host, mismatch JID |
| Справочники | `dim_organizations`, `dim_licenses`, `dim_semd_types`, `dim_document_status` | Клиники, лицензии, типы СЭМД, подписи статусов |
| Правила | `dim_error_rules` | `match_code` + `match_pattern` → `interpretation` |

Представления `rpt_*`:

| Представление | Содержание |
| ------------- | ---------- |
| `rpt_documents` | `documents` + `document_attributes` + справочники |
| `rpt_documents_waiting` | `status = waiting` |
| `rpt_network_errors` | `status = network_error` |
| `rpt_error_breakdown` | Split `error_type` по `·` / ` - ` → атомарный вид |
| `rpt_document_lineage` | OID / host / endpoint по документу |
| `rpt_health_*` | Свежесть и состояние контура |

Имена колонок в Metabase Models:

| Префикс | Примеры |
| ------- | ------- |
| `semd_*` | `semd_code`, `semd_name`, `semd_local_uid`, `semd_emdr_id` |
| `clinic_*` | `clinic_jid`, `clinic_name`, `clinic_jid_mismatch` |
| без префикса | `status`, `status_label`, `error_type`, `error_summary`, `error_text`, `processed_at`, `processed_day`, `arrival_day` |

`processed_at` / `processed_day` — по последней активности (`last_callback_at` → `sent_at` → `document_created_at`). `arrival_day` — день поступления на прокси (`first_sent_at` → `document_created_at`). Карточка «Динамика по дням» строится и фильтруется по `arrival_day`.

---

## Дашборды Metabase

Четыре JSON-дашборда (`metabase_dashboards/`). Импорт — `metabase/setup-dashboards.sh`, модели — `metabase/sync-models.sh`.

**«Интеграция с ЕГИСЗ»** — пять вкладок: оперативный мониторинг, сервис интеграции, документы без ответа, анализ ошибок, архив СЭМД. Drill-through из KPI и ошибок — в архив (модель «Документы»).

| Файл | Содержание |
| ---- | ---------- |
| `01_integration_egisz.json` | Оперативный и архивный срез, healthcheck |
| `05_executive.json` | Сводная статистика |
| `07_client_service.json` | Срез одной клиники |
| `08_client_bianalytic.json` | Клиентская BI-выгрузка |

Metabase Models → витрины DWH:

| Модель | Витрина | Grain |
| ------ | ------- | ----- |
| Документы (`01_documents`) | `rpt_documents` | 1 строка = 1 документ |
| Разбивка ошибок (`02_error_breakdown`) | `rpt_error_breakdown` | 1 строка = 1 тип ошибки на документ |
| Очередь без ответа (`03_no_response`) | `rpt_documents_waiting` | 1 строка = 1 документ без callback |
| Сбои транспорта (`04_network_errors`) | `rpt_network_errors` | 1 строка = 1 сетевая ошибка |

Простые срезы и drill-through — через Models и Query Builder; pivot, stacked и сложный SQL — native-запросы к `rpt_*`. Фильтры дашборда: клиника (`JID Клиники`), тип СЭМД, статус (`field_filter_defaults.yaml`).
---

## Запуск контуров

```
.\up.ps1                          # Airflow + Metabase
.\up.ps1 -Action Airflow          # только Airflow
.\up.ps1 -Action Metabase         # только Metabase
.\up.ps1 -Action Stop             # остановка
```

Airflow UI — `:8080`, Metabase — `:3000`, namespace `egisz-bi`.
---

## Глоссарий

| Аббревиатура              | Расшифровка                                                                   |
| ------------------------- | ----------------------------------------------------------------------------- |
| ЕГИСЗ                     | Единая государственная информационная система в сфере здравоохранения         |
| РЭМД                      | Реестр электронных медицинских документов (подсистема ЕГИСЗ)                  |
| СЭМД                      | Структурированный электронный медицинский документ (CDA-документ для РЭМД)    |
| МО                        | Медицинская организация (клиника)                                             |
| МИС                       | Медицинская информационная система клиники                                    |
| ИС                        | Информационная система (в контексте РЭМД — МИС, зарегистрированная в реестре) |
| ФРМР                      | Федеральный регистр медицинских работников                                    |
| ФРМО                      | Федеральный реестр медицинских организаций                                    |
| ГИП                       | Главный индекс пациента                                                       |
| НСИ                       | Нормативно-справочная информация Минздрава (справочники ЕГИСЗ)               |
| ЭП                        | Электронная подпись                                                           |
| УЦ                        | Удостоверяющий центр (выдаёт сертификаты ЭП)                                  |
| CRL / OCSP                | Списки / протокол проверки отозванных сертификатов                            |
| ДУЛ                       | Документ, удостоверяющий личность                                             |
| СНИЛС                     | Страховой номер индивидуального лицевого счёта                                |
| ОГРН / ИНН / КПП          | Реквизиты юридического лица                                                   |
| OID                       | Object Identifier (для справочников НСИ)                                      |
| `emdrId`                  | Идентификатор зарегистрированного документа в РЭМД                            |
| `localUid`                | Идентификатор документа на стороне МИС (до регистрации в РЭМД)                |
| `messageId` / `relatesTo` | Корреляционные идентификаторы SOAP-сообщений                                  |
| callback                  | Асинхронный ответ РЭМД на исходящее сообщение                                |
| DWH                       | Аналитическое хранилище (`dwh_egisz`)                                         |

---

## Приложение: типы ошибок

Классификатор сопоставляет `code` и текст `<message>` из callback с правилами DWH. Несколько типов в одном документе разделяются `·`.

| Категория | Типы ошибок (`error_type`) |
| --- | --- |
| Данные пациента | <ul><li>Не указан адрес пациента</li><li>ФИО пациента не заполнено или некорректно</li><li>Дата рождения пациента не заполнена или некорректна</li><li>СНИЛС пациента не заполнен или некорректен</li><li>Данные пациента не соответствуют ГИП</li><li>ФИО пациента в ЭМД не соответствует данным ЕГИСЗ</li><li>Пол пациента в ЭМД не соответствует данным ЕГИСЗ</li><li>Неверный формат или контрольная сумма СНИЛС</li><li>СНИЛС не найден или не соответствует данным пациента/медработника</li><li>Получатель из запроса не найден в СЭМД</li><li>Документ, удостоверяющий личность пациента: некорректные реквизиты</li></ul> |
| Данные медработника | <ul><li>Специальность врача не соответствует справочнику НСИ</li><li>СНИЛС автора (врача) не заполнен или некорректен</li><li>Данные организации автора документа не заполнены</li><li>Должность врача не соответствует данным ФРМР</li><li>Медработник не найден в ФРМР</li><li>Данные медработника не соответствуют ФРМР</li><li>Подписант из сертификата не найден в ФРМР</li><li>Отчество врача не соответствует данным СЭМД</li></ul> |
| Ошибки структуры и валидации | <ul><li>Организация не привязана к РМИС</li><li>Некорректно заполнен телефон</li><li>Данные заверителя документа не заполнены или некорректны</li><li>Дата/время создания документа не заполнены или некорректны</li><li>Код типа документа не соответствует справочнику НСИ</li><li>Данные хранителя документа не заполнены</li><li>Ошибка XSD-валидации XML</li><li>Ошибка разбора XML-структуры документа</li></ul> |
| Ошибки справочника НСИ | <ul><li>Неактуальная версия справочника НСИ</li><li>Код отсутствует в справочнике НСИ</li><li>Наименование не соответствует справочнику НСИ</li><li>Ошибка справочника НСИ</li></ul> |
| Ошибки регистрации в РЭМД | <ul><li>Документ уже зарегистрирован в РЭМД</li><li>Документ не найден в РЭМД</li><li>Неверный идентификатор документа РЭМД</li><li>Доступ к операции запрещён в РЭМД</li><li>Дублирующий запрос</li><li>Неподдерживаемый тип СЭМД в РЭМД</li><li>Неверный формат запроса</li><li>Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию</li><li>Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию</li><li>Дата подписи МО позже даты поступления запроса на регистрацию</li><li>Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию</li><li>Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию</li><li>Для данного вида ЭМД запрещена регистрация новых версий</li><li>Метаописание документа не соответствует зарегистрированному в РЭМД</li><li>Вид документа не актуален на дату создания</li><li>Подразделение или запись справочника не найдены на дату документа</li><li>Документ аннулирован</li></ul> |
| Ошибки организации / ИС | <ul><li>Несоответствие данных организации в ФРМО</li><li>Организация не найдена в реестре РЭМД</li><li>Организация не зарегистрирована в РЭМД</li><li>Лицензия организации не найдена</li><li>ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС</li><li>ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные</li><li>Ошибки организации</li></ul> |
| Ошибки получения файла ЭМД | <ul><li>Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile</li><li>Запрашиваемая запись ЭМД не найдена в предоставляющей ИС</li><li>ИС не передала файл ЭМД в ответе getDocumentFile</li><li>Не удалось получить файл ЭМД из предоставляющей ИС</li></ul> |
| Ошибки ЭП и сертификатов | <ul><li>Не удалось проверить электронную подпись</li><li>Данные подписи не соответствуют данным документа</li><li>Подпись роли не соответствует требованиям РЭМД</li><li>Недействительный сертификат подписи</li><li>Сертификат подписи недействителен на дату создания документа</li><li>Сертификат ЭП истёк</li><li>Сертификат ЭП отозван</li><li>Срок действия сертификата организации истек</li><li>Недоступен сервис проверки статуса сертификата (CRL/OCSP)</li></ul> |
| Технические ошибки РЭМД | <ul><li>РЭМД не смог обработать запрос</li><li>Техническая ошибка на стороне РЭМД</li><li>Таймаут асинхронной обработки на стороне РЭМД</li><li>Недоступен сервис проверки подписи (УЦ) на стороне РЭМД</li></ul> |
| Ошибки связи | <ul><li>Сетевая ошибка</li></ul> |

### Справочник активных правил классификации

Порядок: правила из таблицы → code-fallback → эвристики Schematron → «Неизвестная ошибка». Точные regex — в `db/parts/30_error_rules.sql`.

| Код правила | Тип ошибки | Категория | Условие сопоставления |
| --- | --- | --- | --- |
| `schematron_patient_address_type` | Не указан адрес пациента | Данные пациента | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(Schematron|схематрон).*patientRole.*addr.*address:Type` |
| `schematron_org_not_linked_rmis` | Организация не привязана к РМИС | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)не привязана к РМИС` |
| `schematron_telecom_missing` | Некорректно заполнен телефон | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(telecom).*(не пустым значением|@value)|Ошибка заполнения номера телефона` |
| `xsd_validation` | Ошибка XSD-валидации XML | Ошибки структуры и валидации | код fault — любой; текст `<message>` ~* `(?is)(\bcvc-|XML_VALIDATION_ERROR|xsd|Invalid content was found|not complete|not valid)` |
| `document_already_registered` | Документ уже зарегистрирован в РЭМД | Ошибки регистрации в РЭМД | код fault = `NOT_UNIQUE_PROVIDED_ID`; текст `<message>` — любой |
| `patient_data_gip` | Данные пациента не соответствуют ГИП | Данные пациента | код fault = `PATIENT_MPI_MISMATCH`; текст `<message>` — любой |
| `doctor_position_frmr` | Должность врача не соответствует данным ФРМР | Данные медработника | код fault = `PERSON_POST_IN_FRMR_MISMATCH`; текст `<message>` — любой |
| `person_not_found_frmr` | Медработник не найден в ФРМР | Данные медработника | код fault = `PERSON_NOT_FOUND`; текст `<message>` — любой |
| `staff_data_frmr` | Данные медработника не соответствуют ФРМР | Данные медработника | код fault = `VALUE_MISMATCH_METADATA_AND_FRMR`; текст `<message>` — любой |
| `signature_metadata_certificate` | Подписант из сертификата не найден в ФРМР | Данные медработника | код fault = `VALUE_MISMATCH_METADATA_AND_CERTIFICATE`; текст `<message>` ~* `(?is)не найдена актуальная.*карточка МР` |
| `signature_metadata_certificate_mismatch` | Данные подписи не соответствуют данным документа | Ошибки ЭП и сертификатов | код fault = `VALUE_MISMATCH_METADATA_AND_CERTIFICATE`; текст `<message>` — любой |
| `nsi_dictionary_version` | Неактуальная версия справочника НСИ | Ошибки справочника НСИ | код fault = `INVALID_DICTIONARY_OID`; текст `<message>` — любой |
| `nsi_dictionary_code` | Код отсутствует в справочнике НСИ | Ошибки справочника НСИ | код fault = `INVALID_ELEMENT_VALUE_CODE`; текст `<message>` — любой |
| `nsi_dictionary_name` | Наименование не соответствует справочнику НСИ | Ошибки справочника НСИ | код fault = `INVALID_ELEMENT_VALUE_NAME`; текст `<message>` — любой |
| `nsi_dictionary_value` | Ошибка справочника НСИ | Ошибки справочника НСИ | код fault — любой; текст `<message>` ~* `(?is)(Справочник OID|codeSystem|codeSystemVersion|верси[яи].*справочник|значени[ея].*НСИ|не соответствует наименовани...` |
| `rmis_registration_disabled` | ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС | Ошибки организации / ИС | код fault = `DISABLED_RMIS`; текст `<message>` — любой |
| `rmis_registration_missing` | ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные | Ошибки организации / ИС | код fault = `NO_RMIS`; текст `<message>` — любой |
| `document_metadata_mismatch` | Метаописание документа не соответствует зарегистрированному в РЭМД | Ошибки регистрации в РЭМД | код fault = `ATTRIBUTE_MISMATCH`; текст `<message>` — любой |
| `document_provider_unavailable` | Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile | Ошибки получения файла ЭМД | код fault = `MIS_NOT_AVAILABLE`; текст `<message>` — любой |
| `document_registry_item_missing` | Запрашиваемая запись ЭМД не найдена в предоставляющей ИС | Ошибки получения файла ЭМД | код fault = `REGISTRY_ITEM_NOT_FOUND`; текст `<message>` — любой |
| `document_file_not_sent` | ИС не передала файл ЭМД в ответе getDocumentFile | Ошибки получения файла ЭМД | код fault = `FILE_WAS_NOT_SENT`; текст `<message>` — любой |
| `document_provider_response_error` | Не удалось получить файл ЭМД из предоставляющей ИС | Ошибки получения файла ЭМД | код fault = `RMIS_ERROR`; текст `<message>` — любой |
| `document_file_get_error` | Не удалось получить файл ЭМД из предоставляющей ИС | Ошибки получения файла ЭМД | код fault = `GET_DOCUMENT_FILE_ERROR`; текст `<message>` — любой |
| `document_file_runtime_error` | Не удалось получить файл ЭМД из предоставляющей ИС | Ошибки получения файла ЭМД | код fault — любой; текст `<message>` ~* `(?is)(getDocumentFile|получения файла ЭМД|файлового хранилища)` |
| `signature_certificate_chain` | Недействительный сертификат подписи | Ошибки ЭП и сертификатов | код fault — любой; текст `<message>` ~* `(?is)(CANT_BUILD_CERT_CHAIN|цепочк.*сертификат|аккредитованн.*УЦ)` |
| `signature_doc_date_mismatch` | Сертификат подписи недействителен на дату создания документа | Ошибки ЭП и сертификатов | код fault — любой; текст `<message>` ~* `(?is)(DOC_DATE_MISMATCH_CERT_NOT_BEFORE|сертификат.*не действителен.*дат[уы] создания)` |
| `signature_verification_error` | Не удалось проверить электронную подпись | Ошибки ЭП и сертификатов | код fault = `SIGNATURE_VERIFICATION_ERROR`; текст `<message>` — любой |
| `person_snils` | СНИЛС не найден или не соответствует данным пациента/медработника | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)(СНИЛС|SNILS)` |
| `doctor_position_frmr_text` | Должность врача не соответствует данным ФРМР | Данные медработника | код fault — любой; текст `<message>` ~* `(?is)(ФРМР|FRMR).*(должност|specialit|специальност)|(должност|specialit|специальност).*(ФРМР|FRMR)|(должност|speciali...` |
| `patient_data_gip_text` | Данные пациента не соответствуют ГИП | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)(ГИП|GIP).*(пациент|patient)|(пациент|patient).*(ГИП|GIP)|(данн|сведени).*(пациент|patient).*(не соответств|не с...` |
| `person_frmr` | Данные медработника не соответствуют ФРМР | Данные медработника | код fault — любой; текст `<message>` ~* `(?is)(ФРМР|медработник|автор|author)` |
| `recipient_mismatch` | Получатель из запроса не найден в СЭМД | Данные пациента | код fault = `RECIPIENT_INFO_MISMATCH`; текст `<message>` — любой |
| `document_kind_not_actual` | Вид документа не актуален на дату создания | Ошибки регистрации в РЭМД | код fault = `NO_DOCUMENT_KIND_ON_DATE`; текст `<message>` — любой |
| `object_not_found` | Подразделение или запись справочника не найдены на дату документа | Ошибки регистрации в РЭМД | код fault = `OBJECT_NOT_FOUND`; текст `<message>` — любой |
| `doctor_patronymic_mismatch` | Отчество врача не соответствует данным СЭМД | Данные медработника | код fault = `INVALID_DOCTOR_PATRONYMIC`; текст `<message>` — любой |
| `runtime_request_processing` | РЭМД не смог обработать запрос | Технические ошибки РЭМД | код fault = `RUNTIME_ERROR`; текст `<message>` ~* `(?is)Невозможно обработать запрос` |
| `remd_internal` | Техническая ошибка на стороне РЭМД | Технические ошибки РЭМД | код fault — любой; текст `<message>` ~* `(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб)` |
| `schematron_author_specialty` | Специальность врача не соответствует справочнику НСИ | Данные медработника | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(assignedAuthor.*code.*codeSystem|assignedAuthor.*specialit|специальност.*автор|автор.*специальност)` |
| `schematron_author_snils` | СНИЛС автора (врача) не заполнен или некорректен | Данные медработника | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(assignedAuthor.*(SNILS|СНИЛС|snils)|author.*(СНИЛС|snils))` |
| `schematron_patient_birth` | Дата рождения пациента не заполнена или некорректна | Данные пациента | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(patientRole.*birthTime|birthTime.*patient)` |
| `schematron_patient_name` | ФИО пациента не заполнено или некорректно | Данные пациента | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(patientRole.*(name|given|family)|(given|family).*patientRole)` |
| `schematron_patient_snils` | СНИЛС пациента не заполнен или некорректен | Данные пациента | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(patientRole.*(SNILS|СНИЛС)|patient.*(SNILS|СНИЛС))` |
| `schematron_legal_auth` | Данные заверителя документа не заполнены или некорректны | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)legalAuthenticator` |
| `schematron_creation_time` | Дата/время создания документа не заполнены или некорректны | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(creationTime.*(не заполнен|некорректн|не указан|обязател))` |
| `schematron_doc_code` | Код типа документа не соответствует справочнику НСИ | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(ClinicalDocument/code|тип документа.*(справочник|OID|codeSystem))` |
| `schematron_custodian` | Данные хранителя документа не заполнены | Ошибки структуры и валидации | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(custodian|representedCustodianOrganization)` |
| `schematron_org_repr` | Данные организации автора документа не заполнены | Данные медработника | код fault = `VALIDATION_ERROR`; текст `<message>` ~* `(?is)(assignedAuthor.*representedOrganization|representedOrganization.*author)` |
| `document_not_found_remd` | Документ не найден в РЭМД | Ошибки регистрации в РЭМД | код fault = `DOCUMENT_NOT_FOUND`; текст `<message>` — любой |
| `invalid_emdr_id` | Неверный идентификатор документа РЭМД | Ошибки регистрации в РЭМД | код fault = `INVALID_EMDR_ID`; текст `<message>` — любой |
| `organization_not_found` | Организация не найдена в реестре РЭМД | Ошибки организации / ИС | код fault = `ORGANIZATION_NOT_FOUND`; текст `<message>` — любой |
| `access_denied_remd` | Доступ к операции запрещён в РЭМД | Ошибки регистрации в РЭМД | код fault = `ACCESS_DENIED`; текст `<message>` — любой |
| `duplicate_request` | Дублирующий запрос | Ошибки регистрации в РЭМД | код fault = `DUPLICATE_REQUEST`; текст `<message>` — любой |
| `unsupported_document_type` | Неподдерживаемый тип СЭМД в РЭМД | Ошибки регистрации в РЭМД | код fault = `UNSUPPORTED_DOCUMENT_TYPE`; текст `<message>` — любой |
| `invalid_request_format` | Неверный формат запроса | Ошибки регистрации в РЭМД | код fault = `INVALID_REQUEST_FORMAT`; текст `<message>` — любой |
| `organization_license_not_found` | Лицензия организации не найдена | Ошибки организации / ИС | код fault = `ORGANIZATION_LICENSE_NOT_FOUND`; текст `<message>` — любой |
| `invalid_snils_code` | Неверный формат или контрольная сумма СНИЛС | Данные пациента | код fault = `INVALID_SNILS`; текст `<message>` — любой |
| `organization_not_registered` | Организация не зарегистрирована в РЭМД | Ошибки организации / ИС | код fault = `ORGANIZATION_NOT_REGISTERED`; текст `<message>` — любой |
| `certificate_expired` | Сертификат ЭП истёк | Ошибки ЭП и сертификатов | код fault — любой; текст `<message>` ~* `(?is)(сертификат.*истёк|истекш.*сертификат|срок.*действи.*сертификат.*истёк|certificate.*expired)` |
| `certificate_revoked` | Сертификат ЭП отозван | Ошибки ЭП и сертификатов | код fault — любой; текст `<message>` ~* `(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)` |
| `crl_unavailable` | Недоступен сервис проверки статуса сертификата (CRL/OCSP) | Ошибки ЭП и сертификатов | код fault — любой; текст `<message>` ~* `(?is)(CRL|список.*отозванн|OCSP|сервис.*проверк.*сертификат)` |
| `async_response_timeout_code` | Таймаут асинхронной обработки на стороне РЭМД | Технические ошибки РЭМД | код fault = `ASYNC_RESPONSE_TIMEOUT`; текст `<message>` — любой |
| `ca_unavailable_code` | Недоступен сервис проверки подписи (УЦ) на стороне РЭМД | Технические ошибки РЭМД | код fault = `CA_UNAVAILABLE`; текст `<message>` — любой |
| `ca_inaccessibility_code` | Недоступен сервис проверки подписи (УЦ) на стороне РЭМД | Технические ошибки РЭМД | код fault = `CA_INACCESSIBILITY`; текст `<message>` — любой |
| `document_revoked_text` | Документ аннулирован | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)(аннулирован.*документ|документ.*аннулирован)` |
| `xml_parse_error` | Ошибка разбора XML-структуры документа | Ошибки структуры и валидации | код fault — любой; текст `<message>` ~* `(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)` |
| `snils_invalid_text` | Неверный формат или контрольная сумма СНИЛС | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)(СНИЛС.*неверн|неверн.*СНИЛС|СНИЛС.*контрольн|контрольн.*СНИЛС)` |
| `transport_network` | Сетевая ошибка | Ошибки связи | код fault — любой; текст `<message>` ~* `(?is)(network|connection|transport|timeout|timed out|соединени|таймаут|сетевая ошибка)` |
| `cvc_datatype_extended` | Ошибка XSD-валидации XML | Ошибки структуры и валидации | код fault — любой; текст `<message>` ~* `(?is)cvc-datatype-valid|cvc-pattern-valid|cvc-type|cvc-complex-type|cvc-attribute|cvc-elt|cvc-identity-constraint|cvc...` |
| `attribute_not_found_code` | Метаописание документа не соответствует зарегистрированному в РЭМД | Ошибки регистрации в РЭМД | код fault = `ATTRIBUTE_NOT_FOUND`; текст `<message>` — любой |
| `role_occurrence_mismatch_code` | Подпись роли не соответствует требованиям РЭМД | Ошибки ЭП и сертификатов | код fault = `ROLE_OCCURRENCE_MISMATCH`; текст `<message>` — любой |
| `object_not_found_text_extra` | Подразделение или запись справочника не найдены на дату документа | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)Подразделение.*(идентификатор|не найден)|подразделение.*не найден` |
| `recipient_text_extra` | Получатель из запроса не найден в СЭМД | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)RECIPIENT_INFO_MISMATCH|Получатель.*не найден` |
| `dul_patient_text` | Документ, удостоверяющий личность пациента: некорректные реквизиты | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)ДУЛ[^А-Яа-я]|реквизит.*удостоверени` |
| `patient_birth_text` | Дата рождения пациента не заполнена или некорректна | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)Дата рождения пациента|birthTime` |
| `remd_runtime_internal` | Техническая ошибка на стороне РЭМД | Технические ошибки РЭМД | код fault — любой; текст `<message>` ~* `(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб|невозможно обработать)` |
| `cert_org_validity_expired` | Срок действия сертификата организации истек | Ошибки ЭП и сертификатов | код fault = `CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT`; текст `<message>` — любой |
| `org_ogrn_frmo_mismatch` | Несоответствие данных организации в ФРМО | Ошибки организации / ИС | код fault — любой; текст `<message>` ~* `(?is)(ОГРН|ОКПО|КПП|ИНН).*(СЭМД|ФРМО).*(не совпада|не соответств)|ОГРН МО.*не совпада|ФРМО.*(не совпада|не соответств...` |
| `org_generic_fallback` | Ошибки организации | Ошибки организации / ИС | код fault — любой; текст `<message>` ~* `(?is)(организаци|ОГРН|ФРМО|лицензи)` |
| `patient_fio_mismatch` | ФИО пациента в ЭМД не соответствует данным ЕГИСЗ | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)(Имя|Фамилия|Отчество) пациента в ЭМД \[.*?\] отличается` |
| `patient_gender_mismatch` | Пол пациента в ЭМД не соответствует данным ЕГИСЗ | Данные пациента | код fault — любой; текст `<message>` ~* `(?is)Пол пациента в ЭМД \[.*?\] отличается` |
| `document_uid_mismatch_request` | Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)Уникальный идентификатор документа в ЭМД \[.*?\] отличается` |
| `document_creation_date_mismatch_request` | Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)Дата создания документа в ЭМД \[.*?\] отличается` |
| `signature_mo_date_after_request` | Дата подписи МО позже даты поступления запроса на регистрацию | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)Дата и время создания подписи МО \[.*?\] не может быть позже` |
| `provider_org_mismatch_request` | Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)не совпадает с СП providerOrganization` |
| `represented_org_mismatch_request` | Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)не совпадает с СП representedOrganization` |
| `emd_version_registration_forbidden` | Для данного вида ЭМД запрещена регистрация новых версий | Ошибки регистрации в РЭМД | код fault — любой; текст `<message>` ~* `(?is)запрещена регистрация новых версий` |
