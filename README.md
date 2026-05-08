# EGISZ Airflow ELT

Проект для построения витрины аналитики **«Интеграция с ЕГИСЗ»**: инкрементальная выгрузка журнала обмена из Firebird (`proxy_egisz`), загрузка сырых событий в PostgreSQL DWH (`dwh_egisz`), SQL-парсинг SOAP/XML payload и публикация дашбордов Metabase.

Сервис ориентирован на эксплуатационную аналитику обмена с ЕГИСЗ: статусы обработки, ошибки канала, идентификаторы СЭМД, разрезы по клиникам и контроль полноты обработки.

---

## Содержание

- [Архитектура и технологический стек](#архитектура-и-технологический-стек)
- [Логика выгрузки и хранения](#логика-выгрузки-и-хранения)
- [Логика парсинга и интерпретации данных](#логика-парсинга-и-интерпретации-данных)
- [Airflow DAG](#airflow-dag)
- [DWH-модель](#dwh-модель)
- [Metabase и дашборды](#metabase-и-дашборды)
- [Подключения и секреты](#подключения-и-секреты)
- [Запуск в чистом Kubernetes](#запуск-в-чистом-kubernetes)
- [Каталоги репозитория](#каталоги-репозитория)

---

## Архитектура и технологический стек

* **Источник данных:** Firebird 5, база `proxy_egisz`. Используется как OLTP-источник журнала обмена ЕГИСЗ.
* **Оркестрация:** Apache Airflow 2.11.2, DAG `egisz_elt_dag`, TaskFlow API.
* **DWH:** PostgreSQL, база `dwh_egisz`. В ней создаются staging-таблицы, таблица фактов, справочники, функции парсинга и Metabase-facing views.
* **BI:** Metabase v0.60.2.5. Дашборды хранятся как JSON в `metabase_dashboards/` и импортируются скриптом `metabase/setup-dashboards.sh`.
* **Метаданные сервисов:** Airflow и Metabase используют отдельные внешние PostgreSQL-базы (`airflow_db`, `metabase_app`). DWH не используется как служебная база приложений.

Локальные порты для Docker Desktop Kubernetes:

* Airflow: `http://localhost:8080`
* Metabase: `http://localhost:3000`

---

## Логика выгрузки и хранения

Процесс построен как ELT: Python-слой только извлекает и загружает данные, а смысловая трансформация выполняется в PostgreSQL.

1. **Bootstrap DWH**
   Airflow выполняет SQL из `src/egisz_elt/sql/001_dwh_bootstrap.sql`: создаёт таблицы, функции парсинга и базовые представления.

2. **Синхронизация справочника организаций**
   Из Firebird-таблицы `JPERSONS` выбираются `JID`, `JNAME`, `JINN`, `JADDR`; в DWH они загружаются как `jid`, `name`, `inn`, `address`. Пустые значения ИНН или адреса сохраняются как `NULL`. Данные идемпотентно загружаются в `dim_organizations` через `ON CONFLICT`.

3. **Инкрементальная выгрузка журнала**
   Из `EXCHANGELOG` читается батч по курсору `LOGID`:

   ```sql
   SELECT LOGID, LOGDATE, CREATEDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT
   FROM EXCHANGELOG
   WHERE LOGID > :last_log_id
   ORDER BY LOGID
   ROWS :limit
   ```

4. **Загрузка raw-слоя**
   Сырые строки журнала сохраняются в `exchangelog_raw`. Первичный ключ — `logid`; повторная загрузка безопасна, потому что используется `INSERT ... ON CONFLICT DO UPDATE`. `EXCHANGELOG.LOGDATE` хранится как сервисная дата журнала и не используется для аналитики сообщений.

5. **SQL-трансформация**
   Функция `public.egisz_transform_raw_to_facts(max_log_id)` парсит `MSGTEXT`, нормализует статусы и обновляет `fact_egisz_transactions`.

6. **Обновление watermark**
   После успешной трансформации курсор сохраняется в `elt_state.last_log_id`.

---

## Логика парсинга и интерпретации данных

Ключевой принцип: **`MSGTEXT` хранится как сырой payload, а бизнес-поля извлекаются в DWH SQL-функциями**. Это оставляет исходный журнал неизменным и позволяет пересчитать витрину при изменении правил парсинга.

### XML-теги и поля витрины

| Сущность | Источник | Поле DWH | Правило |
| :--- | :--- | :--- | :--- |
| ID строки журнала | `EXCHANGELOG.LOGID` | `fact_egisz_transactions.exchangelog_log_id` | Технический ключ факта и связь с `exchangelog_raw`. |
| MSGID обмена | `EXCHANGELOG.MSGID` или XML `<messageId>` | `message_id` | Приоритет XML `<messageId>`, fallback — `MSGID` из журнала. |
| Связанное сообщение | XML `<relatesToMessage>` или `<relatesTo>` | `relates_to_id` | Коррелятор исходящего запроса и callback; не подменяет идентификатор СЭМД. |
| Обработано IPS | `EGISZ_MESSAGES.CREATEDATE`, затем `EXCHANGELOG.CREATEDATE` | `log_date` | Аналитическая дата обработки сообщения. `EXCHANGELOG.LOGDATE` не используется для этой метрики. |
| Локальный ID СЭМД | XML `<localUid>` или `<DOCUMENTID>` | `local_uid_semd` | Приоритет `localUid`, fallback — `DOCUMENTID`. |
| Федеральный ID | XML `<emdrId>` | `emdr_id` | Идентификатор документа на стороне РЭМД/ЕГИСЗ, если он есть в payload. |
| Номер документа | XML `<documentNumber>` | `doc_number` | Используется как дополнительный fallback для ключа учёта. |
| OID организации | XML `<organization>` | `org_oid` | Используется для связи с лицензиями через `dim_licenses.mo_uid`. |
| Код СЭМД | XML `<kind>` или `<KIND>` | `semd_code` | Поддерживаются оба регистра тега. |
| Название СЭМД | XML `<name>` или `<documentName>` | `semd_name` | Формирует человекочитаемую подпись типа документа. |
| Код ошибки | XML `<code>` | `error_code` | Попадает в `errors_json` и классификацию ошибок. |
| Текст ошибки | XML `<message>` или transport log | `error_message` | Для `LOGSTATE = 3` берётся транспортный текст из `LOGTEXT`. |
| Дата создания СЭМД | XML `<creationDateTime>` или `<creationDate>` | `creation_date` | Приводится к `timestamptz` через безопасный cast. |
| JID клиники | `LOGTEXT` / `MSGTEXT`, pattern `gost-<JID>` | `jid` | Извлекается регулярным выражением `gost-([0-9]+)`. |

### Безопасное извлечение XML

Функция `public.egisz_xml_text(payload, tag_name)`:

* проверяет, что payload похож на XML;
* очищает имя тега от неожиданных символов;
* ищет тег с namespace-префиксом или без него;
* убирает переносы строк, табы и пустые значения;
* при любой ошибке возвращает `NULL`, чтобы одна плохая строка не ломала весь батч.

### Нормализация статуса

Статус факта вычисляется в SQL:

* `LOGSTATE = 3` всегда трактуется как `error`, потому что это транспортная ошибка канала;
* XML status с `success` превращается в `success`;
* XML status с `error` или payload, содержащий `error`, превращается в `error`;
* всё остальное получает статус `unknown`.

### Ошибки и `errors_json`

Для ошибочных фактов формируется JSON-массив:

```json
[{"code": "...", "message": "..."}]
```

Если ошибка транспортная (`LOGSTATE = 3`), текст строится как `Network Error: <LOGTEXT>`. Если ошибка пришла в XML, используется `<message>`.

### Ключ документа для отчётов

В основной витрине документ отображается как:

```sql
COALESCE(local_uid_semd, emdr_id, doc_number, message_id, relates_to_id, exchangelog_log_id::text)
```

Это важно для аналитики: `relatesToMessage` — коррелятор обмена, а не основной идентификатор СЭМД. Он используется для связи callback с исходным `MessageID`, но в ключ учёта попадает только как поздний технический fallback.

### Классификация ошибок

Функция `public.egisz_error_interpretation_type(error_code, error_message)` группирует ошибки для дашбордов:

* пустые код и сообщение — «ошибка без деталей»;
* `network` / `connection` — «ошибка связи (транспорт)»;
* `timeout` / `timed out` — «таймаут канала»;
* `remd` / `рэмд` — «ошибка асинхронного ответа РЭМД»;
* при наличии кода ошибки — `код: текст`;
* иначе берётся сокращённый текст ошибки.

### Подпись типа СЭМД

Функция `public.egisz_semd_type_report_label(semd_code, semd_name)` формирует подпись:

* если нет кода и названия — `(неизвестно)`;
* если есть только название — название;
* если есть только код — код;
* если есть оба значения — `код · название`.

---

## Airflow DAG

DAG `egisz_elt_dag` использует только Airflow Connections:

* `proxy_egisz_fb` — Firebird source connection;
* `dwh_egisz_pg` — PostgreSQL DWH connection.

Задачи:

1. `bootstrap_dwh` — применяет DWH SQL и восстанавливает/обновляет структуру витрины.
2. `sync_dimensions` — синхронизирует `JPERSONS` в `dim_organizations`.
3. `extract_from_proxy` — читает батч `EXCHANGELOG` после `last_log_id`.
4. `load_to_dwh` — загружает батч в `exchangelog_raw`.
5. `transform_data` — вызывает `egisz_transform_raw_to_facts(max_log_id)`.
6. `update_watermark` — фиксирует успешный курсор в `elt_state`.

Данные между задачами передаются через XCom как JSON-сериализуемые словари. Батч ограничен константой `BATCH_SIZE = 500`.

---

## DWH-модель

Основные таблицы:

* `elt_state` — курсоры инкрементальной обработки (`last_log_id`, `last_egmid`).
* `exchangelog_raw` — raw-слой `EXCHANGELOG`.
* `egisz_messages_raw` — подготовленная таблица для raw-слоя `EGISZ_MESSAGES`.
* `dim_organizations` — справочник организаций из `JPERSONS`.
* `dim_licenses` — справочник лицензий и привязок `mo_uid` / `JID`.
* `fact_egisz_transactions` — нормализованные факты обмена.

Основные представления:

* `v_egisz_transactions_enriched_ui` — главная витрина для Metabase.
* `v_rpt_network_errors_detail_ui` — детализация транспортных и сетевых ошибок.
* `v_health_proxy_db_ui` — техническая сводка raw-слоя и фактов.
* `v_health_signals_ui` — агрегированные health-сигналы.

---

## Metabase и дашборды

Дашборды хранятся как код в `metabase_dashboards/`:

* `01_operational.json` — оперативный мониторинг и динамика.
* `02_service.json` — сервис, healthcheck и сбои канала.
* `03_documents_no_response.json` — документы без ответа.
* `04_quality_and_errors.json` — ошибки и качество данных.
* `05_executive.json` — сводная статистика сервиса интеграции.
* `06_semd_archive.json` — архив СЭМД.

Скрипт `metabase/setup-dashboards.sh`:

* создаёт коллекцию «Интеграция с ЕГИСЗ»;
* регистрирует DWH в Metabase;
* проверяет наличие DWH-объектов, которые используются в SQL карточек;
* запускает sync schema;
* подставляет реальные field id для Field Filters;
* импортирует JSON-дашборды.

---

## Подключения и секреты

Перед чистым запуском подготовьте реальные secret-файлы:

```powershell
Copy-Item k8s/airflow/airflow-metadata-secret.example.yaml k8s/airflow/airflow-metadata-secret.yaml
Copy-Item k8s/metabase/metabase-connections-secret.example.yaml k8s/metabase/metabase-connections-secret.yaml
```

Что настроить:

* `k8s/airflow/airflow-metadata-secret.yaml` — подключение Airflow к внешней metadata DB `airflow_db`.
* `k8s/airflow/airflow-connections-configmap.yaml` — Airflow Connections `dwh_egisz_pg` и `proxy_egisz_fb`.
* `k8s/metabase/metabase-connections-secret.yaml` — служебная БД Metabase (`metabase_app`) и BI-доступ к `dwh_egisz`.

Пример локального доступа из контейнеров Docker Desktop:

* PostgreSQL: `host.docker.internal:5432`
* Firebird: `host.docker.internal:3050`, база/alias `proxy_egisz`

---

## Запуск в чистом Kubernetes

### 1. Инсталляция и запуск Airflow

```powershell
kubectl config current-context
kubectl get nodes
.\up.ps1 -Component Airflow
kubectl get pods
kubectl get svc
```

Команда собирает свежий образ Airflow с текущими DAG и пакетом `egisz_elt`, подготавливает Airflow secrets/connections, проверяет права ELT-пользователя `egisz` в `dwh_egisz`, устанавливает/обновляет Airflow через Helm и выполняет `bootstrap_dwh`.

### 2. Инсталляция и запуск Metabase

```powershell
kubectl config current-context
kubectl get nodes
.\up.ps1 -Component Metabase
kubectl get pods
kubectl get svc
```

Команда собирает свежий образ Metabase с текущими provisioning-скриптами и JSON-дашбордами, подготавливает Metabase secrets, применяет Kubernetes-манифест, обновляет image у deployment и импортирует дашборды.

### 3. Полная установка или обновление запущенного проекта

```powershell
.\up.ps1
```

Полный запуск равен последовательному запуску двух компонентных сценариев: `.\up.ps1 -Component Airflow` и `.\up.ps1 -Component Metabase`. В каждом сценарии вместе с основным образом собираются и применяются все сопутствующие ресурсы, поэтому раздельное поднятие Airflow и Metabase даёт полный запуск проекта.

Docker здесь используется только для сборки локальных images `egisz-airflow-worker` и `egisz-metabase`: Docker Desktop Kubernetes запускает pod'ы в локальном Docker Desktop/Kind-кластере и видит эти images. Runtime остаётся Kubernetes: Airflow устанавливается через Helm, Metabase — через `kubectl`.

---

## Каталоги репозитория

* `airflow/dags/` — DAG `egisz_elt_dag.py`.
* `src/egisz_elt/` — Python-клиенты Firebird/PostgreSQL и загрузочная логика.
* `src/egisz_elt/sql/` — DWH schema, функции парсинга и представления.
* `metabase/` — Dockerfile, entrypoint и provisioning-скрипты Metabase.
* `metabase_dashboards/` — JSON-дашборды.
* `k8s/` — Kubernetes manifests и Helm values.
* `up.ps1` — сборка образов и установка в чистый Kubernetes.
