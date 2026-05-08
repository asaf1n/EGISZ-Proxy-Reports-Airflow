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
* **Метаданные сервисов:** Airflow использует встроенный PostgreSQL Helm chart с базой `airflow_db`, Metabase использует отдельную PostgreSQL-базу `metabase_app`. DWH не используется как служебная база приложений.

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

Если ошибка транспортная (`LOGSTATE = 3`), текст строится как `Сетевая ошибка: <LOGTEXT>`. Если ошибка пришла в XML, `errors_json` собирается по элементам `<item><code>...<message>...`, поэтому несколько сообщений Schematron сохраняются отдельными элементами массива.

### Ключ документа для отчётов

В основной витрине документ отображается как:

```sql
COALESCE(local_uid_semd, emdr_id, relates_to_id, doc_number, message_id, exchangelog_log_id::text)
```

Это важно для аналитики: основное сопоставление СЭМД идёт по `localUid`/`DOCUMENTID`, `emdrId` и затем `relatesToMessage`. Хост клиники из `gost-*` используется только как поздняя подсказка, потому что один хост может обслуживать несколько клиник.

### Классификация ошибок

Функции `public.egisz_error_interpretation_item`, `public.egisz_error_interpretation_row` и `public.egisz_error_interpretation_type` интерпретируют и группируют ошибки для дашбордов. Справочные правила хранятся в `public.egisz_error_interpretation_rules`, а отдельная витрина `public.v_rpt_error_interpretations_ui` раскрывает ошибки построчно:

* пустые код и сообщение — «ошибка без деталей»;
* `network` / `connection` / `timeout` / `Сетевая ошибка` — «ошибка связи (транспорт)»;
* каскад Schematron по `ClinicalDocument/recordTarget/patientRole/addr/address:Type` — «Не указан адрес пациента»;
* `remd` / `рэмд` — «ошибка асинхронного ответа РЭМД»;
* XSD/cvc — короткая подсказка по XML-валидации;
* без ошибок в UI-сводке возвращается пустая строка или «Успешно» для успешных документов.

Для BI-группировки используется отдельная функция `public.egisz_error_group_type(code, message)`. Она не дробит «Тип ошибки» по произвольному исходному тексту:

* сетевые и транспортные ошибки попадают в общий тип `Сетевая ошибка`;
* ошибки ответа РЭМД попадают в известную подгруппу справочника, если правило уже накоплено в `egisz_error_interpretation_rules`;
* если справочного правила недостаточно, ошибка попадает в общий тип `Ошибка асинхронного ответа РЭМД`.

Главная витрина `public.v_egisz_transactions_enriched_ui` дополнительно отдаёт поле `Исходный текст ошибки`. Карточки со сводкой ошибки выводят рядом исходный текст или агрегированный пример исходных текстов, чтобы можно было сверить нормализованную группировку с реальным сообщением.

### Подпись типа СЭМД

Функция `public.egisz_semd_type_report_label(semd_code, semd_name)` формирует подпись через DWH-таблицу `dim_semd_types`:

* если нет кода и названия — `(неизвестно)`;
* если код есть в справочнике НСИ — `код · наименование из dim_semd_types`;
* если кода нет в справочнике, но в payload есть осмысленное название — `код · название из payload`;
* если есть только код — `код · Наименование СЭМД отсутствует в НСИ 1520`.

`dim_semd_types.code` соответствует полю `OID` из приложенного файла `1.2.643.5.1.13.13.11.1520_12.48_json.zip`, а `TYPE` хранится отдельно как `type_code`. Обновление справочника ручное: заменить seed-данные в `src/egisz_elt/sql/001_dwh_bootstrap.sql` или обновить строки в `dim_semd_types` напрямую в DWH и затем переимпортировать Metabase.

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
* `dim_semd_types` — справочник наименований типов СЭМД из НСИ `1.2.643.5.1.13.13.11.1520`, версия `12.48`; используется для отображения `Тип СЭМД (код · НСИ)` и `Наименование СЭМД`.
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
Copy-Item k8s/metabase/metabase-connections-secret.example.yaml k8s/metabase/metabase-connections-secret.yaml
```

Что настроить:

* Airflow metadata DB `airflow_db` создаётся встроенным PostgreSQL Helm chart в Kubernetes PVC.
* `k8s/airflow/airflow-connections-configmap.yaml` — Airflow Connections `dwh_egisz_pg` и `proxy_egisz_fb`.
* `k8s/metabase/metabase-connections-secret.yaml` — служебная БД Metabase (`metabase_app`) и BI-доступ к `dwh_egisz`.

Пример локального доступа из контейнеров Docker Desktop:

* PostgreSQL: `host.docker.internal:5432`
* Firebird: `host.docker.internal:3050`, база/alias `proxy_egisz`

### Где расположены компоненты и где задаются параметры

Kubernetes-манифесты в репозитории не задают отдельный namespace. Все команды `kubectl` ниже работают в текущем namespace выбранного Kubernetes-контекста. Если проект развёрнут в отдельный namespace, добавляйте `-n <namespace>` к командам `kubectl`.

| Часть | Где находится runtime / данные | Основной конфиг |
| :--- | :--- | :--- |
| Firebird source `proxy_egisz` | Внешняя БД, не поднимается этим проектом. Для локального Docker Desktop ожидается `host.docker.internal:3050`, база/alias `proxy_egisz`. | `k8s/airflow/airflow-connections-configmap.yaml`, ключ `AIRFLOW_CONN_PROXY_EGISZ_FB`. |
| PostgreSQL DWH `dwh_egisz` | Внешняя PostgreSQL-БД, не контейнер этого проекта. Airflow пишет пользователем `egisz`, Metabase читает BI-пользователем `postgres`. | Airflow: `k8s/airflow/airflow-connections-configmap.yaml`, ключ `AIRFLOW_CONN_DWH_EGISZ_PG`. Metabase: `k8s/metabase/metabase-connections-secret.yaml`, ключи `DWH_DB_*` и `DWH_BI_*`. Bootstrap прав доступа в `up.ps1` читает `EGISZ_PG_*`, `EGISZ_DWH_*`. |
| Airflow metadata DB `airflow_db` | PostgreSQL Helm chart внутри Kubernetes-кластера, pod/statefulset `airflow-postgresql-0`, данные в Kubernetes PVC. Это служебная БД Airflow, не DWH. | `k8s/airflow/values.yaml`, блоки `data.metadataConnection` и `postgresql`. |
| Airflow сервисы | Kubernetes/Helm release `airflow`: `airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`. Образ собирается локально как `egisz-airflow-worker:<tag>`. | `k8s/airflow/values.yaml`, `k8s/airflow/Dockerfile`, `airflow/dags/egisz_elt_dag.py`, `src/egisz_elt/`. Установка через `up.ps1 -Component Airflow`. |
| Metabase application DB `metabase_app` | Внешняя PostgreSQL-БД для внутренних таблиц Metabase. DWH `dwh_egisz` для этого не используется. | `k8s/metabase/metabase-connections-secret.yaml`, ключи `METABASE_DB_*`. |
| Metabase сервис | Kubernetes deployment/service `metabase`, образ `egisz-metabase:<tag>`, web UI на порту `3000`. | `k8s/metabase/metabase.yaml`, `metabase/Dockerfile`, `metabase/provision.sh`, `metabase/setup-dashboards.sh`, `metabase_dashboards/*.json`. Установка через `up.ps1 -Component Metabase`. |

---

## Запуск в чистом Kubernetes

### Полная установка или обновление запущенного проекта

```powershell
.\up.ps1
```
Отдельная установка/запуск Airflow: 
```powershell
.\up.ps1 -Component Airflow
```

Отдельная установка/запуск Metabase: 
```powershell
.\up.ps1 -Component Metabase
```

Полный запуск равен последовательному запуску двух компонентных сценариев: `.\up.ps1 -Component Airflow` и `.\up.ps1 -Component Metabase`. В каждом сценарии вместе с основным образом собираются и применяются все сопутствующие ресурсы, поэтому раздельное поднятие Airflow и Metabase даёт полный запуск проекта.

Docker здесь используется только для сборки локальных images `egisz-airflow-worker` и `egisz-metabase`: Docker Desktop Kubernetes запускает pod'ы в локальном Docker Desktop/Kind-кластере и видит эти images. Runtime остаётся Kubernetes: Airflow устанавливается через Helm, Metabase — через `kubectl`.

### Остановка Airflow и Metabase

Остановить локальные `port-forward` процессы: в каждом окне PowerShell, где запущен `kubectl port-forward`, нажмите `Ctrl+C`.

Остановить только Metabase pod, не удаляя deployment/service и внешнюю БД `metabase_app`:

```powershell
kubectl scale deployment/metabase --replicas=0
```

Остановить только Airflow pod'ы, не удаляя Helm release и metadata PVC:

```powershell
kubectl scale deployment/airflow-webserver deployment/airflow-scheduler --replicas=0
kubectl scale statefulset/airflow-worker statefulset/airflow-triggerer --replicas=0
```

Вернуть Airflow и Metabase после такого stop:

```powershell
kubectl scale deployment/metabase --replicas=1
kubectl scale deployment/airflow-webserver deployment/airflow-scheduler --replicas=1
kubectl scale statefulset/airflow-worker statefulset/airflow-triggerer --replicas=1
```

Остановить Metabase полностью из Kubernetes:

```powershell
kubectl delete deployment/metabase service/metabase
```

Остановить Airflow release полностью:

```powershell
helm uninstall airflow
```

После `helm uninstall airflow` служебные PVC Kubernetes могут остаться в кластере. Если нужно начать Airflow metadata DB с нуля, отдельно проверьте и удалите PVC Airflow PostgreSQL:

```powershell
kubectl get pvc
kubectl delete pvc <airflow-postgresql-pvc-name>
```

### Применение последних правок DWH и дашбордов

После изменения SQL-витрин или JSON-карточек примените DWH-контракт через DAG, затем переимпортируйте Metabase:

```powershell
kubectl port-forward svc/airflow-webserver 8080:8080
```

В другом окне:

```powershell
kubectl exec deploy/airflow-scheduler -- airflow dags trigger egisz_elt_dag
kubectl logs deploy/airflow-scheduler --tail=200 -f
```

Если нужно применить только Metabase-слой после уже успешного `bootstrap_dwh`:

```powershell
.\up.ps1 -Component Metabase
kubectl port-forward svc/metabase 3000:3000
```

--- 

## Каталоги репозитория

* `airflow/dags/` — DAG `egisz_elt_dag.py`.
* `src/egisz_elt/` — Python-клиенты Firebird/PostgreSQL и загрузочная логика.
* `src/egisz_elt/sql/` — DWH schema, функции парсинга и представления.
* `metabase/` — Dockerfile, entrypoint и provisioning-скрипты Metabase.
* `metabase_dashboards/` — JSON-дашборды.
* `k8s/` — Kubernetes manifests и Helm values.
* `up.ps1` — сборка образов и установка в чистый Kubernetes.
