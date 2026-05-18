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

1. **Bootstrap DWH** *(вне DAG, разово)*
   DWH-схема, функции парсинга, фактовые таблицы и представления создаются скриптом `db/dwh_init.sql`. Его нужно прогнать одним суперпользователем PostgreSQL до первого включения DAG:

   ```bash
   psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql
   ```

   Скрипт идемпотентен — повторный запуск безопасен и применяет обновления (`CREATE ... IF NOT EXISTS`, `ALTER TABLE ... ADD/DROP COLUMN IF EXISTS`, `CREATE OR REPLACE`). Этот же файл повторно прогоняется при обновлении DWH-модели.

2. **Синхронизация справочников**
   Из Firebird-таблицы `JPERSONS` выбираются `JID`, `JNAME`, `JINN`, `JADDR`; в DWH они загружаются как `jid`, `name`, `inn`, `address` в `dim_organizations`. Параллельно синхронизируется `dim_licenses` из `EGISZ_LICENSES` для привязки `mo_uid` / `JID` и обогащения колбэков. Пустые значения ИНН или адреса сохраняются как `NULL`. Загрузка обеих размерностей идемпотентна и выполняется через `ON CONFLICT`.

3. **Инкрементальная выгрузка журнала и сообщений**
   Из `EXCHANGELOG` читается батч по курсору `LOGID`, а из `EGISZ_MESSAGES` — отдельный батч по курсору `EGMID`. Текущий размер батча в DAG: `BATCH_SIZE = 5000`.

   Базовый запрос к `EXCHANGELOG`:

   ```sql
   SELECT LOGID, LOGDATE, CREATEDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT
   FROM EXCHANGELOG
   WHERE LOGID > :last_log_id
   ORDER BY LOGID
   ROWS :limit
   ```

4. **Загрузка raw-слоя**
   Сырые строки журнала сохраняются в `exchangelog_raw`, а сообщения из `EGISZ_MESSAGES` — в `egisz_messages_raw`. Первичные ключи — `logid` и `egmid`; повторная загрузка безопасна, потому что используется `INSERT ... ON CONFLICT DO UPDATE`. `EXCHANGELOG.LOGDATE` хранится как сервисная дата журнала и не используется для аналитики сообщений.

5. **SQL-трансформация**
   Функция `public.egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)` парсит `MSGTEXT`, нормализует статусы и обновляет `fact_egisz_transactions`. Кандидатами для обработки идут строки журнала, попавшие в LOGID-окно текущего батча, и старые строки `exchangelog_raw`, к которым в EGMID-окне пришёл поздний callback из `egisz_messages_raw`.

6. **Refresh materialized views**
   Отдельная задача `refresh_materialized_views` обновляет mat-view'ы `v_egisz_transactions_enriched_ui` и `v_stg_channel_errors_by_document` через `REFRESH MATERIALIZED VIEW CONCURRENTLY`.

7. **Обновление watermark**
   После успешной трансформации DAG сохраняет оба курсора в `elt_state`: `last_log_id` и `last_egmid`.

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
| Код ошибки | XML `<code>` | `error_code` | Используется в классификации ошибок (`egisz_error_classify`). |
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

### Ошибки: построение и хранение

Для ошибочных фактов SQL-функция `public.egisz_build_errors_json(status, error_code, error_message, msgtext)` строит эфемерный JSON-массив вида `[{"code": "...", "message": "..."}]`:

* транспортная ошибка (`LOGSTATE = 3`) → один элемент с текстом `Сетевая ошибка: <LOGTEXT>`;
* ответ РЭМД → элементы собираются из XML `<item><code>...<message>...`, поэтому несколько сообщений Schematron сохраняются отдельными элементами массива.

Этот массив не сохраняется в таблице — он используется как вход для классификации и сразу раскладывается в три персистентных поля `fact_egisz_transactions`:

* `error_type` — плоская канонизированная категория (`public.egisz_error_classify`). Несколько разных типов из одного ответа склеиваются через ` · `. В витрине это колонка `Тип ошибки`.
* `error_summary` — человекочитаемая сводка по правилам (`public.egisz_error_interpretation_row`). В витрине — `Сводка ошибки`.
* `error_json_text` — исходные тексты `<message>` без классификации (`public.egisz_error_messages_row`). В витрине — `Исходный текст ошибки`.

### Ключ документа для отчётов

В основной витрине документ отображается как:

```sql
COALESCE(local_uid_semd, emdr_id, relates_to_id, doc_number, message_id, exchangelog_log_id::text)
```

Это важно для аналитики: основное сопоставление СЭМД идёт по `localUid`/`DOCUMENTID`, `emdrId` и затем `relatesToMessage`. Хост клиники из `gost-*` используется только как поздняя подсказка, потому что один хост может обслуживать несколько клиник.

### Классификация ошибок

Справочник правил живёт в таблице `public.egisz_error_interpretation_rules` (колонки `match_code`, `match_pattern`, `interpretation`, `priority`). Алгоритм классификации одного `<item>`:

1. Перебрать активные правила в порядке возрастания `priority`. Выигрывает первое, у которого совпал и `match_code` (если задан), и `match_pattern` (регексп по тексту сообщения).
2. Если ни одно правило не сработало — применяются захардкоженные code-fallback'и в `egisz_error_interpretation_type` (`RUNTIME_ERROR`/`INTERNAL_ERROR`, `CA_INACCESSIBILITY`/`CA_UNAVAILABLE`, `ASYNC_RESPONSE_TIMEOUT`/`TIMEOUT`).
3. Если и они не сработали — возвращается `Неизвестная ошибка`.

`egisz_error_classify` применяет шаг 1–3 к каждому `<item>` в JSON-массиве, дедуплицирует и склеивает через ` · `.

Витрина `public.v_rpt_error_interpretations_ui` раскрывает ошибки построчно (по одной строке на `<item>`), что полезно для drill-down в дашбордах.

#### Полный список правил

Порядок строк = порядок срабатывания. Колонка «Правила определения» читается как «code равен X **И** текст совпадает с regex Y», `;` разделяет независимые альтернативные правила, ведущие к одному и тому же типу.

| № | Тип ошибки | Правила определения | Пример сообщения |
|---|---|---|---|
| 1 | Не указан адрес пациента | code=`VALIDATION_ERROR` и текст ~ `Schematron.*patientRole.*addr.*address:Type` | Ошибка валидации Schematron: У1-21. Элемент ClinicalDocument/recordTarget/patientRole/addr/address:Type должен иметь не пустое значение атрибута @codeSystemVersion |
| 2 | Несоответствие данных организации в ФРМО | текст ~ `(ОГРН\|ОКПО\|КПП\|ИНН).*(СЭМД\|ФРМО).*(не совпада\|не соответств)` или `ОГРН МО.*не совпада` или `ФРМО.*(не совпада\|не соответств).*организац` | ОГРН МО не совпадает с ОГРН в ФРМО |
| 3 | Организация не привязана к РМИС | code=`VALIDATION_ERROR` и текст ~ `не привязана к РМИС` | Организация [1.2.643.5.1.13.13.12.2.64.139333] не привязана к РМИС [emdr-rmis-1754] |
| 4 | Некорректно заполнен телефон | code=`VALIDATION_ERROR` и текст ~ `telecom.*(не пустым значением\|@value)` или `Ошибка заполнения номера телефона` | Schematron У1-4. //telecom со схемой "tel:" должны соответствовать tel:\+?[-0-9().]+ |
| 5 | Специальность врача не соответствует справочнику НСИ | code=`VALIDATION_ERROR` и текст ~ `assignedAuthor.*(code.*codeSystem\|specialit)` или `специальност.*автор` | Specialitia автора не соответствует справочнику НСИ |
| 6 | СНИЛС автора (врача) не заполнен или некорректен | code=`VALIDATION_ERROR` и текст ~ `assignedAuthor.*(SNILS\|СНИЛС)` или `author.*(СНИЛС\|snils)` | assignedAuthor: СНИЛС не указан |
| 7 | Дата рождения пациента не заполнена или некорректна | code=`VALIDATION_ERROR` и текст ~ `patientRole.*birthTime`; ИЛИ текст ~ `Дата рождения пациента\|birthTime` | Дата рождения пациента в ЭМД [19870222] отличается от даты рождения пациента в запросе на регистрацию сведений [1987-03-22] |
| 8 | ФИО пациента не заполнено или некорректно | code=`VALIDATION_ERROR` и текст ~ `patientRole.*(name\|given\|family)` | patientRole/name/given пуст |
| 9 | СНИЛС пациента не заполнен или некорректен | code=`VALIDATION_ERROR` и текст ~ `patientRole.*(SNILS\|СНИЛС)` или `patient.*(SNILS\|СНИЛС)` | patientRole: СНИЛС обязателен |
| 10 | Данные заверителя документа не заполнены или некорректны | code=`VALIDATION_ERROR` и текст ~ `legalAuthenticator` | legalAuthenticator: блок не заполнен |
| 11 | Дата/время создания документа не заполнены или некорректны | code=`VALIDATION_ERROR` и текст ~ `creationTime.*(не заполнен\|некорректн\|не указан\|обязател)` | creationTime: атрибут обязателен |
| 12 | Ошибка XSD-валидации XML | текст ~ `\bcvc-\|XML_VALIDATION_ERROR\|xsd\|Invalid content was found\|not complete\|not valid`; ИЛИ текст ~ `cvc-(datatype-valid\|pattern-valid\|type\|complex-type\|attribute\|elt\|identity-constraint\|particle\|enumeration-valid)` | cvc-complex-type.2.4.a: Invalid content was found starting with element '{"urn:hl7-org:v3":id}'. One of '{"urn:hl7-org:v3":code}' is expected |
| 13 | Код типа документа не соответствует справочнику НСИ | code=`VALIDATION_ERROR` и текст ~ `ClinicalDocument/code` или `тип документа.*(справочник\|OID\|codeSystem)` | ClinicalDocument/code: тип документа не из справочника |
| 14 | Данные хранителя документа не заполнены | code=`VALIDATION_ERROR` и текст ~ `custodian\|representedCustodianOrganization` | custodian/representedCustodianOrganization пуст |
| 15 | Данные организации автора документа не заполнены | code=`VALIDATION_ERROR` и текст ~ `assignedAuthor.*representedOrganization` или `representedOrganization.*author` | assignedAuthor/representedOrganization не заполнен |
| 16 | Документ уже зарегистрирован в РЭМД | code=`NOT_UNIQUE_PROVIDED_ID` | Документ с идентификатором 'F368A14E-31A7-422C-99A1-8043239019DE' уже зарегистрирован с номером [119.77.26.05.030830566] |
| 17 | Данные пациента не соответствуют ГИП | code=`PATIENT_MPI_MISMATCH`; ИЛИ текст ~ `(ГИП\|GIP).*(пациент\|patient)` или `(пациент\|patient).*(ГИП\|GIP)` или `(данн\|сведени).*(пациент\|patient).*(не соответств\|не совпад\|не найден)` | Указанное значение [Пол пациента] [FEMALE] не соответствует данным ГИП [Мужской] |
| 18 | Должность врача не соответствует данным ФРМР | code=`PERSON_POST_IN_FRMR_MISMATCH`; ИЛИ текст ~ `(ФРМР\|FRMR).*(должност\|specialit\|специальност)` или `(должност\|specialit\|специальност).*(ФРМР\|FRMR)` или `(должност\|specialit\|специальност).*(не соответств\|не совпад\|не найден)` | Указанная должность сотрудника со СНИЛС [11439547255] не соответствует занимаемой им должности в организации [...] по данным ФРМР |
| 19 | Медработник не найден в ФРМР | code=`PERSON_NOT_FOUND` | Медработник со СНИЛС [...] не найден в ФРМР |
| 20 | Данные медработника не соответствуют ФРМР | code=`VALUE_MISMATCH_METADATA_AND_FRMR`; ИЛИ текст ~ `ФРМР\|медработник\|автор\|author` | Дата рождения сотрудника со СНИЛС [10971925272] (1970-01-01) не соответствует данным ФРМР [1977-04-18] |
| 21 | Подписант из сертификата не найден в ФРМР | code=`VALUE_MISMATCH_METADATA_AND_CERTIFICATE` и текст ~ `не найдена актуальная.*карточка МР` | В ФРМР не найдена актуальная на дату создания документа карточка МР с данными из сертификата подписи МО: Коваленко Александр (СНИЛС: 20649372570) |
| 22 | Данные подписи не соответствуют данным документа | code=`VALUE_MISMATCH_METADATA_AND_CERTIFICATE` (любой текст) | Несоответствие данных подписанта в запросе и в сертификате. ORG_NAME [ООО "МЕДАС"] в метаданных и [ГОБУЗ ...] |
| 23 | Подпись роли не соответствует требованиям РЭМД | code=`ROLE_OCCURRENCE_MISMATCH` | Подпись роли врача отсутствует |
| 24 | Неактуальная версия справочника НСИ | code=`INVALID_DICTIONARY_OID` | Справочник OID [1.2.643.5.1.13.13.11.1002]. Версия [10.1_old] отсутствует для данного справочника |
| 25 | Код отсутствует в справочнике НСИ | code=`INVALID_ELEMENT_VALUE_CODE` | Справочник OID [1.2.643.5.1.13.13.11.1005], версия [2.24]. Элемент с кодом [M51.1+] отсутствует |
| 26 | Наименование не соответствует справочнику НСИ | code=`INVALID_ELEMENT_VALUE_NAME` | Справочник OID [1.2.643.5.1.13.13.11.1053], версия [3.3]. Наименование [Первая группа] не соответствует наименованию в НСИ [1 группа] |
| 27 | Ошибка справочника НСИ | текст ~ `Справочник OID\|codeSystem\|codeSystemVersion\|верси[яи].*справочник\|значени[ея].*НСИ\|не соответствует наименованию элемента в НСИ\|справочн.*значен` | Справочник OID [1.2.643.5.1.13.13.99.2.197]. Версия [4.41] недопустима для документа вида [227]. Требуется: [4.44] |
| 28 | Документ не найден в РЭМД | code=`DOCUMENT_NOT_FOUND` | Документ с идентификатором [...] не найден в РЭМД |
| 29 | Неверный идентификатор документа РЭМД | code=`INVALID_EMDR_ID` | Идентификатор документа РЭМД некорректен |
| 30 | Организация не найдена в реестре РЭМД | code=`ORGANIZATION_NOT_FOUND` | Организация [...] не найдена в реестре РЭМД |
| 31 | Доступ к операции запрещён в РЭМД | code=`ACCESS_DENIED` | Доступ к операции запрещён |
| 32 | ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС | code=`DISABLED_RMIS` | ИС с идентификатором [...] не активна |
| 33 | ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные | code=`NO_RMIS` | ИС с идентификатором [...] не зарегистрирована |
| 34 | Дублирующий запрос | code=`DUPLICATE_REQUEST` | Дублирующий запрос с messageId [...] |
| 35 | Неподдерживаемый тип СЭМД в РЭМД | code=`UNSUPPORTED_DOCUMENT_TYPE` | Тип документа [...] не поддерживается в РЭМД |
| 36 | Неверный формат запроса | code=`INVALID_REQUEST_FORMAT` | Неверный формат запроса |
| 37 | Лицензия организации не найдена | code=`ORGANIZATION_LICENSE_NOT_FOUND` | Лицензия организации [...] не найдена |
| 38 | Неверный формат или контрольная сумма СНИЛС | code=`INVALID_SNILS`; ИЛИ текст ~ `СНИЛС.*неверн\|неверн.*СНИЛС\|СНИЛС.*контрольн\|контрольн.*СНИЛС` | Неверная контрольная сумма СНИЛС [12345678901] |
| 39 | Организация не зарегистрирована в РЭМД | code=`ORGANIZATION_NOT_REGISTERED` | Организация [...] не зарегистрирована в РЭМД |
| 40 | Метаописание документа не соответствует зарегистрированному в РЭМД | code=`ATTRIBUTE_NOT_FOUND` или code=`ATTRIBUTE_MISMATCH` | Метаописание документа не соответствует зарегистрированному в РЭМД |
| 41 | Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile | code=`MIS_NOT_AVAILABLE` | Сервис предоставляющей ИС недоступен |
| 42 | Запрашиваемая запись ЭМД не найдена в предоставляющей ИС | code=`REGISTRY_ITEM_NOT_FOUND` | Запись ЭМД [...] не найдена в предоставляющей ИС |
| 43 | ИС не передала файл ЭМД в ответе getDocumentFile | code=`FILE_WAS_NOT_SENT` | Файл ЭМД отсутствует в ответе getDocumentFile |
| 44 | Не удалось получить файл ЭМД из предоставляющей ИС | code=`RMIS_ERROR` ИЛИ code=`GET_DOCUMENT_FILE_ERROR`; ИЛИ текст ~ `getDocumentFile\|получения файла ЭМД\|файлового хранилища` | Сетевая ошибка: Synapse TCP/IP Socket error 10061: Connection refused |
| 45 | Срок действия сертификата организации истек | code=`CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT` | Срок действия сертификата организации истек или еще не наступил |
| 46 | Сертификат ЭП истёк | текст ~ `сертификат.*истёк\|истекш.*сертификат\|срок.*действи.*сертификат.*истёк\|certificate.*expired` | ЭП МО не верна: PKUP of the certificate: sn 2f2b71…, EMAILADDRESS=stepin190@gmail.com |
| 47 | Сертификат ЭП отозван | текст ~ `сертификат.*отозван\|certificate.*revoked\|revoked.*certificate` | Сертификат подписи отозван УЦ |
| 48 | Недействительный сертификат подписи | текст ~ `CANT_BUILD_CERT_CHAIN\|цепочк.*сертификат\|аккредитованн.*УЦ` | Невозможно построить цепочку сертификатов до аккредитованного УЦ |
| 49 | Сертификат подписи недействителен на дату создания документа | текст ~ `DOC_DATE_MISMATCH_CERT_NOT_BEFORE\|сертификат.*не действителен.*дат[уы] создания` | Сертификат не действителен на дату создания документа |
| 50 | Не удалось проверить электронную подпись | code=`SIGNATURE_VERIFICATION_ERROR` | ЭП МО не верна: Validation failed for the target: serial 2126f8e0…, subject SURNAME=Галимулин |
| 51 | Недоступен сервис проверки статуса сертификата (CRL/OCSP) | текст ~ `CRL\|список.*отозванн\|OCSP\|сервис.*проверк.*сертификат` | Удостоверяющий центр сертификата недоступен: CRL fetch failed |
| 52 | Таймаут асинхронной обработки на стороне РЭМД | code=`ASYNC_RESPONSE_TIMEOUT` | Время ожидания асинхронного ответа истекло |
| 53 | Недоступен сервис проверки подписи (УЦ) на стороне РЭМД | code=`CA_UNAVAILABLE` или code=`CA_INACCESSIBILITY` | Удостоверяющий центр сертификата недоступен: Время ожидания истекло |
| 54 | Документ аннулирован | текст ~ `аннулирован.*документ\|документ.*аннулирован` | Документ аннулирован отправителем |
| 55 | Ошибка разбора XML-структуры документа | текст ~ `SAXParseException\|org\.xml\|ParseError\|XML.*parse.*error` | org.xml.sax.SAXParseException: Premature end of file |
| 56 | СНИЛС не найден или не соответствует данным пациента/медработника | текст ~ `СНИЛС\|SNILS` | СНИЛС пациента в составе сведений о пациенте обязателен для данного вида документов |
| 57 | Получатель из запроса не найден в СЭМД | code=`RECIPIENT_INFO_MISMATCH`; ИЛИ текст ~ `RECIPIENT_INFO_MISMATCH\|Получатель.*не найден` | Получатель [19835052502] из запроса на регистрацию сведений не найден в СЭМД |
| 58 | Вид документа не актуален на дату создания | code=`NO_DOCUMENT_KIND_ON_DATE` | Вид документов [6] не актуален на дату создания документа |
| 59 | Подразделение или запись справочника не найдены на дату документа | code=`OBJECT_NOT_FOUND`; ИЛИ текст ~ `Подразделение.*(идентификатор\|не найден)` | Подразделение с идентификатором [1.2.643.5.1.13.13.12.2.36.20192.0.704432] не существовало на дату создания документа |
| 60 | Отчество врача не соответствует данным СЭМД | code=`INVALID_DOCTOR_PATRONYMIC` | Отчество врача в метаданных отличается от отчества в СЭМД |
| 61 | Документ, удостоверяющий личность пациента: некорректные реквизиты | текст ~ `ДУЛ[^А-Яа-я]\|реквизит.*удостоверени` | Неверный формат номера ДУЛ. Номер [.] должен содержать цифры |
| 62 | РЭМД не смог обработать запрос | code=`RUNTIME_ERROR` и текст ~ `Невозможно обработать запрос` | Невозможно обработать запрос |
| 63 | Техническая ошибка на стороне РЭМД | текст ~ `INTERNAL_ERROR\|RUNTIME_ERROR\|внутренн.*ошиб\|непредвиденн.*ошиб\|невозможно обработать` | INTERNAL_ERROR: внутренняя ошибка сервиса |
| 64 | Сетевая ошибка | текст ~ `network\|connection\|transport\|timeout\|timed out\|соединени\|таймаут\|сетевая ошибка` | Сетевая ошибка: Synapse TCP/IP Socket error 10060: Connection timed out |
| 65 | Ошибки организации (generic fallback) | текст ~ `организаци\|ОГРН\|ФРМО\|лицензи` | Ошибка валидации организации в реестре |
| 66 | Техническая ошибка на стороне РЭМД: повторите отправку позже | code=`RUNTIME_ERROR` или code=`INTERNAL_ERROR` (хардкод в `egisz_error_interpretation_type`, после промахов по правилам) | RUNTIME_ERROR без распознаваемого текста |
| 67 | Недоступен сервис проверки подписи/УЦ на стороне РЭМД: повторите отправку позже | code=`CA_INACCESSIBILITY` или code=`CA_UNAVAILABLE` (хардкод, после промахов по правилам) | CA_INACCESSIBILITY без распознаваемого текста |
| 68 | Таймаут асинхронной обработки на стороне РЭМД: повторите отправку позже | code=`ASYNC_RESPONSE_TIMEOUT` или code=`TIMEOUT` (хардкод, после промахов по правилам) | ASYNC_RESPONSE_TIMEOUT без распознаваемого текста |
| 69 | Неизвестная ошибка | финальный fallback: ни одно правило/code-fallback не сработали, либо `error_message` пустое | *(нет сообщения)* |

### Код и наименование СЭМД

Основные аналитические поля:

* `Код СЭМД` — это `OID` из справочника НСИ `1.2.643.5.1.13.13.11.1520`.
* `Наименование СЭМД` — это человекочитаемое наименование из `dim_semd_types`, либо payload fallback, если код ещё не заведен в справочнике.

`dim_semd_types.code` соответствует полю `OID` из приложенного файла `1.2.643.5.1.13.13.11.1520_12.48_json.zip`, а `TYPE` хранится отдельно как `type_code`. Обновление справочника ручное: заменить seed-данные в `src/egisz_elt/sql/001_dwh_bootstrap.sql` или обновить строки в `dim_semd_types` напрямую в DWH и затем переимпортировать Metabase.

---

## Airflow DAG

DAG `egisz_elt_dag` использует только Airflow Connections:

* `proxy_egisz_fb` — Firebird source connection;
* `dwh_egisz_pg` — PostgreSQL DWH connection.

Задачи:

1. `sync_dimensions` — синхронизирует `dim_organizations` и `dim_licenses`.
2. `extract_from_proxy` — читает батчи `EXCHANGELOG` и `EGISZ_MESSAGES`, а также добирает связанные сообщения по `MSGID` / `DOCUMENTID`.
3. `load_to_dwh` — загружает raw-данные в `exchangelog_raw` и `egisz_messages_raw`.
4. `transform_data` — вызывает `egisz_transform_raw_to_facts(min_log_id, max_log_id, min_egmid, max_egmid)`.
5. `refresh_materialized_views` — обновляет `v_egisz_transactions_enriched_ui` и `v_stg_channel_errors_by_document` через `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
6. `update_watermark` — фиксирует успешные курсоры `LOGID` и `EGMID` в `elt_state`.

DWH-схема создаётся отдельным скриптом `db/dwh_init.sql` (см. секцию «Логика выгрузки и хранения»), а не задачей DAG.

Данные между задачами передаются через XCom как JSON-сериализуемые словари. Батч ограничен константой `BATCH_SIZE = 5000`.

---

## DWH-модель

Основные таблицы:

* `elt_state` — курсоры инкрементальной обработки (`last_log_id`, `last_egmid`).
* `exchangelog_raw` — raw-слой `EXCHANGELOG`.
* `egisz_messages_raw` — подготовленная таблица для raw-слоя `EGISZ_MESSAGES`.
* `dim_organizations` — справочник организаций из `JPERSONS`.
* `dim_licenses` — справочник лицензий и привязок `mo_uid` / `JID`.
* `dim_semd_types` — справочник наименований типов СЭМД из НСИ `1.2.643.5.1.13.13.11.1520`, версия `12.48`; используется для отображения `Тип СЭМД (код · НСИ)` и `Наименование СЭМД`.
* `fact_egisz_transactions` — нормализованные факты обмена. Ошибочные строки содержат три колонки: `error_type` (плоская канонизированная категория), `error_summary` (человекочитаемая сводка) и `error_json_text` (исходные тексты `<message>`).
* `egisz_error_interpretation_rules` — справочник правил классификации (`match_code` + `match_pattern` → `interpretation`, см. раздел «Классификация ошибок»).

Основные представления:

* `v_egisz_transactions_enriched_ui` — главная витрина для Metabase.
* `v_rpt_documents_no_response_ui` — очередь исходящих сообщений без найденного callback; anti-join строится по нормализованным ключам `message_id` / `relates_to_id` / `local_uid_semd`.
* `v_rpt_network_errors_detail_ui` — детализация транспортных и сетевых ошибок.
* `v_health_proxy_db_ui` — техническая сводка raw-слоя и фактов.
* `v_health_signals_ui` — агрегированные health-сигналы. Включает динамический сигнал `data_freshness` (зелёный/жёлтый/красный в зависимости от возраста последнего факта: ≤1 ч, ≤24 ч, >24 ч), который позволяет отслеживать отставание ELT-пайплайна.

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
* `k8s/airflow/airflow-connections-secret.yaml` — Airflow Connections `dwh_egisz_pg` и `proxy_egisz_fb`.
* `k8s/metabase/metabase-connections-secret.yaml` — служебная БД Metabase (`metabase_app`) и BI-доступ к `dwh_egisz`.

Пример локального доступа из контейнеров Docker Desktop:

* PostgreSQL: `host.docker.internal:5432`
* Firebird: `host.docker.internal:3050`, база/alias `proxy_egisz`

### Где расположены компоненты и где задаются параметры

Kubernetes-манифесты в репозитории не задают отдельный namespace. Все команды `kubectl` ниже работают в текущем namespace выбранного Kubernetes-контекста. Если проект развёрнут в отдельный namespace, добавляйте `-n <namespace>` к командам `kubectl`.

| Часть | Где находится runtime / данные | Основной конфиг |
| :--- | :--- | :--- |
| Firebird source `proxy_egisz` | Внешняя БД, не поднимается этим проектом. Для локального Docker Desktop ожидается `host.docker.internal:3050`, база/alias `proxy_egisz`. | `k8s/airflow/airflow-connections-secret.yaml`, ключ `AIRFLOW_CONN_PROXY_EGISZ_FB`. |
| PostgreSQL DWH `dwh_egisz` | Внешняя PostgreSQL-БД, не контейнер этого проекта. Airflow пишет пользователем `egisz`, Metabase читает BI-пользователем `postgres`. | Airflow: `k8s/airflow/airflow-connections-secret.yaml`, ключ `AIRFLOW_CONN_DWH_EGISZ_PG`. Metabase: `k8s/metabase/metabase-connections-secret.yaml`, ключи `DWH_DB_*` и `DWH_BI_*`. Bootstrap прав доступа в `up.ps1` читает `EGISZ_PG_*`, `EGISZ_DWH_*`. |
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

Остановить Airflow release полностью:

```powershell
helm uninstall airflow
```

После `helm uninstall airflow` служебные PVC Kubernetes могут остаться в кластере. Если нужно начать Airflow metadata DB с нуля, отдельно проверьте и удалите PVC Airflow PostgreSQL:

```powershell
kubectl get pvc
kubectl delete pvc <airflow-postgresql-pvc-name>
```

Остановить Metabase полностью из Kubernetes:

```powershell
kubectl delete deployment/metabase service/metabase
```

## Каталоги репозитория

* `airflow/dags/` — DAG `egisz_elt_dag.py`.
* `src/egisz_elt/` — Python-клиенты Firebird/PostgreSQL и загрузочная логика.
* `src/egisz_elt/sql/` — DWH schema, функции парсинга и представления.
* `metabase/` — Dockerfile, entrypoint и provisioning-скрипты Metabase.
* `metabase_dashboards/` — JSON-дашборды.
* `k8s/` — Kubernetes manifests и Helm values.
* `up.ps1` — сборка образов и установка в чистый Kubernetes.
