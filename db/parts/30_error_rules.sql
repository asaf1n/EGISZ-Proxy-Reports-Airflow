-- ============================================================================
-- 30_error_rules.sql — egisz_error_interpretation_rules table + seed
-- Source: db/dwh_init.sql, lines [536..650).
-- Loaded by db/dwh_init.sql via \i db/parts/30_error_rules.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE TABLE IF NOT EXISTS egisz_error_interpretation_rules (
    rule_code text PRIMARY KEY,
    priority integer NOT NULL,
    match_code text,
    match_pattern text NOT NULL,
    interpretation text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE egisz_error_interpretation_rules
    ADD COLUMN IF NOT EXISTS error_category text NOT NULL DEFAULT 'Прочие';

INSERT INTO egisz_error_interpretation_rules (rule_code, priority, match_code, match_pattern, interpretation, error_category)
VALUES
    ('schematron_patient_address_type', 10, 'VALIDATION_ERROR', '(?is)(Schematron|схематрон).*patientRole.*addr.*address:Type', 'Не указан адрес пациента', 'Данные пациента'),
    ('schematron_org_not_linked_rmis', 11, 'VALIDATION_ERROR', '(?is)не привязана к РМИС', 'Организация не привязана к РМИС', 'Ошибки структуры и валидации'),
    ('schematron_telecom_missing', 12, 'VALIDATION_ERROR', '(?is)(telecom).*(не пустым значением|@value)|Ошибка заполнения номера телефона', 'Некорректно заполнен телефон', 'Ошибки структуры и валидации'),
    ('xsd_validation', 20, NULL, '(?is)(\bcvc-|XML_VALIDATION_ERROR|xsd|Invalid content was found|not complete|not valid)', 'Ошибка XSD-валидации XML', 'Ошибки структуры и валидации'),
    ('document_already_registered', 25, 'NOT_UNIQUE_PROVIDED_ID', '(?is).*', 'Документ уже зарегистрирован в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('patient_data_gip', 26, 'PATIENT_MPI_MISMATCH', '(?is).*', 'Данные пациента не соответствуют ГИП', 'Данные пациента'),
    ('doctor_position_frmr', 27, 'PERSON_POST_IN_FRMR_MISMATCH', '(?is).*', 'Должность врача не соответствует данным ФРМР', 'Данные медработника'),
    ('person_not_found_frmr', 28, 'PERSON_NOT_FOUND', '(?is).*', 'Медработник не найден в ФРМР', 'Данные медработника'),
    ('staff_data_frmr', 29, 'VALUE_MISMATCH_METADATA_AND_FRMR', '(?is).*', 'Данные медработника не соответствуют ФРМР', 'Данные медработника'),
    ('signature_metadata_certificate', 30, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    ('signature_metadata_certificate_mismatch', 31, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is).*', 'Данные подписи не соответствуют данным документа', 'Ошибки ЭП и сертификатов'),
    ('nsi_dictionary_version', 32, 'INVALID_DICTIONARY_OID', '(?is).*', 'Неактуальная версия справочника НСИ', 'Ошибки справочника НСИ'),
    ('nsi_dictionary_code', 33, 'INVALID_ELEMENT_VALUE_CODE', '(?is).*', 'Код отсутствует в справочнике НСИ', 'Ошибки справочника НСИ'),
    ('nsi_dictionary_name', 34, 'INVALID_ELEMENT_VALUE_NAME', '(?is).*', 'Наименование не соответствует справочнику НСИ', 'Ошибки справочника НСИ'),
    ('nsi_dictionary_value', 35, NULL, '(?is)(Справочник OID|codeSystem|codeSystemVersion|верси[яи].*справочник|значени[ея].*НСИ|не соответствует наименованию элемента в НСИ|справочн.*значен)', 'Ошибка справочника НСИ', 'Ошибки справочника НСИ'),
    ('rmis_registration_disabled', 40, 'DISABLED_RMIS', '(?is).*', 'ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС', 'Ошибки организации / ИС'),
    ('rmis_registration_missing', 41, 'NO_RMIS', '(?is).*', 'ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные', 'Ошибки организации / ИС'),
    ('document_metadata_mismatch', 50, 'ATTRIBUTE_MISMATCH', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('document_provider_unavailable', 51, 'MIS_NOT_AVAILABLE', '(?is).*', 'Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile', 'Ошибки получения файла ЭМД'),
    ('document_registry_item_missing', 52, 'REGISTRY_ITEM_NOT_FOUND', '(?is).*', 'Запрашиваемая запись ЭМД не найдена в предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('document_file_not_sent', 53, 'FILE_WAS_NOT_SENT', '(?is).*', 'ИС не передала файл ЭМД в ответе getDocumentFile', 'Ошибки получения файла ЭМД'),
    ('document_provider_response_error', 54, 'RMIS_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('document_file_get_error', 55, 'GET_DOCUMENT_FILE_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('document_file_runtime_error', 56, NULL, '(?is)(getDocumentFile|получения файла ЭМД|файлового хранилища)', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('signature_certificate_chain', 60, NULL, '(?is)(CANT_BUILD_CERT_CHAIN|цепочк.*сертификат|аккредитованн.*УЦ)', 'Недействительный сертификат подписи', 'Ошибки ЭП и сертификатов'),
    ('signature_doc_date_mismatch', 61, NULL, '(?is)(DOC_DATE_MISMATCH_CERT_NOT_BEFORE|сертификат.*не действителен.*дат[уы] создания)', 'Сертификат подписи недействителен на дату создания документа', 'Ошибки ЭП и сертификатов'),
    ('signature_verification_error', 62, 'SIGNATURE_VERIFICATION_ERROR', '(?is).*', 'Не удалось проверить электронную подпись', 'Ошибки ЭП и сертификатов'),
    ('person_snils', 70, NULL, '(?is)(СНИЛС|SNILS)', 'СНИЛС не найден или не соответствует данным пациента/медработника', 'Данные пациента'),
    ('doctor_position_frmr_text', 71, NULL, '(?is)(ФРМР|FRMR).*(должност|specialit|специальност)|(должност|specialit|специальност).*(ФРМР|FRMR)|(должност|specialit|специальност).*(не соответств|не совпад|не найден)', 'Должность врача не соответствует данным ФРМР', 'Данные медработника'),
    ('patient_data_gip_text', 72, NULL, '(?is)(ГИП|GIP).*(пациент|patient)|(пациент|patient).*(ГИП|GIP)|(данн|сведени).*(пациент|patient).*(не соответств|не совпад|не найден)', 'Данные пациента не соответствуют ГИП', 'Данные пациента'),
    ('person_frmr', 73, NULL, '(?is)(ФРМР|медработник|автор|author)', 'Данные медработника не соответствуют ФРМР', 'Данные медработника'),
    ('recipient_mismatch', 74, 'RECIPIENT_INFO_MISMATCH', '(?is).*', 'Получатель из запроса не найден в СЭМД', 'Данные пациента'),
    ('document_kind_not_actual', 75, 'NO_DOCUMENT_KIND_ON_DATE', '(?is).*', 'Вид документа не актуален на дату создания', 'Ошибки регистрации в РЭМД'),
    ('object_not_found', 76, 'OBJECT_NOT_FOUND', '(?is).*', 'Подразделение или запись справочника не найдены на дату документа', 'Ошибки регистрации в РЭМД'),
    ('doctor_patronymic_mismatch', 77, 'INVALID_DOCTOR_PATRONYMIC', '(?is).*', 'Отчество врача не соответствует данным СЭМД', 'Данные медработника'),
    ('runtime_request_processing', 79, 'RUNTIME_ERROR', '(?is)Невозможно обработать запрос', 'РЭМД не смог обработать запрос', 'Технические ошибки РЭМД'),
    ('remd_internal', 80, NULL, '(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб)', 'Техническая ошибка на стороне РЭМД', 'Технические ошибки РЭМД'),
    -- Schematron VALIDATION_ERROR — уточнённые паттерны по полям CDA
    ('schematron_author_specialty', 13, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*code.*codeSystem|assignedAuthor.*specialit|специальност.*автор|автор.*специальност)', 'Специальность врача не соответствует справочнику НСИ', 'Данные медработника'),
    ('schematron_author_snils', 14, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*(SNILS|СНИЛС|snils)|author.*(СНИЛС|snils))', 'СНИЛС автора (врача) не заполнен или некорректен', 'Данные медработника'),
    ('schematron_patient_birth', 15, 'VALIDATION_ERROR', '(?is)(patientRole.*birthTime|birthTime.*patient)', 'Дата рождения пациента не заполнена или некорректна', 'Данные пациента'),
    ('schematron_patient_name', 16, 'VALIDATION_ERROR', '(?is)(patientRole.*(name|given|family)|(given|family).*patientRole)', 'ФИО пациента не заполнено или некорректно', 'Данные пациента'),
    ('schematron_patient_snils', 17, 'VALIDATION_ERROR', '(?is)(patientRole.*(SNILS|СНИЛС)|patient.*(SNILS|СНИЛС))', 'СНИЛС пациента не заполнен или некорректен', 'Данные пациента'),
    ('schematron_legal_auth', 18, 'VALIDATION_ERROR', '(?is)legalAuthenticator', 'Данные заверителя документа не заполнены или некорректны', 'Ошибки структуры и валидации'),
    ('schematron_creation_time', 19, 'VALIDATION_ERROR', '(?is)(creationTime.*(не заполнен|некорректн|не указан|обязател))', 'Дата/время создания документа не заполнены или некорректны', 'Ошибки структуры и валидации'),
    ('schematron_doc_code', 21, 'VALIDATION_ERROR', '(?is)(ClinicalDocument/code|тип документа.*(справочник|OID|codeSystem))', 'Код типа документа не соответствует справочнику НСИ', 'Ошибки структуры и валидации'),
    ('schematron_custodian', 22, 'VALIDATION_ERROR', '(?is)(custodian|representedCustodianOrganization)', 'Данные хранителя документа не заполнены', 'Ошибки структуры и валидации'),
    ('schematron_org_repr', 23, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*representedOrganization|representedOrganization.*author)', 'Данные организации автора документа не заполнены', 'Данные медработника'),
    -- Ошибки регистрации/поиска документов в РЭМД
    ('document_not_found_remd', 36, 'DOCUMENT_NOT_FOUND', '(?is).*', 'Документ не найден в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('invalid_emdr_id', 37, 'INVALID_EMDR_ID', '(?is).*', 'Неверный идентификатор документа РЭМД', 'Ошибки регистрации в РЭМД'),
    ('organization_not_found', 38, 'ORGANIZATION_NOT_FOUND', '(?is).*', 'Организация не найдена в реестре РЭМД', 'Ошибки организации / ИС'),
    ('access_denied_remd', 39, 'ACCESS_DENIED', '(?is).*', 'Доступ к операции запрещён в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('duplicate_request', 42, 'DUPLICATE_REQUEST', '(?is).*', 'Дублирующий запрос', 'Ошибки регистрации в РЭМД'),
    ('unsupported_document_type', 43, 'UNSUPPORTED_DOCUMENT_TYPE', '(?is).*', 'Неподдерживаемый тип СЭМД в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('invalid_request_format', 44, 'INVALID_REQUEST_FORMAT', '(?is).*', 'Неверный формат запроса', 'Ошибки регистрации в РЭМД'),
    ('organization_license_not_found', 45, 'ORGANIZATION_LICENSE_NOT_FOUND', '(?is).*', 'Лицензия организации не найдена', 'Ошибки организации / ИС'),
    ('invalid_snils_code', 46, 'INVALID_SNILS', '(?is).*', 'Неверный формат или контрольная сумма СНИЛС', 'Данные пациента'),
    ('organization_not_registered', 47, 'ORGANIZATION_NOT_REGISTERED', '(?is).*', 'Организация не зарегистрирована в РЭМД', 'Ошибки организации / ИС'),
    -- Ошибки сертификата и подписи
    ('certificate_expired', 57, NULL, '(?is)(сертификат.*истёк|истекш.*сертификат|срок.*действи.*сертификат.*истёк|certificate.*expired)', 'Сертификат ЭП истёк', 'Ошибки ЭП и сертификатов'),
    ('certificate_revoked', 58, NULL, '(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)', 'Сертификат ЭП отозван', 'Ошибки ЭП и сертификатов'),
    ('crl_unavailable', 63, NULL, '(?is)(CRL|список.*отозванн|OCSP|сервис.*проверк.*сертификат)', 'Недоступен сервис проверки статуса сертификата (CRL/OCSP)', 'Ошибки ЭП и сертификатов'),
    -- Таймаут и УЦ (code-based, дополнение к уже существующим)
    ('async_response_timeout_code', 64, 'ASYNC_RESPONSE_TIMEOUT', '(?is).*', 'Таймаут асинхронной обработки на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('ca_unavailable_code', 65, 'CA_UNAVAILABLE', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('ca_inaccessibility_code', 66, 'CA_INACCESSIBILITY', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД', 'Технические ошибки РЭМД'),
    -- Аннулирование, текстовые паттерны
    ('document_revoked_text', 67, NULL, '(?is)(аннулирован.*документ|документ.*аннулирован)', 'Документ аннулирован', 'Ошибки регистрации в РЭМД'),
    ('xml_parse_error', 68, NULL, '(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)', 'Ошибка разбора XML-структуры документа', 'Ошибки структуры и валидации'),
    ('snils_invalid_text', 69, NULL, '(?is)(СНИЛС.*неверн|неверн.*СНИЛС|СНИЛС.*контрольн|контрольн.*СНИЛС)', 'Неверный формат или контрольная сумма СНИЛС', 'Данные пациента'),
    ('transport_network', 90, NULL, '(?is)(network|connection|transport|timeout|timed out|соединени|таймаут|сетевая ошибка)', 'Сетевая ошибка', 'Ошибки связи'),
    -- Additional canonical mappings to suppress raw-text leakage in error_type
    ('cvc_datatype_extended', 24, NULL, '(?is)cvc-datatype-valid|cvc-pattern-valid|cvc-type|cvc-complex-type|cvc-attribute|cvc-elt|cvc-identity-constraint|cvc-particle|cvc-enumeration-valid', 'Ошибка XSD-валидации XML', 'Ошибки структуры и валидации'),
    ('attribute_not_found_code', 50, 'ATTRIBUTE_NOT_FOUND', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('role_occurrence_mismatch_code', 31, 'ROLE_OCCURRENCE_MISMATCH', '(?is).*', 'Подпись роли не соответствует требованиям РЭМД', 'Ошибки ЭП и сертификатов'),
    ('object_not_found_text_extra', 76, NULL, '(?is)Подразделение.*(идентификатор|не найден)|подразделение.*не найден', 'Подразделение или запись справочника не найдены на дату документа', 'Ошибки регистрации в РЭМД'),
    ('recipient_text_extra', 74, NULL, '(?is)RECIPIENT_INFO_MISMATCH|Получатель.*не найден', 'Получатель из запроса не найден в СЭМД', 'Данные пациента'),
    ('dul_patient_text', 78, NULL, '(?is)ДУЛ[^А-Яа-я]|реквизит.*удостоверени', 'Документ, удостоверяющий личность пациента: некорректные реквизиты', 'Данные пациента'),
    ('patient_birth_text', 15, NULL, '(?is)Дата рождения пациента|birthTime', 'Дата рождения пациента не заполнена или некорректна', 'Данные пациента'),
    ('remd_runtime_internal', 80, NULL, '(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб|невозможно обработать)', 'Техническая ошибка на стороне РЭМД', 'Технические ошибки РЭМД'),
    -- Сертификат организации: специальный case для распознанного кода РЭМД
    ('cert_org_validity_expired', 56, 'CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', '(?is).*', 'Срок действия сертификата организации истек', 'Ошибки ЭП и сертификатов'),
    -- Несоответствие данных организации в ФРМО (ОГРН и подобные)
    ('org_ogrn_frmo_mismatch', 11, NULL, '(?is)(ОГРН|ОКПО|КПП|ИНН).*(СЭМД|ФРМО).*(не совпада|не соответств)|ОГРН МО.*не совпада|ФРМО.*(не совпада|не соответств).*организац', 'Несоответствие данных организации в ФРМО', 'Ошибки организации / ИС'),
    -- Generic fallback для прочих организационных ошибок
    ('org_generic_fallback', 95, NULL, '(?is)(организаци|ОГРН|ФРМО|лицензи)', 'Ошибки организации', 'Ошибки организации / ИС')
ON CONFLICT (rule_code) DO UPDATE SET
    priority = EXCLUDED.priority,
    match_code = EXCLUDED.match_code,
    match_pattern = EXCLUDED.match_pattern,
    interpretation = EXCLUDED.interpretation,
    error_category = EXCLUDED.error_category,
    is_active = true,
    updated_at = now();

-- Деактивируем generic-фолбэк, который раньше отдавал «Ошибка регистрации в РЭМД».
-- При отсутствии конкретного типа теперь подставляется «Неизвестная ошибка»
-- в egisz_error_classify (см. ниже).
UPDATE egisz_error_interpretation_rules
SET is_active = false, updated_at = now()
WHERE rule_code = 'remd_async_response';

