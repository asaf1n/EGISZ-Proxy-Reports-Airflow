-- ============================================================================
-- 30_error_rules.sql — dim_error_rules table + seed
-- Loaded by db/dwh_init.sql via \i db/parts/30_error_rules.sql.
-- Идемпотентный DDL: CREATE ... IF NOT EXISTS, CREATE OR REPLACE, ALTER ... IF EXISTS.
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_error_rules (
    rule_code text PRIMARY KEY,
    match_tier integer NOT NULL DEFAULT 3,
    match_code text,
    match_pattern text NOT NULL,
    interpretation text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS error_category text NOT NULL DEFAULT 'Прочие';

-- Ярусный матчинг: правила проверяются по возрастанию match_tier,
-- первый ярус с совпадением закрывает поиск (несколько совпадений внутри яруса легальны).
ALTER TABLE dim_error_rules
    ADD COLUMN IF NOT EXISTS match_tier integer NOT NULL DEFAULT 3;

COMMENT ON COLUMN dim_error_rules.match_tier IS
'Ярус матчинга: 1 — код + специфичный текст; 2 — только код (match_pattern = ''(?is).*''); 3 — специфичный текст без кода; 4 — широкий текстовый фолбэк. Первый ярус с совпадением побеждает.';

INSERT INTO dim_error_rules (rule_code, match_tier, match_code, match_pattern, interpretation, error_category)
VALUES
    -- ------------------------------------------------------------------
    -- Ярус 1: код + специфичный текстовый паттерн
    -- ------------------------------------------------------------------
    ('schematron_patient_address_type', 1, 'VALIDATION_ERROR', '(?is)(Schematron|схематрон).*patientRole.*addr.*address:Type', 'Не указан адрес пациента', 'Данные пациента'),
    ('schematron_patient_addr_generic', 1, 'VALIDATION_ERROR', '(?is)patientRole.*addr', 'Не указан адрес пациента', 'Данные пациента'),
    ('schematron_org_not_linked_rmis', 1, 'VALIDATION_ERROR', '(?is)не привязана к РМИС', 'Организация не привязана к РМИС', 'Ошибки структуры и валидации'),
    ('schematron_telecom_missing', 1, 'VALIDATION_ERROR', '(?is)(telecom).*(не пустым значением|@value)|Ошибка заполнения номера телефона', 'Некорректно заполнен телефон', 'Ошибки структуры и валидации'),
    ('schematron_author_specialty', 1, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*code.*codeSystem|assignedAuthor.*specialit|специальност.*автор|автор.*специальност)', 'Специальность врача не соответствует справочнику НСИ', 'Данные медработника'),
    ('schematron_author_snils', 1, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*(SNILS|СНИЛС|snils)|author.*(СНИЛС|snils))', 'СНИЛС автора (врача) не заполнен или некорректен', 'Данные медработника'),
    ('schematron_patient_birth', 1, 'VALIDATION_ERROR', '(?is)(patientRole.*birthTime|birthTime.*patient)', 'Дата рождения пациента не заполнена или некорректна', 'Данные пациента'),
    ('schematron_patient_name', 1, 'VALIDATION_ERROR', '(?is)(patientRole.*(name|given|family)|(given|family).*patientRole)', 'ФИО пациента не заполнено или некорректно', 'Данные пациента'),
    ('schematron_patient_snils', 1, 'VALIDATION_ERROR', '(?is)(patientRole.*(SNILS|СНИЛС)|patient.*(SNILS|СНИЛС))', 'СНИЛС пациента не заполнен или некорректен', 'Данные пациента'),
    ('schematron_legal_auth', 1, 'VALIDATION_ERROR', '(?is)legalAuthenticator', 'Данные заверителя документа не заполнены или некорректны', 'Ошибки структуры и валидации'),
    ('schematron_creation_time', 1, 'VALIDATION_ERROR', '(?is)(creationTime.*(не заполнен|некорректн|не указан|обязател))', 'Дата/время создания документа не заполнены или некорректны', 'Ошибки структуры и валидации'),
    ('schematron_doc_code', 1, 'VALIDATION_ERROR', '(?is)(ClinicalDocument/code|тип документа.*(справочник|OID|codeSystem))', 'Код типа документа не соответствует справочнику НСИ', 'Ошибки структуры и валидации'),
    ('schematron_custodian', 1, 'VALIDATION_ERROR', '(?is)(custodian|representedCustodianOrganization)', 'Данные хранителя документа не заполнены', 'Ошибки структуры и валидации'),
    ('schematron_org_repr', 1, 'VALIDATION_ERROR', '(?is)(assignedAuthor.*representedOrganization|representedOrganization.*author)', 'Данные организации автора документа не заполнены', 'Данные медработника'),
    -- Кросс-валидация СНИЛС пациента запрос↔СЭМД: значение в [скобках] уникализирует
    -- сообщение, без правила текст утекал в широкий фолбэк person_snils.
    ('patient_snils_mismatch_request', 1, 'VALIDATION_ERROR', '(?is)СНИЛС пациента в ЭМД \[.*?\] отличается', 'СНИЛС пациента не заполнен или некорректен', 'Данные пациента'),
    ('signature_metadata_certificate', 1, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is)не найдена актуальная.*карточка МР', 'Подписант из сертификата не найден в ФРМР', 'Данные медработника'),
    ('runtime_request_processing', 1, 'RUNTIME_ERROR', '(?is)Невозможно обработать запрос', 'РЭМД не смог обработать запрос', 'Технические ошибки РЭМД'),
    -- «Не удается провести проверку ФРМР» — недоступность проверки на стороне РЭМД,
    -- а не несоответствие данных; без правила текст ловил фолбэк person_frmr.
    ('runtime_frmr_check_unavailable', 1, 'RUNTIME_ERROR', '(?is)провер.*ФРМР', 'РЭМД не смог обработать запрос', 'Технические ошибки РЭМД'),
    -- «Не удается произвести проверку в ГИП» и подобные: недоступность проверяющей
    -- системы, без правила текст давал фолбэк-строку с суффиксом «повторите отправку».
    ('runtime_check_unavailable', 1, 'RUNTIME_ERROR', '(?is)Не уда(е|ё)тся про(из)?вести проверку', 'РЭМД не смог обработать запрос', 'Технические ошибки РЭМД'),
    -- ГИП отклоняет ФИО со скобками/звёздочками («Шкильная (полякова)»).
    ('validation_invalid_name_chars', 1, 'VALIDATION_ERROR', '(?is)Недопустимые символы в имени', 'ФИО пациента не заполнено или некорректно', 'Данные пациента'),
    -- Живой текст CRE-126: «Association [RPLC] targetId with unique ID [...] not found
    -- in repository» — замена версии отклонена, заменяемый документ отсутствует.
    -- Ярус-2 генерик xds_document_unique_id_code остаётся для не-RPLC проявлений кода.
    ('xds_document_unique_id_rplc', 1, 'XDSDOCUMENTUNIQUEIDERROR', '(?is)\yRPLC\y|targetId.*not found', 'ИЭМК: заменяемый документ не найден (замена версии)', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 2: только код (match_pattern = '(?is).*', пустой message допустим)
    -- ------------------------------------------------------------------
    ('document_already_registered', 2, 'NOT_UNIQUE_PROVIDED_ID', '(?is).*', 'Документ уже зарегистрирован в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('patient_data_gip', 2, 'PATIENT_MPI_MISMATCH', '(?is).*', 'Данные пациента не соответствуют ГИП', 'Данные пациента'),
    ('doctor_position_frmr', 2, 'PERSON_POST_IN_FRMR_MISMATCH', '(?is).*', 'Должность врача не соответствует данным ФРМР', 'Данные медработника'),
    ('person_not_found_frmr', 2, 'PERSON_NOT_FOUND', '(?is).*', 'Медработник не найден в ФРМР', 'Данные медработника'),
    ('staff_data_frmr', 2, 'VALUE_MISMATCH_METADATA_AND_FRMR', '(?is).*', 'Данные медработника не соответствуют ФРМР', 'Данные медработника'),
    ('signature_metadata_certificate_mismatch', 2, 'VALUE_MISMATCH_METADATA_AND_CERTIFICATE', '(?is).*', 'Данные подписи не соответствуют данным документа', 'Ошибки ЭП и сертификатов'),
    ('nsi_dictionary_version', 2, 'INVALID_DICTIONARY_OID', '(?is).*', 'Неактуальная версия справочника НСИ', 'Ошибки справочника НСИ'),
    ('invalid_dictionary_version_code', 2, 'INVALID_DICTIONARY_VERSION', '(?is).*', 'Неактуальная версия справочника НСИ', 'Ошибки справочника НСИ'),
    ('nsi_dictionary_code', 2, 'INVALID_ELEMENT_VALUE_CODE', '(?is).*', 'Код отсутствует в справочнике НСИ', 'Ошибки справочника НСИ'),
    ('nsi_dictionary_name', 2, 'INVALID_ELEMENT_VALUE_NAME', '(?is).*', 'Наименование не соответствует справочнику НСИ', 'Ошибки справочника НСИ'),
    ('rmis_registration_disabled', 2, 'DISABLED_RMIS', '(?is).*', 'ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС', 'Ошибки организации / ИС'),
    ('rmis_registration_missing', 2, 'NO_RMIS', '(?is).*', 'ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные', 'Ошибки организации / ИС'),
    ('document_metadata_mismatch', 2, 'ATTRIBUTE_MISMATCH', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('attribute_not_found_code', 2, 'ATTRIBUTE_NOT_FOUND', '(?is).*', 'Метаописание документа не соответствует зарегистрированному в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('document_provider_unavailable', 2, 'MIS_NOT_AVAILABLE', '(?is).*', 'Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile', 'Ошибки получения файла ЭМД'),
    ('document_registry_item_missing', 2, 'REGISTRY_ITEM_NOT_FOUND', '(?is).*', 'Запрашиваемая запись ЭМД не найдена в предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('document_file_not_sent', 2, 'FILE_WAS_NOT_SENT', '(?is).*', 'ИС не передала файл ЭМД в ответе getDocumentFile', 'Ошибки получения файла ЭМД'),
    ('document_provider_response_error', 2, 'RMIS_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('document_file_get_error', 2, 'GET_DOCUMENT_FILE_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('signature_verification_error', 2, 'SIGNATURE_VERIFICATION_ERROR', '(?is).*', 'Не удалось проверить электронную подпись', 'Ошибки ЭП и сертификатов'),
    ('recipient_mismatch', 2, 'RECIPIENT_INFO_MISMATCH', '(?is).*', 'Получатель из запроса не найден в СЭМД', 'Данные пациента'),
    ('document_kind_not_actual', 2, 'NO_DOCUMENT_KIND_ON_DATE', '(?is).*', 'Вид документа не актуален на дату создания', 'Ошибки регистрации в РЭМД'),
    ('object_not_found', 2, 'OBJECT_NOT_FOUND', '(?is).*', 'Подразделение или запись справочника не найдены на дату документа', 'Ошибки регистрации в РЭМД'),
    ('doctor_patronymic_mismatch', 2, 'INVALID_DOCTOR_PATRONYMIC', '(?is).*', 'Отчество врача не соответствует данным СЭМД', 'Данные медработника'),
    ('document_not_found_remd', 2, 'DOCUMENT_NOT_FOUND', '(?is).*', 'Документ не найден в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('invalid_emdr_id', 2, 'INVALID_EMDR_ID', '(?is).*', 'Неверный идентификатор документа РЭМД', 'Ошибки регистрации в РЭМД'),
    ('organization_not_found', 2, 'ORGANIZATION_NOT_FOUND', '(?is).*', 'Организация не найдена в реестре РЭМД', 'Ошибки организации / ИС'),
    ('access_denied_remd', 2, 'ACCESS_DENIED', '(?is).*', 'Доступ к операции запрещён в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('duplicate_request', 2, 'DUPLICATE_REQUEST', '(?is).*', 'Дублирующий запрос', 'Ошибки регистрации в РЭМД'),
    ('unsupported_document_type', 2, 'UNSUPPORTED_DOCUMENT_TYPE', '(?is).*', 'Неподдерживаемый тип СЭМД в РЭМД', 'Ошибки регистрации в РЭМД'),
    ('invalid_request_format', 2, 'INVALID_REQUEST_FORMAT', '(?is).*', 'Неверный формат запроса', 'Ошибки регистрации в РЭМД'),
    ('organization_license_not_found', 2, 'ORGANIZATION_LICENSE_NOT_FOUND', '(?is).*', 'Лицензия организации не найдена', 'Ошибки организации / ИС'),
    ('invalid_snils_code', 2, 'INVALID_SNILS', '(?is).*', 'Неверный формат или контрольная сумма СНИЛС', 'Данные пациента'),
    ('no_snils_code', 2, 'NO_SNILS', '(?is).*', 'СНИЛС пациента обязателен для данного вида документов', 'Данные пациента'),
    ('organization_not_registered', 2, 'ORGANIZATION_NOT_REGISTERED', '(?is).*', 'Организация не зарегистрирована в РЭМД', 'Ошибки организации / ИС'),
    ('async_response_timeout_code', 2, 'ASYNC_RESPONSE_TIMEOUT', '(?is).*', 'Таймаут асинхронной обработки на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('ca_unavailable_code', 2, 'CA_UNAVAILABLE', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('ca_inaccessibility_code', 2, 'CA_INACCESSIBILITY', '(?is).*', 'Недоступен сервис проверки подписи (УЦ) на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('role_occurrence_mismatch_code', 2, 'ROLE_OCCURRENCE_MISMATCH', '(?is).*', 'Подпись роли не соответствует требованиям РЭМД', 'Ошибки ЭП и сертификатов'),
    ('cert_org_validity_expired', 2, 'CANT_BUILD_CERT_CHAIN_TO_ACCREDITED_CA_CERT', '(?is).*', 'Срок действия сертификата организации истек', 'Ошибки ЭП и сертификатов'),
    ('xml_validation_error_code', 2, 'XML_VALIDATION_ERROR', '(?is).*', 'Ошибка XSD-валидации XML', 'Ошибки структуры и валидации'),
    ('restrict_new_version_code', 2, 'RESTRICT_NEW_VERSION', '(?is).*', 'Для данного вида ЭМД запрещена регистрация новых версий', 'Ошибки регистрации в РЭМД'),
    -- Defensive-покрытие федерального классификатора РЭМД (НСИ OID
    -- 1.2.643.5.1.13.13.99.2.305): коды регистрационного пути, ещё не встречавшиеся
    -- или утекавшие в текстовые фолбэки. RUNTIME_ERROR ярусом 2 сознательно НЕ покрыт:
    -- blanket-правило закрыло бы ярусы 3–4, уточняющие его тексты.
    ('invalid_content_code', 2, 'INVALID_CONTENT', '(?is).*', 'Формат файла не соответствует требованиям вида документа', 'Ошибки структуры и валидации'),
    ('invalid_doc_content_type_code', 2, 'INVALID_DOC_CONTENT_TYPE', '(?is).*', 'Формат файла не соответствует требованиям вида документа', 'Ошибки структуры и валидации'),
    ('invalid_pluggable_attrs_code', 2, 'INVALID_PLUGGABLE_ATTRS', '(?is).*', 'Дополнительные атрибуты документа не прошли валидацию', 'Ошибки структуры и валидации'),
    ('pluggable_attrs_occurrence_code', 2, 'PLUGGABLE_ATTRS_OCCURRENCE_MISMATCH', '(?is).*', 'Дополнительные атрибуты документа не прошли валидацию', 'Ошибки структуры и валидации'),
    ('no_signature_code', 2, 'NO_SIGNATURE', '(?is).*', 'Электронная подпись отсутствует', 'Ошибки ЭП и сертификатов'),
    ('signature_duplication_code', 2, 'SIGNATURE_DUPLICATION', '(?is).*', 'Дублирование подписи в документе', 'Ошибки ЭП и сертификатов'),
    ('signature_decoding_error_code', 2, 'SIGNATURE_DECODING_ERROR', '(?is).*', 'Не удалось декодировать электронную подпись', 'Ошибки ЭП и сертификатов'),
    ('digest_mismatch_code', 2, 'DIGEST_MISMATCH', '(?is).*', 'Подпись не соответствует содержимому документа', 'Ошибки ЭП и сертификатов'),
    ('inconsistent_digests_code', 2, 'INCONSISTENT_DIGESTS', '(?is).*', 'Подпись не соответствует содержимому документа', 'Ошибки ЭП и сертификатов'),
    ('unknown_algorithm_code', 2, 'UNKNOWN_ALGORITHM', '(?is).*', 'Неподдерживаемый алгоритм электронной подписи', 'Ошибки ЭП и сертификатов'),
    ('wrong_signature_format_code', 2, 'WRONG_SIGNATURE_FORMAT', '(?is).*', 'Неподдерживаемый формат электронной подписи', 'Ошибки ЭП и сертификатов'),
    ('no_end_entity_certificate_code', 2, 'NO_END_ENTITY_CERTIFICATE', '(?is).*', 'В подписи отсутствует сертификат подписанта', 'Ошибки ЭП и сертификатов'),
    ('invalid_cert_key_usage_code', 2, 'INVALID_CERT_KEY_USAGE', '(?is).*', 'Сертификат не предназначен для подписания документов', 'Ошибки ЭП и сертификатов'),
    ('doc_date_cert_not_after_code', 2, 'DOC_DATE_MISMATCH_CERT_NOT_AFTER', '(?is).*', 'Сертификат подписи недействителен на дату создания документа', 'Ошибки ЭП и сертификатов'),
    ('doc_date_cert_not_before_code', 2, 'DOC_DATE_MISMATCH_CERT_NOT_BEFORE', '(?is).*', 'Сертификат подписи недействителен на дату создания документа', 'Ошибки ЭП и сертификатов'),
    ('multiple_signers_code', 2, 'MULTIPLE_SIGNERS', '(?is).*', 'В контейнере подписи более одного подписанта', 'Ошибки ЭП и сертификатов'),
    ('org_signature_occurrence_code', 2, 'ORG_SIGNATURE_OCCURRENCE_MISMATCH', '(?is).*', 'Подпись организации не соответствует требованиям вида документа', 'Ошибки ЭП и сертификатов'),
    ('no_role_policy_on_date_code', 2, 'NO_ROLE_POLICY_ON_DATE', '(?is).*', 'Роль подписанта не предусмотрена для вида документа на дату', 'Ошибки регистрации в РЭМД'),
    ('no_speciality_code', 2, 'NO_SPECIALITY', '(?is).*', 'У подписанта не указана специальность', 'Данные медработника'),
    ('position_to_role_mismatch_code', 2, 'POSITION_TO_ROLE_MISMATCH', '(?is).*', 'Должность подписанта не соответствует роли подписи', 'Данные медработника'),
    ('person_card_not_found_code', 2, 'PERSON_CARD_NOT_FOUND', '(?is).*', 'Карточка медработника не найдена в ФРМР', 'Данные медработника'),
    ('signer_org_mismatch_code', 2, 'SIGNER_ORG_MISMATCH', '(?is).*', 'Организация подписанта не соответствует организации документа', 'Данные медработника'),
    ('invalid_doctor_name_code', 2, 'INVALID_DOCTOR_NAME', '(?is).*', 'Имя врача не соответствует данным СЭМД', 'Данные медработника'),
    ('patient_occurrence_mismatch_code', 2, 'PATIENT_OCCURRENCE_MISMATCH', '(?is).*', 'Сведения о пациенте отсутствуют или дублируются в документе', 'Данные пациента'),
    ('patient_not_found_code', 2, 'PATIENT_NOT_FOUND', '(?is).*', 'Пациент не найден в ГИП', 'Данные пациента'),
    ('patient_already_registered_code', 2, 'PATIENT_ALREADY_REGISTERED', '(?is).*', 'Пациент уже зарегистрирован в ГИП', 'Данные пациента'),
    ('patient_creation_error_code', 2, 'PATIENT_CREATION_ERROR', '(?is).*', 'Не удалось создать пациента в ГИП', 'Данные пациента'),
    ('additional_info_required_code', 2, 'ADDITIONAL_INFO_REQUIRED', '(?is).*', 'Требуются дополнительные сведения о пациенте (ГИП)', 'Данные пациента'),
    ('series_required_code', 2, 'SERIES_REQUIRED', '(?is).*', 'Не указана серия документа, удостоверяющего личность', 'Данные пациента'),
    ('aoguid_not_found_code', 2, 'AOGUID_NOT_FOUND', '(?is).*', 'Адрес пациента не найден в ФИАС', 'Данные пациента'),
    ('houseguid_not_found_code', 2, 'HOUSEGUID_NOT_FOUND', '(?is).*', 'Адрес пациента не найден в ФИАС', 'Данные пациента'),
    ('region_code_different_code', 2, 'REGION_CODE_DIFFERENT', '(?is).*', 'Адрес пациента не соответствует ФИАС', 'Данные пациента'),
    ('aoguid_different_code', 2, 'AOGUID_DIFFERENT', '(?is).*', 'Адрес пациента не соответствует ФИАС', 'Данные пациента'),
    ('org_not_found_in_frmo_code', 2, 'ORG_NOT_FOUND_IN_FRMO', '(?is).*', 'Организация не найдена в ФРМО', 'Ошибки организации / ИС'),
    ('no_org_on_date_code', 2, 'NO_ORG_ON_DATE', '(?is).*', 'Организация не действует в ФРМО на дату документа', 'Ошибки организации / ИС'),
    ('no_department_code', 2, 'NO_DEPARTMENT', '(?is).*', 'Подразделение организации не найдено в ФРМО', 'Ошибки организации / ИС'),
    ('rmis_region_mismatch_code', 2, 'RMIS_REGION_MISMATCH', '(?is).*', 'Регион ИС не соответствует региону организации', 'Ошибки организации / ИС'),
    ('wrong_creation_date_code', 2, 'WRONG_CREATION_DATE', '(?is).*', 'Дата создания документа позже даты регистрации', 'Ошибки регистрации в РЭМД'),
    ('cant_reg_version_code', 2, 'CANT_REG_VERSION', '(?is).*', 'Не удалось зарегистрировать новую версию документа', 'Ошибки регистрации в РЭМД'),
    ('can_not_associate_code', 2, 'CAN_NOT_ASSOCIATE', '(?is).*', 'Не удалось связать документ с записью РЭМД', 'Ошибки регистрации в РЭМД'),
    ('not_unique_association_code', 2, 'NOT_UNIQUE_ASSOCIATION', '(?is).*', 'Регистрируемая связь документов уже существует', 'Ошибки регистрации в РЭМД'),
    ('wrong_message_id_code', 2, 'WRONG_MESSAGE_ID', '(?is).*', 'Неверный идентификатор сообщения (messageId)', 'Ошибки регистрации в РЭМД'),
    ('mis_error_code', 2, 'MIS_ERROR', '(?is).*', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    ('valsys_internal_error_code', 2, 'VALSYS_INTERNAL_ERROR', '(?is).*', 'Техническая ошибка на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('internal_error_code', 2, 'INTERNAL_ERROR', '(?is).*', 'Техническая ошибка на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('timeout_code', 2, 'TIMEOUT', '(?is).*', 'Таймаут асинхронной обработки на стороне РЭМД', 'Технические ошибки РЭМД'),
    ('rate_limit_code', 2, 'RATE_LIMIT', '(?is).*', 'Превышен лимит запросов к РЭМД', 'Технические ошибки РЭМД'),
    -- Контур ИЭМК (IHE XDS.b, ProvideAndRegisterDocumentSet-b): коды приходят в атрибуте
    -- errorCode тега RegistryError. match_code хранится UPPERCASE — движок сравнивает
    -- с upper(btrim(code)), смешанный регистр никогда не совпал бы.
    ('xds_dictionary_validation_code', 2, 'XDSDICTIONARYVALIDATIONERROR', '(?is).*', 'ИЭМК: данные не соответствуют справочнику НСИ', 'Ошибки ИЭМК'),
    ('xds_cda_validation_code', 2, 'XDS.CDA.VALIDATIONERROR', '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки ИЭМК'),
    ('xds_duplicate_unique_id_code', 2, 'XDSDUPLICATEUNIQUEIDINREGISTRY', '(?is).*', 'ИЭМК: документ уже зарегистрирован', 'Ошибки ИЭМК'),
    ('xds_patient_registration_code', 2, 'XDSPATIENTREGISTRATIONERROR', '(?is).*', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),
    ('xds_document_unique_id_code', 2, 'XDSDOCUMENTUNIQUEIDERROR', '(?is).*', 'ИЭМК: некорректный идентификатор документа', 'Ошибки ИЭМК'),
    ('xds_repository_error_code', 2, 'XDSREPOSITORYERROR', '(?is).*', 'ИЭМК: внутренняя ошибка репозитория', 'Ошибки ИЭМК'),
    ('xds_cda_processing_code', 2, 'XDS.CDA.PROCESSINGERROR', '(?is).*', 'ИЭМК: ошибка обработки CDA', 'Ошибки ИЭМК'),
    ('xds_replaced_document_org_code', 2, 'XDSREPLACEDDOCUMENTORGANIZATIONERROR', '(?is).*', 'ИЭМК: замена версии отклонена (другая организация)', 'Ошибки ИЭМК'),
    -- Стандартные коды IHE ITI TF-3 (ebRS) регистрационного пути ITI-41/42, ещё не
    -- встречавшиеся в журнале. Query-коды (XDSStoredQuery*) и warning-код
    -- XDSExtraMetadataNotSaved (severity=Warning при Success) не заводим.
    ('xds_registry_error_code', 2, 'XDSREGISTRYERROR', '(?is).*', 'ИЭМК: внутренняя ошибка реестра', 'Ошибки ИЭМК'),
    ('xds_registry_not_available_code', 2, 'XDSREGISTRYNOTAVAILABLE', '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_registry_busy_code', 2, 'XDSREGISTRYBUSY', '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_repository_busy_code', 2, 'XDSREPOSITORYBUSY', '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_registry_out_of_resources_code', 2, 'XDSREGISTRYOUTOFRESOURCES', '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_repository_out_of_resources_code', 2, 'XDSREPOSITORYOUTOFRESOURCES', '(?is).*', 'ИЭМК: сервис временно недоступен', 'Ошибки ИЭМК'),
    ('xds_missing_document_code', 2, 'XDSMISSINGDOCUMENT', '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки ИЭМК'),
    ('xds_missing_document_metadata_code', 2, 'XDSMISSINGDOCUMENTMETADATA', '(?is).*', 'ИЭМК: состав пакета не согласован (документы/метаданные)', 'Ошибки ИЭМК'),
    ('xds_registry_metadata_error_code', 2, 'XDSREGISTRYMETADATAERROR', '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_repository_metadata_error_code', 2, 'XDSREPOSITORYMETADATAERROR', '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_patient_id_does_not_match_code', 2, 'XDSPATIENTIDDOESNOTMATCH', '(?is).*', 'ИЭМК: ошибка метаданных документа', 'Ошибки ИЭМК'),
    ('xds_registry_dup_uid_msg_code', 2, 'XDSREGISTRYDUPLICATEUNIQUEIDINMESSAGE', '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки ИЭМК'),
    ('xds_repository_dup_uid_msg_code', 2, 'XDSREPOSITORYDUPLICATEUNIQUEIDINMESSAGE', '(?is).*', 'ИЭМК: дублирующийся идентификатор в пакете', 'Ошибки ИЭМК'),
    ('xds_non_identical_hash_code', 2, 'XDSNONIDENTICALHASH', '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки ИЭМК'),
    ('xds_non_identical_size_code', 2, 'XDSNONIDENTICALSIZE', '(?is).*', 'ИЭМК: повторная загрузка с изменённым содержимым', 'Ошибки ИЭМК'),
    ('xds_unknown_patient_id_code', 2, 'XDSUNKNOWNPATIENTID', '(?is).*', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),
    ('xds_invalid_document_content_code', 2, 'XDSINVALIDDOCUMENTCONTENT', '(?is).*', 'ИЭМК: ошибка валидации структуры CDA', 'Ошибки ИЭМК'),
    ('xds_registry_deprecated_doc_code', 2, 'XDSREGISTRYDEPRECATEDDOCUMENTERROR', '(?is).*', 'ИЭМК: замена версии отклонена (документ уже заменён)', 'Ошибки ИЭМК'),
    ('xds_unknown_repository_id_code', 2, 'XDSUNKNOWNREPOSITORYID', '(?is).*', 'ИЭМК: неверный идентификатор репозитория', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 3: специфичный текстовый паттерн без кода
    -- ------------------------------------------------------------------
    -- Ядро XSD-диагностик. Граница слова в ARE — \y (\b в PostgreSQL — литеральный
    -- backspace); без границ слова «xsd» ловит подстроки в идентификаторах.
    ('xsd_validation', 3, NULL, '(?is)(\ycvc-|XML_VALIDATION_ERROR|\yxsd\y|Invalid content was found)', 'Ошибка XSD-валидации XML', 'Ошибки структуры и валидации'),
    ('certificate_expired', 3, NULL, '(?is)(сертификат.*истёк|истекш.*сертификат|срок.*действи.*сертификат.*истёк|certificate.*expired)', 'Сертификат ЭП истёк', 'Ошибки ЭП и сертификатов'),
    ('certificate_revoked', 3, NULL, '(?is)(сертификат.*отозван|certificate.*revoked|revoked.*certificate)', 'Сертификат ЭП отозван', 'Ошибки ЭП и сертификатов'),
    ('signature_certificate_chain', 3, NULL, '(?is)(CANT_BUILD_CERT_CHAIN|цепочк.*сертификат|аккредитованн.*УЦ)', 'Недействительный сертификат подписи', 'Ошибки ЭП и сертификатов'),
    -- Живой текст пишется и слитно («Сертификат МО недействителен на дату создания»).
    ('signature_doc_date_mismatch', 3, NULL, '(?is)(DOC_DATE_MISMATCH_CERT_NOT_(BEFORE|AFTER)|сертификат.*не ?действителен.*дат[уы] создания)', 'Сертификат подписи недействителен на дату создания документа', 'Ошибки ЭП и сертификатов'),
    ('document_revoked_text', 3, NULL, '(?is)(аннулирован.*документ|документ.*аннулирован)', 'Документ аннулирован', 'Ошибки регистрации в РЭМД'),
    ('xml_parse_error', 3, NULL, '(?is)(SAXParseException|org\.xml|ParseError|XML.*parse.*error)', 'Ошибка разбора XML-структуры документа', 'Ошибки структуры и валидации'),
    ('object_not_found_text_extra', 3, NULL, '(?is)Подразделение.*(идентификатор|не найден)|подразделение.*не найден', 'Подразделение или запись справочника не найдены на дату документа', 'Ошибки регистрации в РЭМД'),
    ('recipient_text_extra', 3, NULL, '(?is)RECIPIENT_INFO_MISMATCH|Получатель.*не найден', 'Получатель из запроса не найден в СЭМД', 'Данные пациента'),
    ('dul_patient_text', 3, NULL, '(?is)ДУЛ[^А-Яа-я]|реквизит.*удостоверени', 'Документ, удостоверяющий личность пациента: некорректные реквизиты', 'Данные пациента'),
    ('schematron_identity_card', 3, NULL, '(?is)IdentityCardType|identity:IssueDate', 'Документ, удостоверяющий личность пациента: некорректные реквизиты', 'Данные пациента'),
    -- Голый «birthTime» перекрывал schematron_patient_birth (ярус 1) на schematron-текстах.
    ('patient_birth_text', 3, NULL, '(?is)Дата рождения пациента', 'Дата рождения пациента не заполнена или некорректна', 'Данные пациента'),
    ('org_ogrn_frmo_mismatch', 3, NULL, '(?is)(ОГРН|ОКПО|КПП|ИНН).*(СЭМД|ФРМО).*(не совпада|не соответств)|ОГРН МО.*не совпада|ФРМО.*(не совпада|не соответств).*организац', 'Несоответствие данных организации в ФРМО', 'Ошибки организации / ИС'),
    ('patient_fio_mismatch', 3, NULL, '(?is)(Имя|Фамилия|Отчество) пациента в ЭМД \[.*?\] отличается', 'ФИО пациента в ЭМД не соответствует данным ЕГИСЗ', 'Данные пациента'),
    ('patient_gender_mismatch', 3, NULL, '(?is)Пол пациента в ЭМД \[.*?\] отличается', 'Пол пациента в ЭМД не соответствует данным ЕГИСЗ', 'Данные пациента'),
    -- Кросс-валидация RegisterHealthDocument: значения (id ЭМД/запроса, дата, СП)
    -- приходят в [квадратных скобках] и уникализируют сообщение — нормализуем в тип.
    ('document_uid_mismatch_request', 3, NULL, '(?is)Уникальный идентификатор документа в ЭМД \[.*?\] отличается', 'Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('document_creation_date_mismatch_request', 3, NULL, '(?is)Дата создания документа в ЭМД \[.*?\] отличается', 'Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('signature_mo_date_after_request', 3, NULL, '(?is)Дата и время создания подписи МО \[.*?\] не может быть позже', 'Дата подписи МО позже даты поступления запроса на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('provider_org_mismatch_request', 3, NULL, '(?is)не совпадает с СП providerOrganization', 'Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('represented_org_mismatch_request', 3, NULL, '(?is)не совпадает с СП representedOrganization', 'Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию', 'Ошибки регистрации в РЭМД'),
    ('emd_version_registration_forbidden', 3, NULL, '(?is)запрещена регистрация новых версий', 'Для данного вида ЭМД запрещена регистрация новых версий', 'Ошибки регистрации в РЭМД'),
    ('document_kind_not_actual_text', 3, NULL, '(?is)вид документ.*не актуал', 'Вид документа не актуален на дату создания', 'Ошибки регистрации в РЭМД'),
    -- Внутренний код платформы ИЭМК в codeContext («PAT-001; Пациент не определен»):
    -- подстраховка на случай RegistryError без атрибута errorCode.
    ('xds_pat_001_text', 3, NULL, '(?is)\yPAT-001\y', 'ИЭМК: пациент не определён', 'Ошибки ИЭМК'),

    -- ------------------------------------------------------------------
    -- Ярус 4: широкие текстовые фолбэки — применяются, только если ярусы 1–3 молчат
    -- ------------------------------------------------------------------
    ('person_snils', 4, NULL, '(?is)(СНИЛС|SNILS)', 'СНИЛС не найден или не соответствует данным пациента/медработника', 'Данные пациента'),
    ('snils_invalid_text', 4, NULL, '(?is)(СНИЛС.*неверн|неверн.*СНИЛС|СНИЛС.*контрольн|контрольн.*СНИЛС)', 'Неверный формат или контрольная сумма СНИЛС', 'Данные пациента'),
    ('doctor_position_frmr_text', 4, NULL, '(?is)(ФРМР|FRMR).*(должност|specialit|специальност)|(должност|specialit|специальност).*(ФРМР|FRMR)|(должност|specialit|специальност).*(не соответств|не совпад|не найден)', 'Должность врача не соответствует данным ФРМР', 'Данные медработника'),
    ('patient_data_gip_text', 4, NULL, '(?is)(ГИП|GIP).*(пациент|patient)|(пациент|patient).*(ГИП|GIP)|(данн|сведени).*(пациент|patient).*(не соответств|не совпад|не найден)', 'Данные пациента не соответствуют ГИП', 'Данные пациента'),
    -- Сужен до формулировок про несоответствие: голые «медработник|автор|author» ловили
    -- schematron-тексты про assignedAuthor и технические сообщения про проверку ФРМР.
    ('person_frmr', 4, NULL, '(?is)(ФРМР|FRMR).*(не соответств|не совпад|не найден|отсутств)|(не соответств|не совпад|не найден|отсутств).*(ФРМР|FRMR)', 'Данные медработника не соответствуют ФРМР', 'Данные медработника'),
    ('nsi_dictionary_value', 4, NULL, '(?is)(Справочник OID|codeSystem|codeSystemVersion|верси[яи].*справочник|значени[ея].*НСИ|не соответствует наименованию элемента в НСИ|справочн.*значен)', 'Ошибка справочника НСИ', 'Ошибки справочника НСИ'),
    ('document_file_runtime_error', 4, NULL, '(?is)(getDocumentFile|получения файла ЭМД|файлового хранилища)', 'Не удалось получить файл ЭМД из предоставляющей ИС', 'Ошибки получения файла ЭМД'),
    -- Ветка «сервис проверки сертификата» без якорей CRL/OCSP уводила в CRL-тип
    -- тексты про сервис проверки подписи (это УЦ, ярус 2 по кодам CA_*).
    ('crl_unavailable', 4, NULL, '(?is)(\yCRL\y|\yOCSP\y|список.*отозванн)', 'Недоступен сервис проверки статуса сертификата (CRL/OCSP)', 'Ошибки ЭП и сертификатов'),
    -- Таймауты исключены: таймауты РЭМД закрываются ярусами 1–2 по кодам
    -- ASYNC_RESPONSE_TIMEOUT/TIMEOUT; сетевой слой (LOGSTATE=3) классифицируется в transform.
    ('transport_network', 4, NULL, '(?is)(network|connection|transport|соединени|сетевая ошибка)', 'Сетевая ошибка', 'Ошибки связи'),
    ('org_generic_fallback', 4, NULL, '(?is)(организаци|ОГРН|ФРМО|лицензи)', 'Ошибки организации', 'Ошибки организации / ИС'),
    ('remd_runtime_internal', 4, NULL, '(?is)(INTERNAL_ERROR|RUNTIME_ERROR|внутренн.*ошиб|непредвиденн.*ошиб|невозможно обработать)', 'Техническая ошибка на стороне РЭМД', 'Технические ошибки РЭМД')
ON CONFLICT (rule_code) DO UPDATE SET
    match_tier = EXCLUDED.match_tier,
    match_code = EXCLUDED.match_code,
    match_pattern = EXCLUDED.match_pattern,
    interpretation = EXCLUDED.interpretation,
    error_category = EXCLUDED.error_category,
    is_active = true,
    updated_at = now();

-- Страховка для строк вне seed (ручные вставки):
-- ярус обязан согласовываться с наличием кода до навешивания CHECK.
UPDATE dim_error_rules
SET match_tier = CASE
        WHEN match_code IS NOT NULL AND match_pattern = '(?is).*' THEN 2
        WHEN match_code IS NOT NULL THEN 1
        ELSE greatest(match_tier, 3)
    END,
    updated_at = now()
WHERE (match_tier <= 2) <> (match_code IS NOT NULL);

DO $$
BEGIN
    ALTER TABLE dim_error_rules ADD CONSTRAINT chk_dim_error_rules_match_tier
        CHECK (match_tier BETWEEN 1 AND 4
               AND ((match_tier <= 2) = (match_code IS NOT NULL)));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================================
-- dim_error_type_group — ЕДИНЫЙ источник истины «канонический тип → группа»:
-- тип — PK, группа единственная (конфликт «тип → две группы» невозможен).
-- Содержит интерпретации правил + специальные канонические типы, которые движок
-- классификации выдаёт вне таблицы правил (сеть, техсбои, единый schematron,
-- «Неизвестная ошибка»). Категорию/зону витрины берут JOIN'ом к этой таблице.
-- Колонки responsibility/is_retryable: кто устраняет причину и лечится ли повторной
-- отправкой без правки данных; прокидываются в rpt_error_breakdown.
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_error_type_group (
    error_type text PRIMARY KEY,
    error_category text NOT NULL,
    updated_at timestamptz DEFAULT now()
);

-- DEFAULT сохраняем: производный INSERT из dim_error_rules не знает зону/повторяемость
-- новых типов — их выставляет backfill ниже в этом же прогоне.
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS responsibility text NOT NULL DEFAULT 'смешанная';
ALTER TABLE dim_error_type_group
    ADD COLUMN IF NOT EXISTS is_retryable boolean NOT NULL DEFAULT false;

DO $$
BEGIN
    ALTER TABLE dim_error_type_group ADD CONSTRAINT chk_dim_error_type_group_responsibility
        CHECK (responsibility IN ('клиника', 'МИС', 'интегратор', 'РЭМД', 'смешанная'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Канонические типы из активных правил (тип → его группа).
INSERT INTO dim_error_type_group (error_type, error_category)
SELECT DISTINCT r.interpretation, r.error_category
FROM dim_error_rules r
WHERE r.is_active
ON CONFLICT (error_type) DO UPDATE SET
    error_category = EXCLUDED.error_category,
    updated_at = now();

-- Специальные канонические типы вне таблицы правил (выдаются движком классификации/
-- транспортом).
INSERT INTO dim_error_type_group (error_type, error_category)
VALUES
    ('Неизвестная ошибка', 'Прочие'),
    ('Ошибка Schematron-валидации', 'Ошибки структуры и валидации')
ON CONFLICT (error_type) DO UPDATE SET
    error_category = EXCLUDED.error_category,
    updated_at = now();

-- Фолбэк-ветки error_item_atoms раньше выдавали строки с суффиксом «: повторите
-- отправку позже» — расщепляли один логический тип на две строки витрины.
-- Фолбэки выровнены на канон, варианты из словаря убираем.
DELETE FROM dim_error_type_group
WHERE error_type IN (
    'Техническая ошибка на стороне РЭМД: повторите отправку позже',
    'Недоступен сервис проверки подписи/УЦ на стороне РЭМД: повторите отправку позже',
    'Таймаут асинхронной обработки на стороне РЭМД: повторите отправку позже');

-- ============================================================================
-- Backfill зоны ответственности и повторяемости. Идёт ПОСЛЕ обоих INSERT выше,
-- чтобы типы, впервые появившиеся в этом прогоне, получили значения сразу.
-- Шаг 1 — дефолты по категории, шаг 2 — точечные переопределения по типу.
-- ============================================================================
UPDATE dim_error_type_group g
SET responsibility = v.responsibility, is_retryable = v.is_retryable, updated_at = now()
FROM (VALUES
    ('Технические ошибки РЭМД',      'РЭМД',       true),
    ('Ошибки связи',                 'интегратор', true),
    ('Ошибки получения файла ЭМД',   'МИС',        true),
    ('Ошибки структуры и валидации', 'МИС',        false),
    ('Ошибки справочника НСИ',       'клиника',    false),
    ('Данные пациента',              'клиника',    false),
    ('Данные медработника',          'клиника',    false),
    ('Ошибки ЭП и сертификатов',     'клиника',    false),
    ('Ошибки организации / ИС',      'клиника',    false),
    ('Ошибки регистрации в РЭМД',    'смешанная',  false),
    ('Ошибки ИЭМК',                  'смешанная',  false),
    ('Прочие',                       'смешанная',  false)
) AS v(error_category, responsibility, is_retryable)
WHERE g.error_category = v.error_category
  AND (g.responsibility IS DISTINCT FROM v.responsibility
       OR g.is_retryable IS DISTINCT FROM v.is_retryable);

UPDATE dim_error_type_group g
SET responsibility = v.responsibility, is_retryable = v.is_retryable, updated_at = now()
FROM (VALUES
    -- Доступность getDocumentFile и регистрационные данные ИС — зона интегратора.
    ('Сервис предоставляющей ИС недоступен: проверьте доступность getDocumentFile', 'интегратор', true),
    ('ИС не зарегистрирована в РЭМД или указаны неверные регистрационные данные',   'интегратор', false),
    ('ИС зарегистрирована в РЭМД, но не активна: проверьте уведомления и переподключение ИС', 'интегратор', false),
    -- Сервис проверки статуса сертификата живёт на стороне РЭМД/УЦ — не клиника.
    ('Недоступен сервис проверки статуса сертификата (CRL/OCSP)', 'РЭМД', true),
    -- Метаописание запроса на регистрацию формирует МИС.
    ('Метаописание документа не соответствует зарегистрированному в РЭМД', 'МИС', false),
    ('Идентификатор документа в ЭМД не совпадает с идентификатором в запросе на регистрацию', 'МИС', false),
    ('Дата создания документа в ЭМД не совпадает с датой в запросе на регистрацию', 'МИС', false),
    ('Дата подписи МО позже даты поступления запроса на регистрацию', 'МИС', false),
    ('Структурное подразделение (providerOrganization) в СЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Структурное подразделение (representedOrganization) в СЭМД не совпадает с запросом на регистрацию', 'МИС', false),
    ('Документ уже зарегистрирован в РЭМД', 'МИС', false),
    ('Дублирующий запрос',                  'МИС', false),
    ('Неверный формат запроса',             'МИС', false),
    ('Неверный идентификатор документа РЭМД', 'МИС', false),
    -- Подпись формирует и упаковывает МИС/крипто-прослойка, не клиника.
    ('Не удалось декодировать электронную подпись', 'МИС', false),
    ('Неподдерживаемый формат электронной подписи', 'МИС', false),
    -- Привязка ИС к региону и лимиты обмена — зона интегратора.
    ('Регион ИС не соответствует региону организации', 'интегратор', false),
    ('Превышен лимит запросов к РЭМД',                 'интегратор', true),
    -- Метаданные запроса на регистрацию формирует МИС.
    ('Дата создания документа позже даты регистрации', 'МИС', false),
    ('Неверный идентификатор сообщения (messageId)',   'МИС', false),
    -- Сбой создания пациента — внутренняя ошибка ГИП, лечится повтором.
    ('Не удалось создать пациента в ГИП', 'РЭМД', true),
    -- ИЭМК: технические сбои федеральной стороны лечатся повтором.
    ('ИЭМК: внутренняя ошибка репозитория', 'РЭМД', true),
    ('ИЭМК: внутренняя ошибка реестра',     'РЭМД', true),
    ('ИЭМК: сервис временно недоступен',    'РЭМД', true),
    ('ИЭМК: ошибка обработки CDA',          'РЭМД', true),
    ('ИЭМК: данные не соответствуют справочнику НСИ', 'клиника', false),
    ('ИЭМК: пациент не определён',          'клиника', false),
    ('ИЭМК: ошибка валидации структуры CDA', 'МИС', false),
    ('ИЭМК: документ уже зарегистрирован',  'МИС', false),
    ('ИЭМК: некорректный идентификатор документа', 'МИС', false),
    ('ИЭМК: заменяемый документ не найден (замена версии)', 'МИС', false),
    ('ИЭМК: замена версии отклонена (документ уже заменён)', 'МИС', false),
    ('ИЭМК: состав пакета не согласован (документы/метаданные)', 'МИС', false),
    ('ИЭМК: ошибка метаданных документа',   'МИС', false),
    ('ИЭМК: дублирующийся идентификатор в пакете', 'МИС', false),
    ('ИЭМК: повторная загрузка с изменённым содержимым', 'МИС', false),
    ('ИЭМК: неверный идентификатор репозитория', 'интегратор', false)
) AS v(error_type, responsibility, is_retryable)
WHERE g.error_type = v.error_type
  AND (g.responsibility IS DISTINCT FROM v.responsibility
       OR g.is_retryable IS DISTINCT FROM v.is_retryable);
