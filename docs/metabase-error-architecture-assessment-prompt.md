# Промпт: переход Metabase на новую модель ошибок

Репозиторий: `C:\Users\artem\EGISZ-Proxy-Reports-Airflow`, ветка `claude/error-model-rework`.

Оцени, что нужно изменить в Metabase после переработки обработки ошибок в DWH, и подготовь план перехода. Изменения DWH сделаны в репозитории и 27.09.2026 применены на проде; в этой задаче они не пересматриваются. Metabase выключен до перехода дашбордов: карточки читают снятые объекты. Не выполняй импорт, schema sync, публикацию и изменение прав; живой Metabase — только чтение (GET, заголовок `x-api-key`).

Прочитай README (§«Классификация ошибок», §«DWH-модель», §«Дашборды Metabase»), `docs/dwh-error-taxonomy-audit-2026-09-26.md`, `docs/error-catalog.md` и корпоративный стандарт `bi_schema_naming_standard.md`.

## Что изменилось в DWH

Статус документа определяет только асинхронный ответ. Ошибка связи хранится рядом со статусом и его не меняет. Ошибка в отчётности — два поля: `error_text` (исходный текст) и `error_type` (тип). Уровни: вид («Ошибка связи», «Ошибка асинхронного ответа») → категория (только у асинхронного ответа) → тип.

| Было | Стало |
| --- | --- |
| Статус `network_error` («Ошибка связи») | Снят. Документы перешли в `success`, `async_error` или `sent` по последнему асинхронному ответу |
| `rpt_error_breakdown` (документ × тип) | `mart_egisz_selfservice.document_error`: строка — одна ошибка текущего состояния документа (`dwh_id`, `error_no`) |
| `rpt_network_errors` (документы со статусом `network_error`) | `mart_egisz_selfservice.network_error`: строка — одна ошибка связи по времени сообщения, включая сообщения без документа |
| `rpt_error_breakdown_weekly` / `_monthly` | `mart_egisz.agg_document_error_weekly` / `_monthly`, грейн: период × клиника × вид × категория |
| `rpt_documents.error_types`, `error_text`, `error_details` | Сняты: ошибки документа читаются из `document_error` |
| `base_error_type`, `classification_type`, `network_error_type` | Сняты: у элемента один тип, подпись НСИ вынесена в `nsi_dictionary_name` |
| `rpt_documents_weekly/monthly.docs_async_error` | Снят; `docs_error` = `async_error`, `docs_network_error` — документы с ошибкой связи в текущем состоянии |
| Категории «Ошибки связи», «Ошибки ИЭМК», «Ошибки ФРЛЛО», «Ошибки регистрации в РЭМД», «Технические ошибки РЭМД» | Первая стала видом, ИЭМК и ФРЛЛО разнесены по причинам; «Ошибки регистрации», «Технические ошибки ЕГИСЗ» |
| Заглушки «Неизвестная ошибка», «(без текста)», «Код: …», «Сетевая ошибка» | Сняты: элемент без типа виден сигналом `untyped_errors` |

## Зависимости в репозитории

| Файл | Что затронуто |
| --- | --- |
| `metabase_dashboards/01_integration_egisz.json` | `rpt_error_breakdown`, `rpt_network_errors`, `error_types`, `base_error_type`, `network_error_type`, статус `network_error`; карточки «Типы сетевых ошибок (за период)», «Последние сбои транспорта», «Топ клиник по сбоям транспорта», «Топ типов СЭМД по видам ошибки», «Документы: недоставленные в клинику (ошибка связи шлюз-МО)», ряд состояния «Ошибок связи за последние 24 часа» |
| `metabase_dashboards/05_executive.json` | `rpt_error_breakdown_weekly/monthly`, `docs_async_error`, `error_types`, статус `network_error` в корпусе долей и XmR |
| `metabase_dashboards/07_client_service.json`, `08_client_bianalytic.json` | `rpt_error_breakdown`, `error_types`, статус `network_error` в долях |
| `scripts/apply_dashboard_plan.py`, `scripts/layout_operational_tab.py` | Генераторы тех же карточек; править вместе с JSON (пара генератор + раскладка) |
| `metabase/sync-models.sh`, модели `02_error_breakdown`, `04_network_errors` | Источник моделей «Разбивка ошибок» и «Сбои транспорта» |
| `scripts/verify_metabase_cards.py` | `error_types` в проверочных запросах |
| `tests/test_dashboards.py` | Падают `test_service_network_top_groups_by_typed_label` и `test_operational_error_types_include_network_slice`: карточки читают снятые объекты DWH |
| README §«Дашборды Metabase» | Описания карточек с `network_error`, `error_types`, `rpt_error_breakdown`, `rpt_network_errors` |

На живом Metabase проверь карточки 694 и 695: их списки полей выводили тексты с персональными данными.

## Что оценить

1. Для каждой карточки и модели: источник, грейн, период и знаменатель после перехода. Ошибки текущего состояния документа и ошибки связи по времени сообщения — разные показатели, их нельзя смешивать.
2. Корпус долей: `success + async_error` без `network_error`. Как изменятся доля ошибок, XmR и опорный период фазы; нужна ли новая фаза в `dim_control_chart_phases` (фаза — смена условий работы, а не наблюдаемый сдвиг).
3. Отбор документов по типу и категории: точное равенство вместо `contains` по склеенной строке; несколько значений — «любой из»; документ с несколькими ошибками не увеличивает знаменатель.
4. Подписи: «вид ошибки» теперь означает `error_kind`; где сейчас «вид» называет категорию, заменить на «категория».
5. `error_text` в опубликованный слой не выносится (решение 27.09.2026). До будущего решения о доступе дашборды читают его из слоя разбора в обход правил стандарта: `stg_egisz.document_error_current` (текущие ошибки документа, ключ `dwh_id` + `error_no` общий с `document_error`) и `stg_egisz.message_error` (все элементы по времени сообщения, в том числе ошибки связи). Предложи, как карточки с текстом ошибки подключить к этим объектам и как ограничить к ним доступ.
6. Порядок включения Metabase: DWH уже в новой модели, прежних объектов нет. Предложи последовательность, снимок перед импортом (`export_dashboard.py --backup`) и проверку карточек с параметрами (`scripts/verify_metabase_cards.py`, реальная `clinic_label`).

Результат оформи документом `docs/metabase-error-architecture-assessment.md`: вывод; матрица «карточка/модель → изменение → зависимости → проверка»; вопросы на решение; порядок переключения и критерии приёмки. Подтверждённое состояние отдели от предложений. На оценке остановись.
