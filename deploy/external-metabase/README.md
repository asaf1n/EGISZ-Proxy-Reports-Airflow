# Импорт дашбордов и моделей в сторонний Metabase

Самодостаточный бандл для провижининга **уже развёрнутого** Metabase: регистрация БД DWH,
глобальные настройки (таймзона/локаль/кеш), Metabase Models, карточки и дашборды с
фильтрами и drill-through. Импортёр — тот же идемпотентный скрипт, что провижинит
локальный контур.

```
metabase/                    # корень бандла (dist/external/metabase)
├── setup-dashboards.sh      # главный импортёр (запускать его)
├── sync-models.sh           # sync Metabase Models (подключается импортёром)
├── include/mb_list.sh       # общие функции (подключается импортёром)
├── metabase_dashboards/     # 4 дашборда (*.json) + field_filter_defaults.yaml
├── metabase_models/         # 4 модели (*.json)
├── README.md
└── BUILD_INFO.txt
```

`field_filter_defaults.yaml` — design-time метаданные генераторов; импортёр читает только
`*.json` (правила фильтров уже запечены в JSON дашбордов ключами `metabase-field-filters`).

## 0. Предусловия

- Запущенный **Metabase v0.62.x** (скрипт содержит workaround'ы под API этой версии;
  на других мажорных версиях не проверялся).
- **DWH-схема применена** в целевом PostgreSQL (бандл `dwh`) — импортёр валидирует контракт
  (наличие таблиц/вьюх из SQL карточек) до загрузки и падает, если схемы нет.
- На хосте, откуда запускается скрипт: `bash`, `curl`, `jq`, `psql` (postgresql-client),
  `sha256sum`; `flock` опционален (без него пропускается защита от параллельного запуска).
- Сетевой доступ хоста к Metabase (HTTP) **и** к DWH PostgreSQL (контракт-валидация идёт
  напрямую через `psql`).

## 1. Переменные окружения

Все значения — плейсхолдеры; реальные секреты передавать только через окружение.

| Переменная | Дефолт | Назначение |
| --- | --- | --- |
| `METABASE_URL` (или `MB_URL`) | `http://localhost:3000` | адрес целевого Metabase |
| `ADMIN_EMAIL` / `METABASE_ADMIN_EMAIL` | `admin@egisz.local` | логин администратора |
| `ADMIN_PASSWORD` / `METABASE_ADMIN_PASSWORD` | `egisz` | пароль администратора |
| `METABASE_DASHBOARDS_DIR` | `/app/metabase_dashboards` | путь к JSON дашбордов (в бандле — задать явно) |
| `METABASE_MODELS_DIR` | `/app/metabase_models` | путь к JSON моделей (в бандле — задать явно) |
| `APP_DB_HOST` / `APP_DB_PORT` | `host.docker.internal` / `5432` | хост/порт DWH |
| `APP_DB_NAME` | `dwh_egisz` | БД DWH |
| `APP_DB_USER` / `APP_DB_PASSWORD` | `postgres` / `postgres` | учётка DWH для Metabase |
| `APP_DB_DISPLAY_NAME` | `DWH ЕГИСЗ` | имя подключения в Metabase |
| `METABASE_COLLECTION_NAME` | `Интеграция с ЕГИСЗ` | коллекция для карточек/дашбордов |
| `METABASE_SITE_NAME` | `Интеграция с ЕГИСЗ` | имя инстанса (site-name) |
| `METABASE_FORCE_PROVISION` | `auto` | `always` — форсировать переимпорт при неизменных JSON |
| `METABASE_PUBLIC_CLIENT_DASHBOARD` | `true` | публичная ссылка клиентского дашборда |
| `METABASE_AUTO_APPLY_FILTERS` | `true` | auto-apply фильтров на дашбордах |

## 2. Запуск

Из корня бандла:

```bash
METABASE_URL=https://metabase.example.org \
ADMIN_EMAIL=admin@example.org ADMIN_PASSWORD='***' \
METABASE_DASHBOARDS_DIR="$PWD/metabase_dashboards" \
METABASE_MODELS_DIR="$PWD/metabase_models" \
APP_DB_HOST=pg.example.org APP_DB_NAME=dwh_egisz \
APP_DB_USER=egisz APP_DB_PASSWORD='***' \
./setup-dashboards.sh
```

## 3. Что делает скрипт (идемпотентно, безопасно повторять)

1. Ждёт `/api/health`; на неинициализированном Metabase создаёт администратора через
   `/api/setup`, иначе логинится.
2. Регистрирует (или переиспользует) подключение к DWH; включает `report-timezone
   Europe/Moscow` глобально и на подключении.
3. Локаль `ru`, формат времени `HH:mm`, валюта `RUB`, кеширование запросов (TTL-стратегия).
4. Валидирует контракт DWH, синхронизирует метаданные схемы.
5. Создаёт/обновляет Metabase Models из `metabase_models/*.json`.
6. Создаёт/обновляет карточки и дашборды (вкладки, фильтры, drill-through, публичная
   ссылка клиентского дашборда); архивирует карточки/дашборды коллекции, которых больше
   нет в JSON.

Повторный запуск при неизменных JSON завершается быстрым no-op (sha256-манифест);
`METABASE_FORCE_PROVISION=always` форсирует полный проход.

> `provision.sh` и `entrypoint.sh` из репозитория в бандл не входят — это контейнерные
> обёртки локального образа; `setup-dashboards.sh` сам ждёт готовности API.
