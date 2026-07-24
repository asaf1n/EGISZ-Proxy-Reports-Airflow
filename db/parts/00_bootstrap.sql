-- ============================================================================
-- 00_bootstrap.sql — заголовок, пояс роли, гранты.
-- Подключается из db/dwh_init.sql через \i db/parts/00_bootstrap.sql.
-- Идемпотентно; выполняется под ролью egisz (владелец dwh_egisz).
-- Контракт схемы — README.md §DWH-модель.
-- ============================================================================

\encoding UTF8
-- Инициализация DWH для отчётности EGISZ. Запускать под ролью egisz против dwh_egisz;
-- повторный прогон безопасен. Прежней ветки «под postgres» больше нет — весь dwh_init
-- (части 00–90) идёт под egisz.
--
-- Однократные предусловия (администратор БД, до первого прогона):
--   CREATE ROLE egisz LOGIN PASSWORD '...';
--   CREATE DATABASE dwh_egisz OWNER egisz;   -- egisz как владелец получает public-схему
--
-- Usage:
--   psql -U egisz -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql

-- Пин пояса роли на МСК: наивное Firebird-время (EXCHANGELOG.CREATEDATE, лицензии) пишется
-- как timestamptz; без фиксированного пояса сессии сутки «уехали» бы на границе. Роль вправе
-- менять собственные параметры сессии, поэтому egisz выполняет это сам.
ALTER ROLE egisz SET timezone TO 'Europe/Moscow';

-- egisz — владелец dwh_egisz и public (через pg_database_owner), права уже есть; GRANT
-- идемпотентен и фиксирует контракт для среды, где владение выдано иначе.
GRANT CONNECT ON DATABASE dwh_egisz TO egisz;
GRANT USAGE, CREATE ON SCHEMA public TO egisz;
