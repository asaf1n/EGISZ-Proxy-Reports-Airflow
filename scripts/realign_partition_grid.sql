-- Перестроение месячной сетки партиций на общий якорь — полночь UTC.
--
-- Нужен контурам, где сетка создавалась при другом часовом поясе сессии: до того как
-- границы стали считаться наивным UTC, якорь задавался настройкой роли, и месяцы,
-- созданные из разных сессий, перестают стыковаться. Разрыв в сетке означает, что строка
-- с меткой из зазора не вставится вовсе, перекрытие — что следующий месяц не создастся.
-- Признак — сигнал partition_grid в rpt_health_signals.
--
-- Сценарий идемпотентен: таблица с уже выровненными границами пропускается. Отбор идёт
-- по системному каталогу, поэтому перечень таблиц задавать не нужно. Пустые партиции
-- удаляются и создаются заново, непустые переносятся построчно — это переписывает объём
-- данных таблицы, окно обслуживания планировать по нему.
--
-- Приём фактов на время перестроения останавливать: работа идёт под AccessExclusiveLock.

DO $$
DECLARE
    tbl record;
    part record;
    legacy_tables text[];
    legacy text;
    is_empty boolean;
    oldest timestamptz;
    candidate timestamptz;
    months_back integer;
    moved bigint;
    total bigint;
BEGIN
    FOR tbl IN
        SELECT c.relname AS table_name, c.oid AS table_oid, a.attname AS key_column
        FROM pg_partitioned_table p
        JOIN pg_class c ON c.oid = p.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0]
        WHERE n.nspname = 'public'
          AND p.partstrat = 'r'
          AND p.partnatts = 1
          AND a.atttypid IN ('timestamptz'::regtype, 'timestamp'::regtype)
        ORDER BY c.relname
    LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT 1
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            CROSS JOIN LATERAL (
                SELECT (regexp_match(pg_get_expr(child.relpartbound, child.oid),
                                     'FROM \(''([^'']+)''\)'))[1]::timestamptz AS lower_bound
            ) b
            WHERE i.inhparent = tbl.table_oid
              AND pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
              AND b.lower_bound
                  <> date_trunc('month', timezone('UTC', b.lower_bound)) AT TIME ZONE 'UTC'
        );

        legacy_tables := ARRAY[]::text[];
        oldest := NULL;
        total := 0;

        FOR part IN
            SELECT child.relname AS name
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            WHERE i.inhparent = tbl.table_oid
            ORDER BY child.relname
        LOOP
            EXECUTE format('ALTER TABLE public.%I DETACH PARTITION public.%I',
                           tbl.table_name, part.name);

            EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM public.%I)', part.name)
                INTO is_empty;

            IF is_empty THEN
                EXECUTE format('DROP TABLE public.%I', part.name);
                CONTINUE;
            END IF;

            -- Имя занято будущей партицией той же сетки, поэтому отцепленная таблица
            -- переименовывается до её создания.
            EXECUTE format('ALTER TABLE public.%I RENAME TO %I', part.name, part.name || '_legacy');
            legacy_tables := legacy_tables || (part.name || '_legacy');

            EXECUTE format('SELECT min(%I) FROM public.%I', tbl.key_column, part.name || '_legacy')
                INTO candidate;
            oldest := LEAST(oldest, candidate);
        END LOOP;

        -- После отцепления родитель пуст, и расчёт окна по его содержимому историю
        -- не увидит: глубина задаётся явно по самой ранней отцепленной строке.
        months_back := 12;
        IF oldest IS NOT NULL THEN
            months_back := GREATEST(
                months_back,
                (EXTRACT(YEAR FROM age(now(), oldest)) * 12
                 + EXTRACT(MONTH FROM age(now(), oldest)))::integer + 1
            );
        END IF;
        PERFORM public.ensure_time_partitions(months_back, 24);

        FOREACH legacy IN ARRAY legacy_tables LOOP
            EXECUTE format('INSERT INTO public.%I SELECT * FROM public.%I', tbl.table_name, legacy);
            GET DIAGNOSTICS moved = ROW_COUNT;
            total := total + moved;
            EXECUTE format('DROP TABLE public.%I', legacy);
        END LOOP;

        -- Статистика после массовой вставки: планировщик иначе работает по оценкам
        -- снесённых партиций.
        EXECUTE format('ANALYZE public.%I', tbl.table_name);

        RAISE NOTICE 'Сетка % перестроена на якорь UTC: перенесено % строк(и).',
                     tbl.table_name, total;
    END LOOP;
END
$$;
