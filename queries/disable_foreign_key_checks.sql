SELECT    DISTINCT 'ALTER TABLE agh.' || local_table || CHR(10) || ' ENABLE' || ' TRIGGER ALL;',
          local_table::TEXT
FROM      (
          SELECT    tc.table_name AS local_table
          FROM      information_schema.table_constraints tc
          JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          AND       tc.table_schema = kcu.table_schema
          JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
          AND       tc.table_schema = ccu.table_schema
          WHERE     tc.constraint_type = 'FOREIGN KEY'
          AND       ccu.table_name = 'rap_servidores'
          ) AS t
ORDER BY  2
;

SELECT    DISTINCT 'UPDATE agh.' || local_table || CHR(10) || 'SET ' || column_name || ' = ' || _new_value || CHR(10) || 'WHERE ' || column_name || ' = ' || _old_value || ';',
          local_table::TEXT
FROM      (
          SELECT    tc.table_name AS local_table,
                    kcu.column_name
          FROM      information_schema.table_constraints tc
          JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          AND       tc.table_schema = kcu.table_schema
          JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
          AND       tc.table_schema = ccu.table_schema
          WHERE     tc.constraint_type = 'FOREIGN KEY'
          AND       ccu.table_name = 'rap_servidores'
          AND       kcu.column_name ~~ '%matricula%'
          ) AS t
ORDER BY  2
;

CREATE
OR        REPLACE function pg_temp.test (
    _table TEXT,
    _tables TEXT[]
) RETURNS VOID AS $$
DECLARE r record;
BEGIN
    FOR r in 
    SELECT    DISTINCT 'ALTER TABLE agh.' || local_table || CHR(10) || ' ENABLE' || ' TRIGGER ALL;' AS q,
              local_table::TEXT
    FROM      (
              SELECT    tc.table_name AS local_table
              FROM      information_schema.table_constraints tc
              JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
              AND       tc.table_schema = kcu.table_schema
              JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
              AND       tc.table_schema = ccu.table_schema
              WHERE     tc.constraint_type = 'FOREIGN KEY'
              AND       ccu.table_name = _table
              ) AS t
    ORDER BY  2
    loop
        IF NOT ARRAY[r.local_table] <@ _tables THEN
            RAISE NOTICE '%', r.q;
            _tables := ARRAY_APPEND(_tables, r.local_table);
            PERFORM pg_temp.test(r.local_table, _tables);
    END IF;
    end loop;
END;
$$
LANGUAGE plpgsql
;
