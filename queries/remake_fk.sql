CREATE
OR        REPLACE FUNCTION pg_temp.remake_fk (
          _table_name text,
          _fk TEXT,
          _tables TEXT[] DEFAULT ARRAY[NULL::TEXT],
          _update TEXT DEFAULT '',
          counter INT DEFAULT 0
          ) RETURNS VOID AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT    'ALTER TABLE agh.' || local_table || CHR(10) ||
                  ' DROP CONSTRAINT '|| constraint_name || ';' || CHR(10) ||
                  'ALTER TABLE agh.' || local_table || CHR(10) ||
                  ' ADD CONSTRAINT ' || constraint_name || CHR(10) ||
                  ' FOREIGN KEY (' || local_keys || ')' || CHR(10) || 
                  ' REFERENCES agh.' || foreign_table || '(' || foreign_keys || ') ' ||
                  _update || ';' AS query,
                  local_table::TEXT
        FROM      (
          SELECT    tc.table_name AS local_table,
                    tc.constraint_name,
                    ccu.table_name AS foreign_table,
                    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT kcu.column_name::VARCHAR), ', ') AS local_keys,
                    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ccu.column_name::VARCHAR), ', ') AS foreign_keys
          FROM      information_schema.table_constraints tc
          JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          AND       tc.table_schema = kcu.table_schema
          JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
          AND       tc.table_schema = ccu.table_schema
          WHERE     tc.constraint_type = 'FOREIGN KEY'
          AND       ccu.table_name = _table_name
          GROUP BY  tc.table_name,
                    tc.constraint_name,
                    ccu.table_name
          HAVING    ARRAY_TO_STRING(ARRAY_AGG(ccu.column_name::TEXT), ', ') ~~ _fk
        ) AS t
    LOOP
        RAISE NOTICE 'REMAKING AS %: %', _fk, r.local_table;
        EXECUTE r.query;
        IF NOT ARRAY[r.local_table] <@ _tables THEN
            _tables := array_append(_tables, r.local_table); -- Add to array
            PERFORM pg_temp.remake_fk(r.local_table, _fk, _tables, _update, counter + 1); -- Recursive call
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql
;
