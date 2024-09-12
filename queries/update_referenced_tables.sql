/**
 * Update the target table keys and where it's referenced
 * 
 * Update all the foreign key constraints for a table when the database
 * structure is set to not have cascade updates
 *
 * Parameters:
 *  @tableName          TEXT    Target table
 *  @keyNames           TEXT[]  Array of key column names
 *  @currentValues      TEXT[]  Array of values with current values
 *  @newValues          TEXT[]  Array of values with new values
 *  @cascadeText        TEXT[]  Array of values with new values
 *  @checkedTablesKeys  TEXT[]  Array of already checked tables  Default: [NULL]
 *  @starting           BOOL    Defines the first call           Default: FALSE
 *
 * Returns:
 *  @TABLE(
 *    changed_tabled  TEXT,
 *    constraint_name TEXT,
 *    columns_names   TEXT
 *  )
 */
CREATE OR REPLACE FUNCTION pg_temp.update_referenced_tables (
  tableName         TEXT,
  keyNames          TEXT[],
  currentValues     INT[],
  newValues         INT[]  DEFAULT ARRAY[NULL]::INT[],
  checkedTablesKeys TEXT[] DEFAULT ARRAY[NULL]::TEXT[],
  starting          BOOL   DEFAULT FALSE
) RETURNS TABLE (
  changed_tabled TEXT,
  constraint_name TEXT,
  columns_names TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE r RECORD;
DECLARE checkReferenced BOOL := NULL;
BEGIN
  FOR r IN
    SELECT    tc.table_name::TEXT AS local_table,
              tc.constraint_name::TEXT,
              ccu.table_name::TEXT AS foreign_table,
              ARRAY_AGG(DISTINCT kcu.column_name::TEXT) AS local_keys,
              ARRAY_TO_STRING(
                ARRAY_AGG(DISTINCT kcu.column_name::TEXT),
                ','
              ) AS local_keys_string,
              ARRAY_TO_STRING(
                ARRAY_AGG(DISTINCT ccu.column_name::TEXT),
                ','
              ) AS foreign_keys_string,
              tc.table_name::TEXT || ' ' ||tc.constraint_name::TEXT AS keys
    FROM      information_schema.table_constraints tc
    JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
    AND       tc.table_schema = kcu.table_schema
    JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
    AND       tc.table_schema = ccu.table_schema
    WHERE     tc.constraint_type = 'FOREIGN KEY'
    AND       ccu.table_name = tableName
    GROUP BY  1, 2, 3
    HAVING    ARRAY_AGG(ccu.column_name::TEXT) @> keyNames
  LOOP
    EXECUTE 'SELECT 1'
      ' FROM agh.' || r.local_table ||
      ' WHERE ARRAY[' || r.local_keys_string || ']::BIGINT[] @> ARRAY[' || ARRAY_TO_STRING(currentValues, ',') || ']::BIGINT[]'
    INTO checkReferenced;

    IF NOT COALESCE(checkReferenced, FALSE) THEN
      CONTINUE;
    END IF;

    IF ARRAY[r.keys] <@ checkedTablesKeys THEN
      CONTINUE;
    END IF;

    checkedTablesKeys := array_append(checkedTablesKeys, r.keys);

    RAISE NOTICE 'ALTER TABLE agh.% DROP CONSTRAINT %;', r.local_table, r.constraint_name;
    RAISE NOTICE 'ALTER TABLE agh.% ADD CONSTRAINT % FOREIGN KEY (%) REFERENCES agh.%(%) ON UPDATE CASCADE;', r.local_table, r.constraint_name, r.local_keys_string, r.foreign_table, r.foreign_keys_string;
    --EXECUTE 'ALTER TABLE agh.' || r.local_table || ' DROP CONSTRAINT ' || r.constraint_name;

    --EXECUTE 'ALTER TABLE agh.' || r.local_table || ' ADD CONSTRAINT ' || r.constraint_name ||
    --  ' FOREIGN KEY (' || r.local_keys_string || ') REFERENCES agh.' || r.foreign_table ||
    --  ' (' || r.foreign_keys_string || ') ON UPDATE CASCADE';

    PERFORM pg_temp.update_referenced_tables(
          tableName         := r.local_table,
          keyNames          := r.local_keys,
          currentValues     := currentValues,
          checkedTablesKeys := checkedTablesKeys
    );

    checkReferenced := NULL;

    RETURN QUERY
      SELECT r.local_table, r.constraint_name, r.local_keys_string;

  END LOOP;
  IF starting THEN
    FOR r IN
      SELECT UNNEST(keyNames) AS key, UNNEST(newValues) AS value
    LOOP
      RAISE NOTICE 'UPDATE agh.% SET % = % WHERE ARRAY[%] = ARRAY[%];', tableName, r.key, r.value, ARRAY_TO_STRING(keyNames, ','), ARRAY_TO_STRING(currentValues, ',');
      --EXECUTE 'UPDATE agh.' || tableName ||
      --  ' SET ' || r.key || ' = ' || r.value ||
      --  ' WHERE ARRAY[' || ARRAY_TO_STRING(keyNames, ',') || ']::BIGINT[] = ARRAY[' || ARRAY_TO_STRING(currentValues, ',') || ']::BIGINT[]';
    END LOOP;
    checkedTablesKeys := ARRAY[NULL]::TEXT[];
    FOR r IN
      SELECT    tc.table_name::TEXT AS local_table,
                tc.constraint_name::TEXT,
                ccu.table_name::TEXT AS foreign_table,
                ARRAY_AGG(DISTINCT kcu.column_name::TEXT) AS local_keys,
                ARRAY_TO_STRING(
                  ARRAY_AGG(DISTINCT kcu.column_name::TEXT),
                  ','
                ) AS local_keys_string,
                ARRAY_TO_STRING(
                  ARRAY_AGG(DISTINCT ccu.column_name::TEXT),
                  ','
                ) AS foreign_keys_string,
                tc.table_name::TEXT || ' ' ||tc.constraint_name::TEXT AS keys
      FROM      information_schema.table_constraints tc
      JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
      AND       tc.table_schema = kcu.table_schema
      JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
      AND       tc.table_schema = ccu.table_schema
      WHERE     tc.constraint_type = 'FOREIGN KEY'
      AND       ccu.table_name = tableName
      GROUP BY  1, 2, 3
      HAVING    ARRAY_AGG(ccu.column_name::TEXT) @> keyNames
    LOOP
      EXECUTE 'SELECT 1'
        ' FROM agh.' || r.local_table ||
        ' WHERE ARRAY[' || r.local_keys_string || ']::BIGINT[] @> ARRAY[' || ARRAY_TO_STRING(newValues, ',') || ']::BIGINT[]'
      INTO checkReferenced;
  
      IF NOT COALESCE(checkReferenced, FALSE) THEN
        CONTINUE;
      END IF;
  
      IF ARRAY[r.keys] <@ checkedTablesKeys THEN
        CONTINUE;
      END IF;
  
      checkedTablesKeys := array_append(checkedTablesKeys, r.keys);
  
      RAISE NOTICE 'ALTER TABLE agh.% DROP CONSTRAINT %;', r.local_table, r.constraint_name;
      RAISE NOTICE 'ALTER TABLE agh.% ADD CONSTRAINT % FOREIGN KEY (%) REFERENCES agh.%(%);', r.local_table, r.constraint_name, r.local_keys_string, r.foreign_table, r.foreign_keys_string;
      --EXECUTE 'ALTER TABLE agh.' || r.local_table || ' DROP CONSTRAINT ' || r.constraint_name;
  
      --EXECUTE 'ALTER TABLE agh.' || r.local_table || ' ADD CONSTRAINT ' || r.constraint_name ||
      --  ' FOREIGN KEY (' || r.local_keys_string || ') REFERENCES agh.' || r.foreign_table ||
      --  ' (' || r.foreign_keys_string || ') ON UPDATE CASCADE';
  
      PERFORM pg_temp.update_referenced_tables(
            tableName         := r.local_table,
            keyNames          := r.local_keys,
            currentValues     := currentValues,
            checkedTablesKeys := checkedTablesKeys
      );
  
      checkReferenced := NULL;

    END LOOP;
  END IF;
END;
$$
;

SELECT *
FROM pg_temp.update_referenced_tables(
  tableName         := '',
  keyNames          := ARRAY[''],
  currentValues     := ARRAY[],
  newValues         := ARRAY[],
  starting          := TRUE
)
;
