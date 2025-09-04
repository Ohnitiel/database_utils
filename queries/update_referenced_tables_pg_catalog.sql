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
    tableName TEXT
  , keyNames TEXT[]
  , currentValues INT[]
  , newValues INT[] DEFAULT ARRAY[NULL]::INT[]
  , checkedTablesKeys TEXT[] DEFAULT ARRAY[NULL]::TEXT[]
  , starting BOOL DEFAULT FALSE
) RETURNS TABLE (
    changed_tabled TEXT
  , constraint_name TEXT
  , columns_names TEXT
) LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  checkReferenced BOOL := NULL;
BEGIN
  FOR r IN
    SELECT  c.constraint_name,
            c.local_table,
            c.foreign_table,
            ARRAY_AGG(l.attname) AS local_keys,
            STRING_AGG(l.attname, ',') AS local_keys_string,
            ARRAY_AGG(f.attname) AS foreign_keys,
            STRING_AGG(f.attname, ',') AS foreign_keys_string,
            c.local_table || c.constraint_name AS keys
    FROM (
      SELECT  DISTINCT
              c.conname AS constraint_name,
              c.conrelid::regclass AS local_table,
              c.confrelid::regclass AS foreign_table,
              UNNEST(c.conkey) AS local_keys,
              UNNEST(c.confkey) AS foreign_keys
    FROM      pg_constraint c
    WHERE     c.confrelid = tableName::regclass
    ) AS c
    JOIN    pg_attribute l ON l.attnum = c.local_keys
    AND     l.attrelid = c.local_table
    JOIN    pg_attribute f ON f.attnum = c.foreign_keys
    AND     f.attrelid = c.foreign_table
    GROUP BY 1, 2, 3
--    HAVING  STRING_AGG(l.attname::TEXT, ',') LIKE '%matricula%vin_codigo%'
  LOOP
  --  RAISE INFO '% - % - % - %', r.local_table, r.local_keys_string, r.foreign_table, r.foreign_keys_string;
    EXECUTE 'SELECT 1'
      ' FROM ' || r.local_table ||
      ' WHERE ARRAY[' || r.local_keys_string || ']::TEXT[] @> ARRAY[' || ARRAY_TO_STRING(currentValues, ',') || ']::TEXT[]'
    INTO checkReferenced;

    IF NOT COALESCE(checkReferenced, FALSE) THEN
      CONTINUE;
    END IF;

    IF ARRAY[r.keys] <@ checkedTablesKeys THEN
      CONTINUE;
    END IF;

    checkedTablesKeys := array_append(checkedTablesKeys, r.keys);

    RAISE INFO 'ALTER TABLE % DROP CONSTRAINT %;', r.local_table, r.constraint_name;
    RAISE INFO 'ALTER TABLE % ADD CONSTRAINT % FOREIGN KEY (%) REFERENCES %(%) ON UPDATE CASCADE;', r.local_table, r.constraint_name, r.local_keys_string, r.foreign_table, r.foreign_keys_string;
    --EXECUTE 'ALTER TABLE ' || r.local_table || ' DROP CONSTRAINT ' || r.constraint_name;

    --EXECUTE 'ALTER TABLE ' || r.local_table || ' ADD CONSTRAINT ' || r.constraint_name ||
    --  ' FOREIGN KEY (' || r.local_keys_string || ') REFERENCES ' || r.foreign_table ||
    --  ' (' || r.foreign_keys_string || ') ON UPDATE CASCADE';

    PERFORM pg_temp.update_referenced_tables(
          tableName         := r.local_table::TEXT,
          keyNames          := r.local_keys,
          currentValues     := currentValues,
          checkedTablesKeys := checkedTablesKeys
    );

    checkReferenced := NULL;

    RETURN QUERY
      SELECT r.local_table::TEXT, r.constraint_name::TEXT, r.local_keys_string::TEXT;

  END LOOP;
  IF starting THEN
    FOR r IN
      SELECT UNNEST(keyNames) AS key, UNNEST(newValues) AS value
    LOOP
      RAISE INFO 'UPDATE % SET % = % WHERE ARRAY[%] = ARRAY[%];', tableName, r.key, r.value, ARRAY_TO_STRING(keyNames, ','), ARRAY_TO_STRING(currentValues, ',');
      --EXECUTE 'UPDATE ' || tableName ||
      --  ' SET ' || r.key || ' = ' || r.value ||
      --  ' WHERE ARRAY[' || ARRAY_TO_STRING(keyNames, ',') || ']::BIGINT[] = ARRAY[' || ARRAY_TO_STRING(currentValues, ',') || ']::BIGINT[]';
    END LOOP;

    checkedTablesKeys := ARRAY[NULL]::TEXT[];

    FOR r IN
      SELECT  c.constraint_name,
              c.local_table,
              c.foreign_table,
              ARRAY_AGG(l.attname) AS local_keys,
              STRING_AGG(l.attname, ',') AS local_keys_string,
              ARRAY_AGG(f.attname) AS foreign_keys,
              STRING_AGG(f.attname, ',') AS foreign_keys_string,
              c.local_table || c.constraint_name AS keys
      FROM (
        SELECT  DISTINCT
                c.conname AS constraint_name,
                c.conrelid::regclass AS local_table,
                c.confrelid::regclass AS foreign_table,
                UNNEST(c.conkey) AS local_keys,
                UNNEST(c.confkey) AS foreign_keys
      FROM      pg_constraint c
      WHERE     c.confrelid = tableName::regclass
      ) AS c
      JOIN    pg_attribute l ON l.attnum = c.local_keys
      AND     l.attrelid = c.local_table
      JOIN    pg_attribute f ON f.attnum = c.foreign_keys
      AND     f.attrelid = c.foreign_table
      GROUP BY 1, 2, 3
      HAVING  STRING_AGG(f.attname, ',') LIKE '%matricula%vin_codigo%'
  LOOP
    EXECUTE 'SELECT 1'
      ' FROM ' || r.local_table ||
      ' WHERE ARRAY[' || r.local_keys_string || ']::TEXT[] @> ARRAY[' || ARRAY_TO_STRING(currentValues, ',') || ']::TEXT[]'
    INTO checkReferenced;
  
      IF NOT COALESCE(checkReferenced, FALSE) THEN
        CONTINUE;
      END IF;
  
      IF ARRAY[r.keys] <@ checkedTablesKeys THEN
        CONTINUE;
      END IF;
  
      checkedTablesKeys := array_append(checkedTablesKeys, r.keys);
  
      RAISE INFO 'ALTER TABLE % DROP CONSTRAINT %;', r.local_table, r.constraint_name;
      RAISE INFO 'ALTER TABLE % ADD CONSTRAINT % FOREIGN KEY (%) REFERENCES %(%);', r.local_table, r.constraint_name, r.local_keys_string, tableName, keyNames;
      --EXECUTE 'ALTER TABLE ' || r.local_table || ' DROP CONSTRAINT ' || r.constraint_name;
  
      --EXECUTE 'ALTER TABLE ' || r.local_table || ' ADD CONSTRAINT ' || r.constraint_name ||
      --  ' FOREIGN KEY (' || r.local_keys_string || ') REFERENCES ' || r.foreign_table ||
      --  ' (' || r.foreign_keys_string || ') ON UPDATE CASCADE';
  
      PERFORM pg_temp.update_referenced_tables(
            tableName         := r.local_table::TEXT,
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

SELECT    *
FROM      pg_temp.update_referenced_tables (
          tableName := 'agh.rap_servidores',
          keyNames := ARRAY['matricula', 'vin_codigo'],
          currentValues := ARRAY[2212655, 1],
          newValues := ARRAY[2212655, 955],
          starting := TRUE
          )
;
