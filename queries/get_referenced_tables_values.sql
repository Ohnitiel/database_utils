SELECT *
FROM pg_temp.database_comb(
  'agh.rap_servidores'
, ARRAY['matricula', 'vin_codigo']::TEXT[]
, ARRAY['9999999', '955']::TEXT[]
);


CREATE OR REPLACE FUNCTION pg_temp.database_comb(
    tableName TEXT
  , keyNames TEXT[]
  , lookup TEXT[]
) RETURNS TABLE (
    table_name TEXT
  , table_record JSONB
) LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
  existingReference BOOL := NULL;
  existsQuery TEXT;
  rowJSON JSONB;
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
  LOOP
    existsQuery = format(
      'SELECT 1 FROM %1s WHERE ARRAY[%2s]::TEXT[] @> ''%3s''::TEXT[]',
      r.local_table, r.local_keys_string, lookup
    );
    EXECUTE existsQuery INTO existingReference;

    existsQuery = format(
      'SELECT row_to_json(t.*)::JSONB FROM %1s AS t WHERE ARRAY[%2s]::TEXT[] @> ''%3s''::TEXT[]',
      r.local_table, r.local_keys_string, lookup
    );
    EXECUTE existsQuery INTO rowJSON;

    IF NOT COALESCE(existingReference, FALSE) THEN
      CONTINUE;
    END IF;

    RETURN QUERY
      SELECT
        r.local_table::TEXT
      , rowJSON;
  END LOOP;
END;
$$;
