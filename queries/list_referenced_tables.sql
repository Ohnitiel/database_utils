CREATE
OR        REPLACE FUNCTION pg_temp.list_referenced_tables (key INT) RETURNS TABLE (_table TEXT) LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT    tc.table_name AS local_table,
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
  AND       ccu.table_name = 'aip_pacientes'
  GROUP BY  tc.table_name,
            tc.constraint_name,
            ccu.table_name
  HAVING    ARRAY_TO_STRING(ARRAY_AGG(ccu.column_name::TEXT), ', ') ~~ 'codigo'
  LOOP
    RETURN QUERY EXECUTE E'SELECT \'' || r.local_table::TEXT || E'\'::TEXT'
      ' FROM agh.' || r.local_table::TEXT ||
      ' WHERE ' || r.local_keys || ' = ' || key::VARCHAR;
  END LOOP;
END;
$$
;

SELECT    *
FROM      pg_temp.list_referenced_tables ()
;
