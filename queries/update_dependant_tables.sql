SELECT    DISTINCT 
          'UPDATE agh.' || tc.table_name || ' tbl' || CHR(10) ||
          ' SET ' || kcu.column_name || ' = new_int' || CHR(10) ||
          ' WHERE ' || kcu.column_name || ' = old_int' || CHR(10) ||
          ' AND tbl.' || cl.column_name || ' > date_limit;'
FROM      information_schema.table_constraints AS tc
JOIN      information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
AND       tc.table_schema = kcu.table_schema
JOIN      information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
AND       ccu.table_schema = tc.table_schema
JOIN      information_schema.columns AS cl ON cl.table_name = tc.table_name
AND       cl.table_schema = tc.table_schema
WHERE     tc.constraint_type = 'FOREIGN KEY'
AND       ccu.table_schema = 'agh'
AND       ccu.table_name = :table_name
ORDER BY  1
;
