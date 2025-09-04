 SELECT
  i.relname "Table Name"
 , indexrelname "Index Name"
 , pg_size_pretty(pg_total_relation_size(relid)) AS "Total Size"
 , pg_size_pretty(pg_indexes_size(relid)) AS "Total Size of all Indexes"
 , pg_size_pretty(pg_relation_size(relid)) AS "Table Size"
 , pg_size_pretty(pg_relation_size(indexrelid)) "Index Size"
 , reltuples::bigint "Estimated table row count"
FROM pg_stat_all_indexes i
JOIN pg_class c
  ON i.relid = c.oid 
--WHERE i.relname=''
ORDER BY pg_relation_size(indexrelid) DESC;
