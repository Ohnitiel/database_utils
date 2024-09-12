SELECT
    c.relname AS "table",
    pg_size_pretty(pg_total_relation_size(c.oid)) AS "size"
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n
    ON c.relnamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
AND c.relkind <> 'i'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size(c.oid) DESC
LIMIT 10;
