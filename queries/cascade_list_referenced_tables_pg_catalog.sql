WITH RECURSIVE dependant_tables AS (
    SELECT  c.constraint_name,
            c.local_table,
            c.foreign_table,
            l.attname AS local_key
    FROM (
      SELECT  DISTINCT
              c.conname AS constraint_name,
              c.conrelid::regclass AS local_table,
              c.confrelid::regclass AS foreign_table,
              UNNEST(c.conkey) AS local_keys,
              UNNEST(c.confkey) AS foreign_keys
      FROM    pg_constraint c
      WHERE   c.confrelid = :table_name::regclass
    ) AS c
    JOIN    pg_attribute l ON l.attnum = c.local_keys
    AND     l.attrelid = c.local_table
    JOIN    pg_attribute f ON f.attnum = c.foreign_keys
    AND     f.attrelid = c.foreign_table
    UNION ALL
    SELECT  c.constraint_name,
            c.local_table,
            c.foreign_table,
            l.attname AS local_keys
    FROM (
      SELECT  DISTINCT
              c.conname AS constraint_name,
              c.conrelid::regclass AS local_table,
              c.confrelid::regclass AS foreign_table,
              UNNEST(c.conkey) AS local_keys,
              UNNEST(c.confkey) AS foreign_keys
      FROM    pg_constraint c
    ) AS c
    JOIN    pg_attribute l ON l.attnum = c.local_keys
    AND     l.attrelid = c.local_table
    JOIN    pg_attribute f ON f.attnum = c.foreign_keys
    AND     f.attrelid = c.foreign_table
    JOIN    dependant_tables d ON c.foreign_table = d.local_table
    AND     d.local_table <> c.local_table
    WHERE   l.attname ILIKE '%' || d.local_key || '%'
)
