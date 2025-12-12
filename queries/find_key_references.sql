CREATE OR REPLACE FUNCTION find_key_references(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_key_columns TEXT[],
    p_key_values TEXT[]
)
RETURNS TABLE(
    level_depth INTEGER,
    schema_name TEXT,
    table_name TEXT,
    column_names TEXT[],
    key_values TEXT[],
    reference_path TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE fk_tree AS (
        -- Base case: the original table and key values
        SELECT
            0 AS level_depth,
            p_schema_name::TEXT AS schema_name,
            p_table_name::TEXT AS table_name,
            p_key_columns AS column_names,
            p_key_values AS key_values,
            (p_schema_name || '.' || p_table_name)::TEXT AS reference_path
        
        UNION ALL
        
        -- Recursive case: find tables that reference the current table
        SELECT
            ft.level_depth + 1,
            fk.foreign_schema::TEXT,
            fk.foreign_table::TEXT,
            fk.foreign_columns,
            fk.referenced_values,
            (ft.reference_path || ' -> ' || fk.foreign_schema || '.' || fk.foreign_table)::TEXT
        FROM fk_tree ft
        CROSS JOIN LATERAL (
            -- Find foreign key relationships
            SELECT
                nf.nspname AS foreign_schema,
                cf.relname AS foreign_table,
                array_agg(af.attname::TEXT ORDER BY u.pos) AS foreign_columns,
                array_agg(row_val.val ORDER BY u.pos) AS referenced_values
            FROM pg_constraint c
            JOIN pg_class cp ON c.confrelid = cp.oid
            JOIN pg_namespace np ON cp.relnamespace = np.oid
            JOIN pg_class cf ON c.conrelid = cf.oid
            JOIN pg_namespace nf ON cf.relnamespace = nf.oid
            JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, pos) ON true
            JOIN pg_attribute af ON af.attrelid = cf.oid AND af.attnum = u.attnum
            JOIN unnest(c.confkey) WITH ORDINALITY AS uc(attnum, pos) ON uc.pos = u.pos
            JOIN pg_attribute ap ON ap.attrelid = cp.oid AND ap.attnum = uc.attnum
            CROSS JOIN LATERAL (
                SELECT unnest(ft.key_values) AS val
                OFFSET u.pos - 1 LIMIT 1
            ) row_val
            WHERE c.contype = 'f'
            AND np.nspname = ft.schema_name
            AND cp.relname = ft.table_name
            AND ap.attname::TEXT = ANY(ft.column_names)
            GROUP BY nf.nspname, cf.relname
            HAVING array_agg(ap.attname::TEXT ORDER BY u.pos) = ft.column_names
        ) fk
        WHERE ft.level_depth < 10  -- Prevent infinite recursion
    )
    SELECT 
        ft.level_depth,
        ft.schema_name,
        ft.table_name,
        ft.column_names,
        ft.key_values,
        ft.reference_path
    FROM fk_tree ft
    ORDER BY ft.level_depth, ft.schema_name, ft.table_name;
END;
$$ LANGUAGE plpgsql;
