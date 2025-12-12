-- Function to get primary key columns for a table
CREATE OR REPLACE FUNCTION get_primary_key_columns(
    p_schema_name TEXT,
    p_table_name TEXT
)
RETURNS TEXT[] AS $$
DECLARE
    v_pk_columns TEXT[];
BEGIN
    RAISE NOTICE '[LOG] Entering get_primary_key_columns(p_schema_name:%, p_table_name:%)', p_schema_name, p_table_name;
    SELECT array_agg(a.attname::TEXT ORDER BY u.pos)
    INTO v_pk_columns
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, pos) ON true
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
    WHERE c.contype = 'p'
    AND n.nspname = p_schema_name
    AND t.relname = p_table_name
    GROUP BY c.conname;
    
    RETURN v_pk_columns;
END;
$$ LANGUAGE plpgsql;
