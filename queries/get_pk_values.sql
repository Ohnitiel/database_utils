-- Function to get primary key values for a specific row
CREATE OR REPLACE FUNCTION get_primary_key_values(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_where_columns TEXT[],
    p_where_values TEXT[]
)
RETURNS TEXT[] AS $$
DECLARE
    v_pk_columns TEXT[];
    v_pk_values TEXT[];
    v_sql TEXT;
    v_where TEXT;
    i INTEGER;
BEGIN
    RAISE NOTICE '[LOG] Entering get_primary_key_values(p_schema_name:%, p_table_name:%, p_where_columns:%, p_where_values:%)', p_schema_name, p_table_name, p_where_columns, p_where_values;
    -- Get PK columns
    v_pk_columns := get_primary_key_columns(p_schema_name, p_table_name);
    
    IF v_pk_columns IS NULL THEN
        RAISE EXCEPTION 'No primary key found for table %.%', p_schema_name, p_table_name;
    END IF;
    
    -- Build WHERE clause
    v_where := '';
    FOR i IN 1..array_length(p_where_columns, 1) LOOP
        IF i > 1 THEN
            v_where := v_where || ' AND ';
        END IF;
        v_where := v_where || quote_ident(p_where_columns[i]) || '::TEXT = ' || quote_literal(p_where_values[i]);
    END LOOP;
    
    -- Build SELECT to get PK values
    v_sql := 'SELECT ARRAY[' || array_to_string(
        (SELECT array_agg(quote_ident(col) || '::TEXT') FROM unnest(v_pk_columns) col),
        ', '
    ) || '] FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) ||
    ' WHERE ' || v_where || ' LIMIT 1';
    
    RAISE NOTICE '[LOG] get_primary_key_values: Executing SQL: %', v_sql;
    
    EXECUTE v_sql INTO v_pk_values;
    
    RETURN v_pk_values;
END;
$$ LANGUAGE plpgsql;
