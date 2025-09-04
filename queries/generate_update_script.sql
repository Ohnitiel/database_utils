CREATE OR REPLACE FUNCTION pg_temp.generate_update_script(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_columns TEXT[],
    p_operators TEXT[],
    p_values TEXT[]
) RETURNS SETOF TEXT AS $$
DECLARE
    v_update_script TEXT;
    v_select_script TEXT;
    v_column_record RECORD;
    v_current_value TEXT;
    v_column_type TEXT;
    v_pk_columns TEXT[];
    v_pk_column TEXT;
    v_first_column BOOLEAN := TRUE;
    v_quoted_table TEXT;
    v_index INT;
BEGIN

    v_quoted_table := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);

    -- Get primary key columns for the table
    SELECT array_agg(attname)
    INTO v_pk_columns
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.indisprimary AND n.nspname = p_schema_name AND c.relname = p_table_name;

    v_select_script := E'SELECT ''UPDATE ' || v_quoted_table || E'\nSET\n';
    FOR v_index IN ARRAY_LOWER(p_columns, 1) .. ARRAY_UPPER(p_columns, 1)
    LOOP
      v_select_script := v_select_script || p_columns[v_index] || '='' || ' || p_columns[v_index] || ' || ''';
      IF v_index != ARRAY_UPPER(p_columns, 1) THEN
        v_select_script := v_select_script || E',\n';
      ELSE
        v_select_script := v_select_script || E'\n';
      END IF;
    END LOOP;

    v_select_script := v_select_script || E'WHERE 1 = 1\n';
    FOR v_index IN ARRAY_LOWER(v_pk_columns, 1) .. ARRAY_UPPER(v_pk_columns, 1) LOOP
      v_select_script := v_select_script || 'AND ' || v_pk_columns[v_index] || '='' || ' || v_pk_columns[v_index] || E' || \'\n';
    END LOOP;

    v_select_script := v_select_script || E';\n\'\n';
    v_select_script := v_select_script || 'FROM ' || v_quoted_table ||
    E' WHERE 1 = 1\n';

    FOR v_index IN ARRAY_LOWER(p_columns, 1) .. ARRAY_UPPER(p_columns, 1)
    LOOP
      v_select_script := v_select_script || ' AND ' ||
      p_columns[v_index] || p_operators[v_index] || p_values[v_index] || E'\n';
    END LOOP;
    v_select_script := v_select_script || E';\n';
    RETURN QUERY EXECUTE v_select_script;
END;
$$ LANGUAGE plpgsql;
