-- Function to check if update would violate constraints
CREATE OR REPLACE FUNCTION check_constraint_violation(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_update_columns TEXT[],
    p_new_values TEXT[],
    p_where_columns TEXT[],
    p_where_values TEXT[]
)
RETURNS BOOLEAN AS $$
DECLARE
    v_sql TEXT;
    v_exists BOOLEAN;
    i INTEGER;
    v_constraint_cols TEXT[];
    v_col_idx INTEGER;
    v_current_row_where TEXT := '';
    v_check_where TEXT;
    v_constraint_value TEXT;
BEGIN
    RAISE NOTICE '[LOG] Entering check_constraint_violation(p_schema_name:%, p_table_name:%, p_update_columns:%, p_new_values:%, p_where_columns:%, p_where_values:%)', p_schema_name, p_table_name, p_update_columns, p_new_values, p_where_columns, p_where_values;
    IF p_new_values IS NULL THEN
        RETURN TRUE; -- This signals a delete operation
    END IF;

    -- Build WHERE clause for the current row being updated
    FOR i IN 1..array_length(p_where_columns, 1) LOOP
        IF i > 1 THEN v_current_row_where := v_current_row_where || ' AND '; END IF;
        IF p_where_values[i] IS NULL THEN
            v_current_row_where := v_current_row_where || format('%I IS NULL', p_where_columns[i]);
        ELSE
            v_current_row_where := v_current_row_where || format('%I = %L', p_where_columns[i], p_where_values[i]);
        END IF;
    END LOOP;

    -- Check each unique/pk constraint
    FOR v_constraint_cols IN
        SELECT array_agg(a.attname::TEXT ORDER BY u.pos)
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, pos) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
        WHERE c.contype IN ('p', 'u')
        AND n.nspname = p_schema_name
        AND t.relname = p_table_name
        GROUP BY c.conname
    LOOP
        -- Check if any of the columns being updated are part of this constraint
        IF v_constraint_cols && p_update_columns THEN
            v_check_where := '';
            -- Build a check query to see if the new values would conflict
            FOR i IN 1..array_length(v_constraint_cols, 1) LOOP
                IF i > 1 THEN v_check_where := v_check_where || ' AND '; END IF;

                -- Check if this constraint column is being updated
                SELECT idx INTO v_col_idx
                FROM unnest(p_update_columns) WITH ORDINALITY AS t(col, idx)
                WHERE col = v_constraint_cols[i];

                IF v_col_idx IS NOT NULL THEN -- Column is being updated, use new value
                    v_constraint_value := p_new_values[v_col_idx];
                ELSE -- Column is not being updated, get its current value from the database
                    v_sql := format('SELECT %I FROM %I.%I WHERE %s', v_constraint_cols[i], p_schema_name, p_table_name, v_current_row_where);
                    EXECUTE v_sql INTO v_constraint_value;
                END IF;

                IF v_constraint_value IS NULL THEN
                    v_check_where := v_check_where || format('%I IS NULL', v_constraint_cols[i]);
                ELSE
                    v_check_where := v_check_where || format('%I = %L', v_constraint_cols[i], v_constraint_value);
                END IF;
            END LOOP;
            
            v_sql := format('SELECT EXISTS(SELECT 1 FROM %I.%I WHERE %s AND NOT (%s))', p_schema_name, p_table_name, v_check_where, v_current_row_where);
            
            EXECUTE v_sql INTO v_exists;
            
            IF v_exists THEN
                RETURN TRUE;  -- Constraint violation detected
            END IF;
        END IF;
    END LOOP;
    
    RETURN FALSE;  -- No violation
END;
$$ LANGUAGE plpgsql;