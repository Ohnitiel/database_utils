-- HELPER FUNCTIONS FOR SCRIPT GENERATION
CREATE OR REPLACE FUNCTION get_row_as_json(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_where_columns TEXT[],
    p_where_values TEXT[]
) RETURNS jsonb AS $$
DECLARE
    v_sql TEXT;
    v_where TEXT;
    v_row_json jsonb;
    i INTEGER;
BEGIN
    IF p_schema_name IS NULL OR p_table_name IS NULL THEN
        RAISE EXCEPTION 'get_row_as_json called with NULL schema or table name.';
    END IF;

    IF coalesce(array_length(p_where_columns, 1), 0) = 0 THEN
        RAISE WARNING 'get_row_as_json called with no columns for WHERE clause.';
        RETURN NULL;
    END IF;

    v_where := '';
    FOR i IN 1..array_length(p_where_columns, 1) LOOP
        IF p_where_columns[i] IS NULL THEN
            RAISE EXCEPTION 'NULL column name provided in where clause for get_row_as_json';
        END IF;
        
        IF i > 1 THEN v_where := v_where || ' AND '; END IF;

        IF p_where_values[i] IS NULL THEN
            v_where := v_where || quote_ident(p_where_columns[i]) || ' IS NULL';
        ELSE
            v_where := v_where || quote_ident(p_where_columns[i]) || '::TEXT = ' || quote_literal(p_where_values[i]);
        END IF;
    END LOOP;

    v_sql := 'SELECT row_to_json(t) FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) || ' t WHERE ' || v_where || ' LIMIT 1';
    
    RAISE NOTICE '[LOG] get_row_as_json: Executing SQL: %', v_sql;
    EXECUTE v_sql INTO v_row_json;
    RETURN v_row_json;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION json_to_insert_sql(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_row_json jsonb
) RETURNS TEXT AS $$
DECLARE
    v_columns TEXT;
    v_values TEXT;
BEGIN
    IF p_row_json IS NULL OR p_row_json::text = 'null' THEN
        RETURN '-- Unable to generate backup INSERT statement: row data not found.';
    END IF;

    SELECT string_agg(quote_ident(key), ', '), string_agg(quote_literal(value), ', ')
    INTO v_columns, v_values
    FROM jsonb_each_text(p_row_json);

    RETURN 'INSERT INTO ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) ||
           ' (' || v_columns || ') VALUES (' || v_values || ');';
END;
$$ LANGUAGE plpgsql;

-- Doesn't work properly yet, need to think a whole lot
CREATE OR REPLACE FUNCTION update_or_delete_with_cascade(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_key_columns TEXT[],
    p_key_values TEXT[],
    p_new_values TEXT[],
    p_dry_run BOOLEAN DEFAULT TRUE
)
RETURNS TABLE(
    operation_sql TEXT,
    backup_sql TEXT,
    status TEXT
) AS $$
DECLARE
    v_ref RECORD;
    v_has_violation BOOLEAN;
    v_pk_columns TEXT[];
    v_pk_values TEXT[];
    v_sql TEXT;
    v_where TEXT;
    v_set TEXT;
    i INTEGER;
    v_fk_constraint RECORD;
    v_constraints_disabled TEXT[] := ARRAY[]::TEXT[];
    v_row_json jsonb;
    v_backup_sql TEXT;
    v_backup_set TEXT;
    v_backup_where TEXT;
BEGIN
    RAISE NOTICE '[LOG] Entering update_or_delete_with_cascade(p_schema_name:%, p_table_name:%, p_key_columns:%, p_key_values:%, p_new_values:%, p_dry_run:%)', p_schema_name, p_table_name, p_key_columns, p_key_values, p_new_values, p_dry_run;
    
    -- Disable all FK constraints pointing to the parent table
    IF NOT p_dry_run THEN
        FOR v_fk_constraint IN
            SELECT 
                nf.nspname AS fk_schema,
                cf.relname AS fk_table,
                c.conname AS constraint_name
            FROM pg_constraint c
            JOIN pg_class cp ON c.confrelid = cp.oid
            JOIN pg_namespace np ON cp.relnamespace = np.oid
            JOIN pg_class cf ON c.conrelid = cf.oid
            JOIN pg_namespace nf ON cf.relnamespace = nf.oid
            WHERE c.contype = 'f'
            AND np.nspname = p_schema_name
            AND cp.relname = p_table_name
        LOOP
            v_sql := 'ALTER TABLE ' || quote_ident(v_fk_constraint.fk_schema) || '.' || 
                     quote_ident(v_fk_constraint.fk_table) || 
                     ' DISABLE TRIGGER ALL';
            IF v_sql IS NOT NULL AND v_sql <> '' THEN
                EXECUTE v_sql;
            END IF;
            
            v_constraints_disabled := array_append(v_constraints_disabled, 
                v_fk_constraint.fk_schema || '.' || v_fk_constraint.fk_table);
        END LOOP;
    END IF;

    -- No violation, so update the parent table.
    v_set := '';
    FOR i IN 1..array_length(p_key_columns, 1) LOOP
        IF i > 1 THEN v_set := v_set || ', '; END IF;
        v_set := v_set || quote_ident(p_key_columns[i]) || ' = ' || quote_literal(p_new_values[i]);
    END LOOP;

    v_where := '';
    FOR i IN 1..array_length(p_key_columns, 1) LOOP
        IF i > 1 THEN v_where := v_where || ' AND '; END IF;
        v_where := v_where || quote_ident(p_key_columns[i]) || '::TEXT = ' || quote_literal(p_key_values[i]);
    END LOOP;

    v_sql := 'UPDATE ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) || ' SET ' || v_set || ' WHERE ' || v_where;

    -- Generate backup UPDATE statement
    v_backup_set := '';
    FOR i IN 1..array_length(p_key_columns, 1) LOOP
        IF i > 1 THEN v_backup_set := v_backup_set || ', '; END IF;
        v_backup_set := v_backup_set || quote_ident(p_key_columns[i]) || ' = ' || quote_literal(p_key_values[i]);
    END LOOP;
    v_backup_where := '';
    FOR i IN 1..array_length(p_key_columns, 1) LOOP
        IF i > 1 THEN v_backup_where := v_backup_where || ' AND '; END IF;
        v_backup_where := v_backup_where || quote_ident(p_key_columns[i]) || '::TEXT = ' || quote_literal(p_new_values[i]);
    END LOOP;
    v_backup_sql := 'UPDATE ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) || ' SET ' || v_backup_set || ' WHERE ' || v_backup_where;

    IF NOT p_dry_run THEN
        IF v_sql IS NOT NULL AND v_sql <> '' THEN EXECUTE v_sql; END IF;
    END IF;

    RETURN QUERY SELECT v_sql, v_backup_sql, CASE WHEN p_dry_run THEN 'DRY RUN' ELSE 'EXECUTED' END;
    
    -- Now process child references for cascading update
    FOR v_ref IN
        SELECT * FROM find_key_references(p_schema_name, p_table_name, p_key_columns, p_key_values)
        WHERE level_depth > 0
        ORDER BY level_depth ASC, schema_name, table_name
    LOOP
        -- Check if update would violate constraints on the child table
        v_has_violation := check_constraint_violation(v_ref.schema_name, v_ref.table_name, v_ref.column_names, p_new_values, v_ref.column_names, v_ref.key_values);

        IF v_has_violation THEN
            -- Instead of raising an exception, delete the violating child record
            v_where := '';
            FOR i IN 1..array_length(v_ref.column_names, 1) LOOP
                IF i > 1 THEN v_where := v_where || ' AND '; END IF;
                v_where := v_where || quote_ident(v_ref.column_names[i]) || '::TEXT = ' || quote_literal(v_ref.key_values[i]);
            END LOOP;

            -- Get the row data for backup before deleting
            v_row_json := get_row_as_json(v_ref.schema_name, v_ref.table_name, v_ref.column_names, v_ref.key_values);
            v_backup_sql := json_to_insert_sql(v_ref.schema_name, v_ref.table_name, v_row_json);

            v_sql := 'DELETE FROM ' || quote_ident(v_ref.schema_name) || '.' || quote_ident(v_ref.table_name) || ' WHERE ' || v_where;

            IF NOT p_dry_run THEN
                IF v_sql IS NOT NULL AND v_sql <> '' THEN EXECUTE v_sql; END IF;
            END IF;

            RETURN QUERY SELECT v_sql, v_backup_sql, CASE WHEN p_dry_run THEN 'DELETE DRY RUN (constraint violation)' ELSE 'DELETED (constraint violation)' END;
        ELSE
            -- Update the foreign key reference in the child table (original behavior)
            v_set := '';
            FOR i IN 1..array_length(v_ref.column_names, 1) LOOP
                IF i > 1 THEN v_set := v_set || ', '; END IF;
                v_set := v_set || quote_ident(v_ref.column_names[i]) || ' = ' || quote_literal(p_new_values[i]);
            END LOOP;

            v_where := '';
            FOR i IN 1..array_length(v_ref.column_names, 1) LOOP
                IF i > 1 THEN v_where := v_where || ' AND '; END IF;
                v_where := v_where || quote_ident(v_ref.column_names[i]) || '::TEXT = ' || quote_literal(v_ref.key_values[i]);
            END LOOP;

            v_sql := 'UPDATE ' || quote_ident(v_ref.schema_name) || '.' || quote_ident(v_ref.table_name) || ' SET ' || v_set || ' WHERE ' || v_where;

            -- Generate backup UPDATE statement for child
            v_backup_set := '';
            FOR i IN 1..array_length(v_ref.column_names, 1) LOOP
                IF i > 1 THEN v_backup_set := v_backup_set || ', '; END IF;
                v_backup_set := v_backup_set || quote_ident(v_ref.column_names[i]) || ' = ' || quote_literal(v_ref.key_values[i]);
            END LOOP;
            v_backup_where := '';
            FOR i IN 1..array_length(v_ref.column_names, 1) LOOP
                IF i > 1 THEN v_backup_where := v_backup_where || ' AND '; END IF;
                v_backup_where := v_backup_where || quote_ident(v_ref.column_names[i]) || '::TEXT = ' || quote_literal(p_new_values[i]);
            END LOOP;
            v_backup_sql := 'UPDATE ' || quote_ident(v_ref.schema_name) || '.' || quote_ident(v_ref.table_name) || ' SET ' || v_backup_set || ' WHERE ' || v_backup_where;

            IF NOT p_dry_run THEN
                IF v_sql IS NOT NULL AND v_sql <> '' THEN EXECUTE v_sql; END IF;
            END IF;

            RETURN QUERY SELECT v_sql, v_backup_sql, CASE WHEN p_dry_run THEN 'DRY RUN' ELSE 'EXECUTED' END;
        END IF;
    END LOOP;
    
    -- Re-enable all FK constraints
    IF NOT p_dry_run THEN
        FOR i IN 1..array_length(v_constraints_disabled, 1) LOOP
            v_sql := 'ALTER TABLE ' || v_constraints_disabled[i] || ' ENABLE TRIGGER ALL';
            IF v_sql IS NOT NULL AND v_sql <> '' THEN
                EXECUTE v_sql;
            END IF;
        END LOOP;
    END IF;
END $$ LANGUAGE plpgsql;
