--SELECT * FROM agh.ain_internacoes WHERE seq = 2;
-- Create temp tables for tracking
CREATE TEMP TABLE IF NOT EXISTS _update_tracking(
  id SERIAL PRIMARY KEY,
  level INT DEFAULT 0,
  original_schema TEXT NOT NULL,
  original_table TEXT NOT NULL,
  original_pk_columns TEXT[],
  original_pk_values TEXT[],
  new_pk_values TEXT[],
  backup_sql TEXT,
  update_sql TEXT,
  status TEXT DEFAULT 'pending', -- pending, conflict, resolved, completed, failed
  conflict_details JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TEMP TABLE IF NOT EXISTS _conflict_resolutions(
  id SERIAL PRIMARY KEY,
  tracking_id INT REFERENCES _update_tracking(id),
  conflict_type TEXT, -- primary_key, unique_constraint, foreign_key
  constraint_name TEXT,
  conflicting_values JSON,
  resolution_sql TEXT,
  resolved_at TIMESTAMP
);

-- Helper function to find array position
CREATE OR REPLACE FUNCTION pg_temp.find_array_position(arr ANYARRAY, element ANYELEMENT)
RETURNS INT AS $FUNC$
DECLARE
  i INT;
BEGIN
  FOR i IN 1..array_length(arr, 1) LOOP
    IF arr[i] = element THEN
      RETURN i;
    END IF;
  END LOOP;
  RETURN NULL;
END;
$FUNC$ LANGUAGE plpgsql;

-- Main cascade update function WITH conflict detection
CREATE OR REPLACE FUNCTION pg_temp.cascade_update_with_conflict_detection(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_keycolumns TEXT[],
  p_currvalues TEXT[],
  p_newvalues TEXT[],
  p_level INT DEFAULT 0,
  p_force BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
  backup_script TEXT,
  update_script TEXT,
  affected_quantity INT,
  insert_order INT,
  status TEXT,
  conflict_details TEXT
) AS $FUNC$
DECLARE
  v_quoted_table TEXT := CONCAT_WS('.', quote_ident(p_schema_name), quote_ident(p_table_name));
  v_condition TEXT;
  v_backup_script TEXT;
  v_update_script TEXT;
  v_row_count INT;
  v_where_condition TEXT := '';
  v_has_conflict BOOLEAN := FALSE;
  v_conflict_details TEXT;
  v_conflict_type TEXT;
  v_conflicting_pk TEXT[];
  i INT;
  v_constraint_record RECORD;
  v_pos INT;
BEGIN
  -- Build WHERE condition
  FOR i IN 1..array_length(p_keycolumns, 1) LOOP
    IF i = 1 THEN
      v_where_condition := format('%I = %L', p_keycolumns[i], p_currvalues[i]);
    ELSE
      v_where_condition := v_where_condition || format(' AND %I = %L', p_keycolumns[i], p_currvalues[i]);
    END IF;
  END LOOP;
  
  v_condition := 'WHERE ' || v_where_condition;

  -- CHECK 1: Does the source row exist?
  EXECUTE format('SELECT COUNT(*) FROM %s WHERE %s', v_quoted_table, v_where_condition) INTO v_row_count;
  
  IF v_row_count = 0 THEN
    RETURN QUERY SELECT 
      ''::TEXT as backup_script, 
      ''::TEXT as update_script, 
      0::INT as affected_quantity, 
      p_level::INT as insert_order,
      'SKIPPED'::TEXT as status,
      'Source row does not exist'::TEXT as conflict_details;
    RETURN;
  END IF;

  -- CHECK 2: Does the new PK value already exist? (PRIMARY KEY CONFLICT)
  DECLARE
    v_new_values_list TEXT;
  BEGIN
    v_new_values_list := (SELECT string_agg(quote_literal(val), ', ') FROM unnest(p_newvalues) AS val);
    
    EXECUTE format(
      'SELECT EXISTS(SELECT 1 FROM %s WHERE (%s) = (%s))',
      v_quoted_table,
      array_to_string(p_keycolumns, ', '),
      v_new_values_list
    ) INTO v_has_conflict;
  END;
  
  IF v_has_conflict THEN
    -- Get the conflicting row's current values
    DECLARE
      v_col_list TEXT;
    BEGIN
      v_col_list := (SELECT string_agg(quote_ident(col), ', ') FROM unnest(p_keycolumns) AS col);
      
      EXECUTE format(
        'SELECT ARRAY[%s] FROM %s WHERE (%s) = (%s)',
        v_col_list,
        v_quoted_table,
        array_to_string(p_keycolumns, ', '),
        (SELECT string_agg(quote_literal(val), ', ') FROM unnest(p_newvalues) AS val)
      ) INTO v_conflicting_pk;
    END;
    
    v_conflict_details := format('Primary key conflict: New value (%s) already exists as row with PK (%s)',
      array_to_string(p_newvalues, ', '), array_to_string(v_conflicting_pk, ', '));
    v_conflict_type := 'primary_key';
    
    -- If forced, we need to handle this by cascading the conflict
    IF p_force THEN
      -- We need to update the existing row first to free up the PK
      -- This creates a chain of updates
      DECLARE
        v_temp_new_values TEXT[];
        v_temp_id TEXT;
      BEGIN
        -- Generate a temporary unique value
        v_temp_id := md5(random()::text || clock_timestamp()::text);
        
        -- First update the conflicting row to a temp value
        v_update_script := format(
          'UPDATE %s SET (%s) = (%s) WHERE (%s) = (%s)',
          v_quoted_table,
          array_to_string(p_keycolumns, ', '),
          quote_literal('TEMP_' || v_temp_id),
          array_to_string(p_keycolumns, ', '),
          (SELECT string_agg(quote_literal(val), ', ') FROM unnest(p_newvalues) AS val)
        );
        
        -- Store this as a separate update
        INSERT INTO _update_tracking(
          level, original_schema, original_table, 
          original_pk_columns, original_pk_values, new_pk_values,
          update_sql, status, conflict_details
        ) VALUES (
          p_level + 1, p_schema_name, p_table_name,
          p_keycolumns, p_newvalues, ARRAY['TEMP_' || v_temp_id],
          v_update_script, 'conflict_resolution',
          json_build_object('type', 'temp_move', 'original_target', array_to_json(p_newvalues))
        );
        
        -- Now we can proceed with our original update
        v_has_conflict := FALSE;
        v_conflict_details := 'Conflict resolved by temporary move';
      END;
    ELSE
      -- Not forced, return conflict
      RETURN QUERY SELECT 
        ''::TEXT as backup_script, 
        ''::TEXT as update_script, 
        0::INT as affected_quantity, 
        p_level::INT as insert_order,
        'CONFLICT'::TEXT as status,
        v_conflict_details::TEXT as conflict_details;
      RETURN;
    END IF;
  END IF;

  -- Generate backup script
  v_backup_script := format(
    'INSERT INTO %s SELECT * FROM %s WHERE %s;',
    v_quoted_table,
    v_quoted_table,
    v_where_condition
  );

  -- Get affected row count
  EXECUTE format(
    'SELECT COUNT(*) FROM %s %s',
    v_quoted_table,
    v_condition
  ) INTO v_row_count;

  -- Generate update script
  v_update_script := format(
    'UPDATE %s SET (%s) = (%s) WHERE %s',
    v_quoted_table,
    array_to_string(p_keycolumns, ', '),
    (SELECT string_agg(quote_literal(val), ', ') FROM unnest(p_newvalues) AS val),
    v_where_condition
  );

  -- Store in tracking table
  INSERT INTO _update_tracking(
    level, original_schema, original_table, 
    original_pk_columns, original_pk_values, new_pk_values,
    backup_sql, update_sql, status, conflict_details
  ) VALUES (
    p_level, p_schema_name, p_table_name,
    p_keycolumns, p_currvalues, p_newvalues,
    v_backup_script, v_update_script, 
    CASE WHEN v_has_conflict THEN 'conflict' ELSE 'pending' END,
    CASE WHEN v_conflict_details IS NOT NULL 
         THEN json_build_object('type', v_conflict_type, 'details', v_conflict_details)
         ELSE NULL END
  );

  -- If no conflicts, execute immediately
  IF NOT v_has_conflict THEN
    BEGIN
      EXECUTE v_update_script;
      UPDATE _update_tracking 
      SET status = 'completed' 
      WHERE id = currval('_update_tracking_id_seq');
      
      RETURN QUERY SELECT 
        v_backup_script::TEXT as backup_script,
        v_update_script::TEXT as update_script,
        v_row_count::INT as affected_quantity,
        p_level::INT as insert_order,
        'COMPLETED'::TEXT as status,
        ''::TEXT as conflict_details;
    EXCEPTION WHEN OTHERS THEN
      UPDATE _update_tracking 
      SET status = 'failed', conflict_details = json_build_object('error', SQLERRM)
      WHERE id = currval('_update_tracking_id_seq');
      
      RETURN QUERY SELECT 
        v_backup_script::TEXT as backup_script,
        v_update_script::TEXT as update_script,
        v_row_count::INT as affected_quantity,
        p_level::INT as insert_order,
        'FAILED'::TEXT as status,
        SQLERRM::TEXT as conflict_details;
    END;
  ELSE
    RETURN QUERY SELECT 
      v_backup_script::TEXT as backup_script,
      v_update_script::TEXT as update_script,
      v_row_count::INT as affected_quantity,
      p_level::INT as insert_order,
      'CONFLICT_PENDING'::TEXT as status,
      v_conflict_details::TEXT as conflict_details;
  END IF;
  
  RETURN;
END;
$FUNC$ LANGUAGE plpgsql;

-- Function to resolve conflicts automatically
CREATE OR REPLACE FUNCTION pg_temp.resolve_conflicts_automatically()
RETURNS TABLE (
  tracking_id INT,
  table_name TEXT,
  conflict_type TEXT,
  resolution TEXT,
  status TEXT
) AS $FUNC$
DECLARE
  v_record RECORD;
  v_resolution_sql TEXT;
  v_new_temp_value TEXT;
BEGIN
  FOR v_record IN 
    SELECT t.* 
    FROM _update_tracking t
    WHERE t.status IN ('conflict', 'conflict_pending')
    ORDER BY t.level DESC, t.id
  LOOP
    -- For primary key conflicts, we need to move the conflicting row
    IF v_record.conflict_details->>'type' = 'primary_key' THEN
      -- Generate a temporary unique value
      v_new_temp_value := 'TEMP_' || md5(random()::text || clock_timestamp()::text);
      
      -- Update the conflicting row to temporary value
      v_resolution_sql := format(
        'UPDATE %I.%I SET (%s) = (%s) WHERE (%s) = (%s)',
        v_record.original_schema,
        v_record.original_table,
        array_to_string(v_record.original_pk_columns, ', '),
        quote_literal(v_new_temp_value),
        array_to_string(v_record.original_pk_columns, ', '),
        (SELECT string_agg(quote_literal(val), ', ') FROM unnest(v_record.new_pk_values) AS val)
      );
      
      -- Execute the resolution
      BEGIN
        EXECUTE v_resolution_sql;
        
        -- Now execute the original update
        EXECUTE v_record.update_sql;
        
        -- Update status
        UPDATE _update_tracking 
        SET status = 'resolved' 
        WHERE id = v_record.id;
        
        -- Store resolution
        INSERT INTO _conflict_resolutions(tracking_id, conflict_type, resolution_sql, resolved_at)
        VALUES (v_record.id, 'primary_key', v_resolution_sql, CURRENT_TIMESTAMP);
        
        RETURN QUERY SELECT 
          v_record.id::INT as tracking_id,
          format('%s.%s', v_record.original_schema, v_record.original_table)::TEXT as table_name,
          'primary_key'::TEXT as conflict_type,
          ('Moved conflicting row to ' || v_new_temp_value)::TEXT as resolution,
          'RESOLVED'::TEXT as status;
      EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT 
          v_record.id::INT as tracking_id,
          format('%s.%s', v_record.original_schema, v_record.original_table)::TEXT as table_name,
          'primary_key'::TEXT as conflict_type,
          ('Failed: ' || SQLERRM)::TEXT as resolution,
          'FAILED'::TEXT as status;
      END;
    END IF;
  END LOOP;
END;
$FUNC$ LANGUAGE plpgsql;

-- Function to cascade updates through foreign keys
CREATE OR REPLACE FUNCTION pg_temp.cascade_update_through_fks(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_keycolumns TEXT[],
  p_currvalues TEXT[],
  p_newvalues TEXT[],
  p_level INT DEFAULT 0
) RETURNS void AS $FUNC$
DECLARE
  v_fk_record RECORD;
  v_child_condition TEXT;
  v_child_update_sql TEXT;
  i INT;
  j INT;
  v_found BOOLEAN;
  v_parent_idx INT;
  v_fk_columns TEXT[];
  v_parent_columns TEXT[];
BEGIN
  -- Find all child tables with foreign keys to this table
  FOR v_fk_record IN (
    SELECT
      tc.table_schema AS child_schema,
      tc.table_name AS child_table,
      rc.constraint_name AS fk_constraint_name,
      string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS fk_columns_str,
      string_agg(ccu.column_name, ',' ORDER BY ccu.ordinal_position) AS parent_columns_str
    FROM
      information_schema.referential_constraints rc
    JOIN
      information_schema.table_constraints tc ON tc.constraint_name = rc.constraint_name AND tc.constraint_schema = rc.constraint_schema
    JOIN
      information_schema.key_column_usage kcu ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema
    JOIN
      information_schema.key_column_usage ccu ON ccu.constraint_name = rc.unique_constraint_name AND ccu.constraint_schema = rc.unique_constraint_schema AND ccu.ordinal_position = kcu.ordinal_position
    WHERE
      ccu.table_schema = p_schema_name AND ccu.table_name = p_table_name
    GROUP BY
      tc.table_schema, tc.table_name, rc.constraint_name
  ) LOOP
    -- Convert to arrays
    v_fk_columns := string_to_array(v_fk_record.fk_columns_str, ',');
    v_parent_columns := string_to_array(v_fk_record.parent_columns_str, ',');
    
    -- Build condition for child table
    v_child_condition := '';
    
    FOR i IN 1..array_length(v_fk_columns, 1) LOOP
      -- Find which parent column this FK references
      v_found := FALSE;
      v_parent_idx := NULL;
      
      FOR j IN 1..array_length(p_keycolumns, 1) LOOP
        IF v_parent_columns[i] = p_keycolumns[j] THEN
          v_parent_idx := j;
          v_found := TRUE;
          EXIT;
        END IF;
      END LOOP;
      
      IF v_found AND v_parent_idx IS NOT NULL THEN
        IF i = 1 THEN
          v_child_condition := format('%I = %L', v_fk_columns[i], p_currvalues[v_parent_idx]);
        ELSE
          v_child_condition := v_child_condition || format(' AND %I = %L', v_fk_columns[i], p_currvalues[v_parent_idx]);
        END IF;
      END IF;
    END LOOP;
    
    IF v_child_condition != '' THEN
      -- Build update for child table
      DECLARE
        v_set_clause TEXT := '';
        v_new_values_list TEXT := '';
      BEGIN
        FOR i IN 1..array_length(v_fk_columns, 1) LOOP
          -- Find which parent column this FK references
          v_found := FALSE;
          v_parent_idx := NULL;
          
          FOR j IN 1..array_length(p_keycolumns, 1) LOOP
            IF v_parent_columns[i] = p_keycolumns[j] THEN
              v_parent_idx := j;
              v_found := TRUE;
              EXIT;
            END IF;
          END LOOP;
          
          IF i > 1 THEN
            v_set_clause := v_set_clause || ', ';
            v_new_values_list := v_new_values_list || ', ';
          END IF;
          
          v_set_clause := v_set_clause || quote_ident(v_fk_columns[i]);
          
          IF v_found AND v_parent_idx IS NOT NULL THEN
            v_new_values_list := v_new_values_list || quote_literal(p_newvalues[v_parent_idx]);
          ELSE
            v_new_values_list := v_new_values_list || quote_ident(v_fk_columns[i]);
          END IF;
        END LOOP;
        
        v_child_update_sql := format(
          'UPDATE %I.%I SET (%s) = (%s) WHERE %s',
          v_fk_record.child_schema,
          v_fk_record.child_table,
          v_set_clause,
          v_new_values_list,
          v_child_condition
        );
        
        -- Store child update
        INSERT INTO _update_tracking(
          level, original_schema, original_table, 
          update_sql, status
        ) VALUES (
          p_level + 1, v_fk_record.child_schema, v_fk_record.child_table,
          v_child_update_sql, 'pending'
        );
      END;
    END IF;
  END LOOP;
END;
$FUNC$ LANGUAGE plpgsql;

-- Master function to handle complete cascade update
CREATE OR REPLACE FUNCTION pg_temp.master_cascade_update(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_keycolumns TEXT[],
  p_currvalues TEXT[],
  p_newvalues TEXT[],
  p_resolve_conflicts BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
  step TEXT,
  details TEXT,
  step_status TEXT  -- Changed from 'status' to 'step_status' to avoid ambiguity
) AS $FUNC$
DECLARE
  v_step_result RECORD;
  v_main_update_step_status TEXT;
  v_main_update_details TEXT;
  v_current_step TEXT;
  v_current_details TEXT;
  v_current_status TEXT;
BEGIN
  -- Step 1: Cascade updates through foreign keys
  PERFORM pg_temp.cascade_update_through_fks(
    p_schema_name, p_table_name, p_keycolumns, p_currvalues, p_newvalues, 0
  );
  
  -- Return step 1 result
  v_current_step := 'FK_CASCADE';
  v_current_details := 'Cascaded to child tables';
  v_current_status := 'COMPLETED';
  
  step := v_current_step;
  details := v_current_details;
  step_status := v_current_status;
  RETURN NEXT;
  
  -- Step 2: Try the main update with conflict detection
  SELECT status, conflict_details INTO v_main_update_step_status, v_main_update_details
  FROM pg_temp.cascade_update_with_conflict_detection(
    p_schema_name, p_table_name, p_keycolumns, p_currvalues, p_newvalues, 0, p_resolve_conflicts
  ) LIMIT 1;
  
  v_current_step := 'MAIN_UPDATE';
  v_current_details := format('Updating %s.%s from %s to %s', 
                   p_schema_name, p_table_name, 
                   array_to_string(p_currvalues, ','), 
                   array_to_string(p_newvalues, ','));
  v_current_status := COALESCE(v_main_update_step_status, 'UNKNOWN');
  
  step := v_current_step;
  details := v_current_details;
  step_status := v_current_status;
  RETURN NEXT;
  
  -- Step 3: If conflicts and resolution requested, try to resolve
  IF p_resolve_conflicts AND EXISTS (
    SELECT 1 FROM _update_tracking WHERE status IN ('conflict', 'conflict_pending')
  ) THEN
    FOR v_step_result IN 
      SELECT * FROM pg_temp.resolve_conflicts_automatically()
    LOOP
      v_current_step := 'CONFLICT_RESOLUTION';
      v_current_details := format('%s: %s', v_step_result.table_name, v_step_result.resolution);
      v_current_status := v_step_result.status;
      
      step := v_current_step;
      details := v_current_details;
      step_status := v_current_status;
      RETURN NEXT;
    END LOOP;
  END IF;
  
  -- Step 4: Execute all pending updates
  DECLARE
    v_exec_record RECORD;
  BEGIN
    FOR v_exec_record IN 
      SELECT id, update_sql, format('%s.%s', original_schema, original_table) as table_name
      FROM _update_tracking 
      WHERE status = 'pending'
      ORDER BY level DESC, id
    LOOP
      BEGIN
        EXECUTE v_exec_record.update_sql;
        UPDATE _update_tracking SET status = 'completed' WHERE id = v_exec_record.id;
        
        v_current_step := 'EXECUTE_UPDATE';
        v_current_details := v_exec_record.table_name;
        v_current_status := 'SUCCESS';
        
        step := v_current_step;
        details := v_current_details;
        step_status := v_current_status;
        RETURN NEXT;
      EXCEPTION WHEN OTHERS THEN
        UPDATE _update_tracking SET status = 'failed' WHERE id = v_exec_record.id;
        
        v_current_step := 'EXECUTE_UPDATE';
        v_current_details := v_exec_record.table_name;
        v_current_status := 'FAILED: ' || SQLERRM;
        
        step := v_current_step;
        details := v_current_details;
        step_status := v_current_status;
        RETURN NEXT;
      END;
    END LOOP;
  END;
  
  -- Summary
  v_current_step := 'SUMMARY';
  v_current_details := format('Total operations: %s, Completed: %s, Failed: %s, Conflicts: %s',
                   (SELECT COUNT(*) FROM _update_tracking),
                   (SELECT COUNT(*) FROM _update_tracking WHERE status = 'completed'),
                   (SELECT COUNT(*) FROM _update_tracking WHERE status = 'failed'),
                   (SELECT COUNT(*) FROM _update_tracking WHERE status LIKE 'conflict%'));
  v_current_status := 'DONE';
  
  step := v_current_step;
  details := v_current_details;
  step_status := v_current_status;
  RETURN NEXT;
  
  RETURN;
END;
$FUNC$ LANGUAGE plpgsql;