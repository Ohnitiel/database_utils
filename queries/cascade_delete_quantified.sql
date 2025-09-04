CREATE OR REPLACE FUNCTION pg_temp.cascade_delete_quantified(
  p_schema_name TEXT
, p_table_name TEXT
, p_condition TEXT
, p_level INT
) RETURNS TABLE (
  backup_script TEXT
, delete_script TEXT
, deleted_quantity INT
, insert_order INT
)
 AS $FUNC$
DECLARE
  v_quoted_table TEXT := CONCAT_WS(
    '.'
  , quote_ident(p_schema_name)
  , quote_ident(p_table_name)
  );
  v_pk_columns TEXT[];
  v_pk_column TEXT;
  v_formatted_pk_columns TEXT := '';
  v_pk_values TEXT[];
  v_fk_record RECORD;
  v_fk_condition TEXT;
  v_pk_values_query TEXT;
  v_fk_backup_query TEXT;
  v_backup TEXT;
  v_delete_script TEXT;
  v_row_count INT;
  v_level INT := p_level;
BEGIN
    SELECT array_agg(attname ORDER BY attname) INTO v_pk_columns
    FROM pg_index i
    JOIN pg_attribute a
      ON a.attrelid = i.indrelid
      AND a.attnum = ANY(i.indkey)
    JOIN pg_class c
      ON c.oid = i.indrelid
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE i.indisprimary
      AND n.nspname = p_schema_name
      AND c.relname = p_table_name;

    IF v_pk_columns IS NULL THEN
      EXECUTE format($$
        SELECT 'INSERT INTO %s
        SELECT * FROM JSONB_POPULATE_RECORDSET(NULL::%s, $JSON$' ||
          ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(t))) || '$JSON$);'
        FROM %s t %s$$
      , v_quoted_table
      , v_quoted_table
      , v_quoted_table
      , p_condition
      ) INTO v_backup;

      EXECUTE format(
        'SELECT COUNT(*)
        FROM %s %s'
      , v_quoted_table
      , p_condition
      ) INTO v_row_count;

      v_delete_script := format(
        'DELETE FROM %s.%s %s'
      , p_schema_name, p_table_name, p_condition
      );

      RETURN QUERY SELECT v_backup, v_delete_script, v_row_count, v_level;
      EXECUTE v_delete_script;
      RETURN;
    END IF;

    IF ARRAY_LENGTH(v_pk_columns, 1) > 1 THEN
      FOREACH v_pk_column IN ARRAY v_pk_columns LOOP
        IF v_pk_column = v_pk_columns[1] THEN
          v_formatted_pk_columns := v_formatted_pk_columns ||
            format($$'(''' || %s || ''','$$, v_pk_column);
        ELSIF v_pk_column = v_pk_columns[ARRAY_LENGTH(v_pk_columns, 1)] THEN
          v_formatted_pk_columns := v_formatted_pk_columns ||
            format($$'' || %s || ''')'$$, v_pk_column);
        ELSE
          v_formatted_pk_columns := v_formatted_pk_columns ||
            format($$'' || %s || ''','$$, v_pk_column);
        END IF;
      END LOOP;
    ELSE
      v_formatted_pk_columns := v_formatted_pk_columns ||
        format($$'''' || %s || ''''$$, ARRAY_TO_STRING(v_pk_columns, ''));
    END IF;

    v_pk_values_query := format(
      $$SELECT ARRAY_AGG(%s) AS pk_values FROM %s %s;$$
    , v_formatted_pk_columns
    , v_quoted_table
    , p_condition
    );

    EXECUTE v_pk_values_query INTO v_pk_values;

    IF v_pk_values IS NULL THEN
      RETURN;
    END IF;

    FOR v_fk_record IN (
      SELECT
        CONCAT_WS(
          '.'
        , quote_ident(tc.table_schema)
        , quote_ident(tc.table_name)
        ) AS child_table
      , quote_ident(tc.table_name) AS plain_child_table
      , ARRAY_AGG(DISTINCT quote_ident(kcu.column_name)) AS fk_column
      , ARRAY_AGG(DISTINCT quote_ident(ccu.column_name)) AS parent_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        AND tc.table_schema = ccu.table_schema
      JOIN information_schema.columns kc
        ON kc.table_name = ccu.table_name
        AND kc.column_name = ccu.column_name
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND kc.table_name = p_table_name
      GROUP BY 1, 2, kcu.constraint_name
    ) LOOP

      IF NOT EXISTS (
        SELECT format(
          'SELECT 1 FROM %s t WHERE (%s) IN (%s)'
        , v_fk_record.child_table
        , ARRAY_TO_STRING(v_fk_record.fk_column, ', ')
        , ARRAY_TO_STRING(v_pk_values, ', ')
        )
      ) THEN
        CONTINUE;
      END IF;

      v_fk_condition := format(
        'WHERE (%s) IN (%s)'
      , array_to_string(v_fk_record.fk_column, ', ')
      , ARRAY_TO_STRING(v_pk_values, ', ')
      );

      v_level := p_level + 1;
      RETURN QUERY SELECT * FROM pg_temp.cascade_delete_quantified(p_schema_name, v_fk_record.plain_child_table, v_fk_condition, v_level);
    END LOOP;

    EXECUTE format($$
      SELECT 'INSERT INTO %1$s
      SELECT * FROM JSONB_POPULATE_RECORDSET(NULL::%1$s, $JSON$' ||
        ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(t))) || '$JSON$);'
      FROM %1$s t %s$$
    , v_quoted_table
    , p_condition
    ) INTO v_backup;

    EXECUTE format(
      'SELECT COUNT(*) FROM %s %s'
    , v_quoted_table
    , p_condition
    ) INTO v_row_count;

    v_delete_script := format(
      'DELETE FROM %s.%s %s'
    , p_schema_name, p_table_name, p_condition
    );

    RETURN QUERY SELECT v_backup, v_delete_script, v_row_count, p_level;
    EXECUTE v_delete_script;
END;
$FUNC$ LANGUAGE PLPGSQL;
