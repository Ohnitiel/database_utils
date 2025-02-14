CREATE OR REPLACE FUNCTION pg_temp.cascade_delete(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_key_values JSONB
) RETURNS TABLE(
  table_name TEXT,
  delete_script TEXT,
  insert_scripts TEXT
) AS $$
/*
    Função: pg_temp.cascade_delete
    --------------------------------
    Esta função executa a exclusão em cascata de registros em um banco de dados PostgreSQL.
    Primeiro, ela identifica e faz backup dos registros afetados, garantindo que todas as
    tabelas filhas referenciadas por chaves estrangeiras sejam processadas recursivamente.
    Após o backup, os registros são excluídos seguindo a hierarquia das relações.

    Parâmetros:
    - p_schema_name: Nome do esquema onde a tabela se encontra.
    - p_table_name: Nome da tabela de onde a exclusão será iniciada.
    - p_key_values: Valores da chave primária do registro a ser excluído.

    Exemplos de uso:
    - SELECT * FROM pg_temp.cascade_delete('public', 'table_name', '{"id": 1}');
    - SELECT * FROM pg_temp.cascade_delete('public', 'table_name', '{"key": "string"}');
    - SELECT * FROM pg_temp.cascade_delete('public', 'table_name', '{"key1": 1, "key2": 2}');

    Retorno:
    - Um comando SQL contendo os INSERTs necessários para restaurar os dados excluídos.
*/
DECLARE
    pk_columns              TEXT[];         
    child_pk_columns        TEXT[];         
    fk_record               RECORD;         
    delete_query            TEXT;           
    key_conditions          TEXT;           
    backup_sql              TEXT    := '';          
    row_data                TEXT;           
    backup_query            TEXT;           
    quoted_table            TEXT;           
    child_key_values        JSONB;          
    child_key_values_query  TEXT;           
    child_key_value         JSONB;          
    child_backup_sql        TEXT;           
    affected_rows           INT;            
BEGIN
    -- Garante que o nome da tabela está no formato correto com schema
    quoted_table := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);

    -- Obtém as colunas da chave primária da tabela alvo
    SELECT array_agg(attname)
    INTO pk_columns
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.indisprimary AND n.nspname = p_schema_name AND c.relname = p_table_name;

    IF pk_columns IS NULL THEN
        RAISE EXCEPTION 'Nenhuma chave primária encontrada para a tabela %', p_table_name;
    END IF;

    -- Percorre as tabelas filhas que possuem chaves estrangeiras referenciando esta tabela
    FOR fk_record IN (
          SELECT    quote_ident(tc.table_schema) || '.' || quote_ident(tc.table_name) AS child_table,
                    quote_ident(tc.table_name) AS plain_child_table,
                    ARRAY_AGG(DISTINCT quote_ident(kcu.column_name)) AS fk_column,
                    STRING_AGG(DISTINCT kc.column_name || ' ' || kc.data_type, ', ') AS fk_column_type,
                    quote_ident(ccu.table_schema) || '.' || quote_ident(ccu.table_name) AS parent_table,
                    ARRAY_AGG(DISTINCT quote_ident(ccu.column_name)) AS parent_column
          FROM      information_schema.table_constraints tc
          JOIN      information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          AND       tc.table_schema = kcu.table_schema
          JOIN      information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
          AND       tc.table_schema = ccu.table_schema
          JOIN      information_schema.columns kc ON kc.table_name = ccu.table_name 
          AND       kc.column_name = ccu.column_name
          WHERE     tc.constraint_type = 'FOREIGN KEY'
          AND       ccu.table_name = p_table_name
          GROUP BY  1, 2, 5, kcu.constraint_name
    ) LOOP

        -- Cria a condição WHERE para encontrar registros filhos
        key_conditions := format(
          '(%3$s) IN (
            SELECT *
            FROM json_to_record(%1$L) AS x(%2$s)
          )', p_key_values, fk_record.fk_column_type, array_to_string(fk_record.fk_column, ', ')
        );

        -- Faz backup dos registros filhos antes da exclusão
        backup_query := format('SELECT array_to_json(array_agg(row_to_json(t))) FROM %s t WHERE %s', fk_record.child_table, key_conditions);
        EXECUTE backup_query INTO row_data;
        
        -- Se existir algum registro filho, executa a exclusão recursivamente
        IF row_data IS NOT NULL THEN

            SELECT array_agg(attname)
            INTO child_pk_columns
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary AND n.nspname = p_schema_name AND c.relname = fk_record.plain_child_table;

            child_key_values_query := format('SELECT array_to_json(array_agg(row_to_json(t))) FROM (SELECT %s FROM %s WHERE %s) t', array_to_string(child_pk_columns, ', '), fk_record.child_table, key_conditions);
            EXECUTE child_key_values_query INTO child_key_values;

            -- Executa a exclusão recursivamente
            IF child_key_values IS NOT NULL THEN
                FOR child_key_value IN 
                  EXECUTE format('SELECT * FROM jsonb_array_elements(%L)', child_key_values)
                LOOP
                  RETURN QUERY SELECT * from pg_temp.cascade_delete(p_schema_name, fk_record.plain_child_table, child_key_value);
                END LOOP;
            END IF;

        END IF;
    END LOOP;

    -- Cria a condição WHERE para encontrar o registro principal
    key_conditions := (
        SELECT string_agg(format('%I = %L', col, p_key_values->>col), ' AND ')
        FROM unnest(pk_columns) col
    );

    -- Faz backup do registro principal antes da exclusão
    backup_query := format('SELECT array_to_json(array_agg(row_to_json(t))) FROM %s t WHERE %s', quoted_table, key_conditions);
    EXECUTE backup_query INTO row_data;
    IF row_data IS NOT NULL THEN
        backup_sql := backup_sql || format('INSERT INTO %s SELECT * FROM jsonb_populate_recordset(NULL::%s, %L)' || CHR(10), quoted_table, quoted_table, row_data);
    END IF;

    -- Exclui o registro principal e retorna as linhas afetadas
    delete_query := format('DELETE FROM %s WHERE %s', quoted_table, key_conditions);
    EXECUTE delete_query;
    GET DIAGNOSTICS affected_rows = row_count;
    RAISE INFO 'DELETE FROM % WHERE %', quoted_table, key_conditions;
    RAISE INFO 'Deleted % rows', affected_rows;

    RETURN QUERY SELECT quoted_table, delete_query, backup_sql;
END;
$$ LANGUAGE plpgsql;
