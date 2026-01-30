DO $$
  BEGIN
  
  SET client_min_messages TO 'INFO';
  CREATE SCHEMA IF NOT EXISTS prepare_migration;
  
  CREATE TABLE IF NOT EXISTS prepare_migration.object_dependencies(
    obj_name TEXT,
    obj_type TEXT,
    obj_table TEXT,
    obj_definition TEXT,
    restore_error TEXT
  );
  
  CREATE OR REPLACE FUNCTION handle_dependencies_ddl_drop(_ddl TEXT, _level INT DEFAULT 0)
  RETURNS VOID
  AS $FUNC$
  DECLARE
    err_msg TEXT;
    v_err_msg_array TEXT[];
  
    v_depending_def TEXT;
    v_depending_oid INT;
    v_depending_type TEXT;
    v_depending_name TEXT;
    v_depending_table TEXT;
    v_depending_drop_cmd TEXT;

    v_info_msg_prefix TEXT := 'INFO:handle_dependencies_ddl_drop';
    v_warn_msg_prefix TEXT := 'WARN:handle_dependencies_ddl_drop';
  BEGIN
    IF _level >= 100 THEN
      RAISE EXCEPTION 'Atingido limite de recursão (100) para esta função!';
    END IF;

    EXECUTE _ddl;
    RAISE INFO '%:% - Executado DDL: %', v_info_msg_prefix, NOW(), _ddl;
  EXCEPTION
    WHEN dependent_objects_still_exist THEN
      RAISE INFO '%:% - Encontrado objeto dependente. Realizando operações para manter objeto.', v_info_msg_prefix, NOW();

      GET STACKED DIAGNOSTICS err_msg := PG_EXCEPTION_DETAIL;

      v_err_msg_array := regexp_matches(err_msg, '([a-zA-Z]+)\s+("?[0-9a-zA-Z_.\s]+"?)');
      v_depending_type := v_err_msg_array[1];
      v_depending_name := v_err_msg_array[2];
      v_depending_table := (regexp_matches(err_msg, 'table ("?[0-9A-z_\.\s]+"?)'))[1];
    
      IF v_depending_type = 'constraint' THEN
        SELECT oid INTO v_depending_oid
        FROM pg_catalog.pg_constraint
        WHERE conname = v_depending_name;
  
        v_depending_def := FORMAT('ALTER TABLE %s ADD CONSTRAINT %s ', v_depending_table, v_depending_name);
        v_depending_def := v_depending_def || pg_get_constraintdef(v_depending_oid, TRUE);
  
        v_depending_drop_cmd := FORMAT('ALTER TABLE %s DROP CONSTRAINT %I;', v_depending_table, v_depending_name);
  
      ELSIF v_depending_type = 'view' THEN
        v_depending_def := FORMAT('CREATE OR REPLACE VIEW %s AS ', v_depending_name);
        v_depending_def := v_depending_def || pg_get_viewdef(v_depending_name, TRUE);
        v_depending_drop_cmd := FORMAT('DROP VIEW %s;', v_depending_name);
      END IF;
  
      INSERT INTO prepare_migration.object_dependencies(
        obj_name, obj_type, obj_table, obj_definition
      )
      VALUES (v_depending_name, v_depending_type, v_depending_table, v_depending_def);
      RAISE INFO '%:% - Registrado DDL do objeto (%) em prepare_migration.object_dependencies.', v_info_msg_prefix, NOW(), v_depending_name;
  
      PERFORM handle_dependencies_ddl_drop(v_depending_drop_cmd, _level + 1);
      PERFORM handle_dependencies_ddl_drop(_ddl);
    WHEN feature_not_supported THEN
      RAISE INFO '%:% - Encontrado objeto dependente. Realizando operações para manter objeto.', v_info_msg_prefix, NOW();

      GET STACKED DIAGNOSTICS err_msg := PG_EXCEPTION_DETAIL;

      v_err_msg_array := regexp_matches(err_msg, E'view\s+("?[0-9a-zA-Z_.\s]+"?)\s+depends');
      v_depending_type := 'view';
      v_depending_name := v_err_msg_array[1];
      v_depending_def := FORMAT('CREATE OR REPLACE VIEW %s AS ', v_depending_name);
      v_depending_def := v_depending_def || pg_get_viewdef(v_depending_name, TRUE);
      v_depending_drop_cmd := FORMAT('DROP VIEW %s;', v_depending_name);

      INSERT INTO prepare_migration.object_dependencies(
        obj_name, obj_type, obj_table, obj_definition
      )
      VALUES (v_depending_name, v_depending_type, v_depending_table, v_depending_def);
      RAISE INFO '%:% - Registrado DDL do objeto (%) em prepare_migration.object_dependencies.', v_info_msg_prefix, NOW(), v_depending_name;

      PERFORM handle_dependencies_ddl_drop(v_depending_drop_cmd, _level + 1);
      PERFORM handle_dependencies_ddl_drop(_ddl);
  END $FUNC$ LANGUAGE PLPGSQL;

  CREATE OR REPLACE FUNCTION restore_dependencies_ddl(_ddl TEXT, object_name TEXT, object_type TEXT)
  RETURNS BOOLEAN
  AS $FUNC$
  DECLARE
    err_msg TEXT;
  BEGIN
    EXECUTE _ddl;
    RAISE INFO 'INFO:restore_dependencies_ddl:% - Restaurado objeto %.', NOW(), object_name;
  
    DELETE FROM prepare_migration.object_dependencies
    WHERE obj_name = object_name AND obj_definition = _ddl AND obj_type = object_type;
  
    RETURN TRUE;
  EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS err_msg := MESSAGE_TEXT;

      RAISE WARNING 'WARN:restore_dependencies_ddl:% - Não foi possível restaura o objeto %. Erro: %', NOW(), object_name, err_msg;

      UPDATE prepare_migration.object_dependencies
      SET restore_error = err_msg
      WHERE obj_name = object_name AND obj_definition = _ddl AND obj_type = object_type;
  
      RETURN FALSE;
  END $FUNC$ LANGUAGE PLPGSQL;

  CREATE TABLE __testing__ (
    id INT, tt TEXT
  );
  
  CREATE OR REPLACE VIEW "HOW COME THIS IS ALLOWED" AS
  SELECT id FROM __testing__;

  PERFORM handle_dependencies_ddl_drop('ALTER TABLE __testing__ DROP COLUMN id;', 0);

  RAISE INFO 'INFO:main:% - Finalizado o tratamento de dependências.', NOW();
  RAISE INFO 'INFO:main:% - Iniciando a restauração de dependências.', NOW();

  PERFORM restore_dependencies_ddl(obj_definition, obj_name, obj_type) FROM prepare_migration.object_dependencies;

  IF NOT EXISTS (SELECT 1 FROM prepare_migration.object_dependencies) THEN
    DROP SCHEMA prepare_migration CASCADE;
  ELSE
    RAISE WARNING 'WARN:main:% - Existem objetos não recriados na migração! Verifique a tabela prepare_migration.object_dependencies.', NOW();
  END IF;
END $$;
