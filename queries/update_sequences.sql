CREATE OR REPLACE FUNCTION update_sequences (_schema varchar)
RETURNS VOID
AS $$
DECLARE
	_max INT;
	_table VARCHAR;
	_sequence VARCHAR;
BEGIN
	FOR _table, _sequence IN (
	SELECT
		t.oid::regclass AS table_name,
		s.relname AS sequence_name
	FROM pg_class AS t
	JOIN pg_attribute AS a ON a.attrelid = t.oid
	JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
	JOIN pg_class AS s ON s.oid = d.objid
	WHERE
		d.classid = 'pg_catalog.pg_class'::regclass
		AND d.refclassid = 'pg_catalog.pg_class'::regclass
		AND d.deptype IN ('i', 'a')
		AND t.relkind IN ('r', 'P')
		AND s.relkind = 'S'
		AND s.relname IN (
		SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema = _schema
		)
	)
	LOOP
		EXECUTE 'SELECT MAX(id) FROM ' || _table INTO _max;
		EXECUTE 'ALTER SEQUENCE ' || _sequence || ' RESTART WITH ' || _max;
	END LOOP;
END;
$$ LANGUAGE plpgsql;
