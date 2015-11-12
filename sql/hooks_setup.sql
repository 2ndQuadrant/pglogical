CREATE EXTENSION pglogical_output_plhooks;

CREATE FUNCTION test_filter(relid regclass, action "char", nodeid text)
returns bool stable language plpgsql AS $$
BEGIN
	IF nodeid <> 'foo' THEN
	    RAISE EXCEPTION 'Expected nodeid <foo>, got <%>',nodeid;
	END IF;
	RETURN relid::regclass::text NOT LIKE '%_filter%';
END
$$;

CREATE FUNCTION test_action_filter(relid regclass, action "char", nodeid text)
returns bool stable language plpgsql AS $$
BEGIN
    RETURN action NOT IN ('U', 'D');
END
$$;

CREATE FUNCTION wrong_signature_fn(relid regclass)
returns bool stable language plpgsql as $$
BEGIN
END;
$$;

CREATE TABLE test_filter(id integer);
CREATE TABLE test_nofilt(id integer);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'pglogical_output');

INSERT INTO test_filter(id) SELECT generate_series(1,10);
INSERT INTO test_nofilt(id) SELECT generate_series(1,10);

DELETE FROM test_filter WHERE id % 2 = 0;
DELETE FROM test_nofilt WHERE id % 2 = 0;
UPDATE test_filter SET id = id*100 WHERE id = 5;
UPDATE test_nofilt SET id = id*100 WHERE id = 5;
