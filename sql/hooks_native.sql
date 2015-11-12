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

SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.test_filter',
	'pglo_plhooks.client_hook_arg', 'foo'
	);

SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.test_action_filter'
	);

SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.nosuchfunction'
	);

SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.wrong_signature_fn'
	);


SELECT 'drop' FROM pg_drop_replication_slot('regression_slot');

DROP TABLE test_filter;
DROP TABLE test_nofilt;

DROP EXTENSION pglogical_output_plhooks;
