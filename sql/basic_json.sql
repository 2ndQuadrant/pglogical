\i sql/basic_setup.sql

-- Simple decode with text-format tuples
SELECT data
FROM pg_logical_slot_peek_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'proto_format', 'json',
	'no_txinfo', 't');

\i sql/basic_teardown.sql
