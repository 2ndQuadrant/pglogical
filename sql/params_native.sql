SET synchronous_commit = on;

-- no need to CREATE EXTENSION as we intentionally don't have any catalog presence
-- Instead, just create a slot.

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'pglogical_output');

-- Minimal invocation with no data
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1');

--
-- Various invalid parameter combos:
--

-- Text mode is not supported for native protocol
SELECT data FROM pg_logical_slot_get_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1');

-- error, only supports proto v1
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '2',
	'max_proto_version', '1',
	'startup_params_format', '1');

-- error, only supports proto v1
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '2',
	'max_proto_version', '2',
	'startup_params_format', '1');

-- error, unrecognised startup params format
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '2');

-- Should be OK and result in proto version 1 selection, though we won't
-- see that here.
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '2',
	'startup_params_format', '1');

-- no such encoding / encoding mismatch
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'bork',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1');

-- Different spellings of encodings are OK too
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF-8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1');

-- bogus param format
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'proto_format', 'invalid');

-- native params format explicitly
SELECT data FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'proto_format', 'native');

-- relmeta cache with fixed size (not supported yet, so error)
SELECT count(data) FROM pg_logical_slot_get_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'relmeta_cache_size', '200');

SELECT 'drop' FROM pg_drop_replication_slot('regression_slot');
