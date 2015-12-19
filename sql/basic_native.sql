\i sql/basic_setup.sql

-- Simple decode with text-format tuples
--
-- It's still the logical decoding binary protocol and as such it has
-- embedded timestamps, and pglogical its self has embedded LSNs, xids,
-- etc. So all we can really do is say "yup, we got the expected number
-- of messages".
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1');

-- ... and send/recv binary format
-- The main difference visible is that the bytea fields aren't encoded
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'binary.want_binary_basetypes', '1',
	'binary.basetypes_major_version', (current_setting('server_version_num')::integer / 100)::text);

-- Now enable the relation metadata cache and verify that we get the expected
-- reduction in number of messages. Not much else we can look for.
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'relmeta_cache_size', '-1');

\i sql/basic_teardown.sql
