SET synchronous_commit = on;

-- Schema setup

CREATE TABLE demo (
	seq serial primary key,
	tx text,
	ts timestamp,
	jsb jsonb,
	js json,
	ba bytea
);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'pglogical_output');

-- Queue up some work to decode with a variety of types

INSERT INTO demo(tx) VALUES ('textval');
INSERT INTO demo(ba) VALUES (BYTEA '\xDEADBEEF0001');
INSERT INTO demo(ts, tx) VALUES (TIMESTAMP '2045-09-12 12:34:56.00', 'blah');
INSERT INTO demo(js, jsb) VALUES ('{"key":"value"}', '{"key":"value"}');

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

SELECT 'drop' FROM pg_drop_replication_slot('regression_slot');

DROP TABLE demo;
