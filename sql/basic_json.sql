\i sql/basic_setup.sql

-- Simple decode with text-format tuples
TRUNCATE TABLE json_decoding_output;

INSERT INTO json_decoding_output(ch, rn)
SELECT
  data::jsonb,
  row_number() OVER ()
FROM pg_logical_slot_peek_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'proto_format', 'json',
	'no_txinfo', 't');

SELECT * FROM get_startup_params();
SELECT * FROM get_queued_data();

TRUNCATE TABLE json_decoding_output;

\i sql/basic_teardown.sql
