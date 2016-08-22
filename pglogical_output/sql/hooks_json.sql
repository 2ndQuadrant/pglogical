\i sql/hooks_setup.sql


-- Test table filter
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
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.test_filter',
	'pglo_plhooks.client_hook_arg', 'foo',
	'proto_format', 'json',
	'no_txinfo', 't');

SELECT * FROM get_startup_params();
SELECT * FROM get_queued_data();

-- test action filter
TRUNCATE TABLE json_decoding_output;

INSERT INTO json_decoding_output (ch, rn)
SELECT
  data::jsonb,
  row_number() OVER ()
FROM pg_logical_slot_peek_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.test_action_filter',
	'proto_format', 'json',
	'no_txinfo', 't');

SELECT * FROM get_startup_params();
SELECT * FROM get_queued_data();

TRUNCATE TABLE json_decoding_output;

\i sql/hooks_teardown.sql
