\i sql/hooks_setup.sql

-- Regular hook setup
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

-- Test action filter
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.test_action_filter'
	);

-- Invalid row fiter hook function
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.nosuchfunction'
	);

-- Hook filter functoin with wrong signature
SELECT count(data) FROM pg_logical_slot_peek_binary_changes('regression_slot',
	NULL, NULL,
	'expected_encoding', 'UTF8',
	'min_proto_version', '1',
	'max_proto_version', '1',
	'startup_params_format', '1',
	'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
	'pglo_plhooks.row_filter_hook', 'public.wrong_signature_fn'
	);

\i sql/hooks_teardown.sql
