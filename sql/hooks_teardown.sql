SELECT 'drop' FROM pg_drop_replication_slot('regression_slot');

DROP TABLE test_filter;
DROP TABLE test_nofilt;

DROP FUNCTION test_filter(relid regclass, action "char", nodeid text);
DROP FUNCTION test_action_filter(relid regclass, action "char", nodeid text);
DROP FUNCTION wrong_signature_fn(relid regclass);

DROP EXTENSION pglogical_output_plhooks;
