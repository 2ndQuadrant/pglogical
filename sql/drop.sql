
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c :provider1_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider1');

SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c :orig_provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_orig_provider');

SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c :subscriber_dsn
SELECT * FROM pglogical.drop_subscription('test_subscription');
SELECT * FROM pglogical.drop_subscription('test_subscription1');
SELECT * FROM pglogical.drop_node(node_name := 'test_subscriber');

\c :provider_dsn
SELECT * FROM pglogical.drop_subscription('test_orig_subscription');
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

\c :provider1_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider1');

\c :orig_provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_orig_provider');

SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;
