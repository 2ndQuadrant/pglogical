
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

SELECT slot_name, plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c :subscriber_dsn
SELECT * FROM pglogical.drop_subscription('test_subscription');
SELECT * FROM pglogical.drop_node(node_name := 'test_subscriber');

\c :provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

SELECT slot_name, plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;
