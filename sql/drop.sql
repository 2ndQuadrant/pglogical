SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c :subscriber_dsn
SELECT * FROM pglogical.drop_subscription('test_subscription');
SELECT * FROM pglogical.drop_node(node_name := 'test_subscriber');

\c :provider_dsn
SELECT * FROM pglogical.drop_node(node_name := 'test_provider');

\c :subscriber_dsn
DROP OWNED BY nonsuper, super CASCADE;

\c :provider_dsn
DROP OWNED BY nonsuper, super CASCADE;

\c :provider1_dsn
DROP OWNED BY nonsuper, super CASCADE;

\c :orig_provider_dsn
DROP OWNED BY nonsuper, super CASCADE;

\c :subscriber_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonsuper, super;

\c :provider_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonsuper, super;

\c :provider1_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonsuper, super;

\c :orig_provider_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonsuper, super;
