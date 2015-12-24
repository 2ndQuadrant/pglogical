
SELECT * FROM pglogical_regress_variables();
\gset

\c :provider_dsn
SET client_min_messages = 'warning';
DROP ROLE IF EXISTS nonreplica;
CREATE USER nonreplica;
CREATE EXTENSION IF NOT EXISTS pglogical;
GRANT ALL ON SCHEMA pglogical TO nonreplica;
GRANT ALL ON ALL TABLES IN SCHEMA pglogical TO nonreplica;

\c :subscriber_dsn
SET client_min_messages = 'warning';
CREATE EXTENSION IF NOT EXISTS pglogical;

-- fail (local node not existing)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := 'dbname=regression user=nonreplica',
	forward_origins := '{}');

-- succeed
SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := 'dbname=postgres user=nonreplica');

-- fail (can't connect to remote)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := 'dbname=regression user=nonexisting',
	forward_origins := '{}');

-- fail (remote node not existing)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := 'dbname=regression user=nonreplica',
	forward_origins := '{}');

\c :provider_dsn
-- succeed
SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=nonreplica');

\c :subscriber_dsn

-- fail (can't connect with replication connection to remote)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := 'dbname=regression user=nonreplica',
	forward_origins := '{}');

-- cleanup

SELECT * FROM pglogical.drop_node('test_subscriber');

\c :provider_dsn
SELECT * FROM pglogical.drop_node('test_provider');

SET client_min_messages = 'warning';
DROP OWNED BY nonreplica;
DROP ROLE IF EXISTS nonreplica;
