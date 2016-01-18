
SELECT * FROM pglogical_regress_variables()
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
\set VERBOSITY terse
CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';
ALTER EXTENSION pglogical UPDATE;

-- fail (local node not existing)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=nonreplica',
	forward_origins := '{}');

-- succeed
SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := (SELECT subscriber_dsn FROM pglogical_regress_variables()) || ' user=nonreplica');

-- fail (can't connect to remote)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=nonexisting',
	forward_origins := '{}');

-- fail (remote node not existing)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=nonreplica',
	forward_origins := '{}');

\c :provider_dsn
-- succeed
SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=nonreplica');

\c :subscriber_dsn
\set VERBOSITY terse

-- fail (can't connect with replication connection to remote)
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=nonreplica',
	forward_origins := '{}');

-- cleanup

SELECT * FROM pglogical.drop_node('test_subscriber');
DROP EXTENSION pglogical;

\c :provider_dsn
SELECT * FROM pglogical.drop_node('test_provider');

SET client_min_messages = 'warning';
DROP OWNED BY nonreplica;
DROP ROLE IF EXISTS nonreplica;
DROP EXTENSION pglogical;
