-- This should be done with pg_regress's --create-role option
-- but it's blocked by bug 37906
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SET client_min_messages = 'warning';
DROP USER IF EXISTS nonsuper;
DROP USER IF EXISTS super;

CREATE USER nonsuper WITH replication;
CREATE USER super SUPERUSER;

\c :subscriber_dsn
SET client_min_messages = 'warning';
DROP USER IF EXISTS nonsuper;
DROP USER IF EXISTS super;

CREATE USER nonsuper WITH replication;
CREATE USER super SUPERUSER;

-- Can't because of bug 37906
--GRANT ALL ON DATABASE regress TO nonsuper;
--GRANT ALL ON DATABASE regress TO nonsuper;

\c :provider_dsn
GRANT ALL ON SCHEMA public TO nonsuper;

DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') >= 1000 THEN
		CREATE OR REPLACE FUNCTION public.pg_current_xlog_location() RETURNS pg_lsn
		LANGUAGE SQL AS 'SELECT pg_current_wal_lsn()';
		ALTER FUNCTION public.pg_current_xlog_location() OWNER TO super;
	END IF;
END; $$;

\c :subscriber_dsn
GRANT ALL ON SCHEMA public TO nonsuper;
SELECT E'\'' || current_database() || E'\'' AS subdb;
\gset

\c :provider_dsn
SET client_min_messages = 'warning';

DO $$
BEGIN
        IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
                CREATE EXTENSION IF NOT EXISTS pglogical_origin;
        END IF;
END;$$;

DO $$
BEGIN
	IF version() ~ 'Postgres-XL' THEN
		CREATE EXTENSION IF NOT EXISTS pglogical;
	ELSE
		CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';
	END IF;
END;
$$;
ALTER EXTENSION pglogical UPDATE;

\dx pglogical

SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super');

\c :subscriber_dsn
SET client_min_messages = 'warning';

DO $$
BEGIN
        IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
                CREATE EXTENSION IF NOT EXISTS pglogical_origin;
        END IF;
END;$$;

CREATE EXTENSION IF NOT EXISTS pglogical;

SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := (SELECT subscriber_dsn FROM pglogical_regress_variables()) || ' user=super');

BEGIN;
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := true,
	forward_origins := '{}');
/*
 * Remove the function we added in preseed because otherwise the restore of
 * schema will fail. We do this in same transaction as create_subscription()
 * because the subscription process will only start on commit.
 */
DROP FUNCTION IF EXISTS public.pglogical_regress_variables();
COMMIT;

BEGIN;
SET LOCAL statement_timeout = '30s';
SELECT pglogical.wait_for_subscription_sync_complete('test_subscription');
COMMIT;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('y', 'r') FROM pglogical.local_sync_status ORDER BY 2,3,4;

-- Make sure we see the slot and active connection
\c :provider_dsn
SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;
