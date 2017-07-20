SELECT * FROM pglogical_regress_variables()
\gset

\c :orig_provider_dsn
SET client_min_messages = 'warning';

GRANT ALL ON SCHEMA public TO nonsuper;

CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS $FUNC$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
		PERFORM pg_sleep(0.01);
	END LOOP;
END;$FUNC$ LANGUAGE plpgsql;

CREATE FUNCTION
pglogical_wait_slot_confirm_lsn(slotname name, target pg_lsn)
RETURNS void LANGUAGE c AS 'pglogical','pglogical_wait_slot_confirm_lsn';

SET client_min_messages = 'warning';

DO $$
BEGIN
        IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
                CREATE EXTENSION IF NOT EXISTS pglogical_origin;
        END IF;
END;$$;

CREATE EXTENSION IF NOT EXISTS pglogical VERSION '2.0.0';
ALTER EXTENSION pglogical UPDATE;

SELECT * FROM pglogical.create_node(node_name := 'test_orig_provider', dsn := (SELECT orig_provider_dsn FROM pglogical_regress_variables()) || ' user=super');

\c :provider_dsn
SET client_min_messages = 'warning';
-- test_provider pglogical node already exists here.

BEGIN;
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_orig_subscription',
    provider_dsn := (SELECT orig_provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := false,
	forward_origins := '{}');
COMMIT;

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating') THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status();

DO $$
BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status = 'r') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4;

-- Make sure we see the slot and active connection
\c :orig_provider_dsn
SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

-- Table that replicates from top level provider to mid-level pglogical node.

\c :orig_provider_dsn

SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.top_level_tbl (
		id serial primary key,
		other integer,
		data text,
		something interval
	);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'top_level_tbl');
INSERT INTO top_level_tbl(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

SELECT pglogical_wait_slot_confirm_lsn(NULL, NULL);

\c :provider_dsn
SELECT id, other, data, something FROM top_level_tbl ORDER BY id;

-- Table that replicates from top level provider to mid-level pglogical node.
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.mid_level_tbl (
		id serial primary key,
		other integer,
		data text,
		something interval
	);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'mid_level_tbl');
INSERT INTO mid_level_tbl(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

SELECT pglogical_wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT id, other, data, something FROM mid_level_tbl ORDER BY id;

-- drop the tables
\c :orig_provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.top_level_tbl CASCADE;
$$);

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.mid_level_tbl CASCADE;
$$);

