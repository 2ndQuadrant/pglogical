SELECT * FROM pglogical_regress_variables()
\gset

\c :provider1_dsn
SET client_min_messages = 'warning';

GRANT ALL ON SCHEMA public TO nonsuper;

CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS $FUNC$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
		PERFORM pg_sleep(0.01);
	END LOOP;
END;$FUNC$ LANGUAGE plpgsql;


SET client_min_messages = 'warning';

DO $$
BEGIN
        IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
                CREATE EXTENSION IF NOT EXISTS pglogical_origin;
        END IF;
END;$$;

CREATE EXTENSION IF NOT EXISTS pglogical VERSION '2.0.0';
ALTER EXTENSION pglogical UPDATE;

SELECT * FROM pglogical.create_node(node_name := 'test_provider1', dsn := (SELECT provider1_dsn FROM pglogical_regress_variables()) || ' user=super');

\c :provider_dsn
-- add these entries to provider
SELECT pglogical.replicate_ddl_command($$
      CREATE TABLE public.multi_ups_tbl(id integer primary key, key text unique not null, data text);
$$);

INSERT INTO multi_ups_tbl VALUES(1, 'key1', 'data1');
INSERT INTO multi_ups_tbl VALUES(2, 'key2', 'data2');
INSERT INTO multi_ups_tbl VALUES(3, 'key3', 'data3');

SELECT * FROM pglogical.replication_set_add_table('default', 'multi_ups_tbl', true);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :provider1_dsn

-- add these entries to provider1
CREATE TABLE multi_ups_tbl(id integer primary key, key text unique not null, data text);
INSERT INTO multi_ups_tbl VALUES(4, 'key4', 'data4');
INSERT INTO multi_ups_tbl VALUES(5, 'key5', 'data5');
INSERT INTO multi_ups_tbl VALUES(6, 'key6', 'data6');

SELECT * FROM pglogical.replication_set_add_table('default', 'multi_ups_tbl');

\c :subscriber_dsn

-- We'll use the already existing pglogical node
-- notice synchronize_structure as false when table definition already exists
BEGIN;
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription1',
    provider_dsn := (SELECT provider1_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := false,
	forward_origins := '{}');

/*
 * Remove the function we added in preseed because otherwise the restore of
 * schema will fail. We do this in same transaction as create_subscription()
 * because the subscription process will only start on commit.
 */
DROP FUNCTION IF EXISTS public.pglogical_regress_variables();
COMMIT;

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating' and subscription_name = 'test_subscription1') THEN
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

SELECT * from multi_ups_tbl ORDER BY id;

-- Make sure we see the slot and active connection
\c :provider1_dsn
SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

-- cleanup
\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
        DROP TABLE public.multi_ups_tbl CASCADE;
$$);
