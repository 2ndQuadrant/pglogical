-- This should be done with pg_regress's --create-role option
-- but it's blocked by bug 37906
CREATE USER nonsuper;
CREATE USER super SUPERUSER;

-- Can't because of bug 37906
--GRANT ALL ON DATABASE regress TO nonsuper;
--GRANT ALL ON DATABASE regress TO nonsuper;

\c regression
GRANT ALL ON SCHEMA public TO nonsuper;

CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS $FUNC$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.flush_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
		PERFORM pg_sleep(0.01);
	END LOOP;
END;$FUNC$ LANGUAGE plpgsql;

\c postgres
GRANT ALL ON SCHEMA public TO nonsuper;

\c regression
CREATE EXTENSION pglogical;

INSERT INTO pglogical.local_node SELECT pglogical.create_node('node_reg', 'p', 'dbname=regression');
SELECT pglogical.create_node('node_pg', 's', 'dbname=postgres', 'dbname=regression');
SELECT pglogical.create_connection('node_reg', 'node_pg');

\c postgres
CREATE EXTENSION pglogical;

SELECT pglogical.create_node('node_reg', 'p', 'dbname=regression');
INSERT INTO pglogical.local_node SELECT pglogical.create_node('node_pg', 's', 'dbname=postgres', 'dbname=regression');
SELECT pglogical.create_connection('node_reg', 'node_pg');

SELECT pglogical.wait_for_node_ready();

-- Make sure we see the slot and active connection
\c regression
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;
