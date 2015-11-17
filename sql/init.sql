-- This should be done with pg_regress's --create-role option
-- but it's blocked by bug 37906
SET client_min_messages = 'warning';
DROP USER IF EXISTS nonsuper;
DROP USER IF EXISTS super;

CREATE USER nonsuper WITH replication;
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

SELECT * FROM pglogical.create_provider(provider_name := 'test_provider');

\c postgres
CREATE EXTENSION pglogical;

SELECT * FROM pglogical.create_subscriber(
    subscriber_name := 'test_subscriber',
    local_dsn := 'dbname=postgres user=super',
    provider_name := 'test_provider',
    provider_dsn := 'dbname=regression user=super');

SELECT pglogical.wait_for_subscriber_ready('test_subscriber');

-- Make sure we see the slot and active connection
\c regression
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;
