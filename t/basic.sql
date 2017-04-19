-- basic builtin datatypes
CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS $FUNC$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
                PERFORM pg_sleep(0.01);
        END LOOP;
END;$FUNC$ LANGUAGE plpgsql;

SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.basic_dml (
		id serial primary key,
		other integer,
		data text,
		something interval
	);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- check basic insert replication
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

SELECT id, other, data, something FROM basic_dml ORDER BY id;

