
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_bidirectional',
    provider_dsn := (SELECT subscriber_dsn FROM pglogical_regress_variables()) || ' user=super',
    synchronize_structure := false,
    synchronize_data := false,
    forward_origins := '{}');

DO $$
BEGIN
    FOR i IN 1..100 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status = 'r') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

\c :subscriber_dsn
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

\c :provider_dsn

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml');

-- check basic insert replication
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn
SELECT id, other, data, something FROM basic_dml ORDER BY id;

UPDATE basic_dml SET other = id, something = something - '10 seconds'::interval WHERE id < 3;
UPDATE basic_dml SET other = id, something = something + '10 seconds'::interval WHERE id > 3;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :provider_dsn
SELECT id, other, data, something FROM basic_dml ORDER BY id;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
    DROP TABLE public.basic_dml CASCADE;
$$);

SELECT pglogical.drop_subscription('test_bidirectional');

\c :subscriber_dsn

SELECT count(1) FROM pg_stat_replication;
SELECT slot_name FROM pg_replication_slots;
