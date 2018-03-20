SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT * FROM pglogical.create_replication_set('parallel');

\c :subscriber_dsn

SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription_parallel',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	replication_sets := '{parallel,default}',
	forward_origins := '{}',
	synchronize_structure := false,
	synchronize_data := false
);

SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription_parallel',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	replication_sets := '{parallel}',
	forward_origins := '{}',
	synchronize_structure := false,
	synchronize_data := false
);

BEGIN;
SET LOCAL statement_timeout = '10s';
SELECT pglogical.wait_for_subscription_sync_complete('test_subscription_parallel');
COMMIT;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('y', 'r') FROM pglogical.local_sync_status ORDER BY 2,3,4;

SELECT * FROM pglogical.show_subscription_status();

-- Make sure we see the slot and active connection
\c :provider_dsn
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

SELECT pglogical.replicate_ddl_command($$
    CREATE TABLE public.basic_dml1 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
    CREATE TABLE public.basic_dml2 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml1');
SELECT * FROM pglogical.replication_set_add_table('parallel', 'basic_dml2');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

WITH one AS (
INSERT INTO basic_dml1(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL)
RETURNING *
)
INSERT INTO basic_dml2 SELECT * FROM one;

BEGIN;
UPDATE basic_dml1 SET other = id, something = something - '10 seconds'::interval WHERE id < 3;
DELETE FROM basic_dml2 WHERE id < 3;
COMMIT;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT * FROM basic_dml1;
SELECT * FROM basic_dml2;

\c :subscriber_dsn

SELECT * FROM basic_dml1;
SELECT * FROM basic_dml2;

SELECT pglogical.drop_subscription('test_subscription_parallel');

\c :provider_dsn
\set VERBOSITY terse
SELECT * FROM pglogical.drop_replication_set('parallel');

SELECT pglogical.replicate_ddl_command($$
    DROP TABLE public.basic_dml1 CASCADE;
    DROP TABLE public.basic_dml2 CASCADE;
$$);
