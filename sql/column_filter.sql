-- basic builtin datatypes
SELECT * FROM pglogical_regress_variables()
\gset

-- create and populate table at provider
\c :provider_dsn
CREATE TABLE public.basic_dml (
	id serial primary key,
	other integer,
	data text,
	something interval
);
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

\c :subscriber_dsn
-- create table on subscriber to receive replicated filtered data from provider
-- there are some extra columns too.
CREATE TABLE public.basic_dml (
	id serial primary key,
	data text,
	something interval,
	subonly integer,
	subonly_def integer DEFAULT 99
);
SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml', ARRAY['default']);

\c :provider_dsn
-- At provider, add table to replication set, with filtered columns
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true, columns := '{id, data, something}');
SELECT id, data, something FROM basic_dml ORDER BY id;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

DO $$
BEGIN
    FOR i IN 1..100 LOOP
        IF NOT EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status != 'r' AND sync_relname IN ('basic_dml')) THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);
-- data should get replicated to subscriber
SELECT id, data, something FROM basic_dml ORDER BY id;

-- Test for Table with oids
\c :provider_dsn
CREATE TABLE public.basic_oids_dml (
	id serial primary key,
	other integer,
	data text,
	something interval
) with oids ;

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_oids_dml', columns := '{oid, id, data, something}');

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_oids_dml', columns := '{id, data, something}');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

CREATE TABLE public.basic_oids_dml (
	id serial primary key,
	data text,
	something interval,
	subonly integer,
	subonly_def integer DEFAULT 99
) with oids;

\c :provider_dsn

-- check basic insert replication
INSERT INTO basic_oids_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

UPDATE basic_oids_dml SET other = '40', data = NULL, something = '3 days'::interval WHERE id = 4;

SELECT * from basic_oids_dml ORDER BY id;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT id, data, something FROM basic_oids_dml ORDER BY id;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.basic_dml CASCADE;
	DROP TABLE public.basic_oids_dml CASCADE;
$$);

