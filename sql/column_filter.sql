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

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);

\c :subscriber_dsn
-- create table on subscriber to receive replicated filtered data from provider
-- there are some extra columns too, and we omit 'other' as a non-replicated
-- table on upstream only.
CREATE TABLE public.basic_dml (
	id serial primary key,
	data text,
	something interval,
	subonly integer,
	subonly_def integer DEFAULT 99
);

SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

\c :provider_dsn

-- Fails: the column filter list must include the key
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true, columns := '{data, something}');

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

-- Fails: the column filter list may not include cols that are not in the table
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true, columns := '{data, something, nosuchcol}');

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

-- At provider, add table to replication set, with filtered columns
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true, columns := '{id, data, something}');

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);

SELECT id, data, something FROM basic_dml ORDER BY id;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

BEGIN;
SET LOCAL statement_timeout = '10s';
SELECT pglogical.wait_for_table_sync_complete('test_subscription', 'basic_dml');
COMMIT;

SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);

SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

-- data should get replicated to subscriber
SELECT id, data, something FROM basic_dml ORDER BY id;

\c :provider_dsn

-- Adding a table that's already selectively replicated fails
\set VERBOSITY terse
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true);
\set VERBOSITY default
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

-- So does trying to re-add to change the column set
\set VERBOSITY terse
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', synchronize_data := true, columns := '{id, data}');
\set VERBOSITY default
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;

-- Shouldn't be able to drop a replicated col in a rel
-- but due to RM#5916 you can
BEGIN;
ALTER TABLE public.basic_dml DROP COLUMN data;
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;
SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);
ROLLBACK;

-- Even when wrapped (RM#5916)
BEGIN;
SELECT pglogical.replicate_ddl_command($$
ALTER TABLE public.basic_dml DROP COLUMN data;
$$);
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;
SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);
ROLLBACK;

-- CASCADE should be allowed though
BEGIN;
ALTER TABLE public.basic_dml DROP COLUMN data CASCADE;
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;
SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;
ROLLBACK;

BEGIN;
SELECT pglogical.replicate_ddl_command($$
ALTER TABLE public.basic_dml DROP COLUMN data CASCADE;
$$);
SELECT nspname, relname, att_list, has_row_filter FROM pglogical.show_repset_table_info('basic_dml'::regclass, ARRAY['default']);
SELECT nspname, relname, set_name FROM pglogical.tables
WHERE relid = 'public.basic_dml'::regclass;
ROLLBACK;

-- We can drop a non-replicated col. We must not replicate this DDL because in
-- this case the downstream doesn't have the 'other' column and apply will
-- fail.
ALTER TABLE public.basic_dml DROP COLUMN other;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.basic_dml CASCADE;
$$);

