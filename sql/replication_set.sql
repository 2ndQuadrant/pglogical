/* First test whether a table's replication set can be properly manipulated */
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
CREATE SCHEMA normalschema;
CREATE SCHEMA "strange.schema-IS";
CREATE TABLE public.test_publicschema(id serial primary key, data text);
CREATE TABLE normalschema.test_normalschema(id serial primary key);
CREATE TABLE "strange.schema-IS".test_strangeschema(id serial primary key);
CREATE TABLE public.test_nopkey(id int);
CREATE UNLOGGED TABLE public.test_unlogged(id int primary key);
$$);

SELECT nspname, relname, set_name FROM pglogical.tables
 WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

-- show initial replication sets
SELECT nspname, relname, set_name FROM pglogical.tables
 WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

-- not existing replication set
SELECT * FROM pglogical.replication_set_add_table('nonexisting', 'test_publicschema');

-- create some replication sets
SELECT * FROM pglogical.create_replication_set('repset_replicate_all');
SELECT * FROM pglogical.create_replication_set('repset_replicate_instrunc', replicate_update := false, replicate_delete := false);
SELECT * FROM pglogical.create_replication_set('repset_replicate_insupd', replicate_delete := false, replicate_truncate := false);

-- add tables
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_all', 'test_publicschema');
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_instrunc', 'normalschema.test_normalschema');
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_insupd', 'normalschema.test_normalschema');
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_insupd', '"strange.schema-IS".test_strangeschema');

-- should fail
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_all', 'test_unlogged');
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_all', 'test_nopkey');
-- success
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_instrunc', 'test_nopkey');
SELECT * FROM pglogical.alter_replication_set('repset_replicate_insupd', replicate_truncate := true);
-- fail again
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_insupd', 'test_nopkey');
SELECT * FROM pglogical.replication_set_add_all_tables('default', '{public}');
SELECT * FROM pglogical.alter_replication_set('repset_replicate_instrunc', replicate_update := true);
SELECT * FROM pglogical.alter_replication_set('repset_replicate_instrunc', replicate_delete := true);

-- Adding already-added fails
\set VERBOSITY terse
SELECT * FROM pglogical.replication_set_add_table('repset_replicate_all', 'public.test_publicschema');
\set VERBOSITY default

-- check the replication sets
SELECT nspname, relname, set_name FROM pglogical.tables
 WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

SELECT * FROM pglogical.replication_set_add_all_tables('default_insert_only', '{public}');

SELECT nspname, relname, set_name FROM pglogical.tables
 WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

--too short
SELECT pglogical.create_replication_set('');

-- Can't drop table while it's in a repset
DROP TABLE public.test_publicschema;

-- Can't drop table while it's in a repset
BEGIN;
SELECT pglogical.replicate_ddl_command($$
DROP TABLE public.test_publicschema;
$$);
ROLLBACK;

-- Can CASCADE though, even outside ddlrep
BEGIN;
DROP TABLE public.test_publicschema CASCADE;
ROLLBACK;

-- ... and can drop after repset removal
SELECT pglogical.replication_set_remove_table('repset_replicate_all', 'public.test_publicschema');
SELECT pglogical.replication_set_remove_table('default_insert_only', 'public.test_publicschema');
BEGIN;
DROP TABLE public.test_publicschema;
ROLLBACK;

\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_publicschema CASCADE;
	DROP SCHEMA normalschema CASCADE;
	DROP SCHEMA "strange.schema-IS" CASCADE;
	DROP TABLE public.test_nopkey CASCADE;
	DROP TABLE public.test_unlogged CASCADE;
$$);

\c :subscriber_dsn
SELECT * FROM pglogical.replication_set;
