/* First test whether a table's replication set can be properly manipulated */
\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE SCHEMA normalschema;
CREATE SCHEMA "strange.schema-IS";
CREATE TABLE public.test_publicschema(id serial primary key, data text);
CREATE TABLE normalschema.test_normalschema(id serial primary key);
CREATE TABLE "strange.schema-IS".test_strangeschema(id serial primary key);
CREATE TABLE public.test_nopkey(id int);
CREATE UNLOGGED TABLE public.test_unlogged(id int primary key);
$$);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- show initial replication sets
SELECT * FROM pglogical.tables WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

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
SELECT * FROM pglogical.alter_replication_set('repset_replicate_instrunc', replicate_update := true);
SELECT * FROM pglogical.alter_replication_set('repset_replicate_instrunc', replicate_delete := true);

-- check the replication sets
SELECT * FROM pglogical.tables WHERE relname IN ('test_publicschema', 'test_normalschema', 'test_strangeschema', 'test_nopkey') ORDER BY 1,2,3;

--too short
SELECT pglogical.create_replication_set('');

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_publicschema CASCADE;
	DROP SCHEMA normalschema CASCADE;
	DROP SCHEMA "strange.schema-IS" CASCADE;
$$);

\c postgres
SELECT * FROM pglogical.replication_set;
