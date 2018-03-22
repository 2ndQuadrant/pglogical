-- row based filtering
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
-- testing volatile sampling function in row_filter
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.test_tablesample (id int primary key, name text) WITH (fillfactor=10);
$$);
-- use fillfactor so we don't have to load too much data to get multiple pages
INSERT INTO test_tablesample
  SELECT i, repeat(i::text, 200) FROM generate_series(0, 9) s(i);

create or replace function funcn_get_system_sample_count(integer, integer) returns bigint as
$$ (SELECT count(*) FROM test_tablesample TABLESAMPLE SYSTEM ($1) REPEATABLE ($2)); $$
language sql volatile;

create or replace function funcn_get_bernoulli_sample_count(integer, integer) returns bigint as
$$ (SELECT count(*) FROM test_tablesample TABLESAMPLE BERNOULLI ($1) REPEATABLE ($2)); $$
language sql volatile;

SELECT * FROM pglogical.replication_set_add_table('default', 'test_tablesample', false, row_filter := $rf$id > funcn_get_system_sample_count(100, 3) $rf$);
SELECT * FROM pglogical.replication_set_remove_table('default', 'test_tablesample');
SELECT * FROM pglogical.replication_set_add_table('default', 'test_tablesample', true, row_filter := $rf$id > funcn_get_bernoulli_sample_count(10, 0) $rf$);

SELECT * FROM test_tablesample ORDER BY id limit 5;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

BEGIN;
SET LOCAL statement_timeout = '10s';
SELECT pglogical.wait_for_table_sync_complete('test_subscription', 'test_tablesample');
COMMIT;

SELECT sync_kind, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'test_tablesample';

SELECT * FROM test_tablesample ORDER BY id limit 5;

\c :provider_dsn
\set VERBOSITY terse
DROP FUNCTION funcn_get_system_sample_count(integer, integer);
DROP FUNCTION funcn_get_bernoulli_sample_count(integer, integer);
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_tablesample CASCADE;
$$);
