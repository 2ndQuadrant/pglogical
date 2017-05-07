-- row based filtering
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.basic_dml (
		id serial primary key,
		other integer,
		data text,
		"SomeThing" interval
	);
$$);

-- used to check if initial copy does row filtering
\COPY basic_dml FROM STDIN WITH CSV
5000,1,aaa,1 hour
5001,2,bbb,2 years
5002,3,ccc,3 minutes
5003,4,ddd,4 days
\.

-- create some functions:
CREATE FUNCTION funcn_add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

create function funcn_nochange(text) returns text
  as 'select $1 limit 1' language sql stable;

create or replace function funcn_get_curr_decade() returns integer as
$$ (SELECT EXTRACT(DECADE FROM NOW()):: integer); $$
language sql volatile;

-- we allow volatile functions, it's user's responsibility to not do writes
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := 'current_user = data');
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
-- fail -- subselect
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := '(SELECT count(*) FROM pg_class) > 1');
-- fail -- SELECT
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := 'SELECT true');
-- fail -- nonexisting column
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := 'foobar');
-- fail -- not coercable to bool
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := 'data');

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := $rf$id between 2 AND 4$rf$);
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := NULL);
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := $rf$id > funcn_add(1,2) $rf$);
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := $rf$data = funcn_nochange('baz') $rf$);
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, row_filter := $rf$ other > funcn_get_curr_decade()  $rf$);
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
-- use this filter for rest of the test
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', true, row_filter := $rf$id > 1 AND data IS DISTINCT FROM 'baz' AND data IS DISTINCT FROM 'bbb'$rf$);

-- fail, the membership in repset depends on data column
ALTER TABLE basic_dml DROP COLUMN data;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn

-- wait for the initial data to copy
DO $$
BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status != 'r') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

ALTER TABLE public.basic_dml ADD COLUMN subonly integer;
ALTER TABLE public.basic_dml ADD COLUMN subonly_def integer DEFAULT 99;

\c :provider_dsn

TRUNCATE basic_dml;

-- check basic insert replication
INSERT INTO basic_dml(other, data, "SomeThing")
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- update one row
\c :provider_dsn
UPDATE basic_dml SET other = '4', data = NULL, "SomeThing" = '3 days'::interval WHERE id = 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- update multiple rows
\c :provider_dsn
UPDATE basic_dml SET other = id, data = data || id::text;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

\c :provider_dsn
UPDATE basic_dml SET other = id, "SomeThing" = "SomeThing" - '10 seconds'::interval WHERE id < 3;
UPDATE basic_dml SET other = id, "SomeThing" = "SomeThing" + '10 seconds'::interval WHERE id > 3;
DELETE FROM basic_dml WHERE id = 3;
INSERT INTO basic_dml VALUES (3, 99, 'bazbaz', '2 years 1 hour'::interval);
INSERT INTO basic_dml VALUES (7, 100, 'bazbaz', '2 years 1 hour'::interval);
UPDATE basic_dml SET data = 'baz' WHERE id in (3,7);
-- This update would be filtered at subscriber
SELECT * from basic_dml ORDER BY id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn
SELECT id, other, data, "SomeThing", subonly, subonly_def FROM basic_dml ORDER BY id;

\c :provider_dsn
UPDATE basic_dml SET data = 'bar' WHERE id = 3;
-- This update would again start to be received at subscriber
DELETE FROM basic_dml WHERE data = 'baz';
-- Delete reaches the subscriber for a filtered row
INSERT INTO basic_dml VALUES (6, 100, 'baz', '2 years 1 hour'::interval);
-- insert would be filtered
SELECT * from basic_dml ORDER BY id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn
SELECT id, other, data, "SomeThing", subonly, subonly_def FROM basic_dml ORDER BY id;

\c :provider_dsn
UPDATE basic_dml SET data = 'bar' WHERE id = 6;
UPDATE basic_dml SET data = 'abcd' WHERE id = 6;
-- These updates would continue to be missed on subscriber
-- as it does not have the primary key
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- delete multiple rows
\c :provider_dsn
DELETE FROM basic_dml WHERE id < 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- truncate
\c :provider_dsn
TRUNCATE basic_dml;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- copy
\c :provider_dsn
\COPY basic_dml FROM STDIN WITH CSV
9000,1,aaa,1 hour
9001,2,bbb,2 years
9002,3,ccc,3 minutes
9003,4,ddd,4 days
\.
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.test_jsonb (
		json_type text primary key,
		test_json jsonb
	);
$$);

INSERT INTO test_jsonb VALUES
('scalar','"a scalar"'),
('array','["zero", "one","two",null,"four","five", [1,2,3],{"f1":9}]'),
('object','{"field1":"val1","field2":"val2","field3":null, "field4": 4, "field5": [1,2,3], "field6": {"f1":9}}');

SELECT * FROM pglogical.replication_set_add_table('default', 'test_jsonb', true, row_filter := $rf$test_json ->> 'field2' IS DISTINCT FROM 'val2' $rf$);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn

SELECT * FROM test_jsonb ORDER BY json_type;

\c :provider_dsn
\set VERBOSITY terse
DROP FUNCTION funcn_add(integer, integer);
DROP FUNCTION funcn_nochange(text);
DROP FUNCTION funcn_get_curr_decade();
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.basic_dml CASCADE;
	DROP TABLE public.test_jsonb CASCADE;
$$);
