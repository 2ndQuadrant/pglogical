-- row based filtering
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.basic_dml (
		id serial primary key,
		other integer,
		data text,
		"SomeThing" interval,
		insert_xid bigint DEFAULT txid_current()
	);
$$);

-- used to check if initial copy does row filtering
\COPY basic_dml(id, other, data, "SomeThing") FROM STDIN WITH CSV
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

SELECT nspname, relname, set_name FROM pglogical.tables WHERE relname = 'basic_dml';

-- fail, the membership in repset depends on data column
\set VERBOSITY terse
ALTER TABLE basic_dml DROP COLUMN data;
\set VERBOSITY default

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

-- wait for the initial data to copy
BEGIN;
SET LOCAL statement_timeout = '10s';
SELECT pglogical.wait_for_subscription_sync_complete('test_subscription');
COMMIT;

SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

ALTER TABLE public.basic_dml ADD COLUMN subonly integer;
ALTER TABLE public.basic_dml ADD COLUMN subonly_def integer DEFAULT 99;
ALTER TABLE public.basic_dml ADD COLUMN subonly_def_ts timestamptz DEFAULT current_timestamp;

\c :provider_dsn

TRUNCATE basic_dml;

-- check basic insert replication
INSERT INTO basic_dml(other, data, "SomeThing")
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- update one row
\c :provider_dsn
UPDATE basic_dml SET other = '4', data = NULL, "SomeThing" = '3 days'::interval WHERE id = 4;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- update multiple rows
\c :provider_dsn
UPDATE basic_dml SET other = id, data = data || id::text;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
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
SELECT id, other, data, "SomeThing" from basic_dml ORDER BY id;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT id, other, data, "SomeThing", subonly, subonly_def FROM basic_dml ORDER BY id;

\c :provider_dsn
UPDATE basic_dml SET data = 'bar' WHERE id = 3;
-- This update would again start to be received at subscriber
DELETE FROM basic_dml WHERE data = 'baz';
-- Delete reaches the subscriber for a filtered row
INSERT INTO basic_dml VALUES (6, 100, 'baz', '2 years 1 hour'::interval);
-- insert would be filtered
SELECT id, other, data, "SomeThing" from basic_dml ORDER BY id;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT id, other, data, "SomeThing", subonly, subonly_def FROM basic_dml ORDER BY id;

\c :provider_dsn
UPDATE basic_dml SET data = 'bar' WHERE id = 6;
UPDATE basic_dml SET data = 'abcd' WHERE id = 6;
-- These updates would continue to be missed on subscriber
-- as it does not have the primary key
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- transaction timestamp should be updated for each row (see #148)
SELECT count(DISTINCT subonly_def_ts) = count(DISTINCT insert_xid) FROM basic_dml;

-- delete multiple rows
\c :provider_dsn
DELETE FROM basic_dml WHERE id < 4;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- truncate
\c :provider_dsn
TRUNCATE basic_dml;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT id, other, data, "SomeThing" FROM basic_dml ORDER BY id;

-- copy
\c :provider_dsn
\COPY basic_dml(id, other, data, "SomeThing") FROM STDIN WITH CSV
9000,1,aaa,1 hour
9001,2,bbb,2 years
9002,3,ccc,3 minutes
9003,4,ddd,4 days
\.
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
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

SELECT * FROM pglogical.replication_set_add_table('default', 'test_jsonb', true, row_filter := $rf$(test_json ->> 'field2') IS DISTINCT FROM 'val2' $rf$);

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

DO $$
BEGIN
    FOR i IN 1..100 LOOP
        IF NOT EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status != 'r') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT * FROM test_jsonb ORDER BY json_type;

\c :provider_dsn

-- Filter may refer to not-replicated columns
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false, columns := ARRAY['id', 'data'], row_filter := $rf$other = 2$rf$);

INSERT INTO basic_dml(other, data, "SomeThing") VALUES (2, 'itstwo', '1 second'::interval);

SELECT other, data, "SomeThing" FROM basic_dml WHERE data = 'itstwo';

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

-- 'other' will be NULL as it wasn't in the repset
-- even though we filtered on it. So will SomeThing.
SELECT other, data, "SomeThing" FROM basic_dml WHERE data = 'itstwo';

\c :provider_dsn

---------------------------------------------------
-- Enhanced function tests covering basic plpgsql
---------------------------------------------------

CREATE FUNCTION func_plpgsql_simple(arg integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN arg;
END;
$$;

SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ func_plpgsql_simple(other) = 100 $rf$);

-- Should FAIL due to dependency
--
-- FIXME: Succeeds incorrectly (RM#5880) leading to
--     cache lookup failed for function" errors in logs if allowed to commit
--
BEGIN;
DROP FUNCTION func_plpgsql_simple(integer);
ROLLBACK;

INSERT INTO basic_dml (other) VALUES (100), (101);
SELECT other FROM basic_dml WHERE other IN (100,101);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT other FROM basic_dml WHERE other IN (100,101);

\c :provider_dsn

CREATE FUNCTION func_plpgsql_logic(arg integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
BEGIN
  IF arg = 200 THEN
    RETURN arg;
  ELSE
    RETURN 0;
  END IF;
END;
$$;

SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ func_plpgsql_logic(other) = other $rf$);

INSERT INTO basic_dml (other) VALUES (200), (201);
SELECT other FROM basic_dml WHERE other IN (200,201);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT other FROM basic_dml WHERE other IN (200,201);

\c :provider_dsn


CREATE FUNCTION func_plpgsql_security_definer(arg integer)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RAISE NOTICE 'c_u: %, s_u: %', current_user, session_user;
  RETURN arg;
END;
$$;
CREATE ROLE temp_owner;
ALTER FUNCTION func_plpgsql_security_definer(integer) OWNER TO temp_owner;

SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ func_plpgsql_security_definer(other) = 300 $rf$);

INSERT INTO basic_dml (other) VALUES (300), (301);
SELECT other FROM basic_dml WHERE other IN (300,301);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT other FROM basic_dml WHERE other IN (300,301);

\c :provider_dsn

CREATE FUNCTION func_plpgsql_exception(arg integer)
RETURNS integer
LANGUAGE plpgsql
AS $$
BEGIN
  BEGIN
    SELECT arg/0;
  EXCEPTION
    WHEN division_by_zero THEN
      RETURN arg;
  END;
  RAISE EXCEPTION 'should be unreachable';
END;
$$;

SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ func_plpgsql_exception(other) = 400 $rf$);

INSERT INTO basic_dml (other) VALUES (400), (401);
SELECT other FROM basic_dml WHERE other IN (400,401);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT other FROM basic_dml WHERE other IN (400,401);

\c :provider_dsn

-- Should not be able to use a SETOF or TABLE func directly
-- but we can do it via a wrapper:
CREATE FUNCTION func_plpgsql_srf_retq(arg integer)
RETURNS TABLE (result integer, dummy boolean)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY SELECT arg * x, true FROM generate_series(1,2) x;
  RETURN;
END;
$$;

-- fails with SRF context error
BEGIN;
SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ (func_plpgsql_srf_retq(other)).result = 500 $rf$);
ROLLBACK;

CREATE FUNCTION func_plpgsql_call_set(arg integer)
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN (SELECT true FROM func_plpgsql_srf_retq(arg) WHERE result = arg * 2);
END;
$$;

SELECT * FROM pglogical.replication_set_remove_table('default', 'basic_dml');
SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml', false,
	row_filter := $rf$ func_plpgsql_call_set(other) $rf$);

INSERT INTO basic_dml (other) VALUES (500), (501);
SELECT other FROM basic_dml WHERE other IN (500,501);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT other FROM basic_dml WHERE other IN (500,501);

\c :provider_dsn
DROP FUNCTION func_plpgsql_simple(integer);
DROP FUNCTION func_plpgsql_logic(integer);
DROP FUNCTION func_plpgsql_security_definer(integer);
DROP FUNCTION func_plpgsql_exception(integer);
DROP FUNCTION func_plpgsql_srf_retq(integer);
DROP FUNCTION func_plpgsql_call_set(integer);
DROP ROLE temp_owner;

---------------------------------------------------
-- ^^^ End plpgsql tests
---------------------------------------------------

\c :provider_dsn
\set VERBOSITY terse
DROP FUNCTION funcn_add(integer, integer);
DROP FUNCTION funcn_nochange(text);
DROP FUNCTION funcn_get_curr_decade();

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.basic_dml CASCADE;
	DROP TABLE public.test_jsonb CASCADE;
$$);
