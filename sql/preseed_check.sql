-- Verify data from preseed.sql has correctly been cloned
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl1'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl1 ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl2'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl2 ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl3'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl3 ORDER BY id;

\c :subscriber_dsn

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl1'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl1 ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl2'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl2 ORDER BY id;

SELECT attname, attnotnull, attisdropped from pg_attribute where attrelid = 'some_local_tbl3'::regclass and attnum > 0 order by attnum;
SELECT * FROM some_local_tbl3 ORDER BY id;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP SEQUENCE public.some_local_seq;
	DROP TABLE public.some_local_tbl;
	DROP TABLE public.some_local_tbl1;
	DROP TABLE public.some_local_tbl2;
	DROP TABLE public.some_local_tbl3;
$$);
