/* First test whether a table's replication set can be properly manipulated */
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.test_tbl(id serial primary key, data text);
CREATE MATERIALIZED VIEW public.test_mv AS (SELECT * FROM public.test_tbl);
$$);

SELECT * FROM pglogical.replication_set_add_all_tables('default', '{public}');

INSERT INTO test_tbl VALUES (1, 'a');

REFRESH MATERIALIZED VIEW test_mv;

INSERT INTO test_tbl VALUES (2, 'b');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

SELECT * FROM test_tbl;
SELECT * FROM test_mv;

\c :subscriber_dsn

SELECT * FROM test_tbl;
SELECT * FROM test_mv;

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_tbl CASCADE;
$$);
