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

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT * FROM test_tbl ORDER BY id;
SELECT * FROM test_mv ORDER BY id;

\c :subscriber_dsn

SELECT * FROM test_tbl ORDER BY id;
SELECT * FROM test_mv ORDER BY id;

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
  CREATE UNIQUE INDEX ON public.test_mv(id);
$$);
INSERT INTO test_tbl VALUES (3, 'c');

REFRESH MATERIALIZED VIEW CONCURRENTLY test_mv;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

INSERT INTO test_tbl VALUES (4, 'd');

SELECT pglogical.replicate_ddl_command($$
  REFRESH MATERIALIZED VIEW public.test_mv;
$$);

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

INSERT INTO test_tbl VALUES (5, 'e');

SELECT pglogical.replicate_ddl_command($$
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.test_mv;
$$);

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT * FROM test_tbl ORDER BY id;
SELECT * FROM test_mv ORDER BY id;

\c :subscriber_dsn

SELECT * FROM test_tbl ORDER BY id;
SELECT * FROM test_mv ORDER BY id;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_tbl CASCADE;
$$);
