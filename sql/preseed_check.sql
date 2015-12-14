-- Verify data from preseed.sql has correctly been cloned
\c regression
\d some_local_tbl
SELECT * FROM some_local_tbl ORDER BY id;

\d some_local_tbl1
SELECT * FROM some_local_tbl1 ORDER BY id;

\d some_local_tbl2
SELECT * FROM some_local_tbl2 ORDER BY id;

\d some_local_tbl3
SELECT * FROM some_local_tbl3 ORDER BY id;

\c postgres

\d some_local_tbl
SELECT * FROM some_local_tbl ORDER BY id;

\d some_local_tbl1
SELECT * FROM some_local_tbl1 ORDER BY id;

\d some_local_tbl2
SELECT * FROM some_local_tbl2 ORDER BY id;

\d some_local_tbl3
SELECT * FROM some_local_tbl3 ORDER BY id;

\c regression
SELECT pglogical.replicate_ddl_command($$
        DROP TABLE public.some_local_tbl;
        DROP TABLE public.some_local_tbl1;
        DROP TABLE public.some_local_tbl2;
        DROP TABLE public.some_local_tbl3;
$$);
