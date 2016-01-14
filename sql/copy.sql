--test COPY
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
     CREATE TABLE public.x (
	a serial primary key,
	b int,
	c text not null default 'stuff',
	d text,
	e text
     );
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'x');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

COPY x (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY x (b, d) from stdin;
1	test_1
\.

COPY x (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

SELECT * FROM x ORDER BY a;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn
SELECT * FROM x ORDER BY a;
