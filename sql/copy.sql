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

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

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
6	test_6
7	test_7
8	test_8
9	test_9
10	test_10
11	test_11
12	test_12
13	test_13
14	test_14
15	test_15
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

SELECT * FROM x ORDER BY a;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT * FROM x ORDER BY a;

\c :provider_dsn

\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.x CASCADE;
$$);
