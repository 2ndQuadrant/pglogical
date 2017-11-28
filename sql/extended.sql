-- complex datatype handling
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.tst_one_array (
		a INTEGER PRIMARY KEY,
		b INTEGER[]
		);
	CREATE TABLE public.tst_arrays (
		a INTEGER[] PRIMARY KEY,
		b TEXT[],
		c FLOAT[],
		d INTERVAL[]
		);

	CREATE TYPE public.tst_enum_t AS ENUM ('a', 'b', 'c', 'd', 'e');
	CREATE TABLE public.tst_one_enum (
		a INTEGER PRIMARY KEY,
		b public.tst_enum_t
		);
	CREATE TABLE public.tst_enums (
		a public.tst_enum_t PRIMARY KEY,
		b public.tst_enum_t[]
		);

	CREATE TYPE public.tst_comp_basic_t AS (a FLOAT, b TEXT, c INTEGER);
	CREATE TYPE public.tst_comp_enum_t AS (a FLOAT, b public.tst_enum_t, c INTEGER);
	CREATE TYPE public.tst_comp_enum_array_t AS (a FLOAT, b public.tst_enum_t[], c INTEGER);
	CREATE TABLE public.tst_one_comp (
		a INTEGER PRIMARY KEY,
		b public.tst_comp_basic_t
		);
	CREATE TABLE public.tst_comps (
		a public.tst_comp_basic_t PRIMARY KEY,
		b public.tst_comp_basic_t[]
		);
	CREATE TABLE public.tst_comp_enum (
		a INTEGER PRIMARY KEY,
		b public.tst_comp_enum_t
		);
	CREATE TABLE public.tst_comp_enum_array (
		a public.tst_comp_enum_t PRIMARY KEY,
		b public.tst_comp_enum_t[]
		);
	CREATE TABLE public.tst_comp_one_enum_array (
		a INTEGER PRIMARY KEY,
		b public.tst_comp_enum_array_t
		);
	CREATE TABLE public.tst_comp_enum_what (
		a public.tst_comp_enum_array_t PRIMARY KEY,
		b public.tst_comp_enum_array_t[]
		);

	CREATE TYPE public.tst_comp_mix_t AS (
		a public.tst_comp_basic_t,
		b public.tst_comp_basic_t[],
		c public.tst_enum_t,
		d public.tst_enum_t[]
		);
	CREATE TABLE public.tst_comp_mix_array (
		a public.tst_comp_mix_t PRIMARY KEY,
		b public.tst_comp_mix_t[]
		);
	CREATE TABLE public.tst_range (
		a INTEGER PRIMARY KEY,
		b int4range
	);
	CREATE TABLE public.tst_range_array (
		a INTEGER PRIMARY KEY,
		b TSTZRANGE,
		c int8range[]
	);
$$);

SELECT * FROM pglogical.replication_set_add_all_tables('default', '{public}');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

-- test_tbl_one_array_col
INSERT INTO tst_one_array (a, b) VALUES
    (1, '{1, 2, 3}'),
    (2, '{2, 3, 1}'),
    (3, '{3, 2, 1}'),
    (4, '{4, 3, 2}'),
    (5, '{5, NULL, 3}');

-- test_tbl_arrays
INSERT INTO tst_arrays (a, b, c, d) VALUES
    ('{1, 2, 3}', '{"a", "b", "c"}', '{1.1, 2.2, 3.3}', '{"1 day", "2 days", "3 days"}'),
    ('{2, 3, 1}', '{"b", "c", "a"}', '{2.2, 3.3, 1.1}', '{"2 minutes", "3 minutes", "1 minute"}'),
    ('{3, 1, 2}', '{"c", "a", "b"}', '{3.3, 1.1, 2.2}', '{"3 years", "1 year", "2 years"}'),
    ('{4, 1, 2}', '{"d", "a", "b"}', '{4.4, 1.1, 2.2}', '{"4 years", "1 year", "2 years"}'),
    ('{5, NULL, NULL}', '{"e", NULL, "b"}', '{5.5, 1.1, NULL}', '{"5 years", NULL, NULL}');

-- test_tbl_single_enum
INSERT INTO tst_one_enum (a, b) VALUES
    (1, 'a'),
    (2, 'b'),
    (3, 'c'),
    (4, 'd'),
    (5, NULL);

-- test_tbl_enums
INSERT INTO tst_enums (a, b) VALUES
    ('a', '{b, c}'),
    ('b', '{c, a}'),
    ('c', '{b, a}'),
    ('d', '{c, b}'),
    ('e', '{d, NULL}');

-- test_tbl_single_composites
INSERT INTO tst_one_comp (a, b) VALUES
    (1, ROW(1.0, 'a', 1)),
    (2, ROW(2.0, 'b', 2)),
    (3, ROW(3.0, 'c', 3)),
    (4, ROW(4.0, 'd', 4)),
    (5, ROW(NULL, NULL, 5));

-- test_tbl_composites
INSERT INTO tst_comps (a, b) VALUES
    (ROW(1.0, 'a', 1), ARRAY[ROW(1, 'a', 1)::tst_comp_basic_t]),
    (ROW(2.0, 'b', 2), ARRAY[ROW(2, 'b', 2)::tst_comp_basic_t]),
    (ROW(3.0, 'c', 3), ARRAY[ROW(3, 'c', 3)::tst_comp_basic_t]),
    (ROW(4.0, 'd', 4), ARRAY[ROW(4, 'd', 3)::tst_comp_basic_t]),
    (ROW(5.0, 'e', NULL), ARRAY[NULL, ROW(5, NULL, 5)::tst_comp_basic_t]);

-- test_tbl_composite_with_enums
INSERT INTO tst_comp_enum (a, b) VALUES
    (1, ROW(1.0, 'a', 1)),
    (2, ROW(2.0, 'b', 2)),
    (3, ROW(3.0, 'c', 3)),
    (4, ROW(4.0, 'd', 4)),
    (5, ROW(NULL, 'e', NULL));

-- test_tbl_composite_with_enums_array
INSERT INTO tst_comp_enum_array (a, b) VALUES
    (ROW(1.0, 'a', 1), ARRAY[ROW(1, 'a', 1)::tst_comp_enum_t]),
    (ROW(2.0, 'b', 2), ARRAY[ROW(2, 'b', 2)::tst_comp_enum_t]),
    (ROW(3.0, 'c', 3), ARRAY[ROW(3, 'c', 3)::tst_comp_enum_t]),
    (ROW(4.0, 'd', 3), ARRAY[ROW(3, 'd', 3)::tst_comp_enum_t]),
    (ROW(5.0, 'e', 3), ARRAY[ROW(3, 'e', 3)::tst_comp_enum_t, NULL]);

-- test_tbl_composite_with_single_enums_array_in_composite
INSERT INTO tst_comp_one_enum_array (a, b) VALUES
    (1, ROW(1.0, '{a, b, c}', 1)),
    (2, ROW(2.0, '{a, b, c}', 2)),
    (3, ROW(3.0, '{a, b, c}', 3)),
    (4, ROW(4.0, '{c, b, d}', 4)),
    (5, ROW(5.0, '{NULL, e, NULL}', 5));

-- test_tbl_composite_with_enums_array_in_composite
INSERT INTO tst_comp_enum_what (a, b) VALUES
    (ROW(1.0, '{a, b, c}', 1), ARRAY[ROW(1, '{a, b, c}', 1)::tst_comp_enum_array_t]),
    (ROW(2.0, '{b, c, a}', 2), ARRAY[ROW(2, '{b, c, a}', 1)::tst_comp_enum_array_t]),
    (ROW(3.0, '{c, a, b}', 1), ARRAY[ROW(3, '{c, a, b}', 1)::tst_comp_enum_array_t]),
    (ROW(4.0, '{c, b, d}', 4), ARRAY[ROW(4, '{c, b, d}', 4)::tst_comp_enum_array_t]),
    (ROW(5.0, '{c, NULL, b}', NULL), ARRAY[ROW(5, '{c, e, b}', 1)::tst_comp_enum_array_t]);

-- test_tbl_mixed_composites
INSERT INTO tst_comp_mix_array (a, b) VALUES
    (ROW(
        ROW(1,'a',1),
        ARRAY[ROW(1,'a',1)::tst_comp_basic_t, ROW(2,'b',2)::tst_comp_basic_t],
        'a',
        '{a,b,NULL,c}'),
    ARRAY[
        ROW(
            ROW(1,'a',1),
            ARRAY[
                ROW(1,'a',1)::tst_comp_basic_t,
                ROW(2,'b',2)::tst_comp_basic_t,
                NULL
                ],
            'a',
            '{a,b,c}'
            )::tst_comp_mix_t
        ]
    );

-- test_tbl_range
INSERT INTO tst_range (a, b) VALUES
    (1, '[1, 10]'),
    (2, '[2, 20]'),
    (3, '[3, 30]'),
    (4, '[4, 40]'),
    (5, '[5, 50]');

-- test_tbl_range_array
INSERT INTO tst_range_array (a, b, c) VALUES
    (1, tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz, 'infinity'), '{"[1,2]", "[10,20]"}'),
    (2, tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz - interval '2 days', 'Mon Aug 04 00:00:00 2014 CEST'::timestamptz), '{"[2,3]", "[20,30]"}'),
    (3, tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz - interval '3 days', 'Mon Aug 04 00:00:00 2014 CEST'::timestamptz), '{"[3,4]"}'),
    (4, tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz - interval '4 days', 'Mon Aug 04 00:00:00 2014 CEST'::timestamptz), '{"[4,5]", NULL, "[40,50]"}'),
    (5, NULL, NULL);

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;
SELECT a, b, c, d FROM tst_arrays ORDER BY a;
SELECT a, b FROM tst_one_enum ORDER BY a;
SELECT a, b FROM tst_enums ORDER BY a;
SELECT a, b FROM tst_one_comp ORDER BY a;
SELECT a, b FROM tst_comps ORDER BY a;
SELECT a, b FROM tst_comp_enum ORDER BY a;
SELECT a, b FROM tst_comp_enum_array ORDER BY a;
SELECT a, b FROM tst_comp_one_enum_array ORDER BY a;
SELECT a, b FROM tst_comp_enum_what ORDER BY a;
SELECT a, b FROM tst_comp_mix_array ORDER BY a;
SELECT a, b FROM tst_range ORDER BY a;
SELECT a, b, c FROM tst_range_array ORDER BY a;

-- test_tbl_one_array_col
\c :provider_dsn
UPDATE tst_one_array SET b = '{4, 5, 6}' WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;
\c :provider_dsn
UPDATE tst_one_array SET b = '{4, 5, 6, 1}' WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;

\c :provider_dsn
DELETE FROM tst_one_array WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_one_array WHERE b = '{2, 3, 1}';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_one_array WHERE 1 = ANY(b);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_array ORDER BY a;

-- test_tbl_arrays
\c :provider_dsn
UPDATE tst_arrays SET b = '{"1a", "2b", "3c"}', c = '{1.0, 2.0, 3.0}', d = '{"1 day 1 second", "2 days 2 seconds", "3 days 3 second"}' WHERE a = '{1, 2, 3}';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c, d FROM tst_arrays ORDER BY a;
\c :provider_dsn
UPDATE tst_arrays SET b = '{"c", "d", "e"}', c = '{3.0, 4.0, 5.0}', d = '{"3 day 1 second", "4 days 2 seconds", "5 days 3 second"}' WHERE a[1] > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c, d FROM tst_arrays ORDER BY a;

\c :provider_dsn
DELETE FROM tst_arrays WHERE a = '{1, 2, 3}';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c, d FROM tst_arrays ORDER BY a;
\c :provider_dsn
DELETE FROM tst_arrays WHERE a[1] = 2;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c, d FROM tst_arrays ORDER BY a;
\c :provider_dsn
DELETE FROM tst_arrays WHERE b[1] = 'c';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c, d FROM tst_arrays ORDER BY a;

-- test_tbl_single_enum
\c :provider_dsn
UPDATE tst_one_enum SET b = 'c' WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_enum ORDER BY a;
\c :provider_dsn
UPDATE tst_one_enum SET b = NULL WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_enum ORDER BY a;

\c :provider_dsn
DELETE FROM tst_one_enum WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_enum ORDER BY a;
\c :provider_dsn
DELETE FROM tst_one_enum WHERE b = 'b';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_one_enum ORDER BY a;

-- test_tbl_enums
\c :provider_dsn
UPDATE tst_enums SET b = '{e, NULL}' WHERE a = 'a';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_enums;
\c :provider_dsn
UPDATE tst_enums SET b = '{e, d}' WHERE a > 'c';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_enums;

\c :provider_dsn
DELETE FROM tst_enums WHERE a = 'a';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_enums;
\c :provider_dsn
DELETE FROM tst_enums WHERE 'c' = ANY(b);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_enums;
\c :provider_dsn
DELETE FROM tst_enums WHERE b[1] = 'b';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_enums;

-- test_tbl_single_composites
\c :provider_dsn
UPDATE tst_one_comp SET b = ROW(1.0, 'A', 1) WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_one_comp ORDER BY a;
\c :provider_dsn
UPDATE tst_one_comp SET b = ROW(NULL, 'x', -1) WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_one_comp ORDER BY a;

\c :provider_dsn
DELETE FROM tst_one_comp WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_one_comp ORDER BY a;
\c :provider_dsn
DELETE FROM tst_one_comp WHERE (b).a = 2.0;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_one_comp ORDER BY a;

-- test_tbl_composites
\c :provider_dsn
UPDATE tst_comps SET b = ARRAY[ROW(9, 'x', -1)::tst_comp_basic_t] WHERE (a).a = 1.0;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comps ORDER BY a;
\c :provider_dsn
UPDATE tst_comps SET b = ARRAY[NULL, ROW(9, 'x', NULL)::tst_comp_basic_t] WHERE (a).a > 3.9;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comps ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comps WHERE (a).b = 'a';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comps ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comps WHERE (b[1]).a = 2.0;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comps ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comps WHERE ROW(3, 'c', 3)::tst_comp_basic_t = ANY(b);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comps ORDER BY a;

-- test_tbl_composite_with_enums
\c :provider_dsn
UPDATE tst_comp_enum SET b = ROW(1.0, NULL, NULL) WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum ORDER BY a;
\c :provider_dsn
UPDATE tst_comp_enum SET b = ROW(4.0, 'd', 44) WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comp_enum WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_enum WHERE (b).a = 2.0;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum ORDER BY a;

-- test_tbl_composite_with_enums_array
\c :provider_dsn
UPDATE tst_comp_enum_array SET b = ARRAY[NULL, ROW(3, 'd', 3)::tst_comp_enum_t] WHERE a = ROW(1.0, 'a', 1)::tst_comp_enum_t;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum_array ORDER BY a;
\c :provider_dsn
UPDATE tst_comp_enum_array SET b = ARRAY[ROW(1, 'a', 1)::tst_comp_enum_t, ROW(2, 'b', 2)::tst_comp_enum_t] WHERE (a).a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum_array ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comp_enum_array WHERE a = ROW(1.0, 'a', 1)::tst_comp_enum_t;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_enum_array WHERE (b[1]).b = 'b';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_enum_array WHERE ROW(3, 'c', 3)::tst_comp_enum_t = ANY(b);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_array ORDER BY a;

-- test_tbl_composite_with_single_enums_array_in_composite
\c :provider_dsn
UPDATE tst_comp_one_enum_array SET b = ROW(1.0, '{a, e, c}', NULL) WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_one_enum_array ORDER BY a;
\c :provider_dsn
UPDATE tst_comp_one_enum_array SET b = ROW(4.0, '{c, b, d}', 4) WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_one_enum_array ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comp_one_enum_array WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_one_enum_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_one_enum_array WHERE (b).c = 2;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_one_enum_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_one_enum_array WHERE 'a' = ANY((b).b);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_one_enum_array ORDER BY a;

-- test_tbl_composite_with_enums_array_in_composite
\c :provider_dsn
UPDATE tst_comp_enum_what SET b = ARRAY[NULL, ROW(1, '{a, b, c}', 1)::tst_comp_enum_array_t, ROW(NULL, '{a, e, c}', 2)::tst_comp_enum_array_t] WHERE (a).a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum_what ORDER BY a;
\c :provider_dsn
UPDATE tst_comp_enum_what SET b = ARRAY[ROW(5, '{a, b, c}', 5)::tst_comp_enum_array_t] WHERE (a).a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b from tst_comp_enum_what ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comp_enum_what WHERE (a).a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_what ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_enum_what WHERE (b[1]).a = 2;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_what ORDER BY a;
\c :provider_dsn
DELETE FROM tst_comp_enum_what WHERE (b[1]).b = '{c, a, b}';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_enum_what ORDER BY a;

-- test_tbl_mixed_composites
\c :provider_dsn
UPDATE tst_comp_mix_array SET b[2] = NULL WHERE ((a).a).a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_mix_array ORDER BY a;

\c :provider_dsn
DELETE FROM tst_comp_mix_array WHERE ((a).a).a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_comp_mix_array ORDER BY a;

-- test_tbl_range
\c :provider_dsn
UPDATE tst_range SET b = '[100, 1000]' WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_range ORDER BY a;
\c :provider_dsn
UPDATE tst_range SET b = '(1, 90)' WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_range ORDER BY a;

\c :provider_dsn
DELETE FROM tst_range WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_range ORDER BY a;
\c :provider_dsn
DELETE FROM tst_range WHERE b = '[2, 20]';
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_range ORDER BY a;
\c :provider_dsn
DELETE FROM tst_range WHERE '[10,20]' && b;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b FROM tst_range ORDER BY a;

-- test_tbl_range_array
\c :provider_dsn
UPDATE tst_range_array SET c = '{"[100, 1000]"}' WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c FROM tst_range_array ORDER BY a;
\c :provider_dsn
UPDATE tst_range_array SET b = tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz, 'infinity'), c = '{NULL, "[11,9999999]"}' WHERE a > 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c FROM tst_range_array ORDER BY a;

\c :provider_dsn
DELETE FROM tst_range_array WHERE a = 1;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c FROM tst_range_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_range_array WHERE b = tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz - interval '2 days', 'Mon Aug 04 00:00:00 2014 CEST'::timestamptz);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c FROM tst_range_array ORDER BY a;
\c :provider_dsn
DELETE FROM tst_range_array WHERE tstzrange('Mon Aug 04 00:00:00 2014 CEST'::timestamptz, 'Mon Aug 05 00:00:00 2014 CEST'::timestamptz) && b;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT a, b, c FROM tst_range_array ORDER BY a;

\c :provider_dsn
-- Verify that swap_relation_files(...) breaks replication
-- as invoked by CLUSTER, VACUUM FULL, or REFRESH MATERIALIZED VIEW
VACUUM FULL tst_one_array;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);


\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.tst_one_array CASCADE;
	DROP TABLE public.tst_arrays CASCADE;
	DROP TABLE public.tst_one_enum CASCADE;
	DROP TABLE public.tst_enums CASCADE;
	DROP TABLE public.tst_one_comp CASCADE;
	DROP TABLE public.tst_comps CASCADE;
	DROP TABLE public.tst_comp_enum CASCADE;
	DROP TABLE public.tst_comp_enum_array CASCADE;
	DROP TABLE public.tst_comp_one_enum_array CASCADE;
	DROP TABLE public.tst_comp_enum_what CASCADE;
	DROP TABLE public.tst_comp_mix_array CASCADE;
	DROP TABLE public.tst_range CASCADE;
	DROP TABLE public.tst_range_array CASCADE;

	DROP TYPE public.tst_comp_mix_t;
	DROP TYPE public.tst_comp_enum_array_t;
	DROP TYPE public.tst_comp_enum_t;
	DROP TYPE public.tst_comp_basic_t;
	DROP TYPE public.tst_enum_t;
$$);
