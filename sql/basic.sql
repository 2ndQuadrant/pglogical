-- basic builtin datatypes
\c regression
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.basic_dml (
		id serial primary key,
		other integer,
		data text,
		something interval
	);
$$);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- check basic insert replication
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- update one row
\c regression
UPDATE basic_dml SET other = '4', data = NULL, something = '3 days'::interval WHERE id = 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- update multiple rows
\c regression
UPDATE basic_dml SET other = id, data = data || id::text;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

\c regression
UPDATE basic_dml SET other = id, something = something - '10 seconds'::interval WHERE id < 3;
UPDATE basic_dml SET other = id, something = something + '10 seconds'::interval WHERE id > 3;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- delete one row
\c regression
DELETE FROM basic_dml WHERE id = 2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- delete multiple rows
\c regression
DELETE FROM basic_dml WHERE id < 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- truncate
\c regression
TRUNCATE basic_dml;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- copy
\c regression
\COPY basic_dml FROM STDIN WITH CSV
9000,1,aaa,1 hour
9001,2,bbb,2 years
9002,3,ccc,3 minutes
9003,4,ddd,4 days
\.
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

\c regression
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.basic_dml;
$$);
