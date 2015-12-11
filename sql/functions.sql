--Immutable, volatile functions and nextval in DEFAULT clause

\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE FUNCTION public.add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE TABLE public.funct2(
	a integer,
	b integer,
	c integer DEFAULT public.add(10,12 )
) ;
$$);

SELECT * FROM pglogical.replication_set_add_table('default_insert_only', 'public.funct2');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

INSERT INTO public.funct2(a,b) VALUES (1,2);--c should be 22
INSERT INTO public.funct2(a,b,c) VALUES (3,4,5);-- c should be 5

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c postgres
SELECT * from public.funct2;


\c regression

SELECT pglogical.replicate_ddl_command($$
create or replace function public.get_curr_century() returns double precision as
 'SELECT EXTRACT(CENTURY FROM NOW());'
language sql volatile;

CREATE TABLE public.funct5(
	a integer,
	b integer,
	c double precision DEFAULT public.get_curr_century()
);
$$);

SELECT * FROM pglogical.replication_set_add_all_tables('default_insert_only', '{public}');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

INSERT INTO public.funct5(a,b) VALUES (1,2);--c should be e.g. 21 for 2015
INSERT INTO public.funct5(a,b,c) VALUES (3,4,20);-- c should be 20

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * from public.funct5;

--nextval check
\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE SEQUENCE public.INSERT_SEQ;

CREATE TABLE public.funct (
	a integer,
	b INT DEFAULT nextval('public.insert_seq')
);
$$);

SELECT * FROM pglogical.replication_set_add_all_tables('default_insert_only', '{public}');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

INSERT INTO public.funct (a) VALUES (1);
INSERT INTO public.funct (a) VALUES (2);
INSERT INTO public.funct (a) VALUES (3);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.funct;


\c regression
BEGIN;
COMMIT;--empty transaction

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.funct;

-- test replication where the destination table has extra (nullable) columns that are not in the origin table
\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.nullcheck_tbl(
	id integer PRIMARY KEY,
	id1 integer,
	name text
) ;
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'nullcheck_tbl');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

INSERT INTO public.nullcheck_tbl(id,id1,name) VALUES (1,1,'name1');
INSERT INTO public.nullcheck_tbl(id,id1,name) VALUES (2,2,'name2');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM public.nullcheck_tbl;

ALTER TABLE public.nullcheck_tbl ADD COLUMN name1 text;

SELECT * FROM public.nullcheck_tbl;

\c regression

INSERT INTO public.nullcheck_tbl(id,id1,name) VALUES (3,3,'name3');
INSERT INTO public.nullcheck_tbl(id,id1,name) VALUES (4,4,'name4');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM public.nullcheck_tbl;

\c regression

UPDATE public.nullcheck_tbl SET name='name31' where id = 3;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

INSERT INTO public.nullcheck_tbl(id,id1,name) VALUES (6,6,'name6');
SELECT * FROM public.nullcheck_tbl;

\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.not_nullcheck_tbl(
	id integer PRIMARY KEY,
	id1 integer,
	name text
) ;
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'not_nullcheck_tbl');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

ALTER TABLE public.not_nullcheck_tbl ADD COLUMN id2 integer not null;

\c regression

INSERT INTO public.not_nullcheck_tbl(id,id1,name) VALUES (1,1,'name1');
INSERT INTO public.not_nullcheck_tbl(id,id1,name) VALUES (2,2,'name2');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM public.not_nullcheck_tbl;
INSERT INTO public.not_nullcheck_tbl(id,id1,name) VALUES (3,3,'name3');
SELECT * FROM public.not_nullcheck_tbl;

SELECT pglogical.alter_subscription_disable('test_subscription', true);

\c regression

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pg_replication_slots WHERE active = false) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT data FROM pg_logical_slot_get_changes((SELECT slot_name FROM pg_replication_slots), NULL, 1, 'min_proto_version', '1', 'max_proto_version', '1', 'startup_params_format', '1', 'proto_format', 'json');
SELECT data FROM pg_logical_slot_get_changes((SELECT slot_name FROM pg_replication_slots), NULL, 1, 'min_proto_version', '1', 'max_proto_version', '1', 'startup_params_format', '1', 'proto_format', 'json');

\c postgres

SELECT pglogical.alter_subscription_enable('test_subscription', true);
ALTER TABLE public.not_nullcheck_tbl ALTER COLUMN id2 SET default 99;

\c regression

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pg_replication_slots WHERE active = true) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

INSERT INTO public.not_nullcheck_tbl(id,id1,name) VALUES (4,4,'name4'); -- id2 will be 99 on subsriber
ALTER TABLE public.not_nullcheck_tbl ADD COLUMN id2 integer not null default 0;
INSERT INTO public.not_nullcheck_tbl(id,id1,name) VALUES (5,5,'name5'); -- id2 will be 0 on both
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.not_nullcheck_tbl;

\c regression

SELECT pglogical.replicate_ddl_command($$
DROP TABLE public.funct CASCADE;
DROP SEQUENCE public.INSERT_SEQ;
DROP TABLE public.funct2 CASCADE;
DROP TABLE public.funct5 CASCADE;
DROP FUNCTION public.get_curr_century();
DROP FUNCTION public.add(integer, integer);
DROP TABLE public.nullcheck_tbl CASCADE;
DROP TABLE public.not_nullcheck_tbl CASCADE;
$$);
