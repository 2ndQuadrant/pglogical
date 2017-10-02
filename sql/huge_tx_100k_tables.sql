-- test huge transactions
-- Set 'max_locks_per_transaction' to 10000 to run test
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
-- medium number of rows in many different tables (100k): replication with DDL outside transaction

create or replace function create_many_tables(int, int) returns void language plpgsql as $$
DECLARE
        i int;
        cr_command varchar;
BEGIN
        FOR i IN $1 .. $2 LOOP
                cr_command := 'SELECT pglogical.replicate_ddl_command(''
                CREATE TABLE public.HUGE' || i || ' (
                id integer primary key,
                id1 integer,
                data text default ''''data'''',
                data1 text default ''''data1''''
        );
        '')';
        EXECUTE cr_command;
        END LOOP;
END;
$$;

-- write multile version of this statement

create or replace function add_many_tables_to_replication_set(int, int) returns void language plpgsql as $$
DECLARE
        i int;
        cr_command varchar;
BEGIN
        FOR i IN $1 .. $2 LOOP
                cr_command := 'SELECT * FROM pglogical.replication_set_add_table(
                ''default'', ''HUGE' || i || ''' );';
        EXECUTE cr_command;
        END LOOP;
END;
$$;

create or replace function insert_into_many_tables(int, int) returns void language plpgsql as $$
DECLARE
        i int;
        cr_command varchar;
BEGIN
        FOR i IN $1 .. $2 LOOP
                cr_command := 'INSERT INTO public.HUGE' || i || ' VALUES (generate_series(1, 200), generate_series(1, 200))';

        EXECUTE cr_command;
        END LOOP;
END;
$$;

create or replace function drop_many_tables(int, int) returns void language plpgsql as $$
DECLARE
        i int;
        cr_command varchar;
BEGIN
        FOR i IN $1 .. $2 LOOP
                cr_command := 'SELECT pglogical.replicate_ddl_command(''
                         DROP TABLE public.HUGE' || i ||' CASCADE;
                      '')';
        EXECUTE cr_command;
        END LOOP;
END;
$$;

SELECT * FROM create_many_tables(1,100000);
SELECT * FROM add_many_tables_to_replication_set(1,100000);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
BEGIN;
SELECT * FROM insert_into_many_tables(1,100000);
COMMIT;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT count(*) FROM public.HUGE2;
\dtS+ public.HUGE2;

\c :provider_dsn

\set VERBOSITY terse
SELECT * FROM drop_many_tables(1,100000);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

-- medium number of rows in many different tables: replication with DDL inside transaction
BEGIN;
SELECT * FROM create_many_tables(1,100000);
SELECT * FROM add_many_tables_to_replication_set(1,100000);
SELECT * FROM insert_into_many_tables(1,100000);
COMMIT;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT count(*) FROM public.HUGE2;
\dtS+ public.HUGE2;

\c :provider_dsn

\set VERBOSITY terse
SELECT * FROM drop_many_tables(1,100000);

DROP function create_many_tables(int, int);
DROP function add_many_tables_to_replication_set(int,int);
DROP function insert_into_many_tables(int, int);
DROP function drop_many_tables(int, int);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

