/* First test whether a table's replication set can be properly manipulated */
\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE SCHEMA "strange.schema-IS";
CREATE TABLE public.test_publicschema(id serial primary key, data text);
CREATE TABLE public.test_nosync(id serial primary key, data text);
CREATE TABLE "strange.schema-IS".test_strangeschema(id serial primary key);
$$);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- create some replication sets
SELECT * FROM pglogical.create_replication_set('repset_test');

-- move tables to replication set that is not subscribed
SELECT * FROM pglogical.replication_set_add_table('repset_test', 'test_publicschema');
SELECT * FROM pglogical.replication_set_add_table('repset_test', 'test_nosync');
SELECT * FROM pglogical.replication_set_add_table('repset_test', '"strange.schema-IS".test_strangeschema');

INSERT INTO public.test_publicschema VALUES(1, 'a');
INSERT INTO public.test_publicschema VALUES(2, 'b');
INSERT INTO public.test_nosync VALUES(1, 'a');
INSERT INTO public.test_nosync VALUES(2, 'b');
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(1);
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(2);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.test_publicschema;
\c regression

-- move tables back to the subscribed replication set
SELECT * FROM pglogical.replication_set_add_table('default', 'test_publicschema', true);
SELECT * FROM pglogical.replication_set_add_table('default', 'test_nosync', false);
SELECT * FROM pglogical.replication_set_add_table('default', '"strange.schema-IS".test_strangeschema', true);

\c postgres
DO $$
-- give it 10 seconds to syncrhonize the tabes
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pglogical.local_sync_status WHERE sync_status = 'r' AND sync_relname IN ('test_publicschema', 'test_strangeschema')) > 1 THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4;

\c regression
INSERT INTO public.test_publicschema VALUES(3, 'c');
INSERT INTO public.test_publicschema VALUES(4, 'd');
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(3);
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(4);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.test_publicschema;
SELECT * FROM "strange.schema-IS".test_strangeschema;

SELECT * FROM pglogical.alter_subscription_synchronize('test_subscription');

DO $$
-- give it 10 seconds to syncrhonize the tabes
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pglogical.local_sync_status WHERE sync_status = 'r' AND sync_relname IN ('test_nosync')) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4;

SELECT * FROM public.test_nosync;

DELETE FROM public.test_publicschema WHERE id > 1;
SELECT * FROM public.test_publicschema;

SELECT * FROM pglogical.alter_subscription_resynchronize_table('test_subscription', 'test_publicschema');

DO $$
-- give it 10 seconds to syncrhonize the tabes
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pglogical.local_sync_status WHERE sync_status = 'r' AND sync_relname IN ('test_publicschema')) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4;

SELECT * FROM public.test_publicschema;

\c regression
SELECT plugin, slot_type, database, active FROM pg_replication_slots;

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_publicschema CASCADE;
	DROP TABLE public.test_nosync CASCADE;
	DROP SCHEMA "strange.schema-IS" CASCADE;
$$);
