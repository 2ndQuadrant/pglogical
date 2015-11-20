/* First test whether a table's replication set can be properly manipulated */
\c regression

SELECT pglogical.replicate_ddl_command($$
CREATE SCHEMA "strange.schema-IS";
CREATE TABLE public.test_publicschema(id serial primary key, data text);
CREATE TABLE "strange.schema-IS".test_strangeschema(id serial primary key);
$$);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- create some replication sets
SELECT * FROM pglogical.create_replication_set('repset_test');

-- move tables to replication set that is not subscribed
SELECT * FROM pglogical.replication_set_add_table('repset_test', 'test_publicschema');
SELECT * FROM pglogical.replication_set_add_table('repset_test', '"strange.schema-IS".test_strangeschema');

INSERT INTO public.test_publicschema VALUES(1, 'a');
INSERT INTO public.test_publicschema VALUES(2, 'b');
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(1);
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(2);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.test_publicschema;
\c regression

-- move tables back to the subscribed replication set
SELECT * FROM pglogical.replication_set_add_table('default', 'test_publicschema');
SELECT * FROM pglogical.replication_set_add_table('default', '"strange.schema-IS".test_strangeschema');

INSERT INTO pglogical.queue VALUES(now(), 'foo', 'pjmodos', 'C', '{"schema_name": "public", "table_name": "test_publicschema"}');
INSERT INTO pglogical.queue VALUES(now(), 'foo', 'pjmodos', 'C', '{"schema_name": "strange.schema-IS", "table_name": "test_strangeschema"}');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
DO $$
-- give it 10 seconds to syncrhonize the tabes
BEGIN
	FOR i IN 1..100 LOOP
		IF EXISTS(SELECT 1 FROM public.test_publicschema) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

\c regression
INSERT INTO public.test_publicschema VALUES(3, 'c');
INSERT INTO public.test_publicschema VALUES(4, 'd');
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(3);
INSERT INTO "strange.schema-IS".test_strangeschema VALUES(4);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
SELECT * FROM public.test_publicschema;
SELECT * FROM "strange.schema-IS".test_strangeschema;

\c regression
SELECT plugin, slot_type, database, active FROM pg_replication_slots;

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_publicschema CASCADE;
	DROP SCHEMA "strange.schema-IS" CASCADE;
$$);
