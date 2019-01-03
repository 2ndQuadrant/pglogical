SELECT * FROM pglogical_regress_variables()
\gset

/*
Covered cases:

  - 1 table replicated with a distinct name in a distinct schema. Init + DML + resync + TRUNCATE
  - 1 table replicated with a distinct name in the same schema. Init + DML + resync + TRUNCATE
  - 1 table replicated with the same name in a distinct schema. Init + DML + resync + TRUNCATE
  - 1 table replicated with distinct target in 2 distinct sets (a.b -> c.d and a.b -> e.f)
  - 2 tables merged from distinct sets
  - test resynchronize when multiple origin for the same table (origin indistincts sets

  - Not supported: 2 tables merged in the same set
  - Trying to add twice the same table in the same set (with distinct targets): FORBIDEN (XXX should/can we allow ?)
*/

\c :provider_dsn
CREATE SCHEMA "provider.ping";
CREATE SCHEMA "provider2.ping2";
CREATE SCHEMA provsub;

CREATE TABLE "provider.ping".test_origin(id serial primary key, data text DEFAULT '');
INSERT INTO "provider.ping".test_origin(data) VALUES ('a');
INSERT INTO "provider.ping".test_origin(data) VALUES ('b');
SELECT * FROM "provider.ping".test_origin ORDER by 1;

CREATE TABLE "provider.ping".test_origin2(id serial primary key, data text DEFAULT '');
INSERT INTO "provider.ping".test_origin2(data) VALUES ('y');
INSERT INTO "provider.ping".test_origin2(data) VALUES ('z');
SELECT * FROM "provider.ping".test_origin2 ORDER by 1;

CREATE TABLE provsub.test_origin3(id serial primary key, data text DEFAULT '');
INSERT INTO provsub.test_origin3(data) VALUES ('a');
INSERT INTO provsub.test_origin3(data) VALUES ('b');
SELECT * FROM provsub.test_origin3 ORDER by 1;

CREATE TABLE "provider.ping".provsub(id serial primary key, data text DEFAULT '');
INSERT INTO "provider.ping".provsub(data) VALUES ('a');
INSERT INTO "provider.ping".provsub(data) VALUES ('b');
SELECT * FROM "provider.ping".provsub ORDER by 1;

CREATE TABLE "provider.ping".bad(id serial primary key, data text DEFAULT '');

\c :subscriber_dsn
CREATE SCHEMA "subscriber.pong";
CREATE SCHEMA "subscriber2.pong2";
CREATE SCHEMA provsub;

CREATE TABLE "subscriber.pong".test_target(id serial primary key, data text DEFAULT '');
CREATE TABLE "subscriber2.pong2".test_target2(id serial primary key, data text DEFAULT '');
CREATE TABLE provsub.test_target3(id serial primary key, data text DEFAULT '');
CREATE TABLE "subscriber.pong".provsub(id serial primary key, data text DEFAULT '');

-- test replication with initial copy
-- add table and sequence to the subscribed replication set
\c :provider_dsn
SELECT * FROM pglogical.create_replication_set('map1',
       replicate_insert:=true,
       replicate_update:=true,
       replicate_delete:=true,
      replicate_truncate:=true);

-- distinct name and schema
SELECT * FROM pglogical.replication_set_add_table('map1', '"provider.ping".test_origin', true, nsptarget:='subscriber.pong', reltarget:='test_target');
SELECT * FROM pglogical.replication_set_add_sequence('map1', pg_get_serial_sequence('"provider.ping".test_origin', 'id'), nsptarget:='subscriber.pong',  reltarget:='test_target_id_seq'); -- XXX not  a dynamic name ...

-- distinct name, same schema
SELECT * FROM pglogical.replication_set_add_table('map1', 'provsub.test_origin3', true, reltarget:='test_target3');
SELECT * FROM pglogical.replication_set_add_sequence('map1', pg_get_serial_sequence('provsub.test_origin3', 'id'), reltarget:='test_target3_id_seq'); -- XXX not  a dynamic name ...

-- same name, distinct schema
SELECT * FROM pglogical.replication_set_add_table('map1', '"provider.ping".provsub', true, nsptarget:='subscriber.pong');
SELECT * FROM pglogical.replication_set_add_sequence('map1', pg_get_serial_sequence('"provider.ping".provsub', 'id'), nsptarget:='subscriber.pong');
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
SELECT * FROM pglogical.replication_set_seq order by 1,2;
SELECT * FROM pglogical.replication_set_table order by 1,2;

\c :subscriber_dsn
-- init
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'sub_map1',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := 'none',
	forward_origins := '{}',
        replication_sets := '{map1}');
SELECT pglogical.wait_for_subscription_sync_complete('sub_map1');

SELECT * FROM "subscriber.pong".test_target;
SELECT * FROM provsub.test_target3;
SELECT * FROM "subscriber.pong".provsub;


-- test resynchronize
\c :subscriber_dsn
DELETE FROM "subscriber.pong".test_target WHERE id > 1;
DELETE FROM provsub.test_target3 WHERE id > 1;
DELETE FROM "subscriber.pong".provsub WHERE id > 1;
SELECT * FROM pglogical.alter_subscription_resynchronize_table('sub_map1', '"subscriber.pong".test_target', true);

SELECT * FROM pglogical.alter_subscription_resynchronize_table('sub_map1', 'provsub.test_target3', true);
SELECT * FROM pglogical.alter_subscription_resynchronize_table('sub_map1', '"subscriber.pong".provsub', true);
SELECT pglogical.wait_for_table_sync_complete('sub_map1', '"subscriber.pong".test_target');
SELECT pglogical.wait_for_table_sync_complete('sub_map1', 'provsub.test_target3');
SELECT pglogical.wait_for_table_sync_complete('sub_map1', '"subscriber.pong".provsub');
SELECT * FROM "subscriber.pong".test_target;
SELECT * FROM provsub.test_target3;
SELECT * FROM "subscriber.pong".provsub;


-- test synchronize
\c :subscriber_dsn
CREATE TABLE "subscriber.pong".test_synchronize(id serial primary key, data text DEFAULT '');
\c :provider_dsn
CREATE TABLE "provider.ping".test_synchronize(id serial primary key, data text DEFAULT '');
INSERT INTO "provider.ping".test_synchronize(data) VALUES ('a');
SELECT * FROM pglogical.replication_set_add_table('map1', '"provider.ping".test_synchronize', true, nsptarget:='subscriber.pong');
SELECT * FROM pglogical.replication_set_add_sequence('map1', pg_get_serial_sequence('"provider.ping".test_synchronize', 'id'), nsptarget:='subscriber.pong'); -- XXX not  a dynamic name ...

\c :subscriber_dsn
SELECT * FROM pglogical.alter_subscription_synchronize('sub_map1');
SELECT pglogical.wait_for_table_sync_complete('sub_map1', '"subscriber.pong".test_synchronize');
SELECT * FROM "subscriber.pong".test_synchronize;

-- test DML replication after init
\c :provider_dsn
INSERT INTO "provider.ping".test_origin(data) VALUES ('c');
INSERT INTO "provider.ping".test_origin(data) VALUES ('d');
UPDATE "provider.ping".test_origin SET data = 'data';
DELETE FROM "provider.ping".test_origin WHERE id < 3;

INSERT INTO provsub.test_origin3(data) VALUES ('c');
INSERT INTO provsub.test_origin3(data) VALUES ('d');
UPDATE provsub.test_origin3 SET data = 'data';
DELETE FROM provsub.test_origin3 WHERE id < 3;

INSERT INTO "provider.ping".provsub(data) VALUES ('c');
INSERT INTO "provider.ping".provsub(data) VALUES ('d');
UPDATE "provider.ping".provsub SET data = 'data';
DELETE FROM "provider.ping".provsub WHERE id < 3;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT * FROM "subscriber.pong".test_target;
SELECT * FROM provsub.test_target3;
SELECT * FROM "subscriber.pong".provsub;

-- truncate
\c :provider_dsn
TRUNCATE "provider.ping".test_origin;
TRUNCATE provsub.test_origin3;
TRUNCATE "provider.ping".provsub;
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT * FROM "subscriber.pong".test_target;
SELECT * FROM provsub.test_target3;
SELECT * FROM "subscriber.pong".provsub;

-- Merging tables
-- test merge data from 2 tables into 1 in distinct sets
\c :subscriber_dsn
CREATE TABLE "subscriber.pong".test_merge(id serial primary key, data text DEFAULT '');
\c :provider_dsn
CREATE TABLE "provider.ping".test_merge(id serial primary key, data text DEFAULT '');
INSERT INTO "provider.ping".test_merge(id,data) VALUES (9, 'm');
INSERT INTO "provider.ping".test_origin(data) VALUES ('n');
SELECT * FROM "provider.ping".test_merge ORDER by 1;
SELECT * FROM "provider.ping".test_origin ORDER by 1;

SELECT * FROM pglogical.create_replication_set('map2',
       replicate_insert:=true,
       replicate_update:=true,
       replicate_delete:=true,
       replicate_truncate:=true);

SELECT * FROM pglogical.replication_set_add_table('map2', '"provider.ping".test_merge', true, nsptarget:='subscriber.pong', reltarget:='test_target');
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT * FROM "subscriber.pong".test_target;
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'sub_map2',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := 'none',
	forward_origins := '{}',
        replication_sets := '{map2}');
SELECT pglogical.wait_for_subscription_sync_complete('sub_map2');
SELECT * FROM "subscriber.pong".test_target;

TRUNCATE  "subscriber.pong".test_target;

-- test resynchronize when multiple origin for the same table (origin indistincts sets
SELECT * FROM pglogical.alter_subscription_resynchronize_table('sub_map1', '"subscriber.pong".test_target', true);
SELECT pglogical.wait_for_table_sync_complete('sub_map1', '"subscriber.pong".test_target');
SELECT * FROM pglogical.alter_subscription_resynchronize_table('sub_map2', '"subscriber.pong".test_target', false);
SELECT pglogical.wait_for_table_sync_complete('sub_map2', '"subscriber.pong".test_target');
SELECT * FROM "subscriber.pong".test_target;

-- Splitting
-- 1 table replicated with distinct target in 2 distinct sets (a.b -> c.d and a.b -> e.f)
\c :provider_dsn
SELECT * FROM pglogical.replication_set_add_table('map2', '"provider.ping".test_origin', true, nsptarget:='subscriber2.pong2', reltarget:='test_target2');
SELECT * FROM pglogical.replication_set_add_sequence('map2', pg_get_serial_sequence('"provider.ping".test_origin', 'id'), nsptarget:='subscriber2.pong2',  reltarget:='test_target2_id_seq'); -- XXX not  a dynamic name ...
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);
\c :subscriber_dsn
SELECT pglogical.wait_for_subscription_sync_complete('sub_map2');
SELECT * FROM "subscriber2.pong2".test_target2;

-- Not supported cases:
-- test merging 2 sequences to the same target: not allowed !
\c :provider_dsn
-- same set
SELECT * FROM pglogical.replication_set_add_sequence('map1', pg_get_serial_sequence('"provider.ping".bad', 'id'), nsptarget:='subscriber.pong',  reltarget:='test_target_id_seq'); -- XXX not  a dynamic name ...
-- distinct set
SELECT * FROM pglogical.replication_set_add_sequence('map2', pg_get_serial_sequence('"provider.ping".bad', 'id'), nsptarget:='subscriber.pong',  reltarget:='test_target_id_seq'); -- XXX not  a dynamic name ...
DROP TABLE "provider.ping".bad;

-- Merging tables
-- test merge data from 2 tables into 1 in the same set: not allowed
\c :provider_dsn
SELECT * FROM pglogical.replication_set_add_table('map1', '"provider.ping".test_origin2', true, nsptarget:='subscriber.pong', reltarget:='test_target');

-- XXX copy test required ?

-- synchronize sequences
\c :provider_dsn
SELECT pglogical.synchronize_sequence('"provider.ping".test_origin_id_seq');
SELECT pglogical.synchronize_sequence('provsub.test_origin3_id_seq');
SELECT pglogical.synchronize_sequence('"provider.ping".provsub_id_seq');
SELECT pglogical.synchronize_sequence('"provider.ping".test_synchronize_id_seq');
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SELECT N.nspname AS schemaname, C.relname AS tablename, (nextval(C.oid) > 1000) as synced
  FROM pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE C.relkind = 'S' AND C.relname IN ('test_target_id_seq', 'test_target2_id_seq', 'test_target3_id_seq'
                                         ,'provsub_id_seq', 'test_synchronize_id_seq')
  ORDER BY 1, 2;

-- show and cleaning
\c :subscriber_dsn
SELECT * FROM pglogical.show_subscription_status('sub_map1');
SELECT * FROM pglogical.show_subscription_table('sub_map1','"subscriber.pong".test_target');
--- XXX add more here
SELECT * FROM pglogical.show_subscription_status('sub_map2');
SELECT * FROM pglogical.drop_subscription('sub_map1');
SELECT * FROM pglogical.drop_subscription('sub_map2');

\c :provider_dsn
SELECT nspname, relname, att_list, has_row_filter, nsptarget, reltarget
FROM pglogical.show_repset_table_info_by_target('subscriber.pong','test_target', ARRAY['map1','map2']) order by 1,2;
-- XXX fonction pglogical.table_data_filtered(anyelement,regclass,text[]) ?
SELECT * FROM pglogical.replication_set_seq order by 1,2;
SELECT * FROM pglogical.replication_set_table order by 1,2;
SELECT cache_size,last_value FROM pglogical.sequence_state;
SELECT * FROM pglogical.drop_replication_set('map1');
SELECT * FROM pglogical.drop_replication_set('map2');
DROP SCHEMA "provider.ping" CASCADE;
DROP SCHEMA "provider2.ping2" CASCADE;
DROP SCHEMA provsub CASCADE;

\c :subscriber_dsn
DROP SCHEMA "subscriber.pong" CASCADE;
DROP SCHEMA "subscriber2.pong2" CASCADE;
DROP SCHEMA provsub CASCADE;
