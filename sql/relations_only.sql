-- This should be done with pg_regress's --create-role option
-- but it's blocked by bug 37906
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SET client_min_messages = 'warning';
CREATE SCHEMA relations_only;
CREATE TABLE relations_only.rel_only(id int primary key);
CREATE TABLE relations_only.rel_seq(id serial primary key);
CREATE SEQUENCE relations_only.seq_only;

CREATE TABLE relations_only.no_rel_only(id int primary key);
CREATE TABLE relations_only.no_rel_seq(id serial primary key);
CREATE SEQUENCE relations_only.no_seq_only;

SELECT * FROM pglogical.create_replication_set('repset_relations_only');
SELECT * FROM pglogical.replication_set_add_table('repset_relations_only', 'relations_only.rel_only', true);
SELECT * FROM pglogical.replication_set_add_table('repset_relations_only', 'relations_only.rel_seq', true);
SELECT * FROM pglogical.replication_set_add_sequence('repset_relations_only', 'relations_only.seq_only', true);

-- test adding a sequence with add_all_sequences (special case to get schema and
-- relation names)
CREATE SEQUENCE test_sequence;
SELECT * FROM pglogical.replication_set_add_all_sequences('repset_relations_only', '{public}');

\c :subscriber_dsn
SET client_min_messages = 'warning';
CREATE SCHEMA relations_only;

BEGIN;
SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription_relations_only',
    provider_dsn := (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super',
	synchronize_structure := 'relations_only',
	forward_origins := '{}',
        replication_sets := '{repset_relations_only}');
COMMIT;

BEGIN;
SET LOCAL statement_timeout = '30s';
SELECT pglogical.wait_for_subscription_sync_complete('test_subscription_relations_only');
COMMIT;

select table_name from information_schema.tables where table_schema = 'relations_only' order by 1;
select sequence_name from information_schema.sequences where sequence_schema = 'relations_only' order by 1;
select sequence_name from information_schema.sequences where sequence_name = 'test_sequence' order by 1;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('y', 'r') FROM pglogical.local_sync_status ORDER BY 2,3,4;

SELECT * FROM pglogical.drop_subscription('test_subscription_relations_only');
DROP SCHEMA relations_only CASCADE;
DROP SEQUENCE test_sequence CASCADE;

\c :provider_dsn
SELECT * FROM pglogical.drop_replication_set('repset_relations_only');
DROP SCHEMA relations_only CASCADE;
DROP SEQUENCE test_sequence CASCADE;
