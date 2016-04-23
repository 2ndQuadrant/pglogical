CREATE FUNCTION pglogical.alter_node_add_interface(node_name name, interface_name name, dsn text)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_node_add_interface';
CREATE FUNCTION pglogical.alter_node_drop_interface(node_name name, interface_name name)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_node_drop_interface';

CREATE FUNCTION pglogical.alter_subscription_interface(subscription_name name, interface_name name)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_subscription_interface';

DROP FUNCTION pglogical.replicate_ddl_command(command text);
CREATE OR REPLACE FUNCTION pglogical.replicate_ddl_command(command text, replication_sets text[] DEFAULT '{ddl_sql}')
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replicate_ddl_command';

ALTER TABLE pglogical.replication_set_table RENAME TO replication_set_relation;

CREATE TABLE pglogical.sequence_state (
	seqoid oid NOT NULL PRIMARY KEY,
	cache_size integer NOT NULL,
	last_value bigint NOT NULL
) WITH (user_catalog_table=true);

CREATE OR REPLACE VIEW pglogical.TABLES AS
    WITH set_relations AS (
        SELECT s.set_name, r.set_reloid
          FROM pglogical.replication_set_relation r,
               pglogical.replication_set s,
               pglogical.local_node n
         WHERE s.set_nodeid = n.node_id
           AND s.set_id = r.set_id
    ),
    user_tables AS (
        SELECT r.oid, n.nspname, r.relname, r.relreplident
          FROM pg_catalog.pg_class r,
               pg_catalog.pg_namespace n
         WHERE r.relkind = 'r'
           AND r.relpersistence = 'p'
           AND n.oid = r.relnamespace
           AND n.nspname !~ '^pg_'
           AND n.nspname != 'information_schema'
           AND n.nspname != 'pglogical'
    )
    SELECT n.nspname, r.relname, s.set_name
      FROM pg_catalog.pg_namespace n,
           pg_catalog.pg_class r,
           set_relations s
     WHERE r.relkind = 'r'
       AND n.oid = r.relnamespace
       AND r.oid = s.set_reloid
     UNION
    SELECT t.nspname, t.relname, NULL
      FROM user_tables t
     WHERE t.oid NOT IN (SELECT set_reloid FROM set_relations);

CREATE FUNCTION pglogical.replication_set_add_sequence(set_name name, relation regclass, synchronize_data boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_sequence';
CREATE FUNCTION pglogical.replication_set_add_all_sequences(set_name name, schema_names text[], synchronize_data boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_all_sequences';
CREATE FUNCTION pglogical.replication_set_remove_sequence(set_name name, relation regclass)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_remove_sequence';

ALTER EVENT TRIGGER pglogical_truncate_trigger_add ENABLE ALWAYS;
ALTER EVENT TRIGGER pglogical_dependency_check_trigger ENABLE ALWAYS;

DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[], synchronize_structure boolean, synchronize_data boolean, forward_origins text[]);
CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = false,
    synchronize_data boolean = true, forward_origins text[] = '{all}')
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

DROP FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize boolean);
CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';

DROP FUNCTION pglogical.replication_set_add_all_tables(set_name name, schema_names text[], synchronize boolean);
CREATE FUNCTION pglogical.replication_set_add_all_tables(set_name name, schema_names text[], synchronize_data boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_all_tables';
