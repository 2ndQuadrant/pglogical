DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
	replication_sets text[], synchronize_structure text, synchronize_data boolean,
	forward_origins text[], apply_delay interval, force_text_transfer boolean);

CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
	replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = false,
	synchronize_data boolean = true, forward_origins text[] = '{all}', apply_delay interval DEFAULT '0',
	force_text_transfer boolean = false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

DROP FUNCTION pglogical.show_repset_table_info(regclass, text[]);
CREATE FUNCTION pglogical.show_repset_table_info(relation regclass, repsets text[], OUT relid oid, OUT nspname text,
   OUT relname text, OUT att_list text[], OUT has_row_filter boolean)
RETURNS record STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_show_repset_table_info';

DROP FUNCTION pglogical.show_repset_table_info_by_target(name, name, text[]);

DROP FUNCTION pglogical.replication_set_add_table(name, regclass, boolean, text[], text, name, name);
CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false,
        columns text[] DEFAULT NULL, row_filter text DEFAULT NULL)
RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';

DROP FUNCTION pglogical.replication_set_add_sequence(name, regclass, boolean, name, name);
CREATE FUNCTION pglogical.replication_set_add_sequence(set_name name, relation regclass, synchronize_data boolean DEFAULT false)
RETURNS boolean VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_sequence';


ALTER TABLE pglogical.replication_set_table RENAME TO replication_set_table_old;

CREATE TABLE pglogical.replication_set_table (
    set_id oid NOT NULL,
    set_reloid regclass NOT NULL,
    set_att_list text[],
    set_row_filter pg_node_tree,
    PRIMARY KEY(set_id, set_reloid)
) WITH (user_catalog_table=true);

INSERT INTO pglogical.replication_set_table
    SELECT set_id, set_reloid, set_att_list, set_row_filter FROM pglogical.replication_set_table_old;

DROP VIEW pglogical.tables;
DROP TABLE pglogical.replication_set_table_old;

ALTER TABLE pglogical.replication_set_seq RENAME TO replication_set_seq_old;

CREATE TABLE pglogical.replication_set_seq (
    set_id oid NOT NULL,
    set_seqoid regclass NOT NULL,
    PRIMARY KEY(set_id, set_seqoid)
) WITH (user_catalog_table=true);

INSERT INTO pglogical.replication_set_seq
    SELECT set_id, set_seqoid FROM pglogical.replication_set_seq_old;

DROP TABLE pglogical.replication_set_seq_old;


-- must recreate on top of new replication_set_table
CREATE VIEW pglogical.TABLES AS
    WITH set_relations AS (
        SELECT s.set_name, r.set_reloid
          FROM pglogical.replication_set_table r,
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
    SELECT r.oid AS relid, n.nspname, r.relname, s.set_name
      FROM pg_catalog.pg_namespace n,
           pg_catalog.pg_class r,
           set_relations s
     WHERE r.relkind = 'r'
       AND n.oid = r.relnamespace
       AND r.oid = s.set_reloid
     UNION
    SELECT t.oid AS relid, t.nspname, t.relname, NULL
      FROM user_tables t
     WHERE t.oid NOT IN (SELECT set_reloid FROM set_relations);
