ALTER TABLE pglogical.subscription ADD COLUMN sub_apply_delay interval NOT NULL DEFAULT '0';

CREATE TABLE pglogical.replication_set_seq (
    set_id oid NOT NULL,
    set_seqoid regclass NOT NULL,
    PRIMARY KEY(set_id, set_seqoid)
) WITH (user_catalog_table=true);

WITH seqs AS (
	SELECT r.set_id, r.set_reloid
	  FROM pg_class c
	  JOIN replication_set_relation r ON (r.set_reloid = c.oid)
	 WHERE c.relkind = 'S'
), inserted AS (
	INSERT INTO replication_set_seq SELECT set_id, set_reloid FROM seqs
)
DELETE FROM replication_set_relation r USING seqs s WHERE r.set_reloid = s.set_reloid;

ALTER TABLE pglogical.replication_set_relation RENAME TO replication_set_table;
ALTER TABLE pglogical.replication_set_table
    ADD COLUMN set_att_filter text[],
    ADD COLUMN set_row_filter pg_node_tree;

DROP FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean);
CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false, att_filter text[] DEFAULT NULL, row_filter text DEFAULT NULL)
RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';

DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[], synchronize_structure boolean,
    synchronize_data boolean, forward_origins text[]);
CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = false,
    synchronize_data boolean = true, forward_origins text[] = '{all}', apply_delay interval DEFAULT '0')
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

DROP VIEW pglogical.TABLES;
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

CREATE FUNCTION pglogical.show_repset_table_info(relation regclass, repsets text[], OUT relid oid, OUT nspname text,
	OUT relname text, OUT att_filter text[], OUT has_row_filter boolean)
RETURNS record STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_show_repset_table_info';

CREATE FUNCTION pglogical.table_data_filtered(reltyp anyelement, relation regclass, repsets text[])
RETURNS SETOF anyelement CALLED ON NULL INPUT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_table_data_filtered';
