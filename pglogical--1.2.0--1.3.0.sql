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

