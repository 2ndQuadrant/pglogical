ALTER TABLE pglogical.subscription ADD COLUMN sub_force_text_transfer boolean NOT NULL DEFAULT 'f';

CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
	    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure text = 'none',
	    synchronize_data boolean = true, forward_origins text[] = '{all}', apply_delay interval DEFAULT '0',
	    force_text_transfer boolean = false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[], synchronize_structure boolean,
    synchronize_data boolean, forward_origins text[], apply_delay interval);

ALTER TABLE pglogical.replication_set_table
      ADD COLUMN set_nsptarget name
    , ADD COLUMN set_reltarget name ;
ALTER TABLE pglogical.replication_set_seq
      ADD COLUMN set_nsptarget name
    , ADD COLUMN set_seqtarget name;
DROP FUNCTION pglogical.show_repset_table_info(regclass, text[]);
CREATE FUNCTION pglogical.show_repset_table_info(relation regclass, repsets text[], OUT relid oid, OUT nspname text,
   OUT relname text, OUT att_list text[], OUT has_row_filter boolean, OUT nsptarget text, OUT reltarget text)
RETURNS record STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_show_repset_table_info';

CREATE FUNCTION pglogical.show_repset_table_info_by_target(nsptarget name, reltarget name, repsets text[], OUT relid oid, OUT nspname text,
   OUT relname text, OUT att_list text[], OUT has_row_filter boolean, OUT nsptarget text, OUT reltarget text)
RETURNS SETOF record STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_show_repset_table_info_by_target';

UPDATE pglogical.replication_set_table
  SET set_nsptarget = n.nspname
    , set_reltarget = c.relname
FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = set_reloid;

UPDATE pglogical.replication_set_seq
  SET set_nsptarget = n.nspname
    , set_seqtarget = c.relname
FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = set_seqoid;

ALTER TABLE pglogical.replication_set_table
      ALTER COLUMN set_nsptarget SET NOT NULL
    , ALTER COLUMN set_reltarget SET NOT NULL;
ALTER TABLE pglogical.replication_set_seq
      ALTER COLUMN set_nsptarget SET NOT NULL
    , ALTER COLUMN set_seqtarget SET NOT NULL;
-- a VACUUM FULL of the table above would be nice here.

DROP FUNCTION pglogical.replication_set_add_table(name, regclass, boolean,
     text[], text);
CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false,
	columns text[] DEFAULT NULL, row_filter text DEFAULT NULL, nsptarget name DEFAULT NULL, reltarget name DEFAULT NULL)
RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';

DROP FUNCTION pglogical.replication_set_add_sequence(name, regclass, boolean);
CREATE FUNCTION pglogical.replication_set_add_sequence(set_name name, relation regclass, synchronize_data boolean DEFAULT false, nsptarget name DEFAULT NULL, reltarget name DEFAULT NULL)
RETURNS boolean VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_sequence';
