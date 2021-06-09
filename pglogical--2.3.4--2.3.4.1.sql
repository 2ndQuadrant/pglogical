ALTER TABLE pglogical.subscription ADD COLUMN sub_data_replace boolean NOT NULL DEFAULT true;
ALTER TABLE pglogical.subscription ADD COLUMN sub_after_sync_queries text[];

DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[], synchronize_structure boolean,
    synchronize_data boolean, forward_origins text[], apply_delay interval,
    force_text_transfer boolean);
CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = false,
    synchronize_data boolean = true, data_replace boolean = true, after_sync_queries text[] = '{}',
    forward_origins text[] = '{all}', apply_delay interval DEFAULT '0',
    force_text_transfer boolean = false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

ALTER TABLE pglogical.replication_set_table ADD COLUMN set_sync_clear_filter text;

DROP FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean,
                                                    columns text[], row_filter text);
CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false,
    columns text[] DEFAULT NULL, row_filter text DEFAULT NULL, sync_clear_filter text DEFAULT NULL)
RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';

TRUNCATE pglogical.queue;
ALTER TABLE pglogical.queue ADD COLUMN node_id oid REFERENCES node(node_id);
ALTER TABLE pglogical.queue ADD COLUMN original_node_id oid REFERENCES node(node_id);


DROP FUNCTION pglogical.wait_for_subscription_sync_complete(subscription_name name);
CREATE FUNCTION pglogical.wait_for_subscription_sync_complete(subscription_name name)
    RETURNS boolean RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_subscription_sync_complete';

DROP FUNCTION pglogical.wait_for_table_sync_complete(subscription_name name, relation regclass);
CREATE FUNCTION pglogical.wait_for_table_sync_complete(subscription_name name, relation regclass)
    RETURNS boolean RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_table_sync_complete';
