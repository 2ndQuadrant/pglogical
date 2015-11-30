CREATE TABLE pglogical.node (
	node_id oid NOT NULL PRIMARY KEY,
	node_name name NOT NULL UNIQUE
) WITH (user_catalog_table=true);

CREATE TABLE pglogical.node_interface (
	if_id oid NOT NULL PRIMARY KEY,
	if_name name NOT NULL, -- default same as node name
	if_nodeid oid REFERENCES node(node_id),
	if_dsn text NOT NULL,
	UNIQUE (if_nodeid, if_name)
);

CREATE TABLE pglogical.local_node (
	node_id oid PRIMARY KEY REFERENCES node(node_id),
	node_local_interface oid NOT NULL REFERENCES node_interface(if_id)
);

-- Currently we allow only one node record per database
CREATE UNIQUE INDEX local_node_onlyone ON pglogical.local_node ((true));

CREATE TABLE pglogical.subscription (
	sub_id oid NOT NULL PRIMARY KEY,
	sub_name name NOT NULL UNIQUE,
	sub_origin oid NOT NULL REFERENCES node(node_id),
	sub_target oid NOT NULL REFERENCES node(node_id),
    sub_origin_if oid NOT NULL REFERENCES node_interface(if_id),
	sub_target_if oid NOT NULL REFERENCES node_interface(if_id),
	sub_enabled boolean NOT NULL DEFAULT true,
	sub_sync_structure boolean DEFAULT true,
	sub_sync_data boolean DEFAULT true,
	sub_replication_sets text[],
	UNIQUE (sub_origin, sub_target)
);

CREATE TABLE pglogical.local_sync_status (
	sync_kind "char" NOT NULL CHECK (sync_kind IN ('i', 's', 'd', 'f')),
	sync_subid oid NOT NULL REFERENCES pglogical.subscription(sub_id),
	sync_nspname name,
	sync_relname name,
	sync_status "char" NOT NULL,
	UNIQUE (sync_subid, sync_nspname, sync_relname)
);


CREATE FUNCTION pglogical.create_node(node_name name, dsn text)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_node';
CREATE FUNCTION pglogical.drop_node(mode_name name, ifexists boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_node';

CREATE FUNCTION pglogical.create_subscription(subscription_name name, origin_dsn text,
	replication_sets text[] = '{default}', synchronize_structure boolean = true, synchronize_data boolean = true)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';
CREATE FUNCTION pglogical.drop_subscription(subscription_name name, ifexists boolean DEFAULT false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_subscription';

CREATE FUNCTION pglogical.alter_subscription_disable(subscription_name name, immediate boolean DEFAULT false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_subscription_disable';
CREATE FUNCTION pglogical.alter_subscription_enable(subscription_name name, immediate boolean DEFAULT false)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_subscription_enable';


CREATE TABLE pglogical.replication_set (
	set_id oid NOT NULL PRIMARY KEY,
	set_nodeid oid NOT NULL,
    set_name name NOT NULL,
    replicate_insert boolean NOT NULL DEFAULT true,
    replicate_update boolean NOT NULL DEFAULT true,
    replicate_delete boolean NOT NULL DEFAULT true,
    replicate_truncate boolean NOT NULL DEFAULT true,
	UNIQUE (set_nodeid, set_name)
) WITH (user_catalog_table=true);

CREATE TABLE pglogical.replication_set_table (
    set_id integer NOT NULL,
	set_reloid regclass NOT NULL,
	PRIMARY KEY(set_id, set_reloid)
) WITH (user_catalog_table=true);

CREATE VIEW pglogical.TABLES AS
	WITH set_tables AS (
		SELECT s.set_name, t.set_reloid
		  FROM pglogical.replication_set_table t,
			   pglogical.replication_set s,
               pglogical.local_node n
		 WHERE s.set_nodeid = n.node_id
		   AND s.set_id = t.set_id
    ),
	user_tables AS (
		SELECT r.oid, n.nspname, r.relname, r.relreplident
		  FROM pg_catalog.pg_class r,
		       pg_catalog.pg_namespace n
		 WHERE r.relkind = 'r'
		   AND n.oid = r.relnamespace
		   AND n.nspname !~ '^pg_'
		   AND n.nspname != 'information_schema'
		   AND n.nspname != 'pglogical'
	)
    SELECT s.set_name, n.nspname, r.relname
	  FROM pg_catalog.pg_namespace n,
		   pg_catalog.pg_class r,
		   set_tables s
     WHERE r.relkind = 'r'
	   AND n.oid = r.relnamespace
	   AND r.oid = s.set_reloid
	 UNION
    SELECT rs.set_name, t.nspname, t.relname
	  FROM user_tables t,
		   pglogical.replication_set rs,
		   pg_catalog.pg_index i
     WHERE rs.set_name = 'default'
	   AND t.oid NOT IN (SELECT set_reloid FROM set_tables)
	   AND i.indrelid = t.oid
           /* Only tables with replica identity index can be in default replication set. */
	   AND ((relreplident = 'd' AND i.indisprimary) OR (relreplident = 'i' AND i.indisreplident));


CREATE FUNCTION pglogical.create_replication_set(set_name name,
	replicate_insert boolean = true, replicate_update boolean = true,
	replicate_delete boolean = true, replicate_truncate boolean = true)
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_replication_set';
CREATE FUNCTION pglogical.alter_replication_set(set_name name,
	replicate_insert boolean DEFAULT NULL, replicate_update boolean DEFAULT NULL,
	replicate_delete boolean DEFAULT NULL, replicate_truncate boolean DEFAULT NULL)
RETURNS oid CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_alter_replication_set';
CREATE FUNCTION pglogical.drop_replication_set(set_name name, ifexists boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_replication_set';

CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass, synchronize boolean DEFAULT false)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';
CREATE FUNCTION pglogical.replication_set_remove_table(set_name name, relation regclass)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_remove_table';


CREATE TABLE pglogical.queue (
    queued_at timestamp with time zone NOT NULL,
    ROLE name NOT NULL,
	replication_set name NOT NULL,
    message_type "char" NOT NULL,
    message json NOT NULL
);

CREATE FUNCTION pglogical.replicate_ddl_command(command text)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replicate_ddl_command';

CREATE OR REPLACE FUNCTION pglogical.queue_truncate()
RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_queue_truncate';

CREATE OR REPLACE FUNCTION pglogical.truncate_trigger_add()
RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_truncate_trigger_add';

CREATE EVENT TRIGGER pglogical_truncate_trigger_add
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE PROCEDURE pglogical.truncate_trigger_add();

CREATE OR REPLACE FUNCTION pglogical.dependency_check_trigger()
RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_dependency_check_trigger';

CREATE EVENT TRIGGER pglogical_dependency_check_trigger
ON sql_drop
EXECUTE PROCEDURE pglogical.dependency_check_trigger();

CREATE FUNCTION pglogical.pglogical_hooks_setup(internal)
RETURNS void
STABLE LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION pglogical.pglogical_node_info(OUT node_id oid, OUT node_name text, OUT sysid text, OUT dbname text, OUT replication_sets text)
RETURNS record
STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION pglogical.pglogical_gen_slot_name(name)
RETURNS name
IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME';
