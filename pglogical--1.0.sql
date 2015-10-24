CREATE TABLE pglogical.node (
	node_id integer NOT NULL PRIMARY KEY,
	node_name name NOT NULL,
	node_status "char" NOT NULL,
	node_dsn text NOT NULL,
	CHECK (node_status IN ('i', 'c', 'r', 'k')),
	UNIQUE (node_name)
);

CREATE TABLE pglogical.local_node (
	node_id integer NOT NULL REFERENCES pglogical.node(node_id),
	node_local_dsn text
);
COMMENT ON TABLE pglogical.local_node IS 'Holds information about which node is local to the current server/database';
COMMENT ON COLUMN pglogical.local_node.node_id IS 'Id of the node in the pglogical.nodes table which is considered local';
COMMENT ON COLUMN pglogical.local_node.node_id IS 'Loopback connection string';

CREATE UNIQUE INDEX local_node_onlyone ON pglogical.local_node ((true));

CREATE FUNCTION pglogical.create_node(node_name name, node_dns text)
RETURNS int STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_node';
CREATE FUNCTION pglogical.drop_node(node_name name)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_node';

CREATE FUNCTION pglogical.wait_for_node_ready()
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_node_ready';


CREATE TABLE pglogical.connection (
	conn_id integer NOT NULL PRIMARY KEY,
	conn_origin_id integer NOT NULL,
	conn_target_id integer NOT NULL,
	conn_replication_sets text[],
	UNIQUE (conn_origin_id, conn_target_id)
);

CREATE FUNCTION pglogical.create_connection(origin name, target name, replication_sets text[] = '{default}')
RETURNS int STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_connection';
CREATE FUNCTION pglogical.drop_connection(origin name, target name)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_connection';


CREATE TABLE pglogical.replication_set (
	set_id integer NOT NULL PRIMARY KEY,
    set_name name NOT NULL,
    replicate_insert boolean NOT NULL DEFAULT true,
    replicate_update boolean NOT NULL DEFAULT true,
    replicate_delete boolean NOT NULL DEFAULT true,
    replicate_truncate boolean NOT NULL DEFAULT true,
	UNIQUE (set_name)
) WITH (user_catalog_table=true);

INSERT INTO pglogical.replication_set VALUES (-1, 'default', true, true, true, true);
INSERT INTO pglogical.replication_set VALUES (-2, 'all', false, false, false, false);

CREATE TABLE pglogical.replication_set_table (
    set_id integer NOT NULL,
	set_relation regclass NOT NULL,
	PRIMARY KEY(set_id, set_relation)
) WITH (user_catalog_table=true);

CREATE VIEW pglogical.tables AS
	WITH set_tables AS (
		SELECT s.set_name, t.set_relation
		  FROM pglogical.replication_set_table t,
			   pglogical.replication_set s
		 WHERE s.set_id = t.set_id
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
	   AND r.oid = s.set_relation
	 UNION
    SELECT rs.set_name, t.nspname, t.relname
	  FROM user_tables t,
		   pglogical.replication_set rs,
		   pg_catalog.pg_index i
     WHERE rs.set_id = -1
	   AND t.oid NOT IN (SELECT set_relation FROM set_tables)
	   AND i.indrelid = t.oid
           /* Only tables with replica identity index can be in default replication set. */
	   AND ((relreplident = 'd' AND i.indisprimary) OR (relreplident = 'i' AND i.indisreplident))
	 UNION ALL
    SELECT rs.set_name, t.nspname, t.relname
	  FROM user_tables t,
		   pglogical.replication_set rs
     WHERE rs.set_id = -2;


CREATE FUNCTION pglogical.create_replication_set(set_name name,
	replicate_insert boolean = true, replicate_update boolean = true,
	replicate_delete boolean = true, replicate_truncate boolean = true)
RETURNS int STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_replication_set';
CREATE FUNCTION pglogical.drop_replication_set(set_name name)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_replication_set';

CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';
CREATE FUNCTION pglogical.replication_set_remove_table(set_name name, relation regclass)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_remove_table';

CREATE FUNCTION pglogical.origin_filter(filter text, origin text)
RETURNS boolean STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_origin_filter';
CREATE FUNCTION pglogical.table_filter(nodename text, relid oid, action "char")
RETURNS boolean STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_table_filter';


CREATE TABLE pglogical.queue (
    queued_at timestamp with time zone NOT NULL,
    role name NOT NULL,
    message_type "char" NOT NULL,
    message json NOT NULL
);

CREATE FUNCTION pglogical.replicate_ddl_command(command text)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replicate_ddl_command';

CREATE OR REPLACE FUNCTION pglogical.queue_truncate()
RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_queue_truncate';

CREATE OR REPLACE FUNCTION pglogical.truncate_trigger_add()
RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_truncate_trigger_add';

CREATE EVENT TRIGGER pglogical_truncate_trigger_add
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE PROCEDURE pglogical.truncate_trigger_add();
