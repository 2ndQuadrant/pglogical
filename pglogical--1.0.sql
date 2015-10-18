CREATE TABLE pglogical.nodes (
	node_id integer NOT NULL PRIMARY KEY,
	node_name name NOT NULL,
	node_role "char" NOT NULL,
	node_status "char" NOT NULL,
	node_dsn text NOT NULL,
	node_init_dsn text,
	CHECK (node_role IN ('p', 's', 'f')),
	CHECK (node_status IN ('i', 'c', 'r', 'k')),
	UNIQUE (node_name)
);

CREATE TABLE pglogical.local_node (
	node_id integer
);

CREATE UNIQUE INDEX local_node_onlyone ON pglogical.local_node ((true));

CREATE FUNCTION pglogical.create_node(node_name name, node_role "char", node_dns text, node_init_dsn text = NULL)
RETURNS int STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_node';
CREATE FUNCTION pglogical.drop_node(node_name name)
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_node';

CREATE FUNCTION pglogical.wait_for_node_ready()
RETURNS void STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_node_ready';


CREATE TABLE pglogical.connections (
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


CREATE TABLE pglogical.replication_sets (
	set_id integer NOT NULL PRIMARY KEY,
    set_name name NOT NULL,
    replicate_inserts boolean NOT NULL DEFAULT true,
    replicate_updates boolean NOT NULL DEFAULT true,
    replicate_deletes boolean NOT NULL DEFAULT true,
	UNIQUE (set_name)
) WITH (user_catalog_table=true);

INSERT INTO pglogical.replication_sets VALUES (-1, 'default', true, true, true);
INSERT INTO pglogical.replication_sets VALUES (-2, 'all', true, true, true);

CREATE TABLE pglogical.replication_set_tables (
    set_id integer NOT NULL,
	set_relation regclass NOT NULL,
	PRIMARY KEY(set_id, set_relation)
) WITH (user_catalog_table=true);

CREATE VIEW pglogical.tables AS
	WITH set_tables AS (
		SELECT s.set_name, t.set_relation
		  FROM pglogical.replication_set_tables t,
			   pglogical.replication_sets s
		 WHERE s.set_id = t.set_id
    ),
	user_tables AS (
		SELECT r.oid, n.nspname, r.relname
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
		   pglogical.replication_sets rs
     WHERE rs.set_id = -1
	   AND t.oid NOT IN (SELECT set_relation FROM set_tables)
	 UNION ALL
    SELECT rs.set_name, t.nspname, t.relname
	  FROM user_tables t,
		   pglogical.replication_sets rs
     WHERE rs.set_id = -2;


CREATE FUNCTION pglogical.create_replication_set(set_name name,
	replicate_inserts boolean = true, replicate_updates boolean = true,
	replicate_deletes boolean = true)
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
