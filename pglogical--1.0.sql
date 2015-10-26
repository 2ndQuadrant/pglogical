CREATE TABLE pglogical.provider (
	provider_id oid NOT NULL PRIMARY KEY,
	provider_name name NOT NULL
) WITH (user_catalog_table=true);

-- Currently we allow only one provider record per database, this may change in the future
CREATE UNIQUE INDEX provider_onlyone ON pglogical.provider ((true));

CREATE FUNCTION pglogical.create_provider(provider_name name)
RETURNS oid STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_provider';


CREATE TABLE pglogical.subscriber (
	subscriber_id oid NOT NULL PRIMARY KEY,
	subscriber_name name NOT NULL,
	subscriber_status "char" NOT NULL,
	subscriber_provider_name name NOT NULL,
	subscriber_provider_dsn text NOT NULL,
	subscriber_replication_sets text[],
	CHECK (subscriber_status IN ('i', 's', 'd', 'c', 'u', 'r')),
	UNIQUE (subscriber_name)
);

CREATE FUNCTION pglogical.create_subscriber(subscriber_name name, provider_name name, provider_dsn text,
	replication_sets text[] = '{default}', synchronize_schema boolean = true, syncrhonize_data boolean = true)
RETURNS oid STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscriber';

CREATE FUNCTION pglogical.wait_for_subscriber_ready(subscriber_name name)
RETURNS boolean STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_subscriber_ready';


CREATE TABLE pglogical.replication_set (
	set_id oid NOT NULL PRIMARY KEY,
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
RETURNS oid STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_replication_set';
CREATE FUNCTION pglogical.drop_replication_set(set_name name)
RETURNS boolean STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_drop_replication_set';

CREATE FUNCTION pglogical.replication_set_add_table(set_name name, relation regclass)
RETURNS boolean STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_add_table';
CREATE FUNCTION pglogical.replication_set_remove_table(set_name name, relation regclass)
RETURNS boolean STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replication_set_remove_table';


CREATE TABLE pglogical.queue (
    queued_at timestamp with time zone NOT NULL,
	provider_name name NOT NULL,
    role name NOT NULL,
    message_type "char" NOT NULL,
    message json NOT NULL
);

CREATE FUNCTION pglogical.replicate_ddl_command(command text)
RETURNS boolean STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_replicate_ddl_command';

CREATE OR REPLACE FUNCTION pglogical.queue_truncate()
RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_queue_truncate';

CREATE OR REPLACE FUNCTION pglogical.truncate_trigger_add()
RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_truncate_trigger_add';

CREATE EVENT TRIGGER pglogical_truncate_trigger_add
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE PROCEDURE pglogical.truncate_trigger_add();


