CREATE TABLE pglogical.nodes (
	node_id integer NOT NULL PRIMARY KEY,
	node_name name NOT NULL,
	node_role "char" NOT NULL,
	node_status "char" NOT NULL,
	node_dsn text NOT NULL,
	CHECK (node_role IN ('p', 's', 'f')),
	CHECK (node_status IN ('i', 'c', 'r', 'k')),
	UNIQUE (node_name)
);

CREATE TABLE pglogical.connections (
	conn_id integer NOT NULL PRIMARY KEY,
	conn_origin_id integer NOT NULL,
	conn_target_id integer NOT NULL,
	conn_replication_sets text[],
	UNIQUE (conn_origin_id, conn_target_id),
);


CREATE TABLE pglogical.local_node (
	node_id integer
);

CREATE UNIQUE INDEX local_node_onlyone ON pglogical.local_node ((true));


CREATE TABLE pglogical.replication_sets (
	set_id integer NOT NULL PRIMARY KEY,
    set_name name NOT NULL,
    replicate_inserts bool NOT NULL DEFAULT true,
    replicate_updates bool NOT NULL DEFAULT true,
    replicate_deletes bool NOT NULL DEFAULT true,
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
