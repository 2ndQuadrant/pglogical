CREATE TABLE pg_logical.nodes (
	node_id serial primary key;
	node_name text not null unique;
	node_role "char" not null;
	node_status "char" not null;
	node_dsn text not null;
	check (node_role in ('p', 's', 'f'));
	check (node_status in ('i', 'c', 'r', 'k'));
);

CREATE TABLE pg_logical.connections (
	conn_id serial primary key;
	conn_origin_id integer not null references pg_logical_nodes(node_id);
	conn_target_id integer not null references pg_logical_nodes(node_id);
	conn_replication_sets text[];
	unique (conn_origin_name, conn_target_name);
);


CREATE TABLE pg_logical.local_node (
	node_id integer;
);

CREATE UNIQUE INDEX local_node_onlyone ON pg_logical.local_node ((true));

