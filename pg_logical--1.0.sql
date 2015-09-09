CREATE TABLE pg_logical.nodes (
	node_name text primary key;
	node_role "char" not null;
	node_status "char" not null;
	node_dsn text not null;
	check (node_role in ('p', 's', 'f'));
	check (node_status in ('i', 'c', 'r', 'k'));
);

CREATE TABLE pg_logical.connections (
	conn_origin_name text not null references pg_logical_nodes(node_name);
	conn_target_name text not null references pg_logical_nodes(node_name);
	conn_replication_sets text[];
	primary key (conn_origin_name, conn_target_name);
);


CREATE TABLE pg_logical.local_node (
	node_name text;
);

CREATE UNIQUE INDEX local_node_onlyone ON pg_logical.local_node ((true));

