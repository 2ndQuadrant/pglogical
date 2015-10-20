# pglogical replication

The pglogical module is a logical replication solution based on the
`pglogical_output`. It should be installed on both sides of the replication
connection.

Provider node must run PostgreSQL 9.4+
Subscriber node must run PostgreSQL 9.5+

Use cases supported are
* Data transfer between a master and downstream nodes
* Upgrades between major versions

Long term replication across major versions is not a design target. It very probably works, but restrictions may be introduced and we don't guarantee it will work.

Currently, only provider to subscriber replication is supported. But one subscriber can
merge changes from several masters and detect conflict between changes with
automatic and configurable conflict resolution. Obviously multiple subscribers can
be connected to one master. Cascading replication is implemented in the form of
change forwarding.

Selective replication is also supported in the form of replication sets.

## Usage

This section describes basic usage of the pglogical replication extension.

### Quick setup

First the PostgreSQL server has to be properly configured to support logical
decoding:

    wal_level = 'logical'
    max_worker_processes = 10	# one per database needed on provider node
								# one per node needed on subscriber node
    max_replication_slots = 10	# one per node needed on provider node
    max_wal_senders = 10		# one per node needed on provider node
    shared_preload_libraries = 'pglogical'
    track_commit_timestamp = on	# needed for last/first update wins conflict resolution

Next the `pglogical` extension has to be installed on all nodes:

    CREATE EXTENSION pglogical;

Now create the provider node:

    SELECT pglogical.create_provider(
        node_name := 'node1',
        node_dsn := 'host=hostname1 port=5432 dbname=db'
    );

Optionally you can also create replication sets and add tables to them (see
[Replication sets](#replication-sets)). It's usually better to create replication sets beforehand.

Once the provider node is setup, subscribers can be subscribed to it:

	SELECT pglogical.create_subscriber(
		node_name := 'node1',
        node_dsn := 'host=hostname2 port=5432 dbname=db',
        init_from_dsn := 'host=hostname1 port=5432 dbname=db' # should be same as node_dsn of the provider
	);

### Node management

Nodes can be added and removed dynamically using the SQL interfaces.

- `pglogical.create_provider(node_name name, node_dsn text)`
  Creates a provider node.

  Parameters:
  - `node_name` - name of the new node, must be unique
  - `node_dsn` - connection string used both for connections by other nodes as
    well as back connections on the same node, must be accessible as both
    standard connection and replication connection mode

- `pglogical.create_subscriber(node_name name, node_dsn text, init_from_dsn
  text, replication_sets name[], synchronize pglogical.synchronization_type)`
  Creates a subsriber node.

  Parameters:
  - `node_name` - name of the new node, must be unique
  - `node_dsn` - connection string used both for connections by other nodes as
    well as back connections on the same node, only standard connection will be
    used
  - `init_from_dsn` - connection string to a node which will be used for
    initializing this node (usually the provider node)
  - `replication_sets` - array of replication sets to subscribe to, these must
    already exist, default is "default"
  - `synchronize` - specifies what to synchronize during initialization, can be
    one of "full", which means synchronize structure and data or "none" which
    means the initial syncrhonization should be skipped

- `pglogical.drop_node(node_name name)`
  Disconnect and drop existing node, can be any type of node.

  Parameters:
  - `node_name` - name of the existing node

### Replication sets

Replication sets provide a mechanism to control which tables in the database
will be replicated and which actions on those tables will be replicated.

Each replicated set can specify individually if `INSERTs`, `UPDATEs` and
`DELETEs` on the set are replicated. Every table can be in multiple replication
sets and every node can subscribe to multiple replication sets as well. The
resulting set of tables and actions replicated is the union of the sets the
table is in.

There are two preexisting replication sets named "all" and "default". The "all"
replication set contains every user table in the database and every table that
has not been added to specific replication set will be in the "default" set.

The following functions are provided for managing the replication sets:

- `pglogical.create_replication_set(set_name name, replicate_inserts bool, replicate_updates bool, replicate_deletes bool)`
  This function creates new replication set.

  Parameters:
  - `set_name` - name of the set, must be unique
  - `replicate_inserts` - specifies if `INSERTs` are replicated, default true
  - `replicate_updates` - specifies if `UPDATEs` are replicated, default true
  - `replicate_deletes` - specifies if `DELETEs` are replicated, default true

- `pglogical.alter_replication_set(set_name name, replicate_inserts bool, replicate_updates bool, replicate_deletes bool, replicate_truncate bool)`
  This function change the parameters of the existing replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `replicate_inserts` - specifies if `INSERTs` are replicated, default true
  - `replicate_updates` - specifies if `UPDATEs` are replicated, default true
  - `replicate_deletes` - specifies if `DELETEs` are replicated, default true
  - `replicate_truncate` - specifies if `TRUNCATE` is replicated, default true

- `pglogical.delete_replication_set(set_name text)`
  Removes the replication set.

  Parameters:
  - `set_name` - name of the existing replication set

- `pglogical.replication_set_add_table(set_name name, table_name regclass)`
  Adds a table to replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `table_name` - name or OID of the table to be added to the set

- `pglogical.replication_set_remove_table(set_name name, table_name regclass)`
  Remove a table from replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `table_name` - name or OID of the table to be removed from the set

You can view the information about which table is in which set by querying the
`pglogical.replication_set_tables` view.

## Conflicts

In case the node is subscribed to multiple providers, conflicts between the
incomming changes can arise. These are automatically detected and can be acted
on depending on the configuration.

The configuration of the conflicts resolver is done via the
`pglogical.conflict_resolution` setting. The supported values for the
`pglogical.conflict_resolution` are:

- `error` - the replication will stop on error if conflict is detected and
  manual action is needed for resolving, this is the default
- `apply_remote` - always apply the change that's conflicting with local data
- `keep_local` - keep the local version of the data and ignore the conflicting
  change that is coming from remote node
- `last_update_wins` - the version of data with newest commit timestamp will be
  be kept (this can be either local or remote version), this option requires
  the `track_commit_timestamp` to be enable on all nodes to work
- `first_update_wins` - the version of the data with oldest timestamp will be
  kept (this can be either local or remote version), this option requires the
  `track_commit_timestamp` to be enable on all nodes to work
