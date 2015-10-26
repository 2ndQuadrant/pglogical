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
        provider_name := 'provider1',
    );

Optionally you can also create replication sets and add tables to them (see
[Replication sets](#replication-sets)). It's usually better to create replication sets beforehand.

Once the provider node is setup, subscribers can be subscribed to it:

	SELECT pglogical.create_subscriber(
		subscriber_name := 'subscriber1',
        provider_name := 'provider1',
        provider_dsn := 'host=providerhost port=5432 dbname=db'
	);

### Node management

Nodes can be added and removed dynamically using the SQL interfaces.

- `pglogical.create_provider(provider_name name)`
  Creates a provider node.

  Parameters:
  - `provider_name` - name of the new provider, currently only one provider
    is allowed per database

- `pglogical.create_subscriber(subscriner_name name, provider_name name,
  provider_dsn text, replication_sets name[], synchronize_schema boolean
  synchronize_data boolean)`
  Creates a subsriber node.

  Parameters:
  - `subscriner_name` - name of the subscriber, must be unique
  - `provider_name` - name of the provider to which this subscriber will be
     connected
  - `provider_dsn` - connection string to a provider node
  - `replication_sets` - array of replication sets to subscribe to, these must
    already exist, default is "default"
  - `synchronize_schema` - specifies is to synchronize schema from provider to
    the subscriber, default true
  - `synchronize_data` - specifies is to synchronize data from provider to
    the subscriber, default true

- `pglogical.drop_provider(provider_name name)`
  Removes the provider and disconnect all subscrbers from it.

  Parameters:
  - `subscriber_name` - name of the existing provider

- `pglogical.drop_subscriber(subscriber_name name)`
  Disconnects the subsriber and removes it from the catalog.

  Parameters:
  - `subscriber_name` - name of the existing subscriber

### Replication sets

Replication sets provide a mechanism to control which tables in the database
will be replicated and which actions on those tables will be replicated.

Each replicated set can specify individually if `INSERTs`, `UPDATEs`,
`DELETEs` and `TRUNCATEs` on the set are replicated. Every table can be in
multiple replication sets and every subscriber can subscribe to multiple
replication sets as well. The resulting set of tables and actions replicated
is the union of the sets the table is in.

There are two preexisting replication sets named "all" and "default". The "all"
replication set contains every user table in the database and every table that
has not been added to specific replication set will be in the "default" set.

The following functions are provided for managing the replication sets:

- `pglogical.create_replication_set(set_name name, replicate_insert bool, replicate_update bool, replicate_delete bool, replicate_truncate bool)`
  This function creates new replication set.

  Parameters:
  - `set_name` - name of the set, must be unique
  - `replicate_insert` - specifies if `INSERT` is replicated, default true
  - `replicate_update` - specifies if `UPDATE` is replicated, default true
  - `replicate_delete` - specifies if `DELETE` is replicated, default true
  - `replicate_truncate` - specifies if `TRUNCATE` is replicated, default true

- `pglogical.alter_replication_set(set_name name, replicate_inserts bool, replicate_updates bool, replicate_deletes bool, replicate_truncate bool)`
  This function change the parameters of the existing replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `replicate_insert` - specifies if `INSERT` is replicated, default true
  - `replicate_update` - specifies if `UPDATE` is replicated, default true
  - `replicate_delete` - specifies if `DELETE` is replicated, default true
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
