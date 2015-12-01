# pglogical replication

The pglogical module provides logical streaming replication for PostgreSQL, using a publish/subscribe module.

We use the following terms to describe data streams between nodes, deliberately reused from the earlier Slony technology.
* Nodes - PostgreSQL database instances
* Providers and Subscribers - roles taken by Nodes
* Replication Set - a collection of tables

pglogical is new technology utilising the latest in-core features, so we have these version restrictions
* Provider node must run PostgreSQL 9.4+
* Subscriber node must run PostgreSQL 9.5+

Use cases supported are
* Upgrades between major versions (given the above restrictions)
* Full database replication
* Selective replication of sets of tables using replication sets

Architectural details
* pglogical works at the database level, not whole server level like physical streaming replication
* One Provider may feed multiple Subscribers without incurring additional write overhead
* One Subscriber can merge changes from several origins and detect conflict between changes with
automatic and configurable conflict resolution (some, but not all aspects required for multi-master).
* Cascading replication is implemented in the form of change forwarding.

## Requirements

DDL replication is not supported; DDL is the responsibility of the user. As such, long term cross-version replication is not considered a design target, though it may often work.

* Currently pglogical replication requires superuser. It may be later extended to user with replication privileges. 
* UNLOGGED and TEMP tables will not and cannot be replicated. 
* Updates and Deletes cannot be replicated for tables that lack both a primary key and a replica identity - we have no way to find the tuple that should be updated/deleted since there is no unique identifier.
* `pglogical_output` needs to be installed on both provider and subscriber. 

## Usage

This section describes basic usage of the pglogical replication extension.

### Quick setup

First the PostgreSQL server has to be properly configured to support logical
decoding:

    wal_level = 'logical'
    max_worker_processes = 10   # one per database needed on provider node
                                # one per node needed on subscriber node
    max_replication_slots = 10  # one per node needed on provider node
    max_wal_senders = 10        # one per node needed on provider node
    shared_preload_libraries = 'pglogical'
    track_commit_timestamp = on # needed for last/first update wins conflict resolution

Next the `pglogical` extension has to be installed on all nodes:

    CREATE EXTENSION pglogical;

Now create the provider node:

    SELECT pglogical.create_node(
        node_name := 'provider1',
        dsn := 'host=providerhost port=5432 dbname=db'
    );

Optionally you can also create replication sets and add tables to them (see
[Replication sets](#replication-sets)). It's usually better to create replication sets beforehand.

Once the provider node is setup, subscribers can be subscribed to it:

    SELECT pglogical.create_node(
        node_name := 'subscriber1',
        dsn := 'host=thishost port=5432 dbname=db'
    );

    SELECT pglogical.create_subscription(
        subscription_name := 'subscription1',
        provider_dsn := 'host=providerhost port=5432 dbname=db'
    );
(run this on the subscriber node)

### Node management

Nodes can be added and removed dynamically using the SQL interfaces.

- `pglogical.create_node(node_name name, dsn text)`
  Creates a node.

  Parameters:
  - `node_name` - name of the new node, only one node is allowed per database
  - `dsn` - connection string to the node, for nodes that are supposed to be
    providers, this should be reacheble from outside

- `pglogical.pglogical_drop_node(node_name name, ifexists bool)`
  Drops the pglogical node.

  Parameters:
  - `node_name` - name of the existing node
  - `ifexists` - if true, error is not thrown when subscription does not exist,
    default is false

### Subscription management

- `pglogical.create_subscription(subscription_name name, provider_dsn text,
  replication_sets text[], synchronize_structure boolean,
  synchronize_data boolean)`
  Creates a subscription from current node to the provider node. Command does
  not block, just initiates the action.

  Parameters:
  - `subscription_name` - name of the subscription, must be unique
  - `provider_dsn` - connection string to a provider
  - `replication_sets` - array of replication sets to subscribe to, these must
    already exist, default is "default"
  - `synchronize_structure` - specifies if to synchronize structure from
    provider to the subscriber, default true
  - `synchronize_data` - specifies if to synchronize data from provider to
    the subscriber, default true

- `pglogical.pglogical_drop_subscription(subscription_name name, ifexists bool)`
  Disconnects the subscription and removes it from the catalog.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `ifexists` - if true, error is not thrown when subscription does not exist,
    default is false

- `pglogical.alter_subscription_disable(subscription_name name, immediate bool)`
   Disables a subscription and disconnects it from the provider.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `immediate` - if true, the subscription is stopped immediately, otherwise
    it will be only stopped at the end of current transaction, default is false

- `pglogical.alter_subscription_enable(subscription_name name, immediate bool)`
  Enables disabled subscription.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `immediate` - if true, the subscription is started immediately, otherwise
    it will be only started at the end of current transaction, default is false

- `pglogical.alter_subscription_synchronize(subscription_name name, truncate bool)`
  All unsynchronized tables in all sets are synchronized in a single operation.
  Tables are copied and synchronized one by one. Command does not block, just
  initiates the action.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `truncate` - if true, tables will be truncated before copy, default false

- `pglogical.alter_subscription_resynchronize_table(subscription_name name,
  relation regclass)`
  Resynchronize one existing table.
  **WARNING: This function will truncate the table first.**

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `relation` - name of existing table, optinally qualified

- `pglogical.show_subscription_table(subscription_name name,
  relation regclass)`
  Shows syncrhonization status of a table.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `relation` - name of existing table, optinally qualified

- `pglogical.alter_subscriber_add_replication_set(subscription_name name,
  replication_set name)`
  Adds one replication set into a subscriber. Does not synchronize, only
  activates consumption of events.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `replication_set` - name of replication set to add

- `pglogical.alter_subscriber_remove_replication_set(subscription_name name,
  replication_set name)`
  Removes one replication set from a subscriber.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `replication_set` - name of replication set to remove

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
  This function creates a new replication set.

  Parameters:
  - `set_name` - name of the set, must be unique
  - `replicate_insert` - specifies if `INSERT` is replicated, default true
  - `replicate_update` - specifies if `UPDATE` is replicated, default true
  - `replicate_delete` - specifies if `DELETE` is replicated, default true
  - `replicate_truncate` - specifies if `TRUNCATE` is replicated, default true

- `pglogical.alter_replication_set(set_name name, replicate_inserts bool, replicate_updates bool, replicate_deletes bool, replicate_truncate bool)`
  This function changes the parameters of the existing replication set.

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

- `pglogical.replication_set_add_table(set_name name, table_name regclass, synchronize boolean)`
  Adds a table to replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `table_name` - name or OID of the table to be added to the set
  - `synchronize` - if true, the table data is synchronized on all subscribers
    which are subscribed to given replication set, default false

- `pglogical.replication_set_remove_table(set_name name, table_name regclass)`
  Remove a table from replication set.

  Parameters:
  - `set_name` - name of the existing replication set
  - `table_name` - name or OID of the table to be removed from the set

You can view the information about which table is in which set by querying the
`pglogical.replication_set_tables` view.

#### Automatic assignment of replication sets for new tables

The event trigger facility can be used for describing rules which define
replication sets for newly created tables.

Example:

    CREATE OR REPLACE FUNCTION pglogical_assign_repset()
    RETURNS event_trigger AS $$
    DECLARE obj record;
    BEGIN
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF obj.object_type = 'table' AND obj.schema_name = 'config' THEN
                PERFORM pglogical.replication_set_add_table('configuration', obj.objid);
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    CREATE EVENT TRIGGER pglogical_assign_repset_trg
        ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
        EXECUTE PROCEDURE pglogical_assign_repset();

The above example will put all new tables created in schema `config` into
replication set `configuration`.

## Conflicts

In case the node is subscribed to multiple providers, conflicts between the
incomming changes can arise. These are automatically detected and can be acted
on depending on the configuration.

The configuration of the conflicts resolver is done via the
`pglogical.conflict_resolution` setting. The supported values for the
`pglogical.conflict_resolution` are:

- `error` - the replication will stop on error if conflict is detected and
  manual action is needed for resolving
- `apply_remote` - always apply the change that's conflicting with local data,
  this is the default
- `keep_local` - keep the local version of the data and ignore the conflicting
  change that is coming from the remote node
- `last_update_wins` - the version of data with newest commit timestamp will be
  be kept (this can be either local or remote version)
- `first_update_wins` - the version of the data with oldest timestamp will be
  kept (this can be either local or remote version)

When `track_commit_timestamp` is disabled, the only allowed value is
`apply_remote`.
