# pglogical replication

The pglogical module provides logical streaming replication for PostgreSQL,
using a publish/subscribe module. It is based on technology developed as part
of the BDR project (http://2ndquadrant.com/BDR).

We use the following terms to describe data streams between nodes, deliberately
reused from the earlier Slony technology:
* Nodes - PostgreSQL database instances
* Providers and Subscribers - roles taken by Nodes
* Replication Set - a collection of tables

pglogical is new technology utilising the latest in-core features, so we have these version restrictions:
* Provider & subscriber nodes must run PostgreSQL 9.4+
* PostgreSQL 9.5+ is required for replication origin filtering and conflict detection

Use cases supported are:
* Upgrades between major versions (given the above restrictions)
* Full database replication
* Selective replication of sets of tables using replication sets
* Data gather/merge from multiple upstream servers

Architectural details:
* pglogical works on a per-database level, not whole server level like
  physical streaming replication
* One Provider may feed multiple Subscribers without incurring additional disk
  write overhead
* One Subscriber can merge changes from several origins and detect conflict
  between changes with automatic and configurable conflict resolution (some,
  but not all aspects required for multi-master).
* Cascading replication is implemented in the form of changeset forwarding.

## Requirements

To use pglogical the provider and subscriber must be running PostgreSQL 9.4 or newer.

The `pglogical_output` extension needs to be installed on both provider and
subscriber. No actual `CREATE EXTENSION` is required, it must just be present
in the PostgreSQL installation.

The `pglogical` extension must be installed on both provider and subscriber.
You must `CREATE EXTENSION pglogical` on both.

Tables on the provider and subscriber must have the same names and be in the
same schema. Future revisions may add mapping features.

Tables on the provider and subscriber must have the same columns, with the same
data types in each column. `CHECK` constraints, `NOT NULL` constraints, etc must
be the same or weaker (more permissive) on the subscriber than the provider.

Tables must have the same `PRIMARY KEY`s. It is not recommended to add additional
`UNIQUE` constraints other than the `PRIMARY KEY` (see below).

Any `FOREIGN KEY`s on the subscriber must also be present on the provider with
all rows on the subscriber present on the provider. In other words a
subscriber's FK constraints must always permit a write if the provider's FK
constraints permitted it.

Some additional requirements are covered in "Limitations and Restrictions", below.

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

If you are using PostgreSQL 9.5+ (this won't work on 9.4) and want to handle
conflict resolution with last/first update wins (see [Conflicts](#conflicts)),
you can add this additional option to postgresql.conf:

    track_commit_timestamp = on # needed for last/first update wins conflict resolution
                                # property available in PostgreSQL 9.5+

`pg_hba.conf` has to allow replication connections from localhost, by a user
with replication privilege.

Next the `pglogical` extension has to be installed on all nodes:

    CREATE EXTENSION pglogical;

Now create the provider node:

    SELECT pglogical.create_node(
        node_name := 'provider1',
        dsn := 'host=providerhost port=5432 dbname=db'
    );

Add all tables in `public` schema to the `default` replication set.

    SELECT pglogical.replication_set_add_all_tables('default', ARRAY['public']);

Optionally you can also create additional replication sets and add tables to
them (see [Replication sets](#replication-sets)).

It's usually better to create replication sets before subscribing so that all
tables are synchronized during initial replication setup in a single initial
transaction. However, users of bigger databases may instead wish to create them
incrementally for better control.

Once the provider node is setup, subscribers can be subscribed to it. First the
subscriber node must be created:

    SELECT pglogical.create_node(
        node_name := 'subscriber1',
        dsn := 'host=thishost port=5432 dbname=db'
    );

And finally on the subscriber node you can create the subscription which will
start synchronization and replication process in the background:

    SELECT pglogical.create_subscription(
        subscription_name := 'subscription1',
        provider_dsn := 'host=providerhost port=5432 dbname=db'
    );

### Node management

Nodes can be added and removed dynamically using the SQL interfaces.

- `pglogical.create_node(node_name name, dsn text)`
  Creates a node.

  Parameters:
  - `node_name` - name of the new node, only one node is allowed per database
  - `dsn` - connection string to the node, for nodes that are supposed to be
    providers, this should be reachable from outside

- `pglogical.drop_node(node_name name, ifexists bool)`
  Drops the pglogical node.

  Parameters:
  - `node_name` - name of the existing node
  - `ifexists` - if true, error is not thrown when subscription does not exist,
    default is false

### Subscription management

- `pglogical.create_subscription(subscription_name name, provider_dsn text,
  replication_sets text[], synchronize_structure boolean,
  synchronize_data boolean, forward_origins text[])`
  Creates a subscription from current node to the provider node. Command does
  not block, just initiates the action.

  Parameters:
  - `subscription_name` - name of the subscription, must be unique
  - `provider_dsn` - connection string to a provider
  - `replication_sets` - array of replication sets to subscribe to, these must
    already exist, default is "{default,default_insert_only}"
  - `synchronize_structure` - specifies if to synchronize structure from
    provider to the subscriber, default true
  - `synchronize_data` - specifies if to synchronize data from provider to
    the subscriber, default true
  - `forward_origins` - array of origin names to forward, currently only
    supported values are empty array meaning don't forward any changes
    that didn't originate on provider node, or "{all}" which means replicate
    all changes no matter what is their origin, default is "{all}"

- `pglogical.drop_subscription(subscription_name name, ifexists bool)`
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
  - `relation` - name of existing table, optionally qualified

- `pglogical.show_subscription_status(subscription_name name)`
  Shows status and basic information about subscription.

  Parameters:
  - `subscription_name` - optional name of the existing subscription, when no
    name was provided, the function will show status for all subscriptions on
    local node

- `pglogical.show_subscription_table(subscription_name name,
  relation regclass)`
  Shows synchronization status of a table.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `relation` - name of existing table, optionally qualified

- `pglogical.alter_subscription_add_replication_set(subscription_name name,
  replication_set name)`
  Adds one replication set into a subscriber. Does not synchronize, only
  activates consumption of events.

  Parameters:
  - `subscription_name` - name of the existing subscription
  - `replication_set` - name of replication set to add

- `pglogical.alter_subscription_remove_replication_set(subscription_name name,
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
is the union of the sets the table is in. The tables are not replicated until
they are added into a replication set.

There are two preexisting replication sets named "default" and
"default_insert_only". The "default" replication set is defined to replicate
all changes to tables in in. The "default_insert_only" only replicates INSERTs
and is meant for tables that don't have primary key (see
[Limitations](#primary-key-or-replica-identity-required) section for details).

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

- `pglogical.drop_replication_set(set_name text)`
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

- `pglogical.replication_set_add_all_tables(set_name name, schema_names text[], synchronize boolean)`
  Adds all tables in given schemas. Only existing tables are added, table that
  will be created in future will not be added automatically. For how to ensure
  that tables created in future are added to correct replication set, see
  [Automatic assignment of replication sets for new tables](#automatic-assignment-of-replication-sets-for-new-tables).

  Parameters:
  - `set_name` - name of the existing replication set
  - `schema_names` - array of names name of existing schemas from which tables
    should be added
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
            IF obj.object_type = 'table' THEN
                IF obj.schema_name = 'config' THEN
                    PERFORM pglogical.replication_set_add_table('configuration', obj.objid);
                ELSIF NOT obj.in_extension THEN
                    PERFORM pglogical.replication_set_add_table('default', obj.objid);
                END IF;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    CREATE EVENT TRIGGER pglogical_assign_repset_trg
        ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
        EXECUTE PROCEDURE pglogical_assign_repset();

The above example will put all new tables created in schema `config` into
replication set `configuration` and all other new tables which are not created
by extensions will go to `default` replication set.

## Conflicts

In case the node is subscribed to multiple providers, or when local writes
happen on a subscriber, conflicts can arise for the incoming changes. These
are automatically detected and can be acted on depending on the configuration.

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
`apply_remote`. As `track_commit_timestamp` is not available in PostgreSQL 9.4
`pglogical.conflict_resolution` can only be `apply_remote` (default)

## Limitations and restrictions

### Superuser is required

Currently pglogical replication requires superuser. It may be later extended to
user with replication privileges.

### `UNLOGGED` and `TEMPORARY` not replicated

`UNLOGGED` and `TEMPORARY` tables will not and cannot be replicated, much like
with physical streaming replication.

### One database at a time

To replicate multiple databases you must set up individual provider/subscriber
relationships for each. There is no way to configure replication for all databases
in a PostgreSQL install at once.

### PRIMARY KEY or REPLICA IDENTITY required

`UPDATE`s and `DELETE`s cannot be replicated for tables that lack a `PRIMARY
KEY` or other valid replica identity such as a `UNIQUE` constraint. Replication
has no way to find the tuple that should be updated/deleted since there is no
unique identifier.

See http://www.postgresql.org/docs/current/static/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY for details on replica identity.

### Only one unique index/constraint/PK

If more than one upstream is configured or the downstream accepts local writes
then only one `UNIQUE` index should be present on downstream replicated tables.
Conflict resolution can only use one index at a time so conflicting rows may
`ERROR` if a row satisfies the `PRIMARY KEY` but violates a `UNIQUE` constraint
on on the downstream side. This will stop replication until the downstream table
is modified to remove the violation.

It's fine to have extra unique constraints on an upstream if the downstream only
gets writes from that upstream and nowhere else. The rule is that the downstream
constraints must *not be more restrictive* than those on the upstream(s).

### DDL

Automatic DDL replication is not supported. Managing DDL so that the provider and
subscriber database(s) remain compatible is the responsibility of the user.

pglogical provides the `pglogical.replicate_ddl_command` function to allow DDL
to be run on the provider and subscriber at a consistent point.

### No replication queue flush

There's no support for freezing transactions on the master and waiting until
all pending queued xacts are replayed from slots. Support for making the
upstream read-only for this will be added in a future release.

This means that care must be taken when applying table structure changes. If
there are committed transactions that aren't yet replicated and the table
structure of the provider and subscriber are changed at the same time in a way
that makes the subscriber table incompatible with the queued transactions
replication will stop.

Administrators should either ensure that writes to the master are stopped
before making schema changes, or use the `pglogical.replicate_ddl_command`
function to queue schema changes so they're replayed at a consistent point
on the replica.

Once multi-master replication support is added then then using
`pglogical.replicate_ddl_command` will not be enough, as the subscriber may be
generating new xacts with the old structure after the schema change is
committed on the publisher. Users will have to ensure writes are stopped on all
nodes and all slots are caught up before making schema changes.

### TRUNCATE

Truncating a table that has foreign key relationships on the subscriber but not
the provider will fail on the subscriber. Replication will stop. To allow
replication to continue the foreign key relationship must be dropped. Using
`TRUNCATE ... CASCADE` will not help because the `CASCADE` only applies to tables
on the provider side.

(Properly handling this would probably require the addition of `ON TRUNCATE CASCADE`
support for foreign keys in PostgreSQL).

`TRUNCATE ... RESTART IDENTITY` is not supported. The identity restart step is
not replicated to the replica.

### Sequences

Sequence state is local to the provider or subscriber node(s). Advancing a
sequence on the provider has no effect on the subscriber or vice versa.

Support for replicating sequences is important for failover support and will
be added in a future version.

Users may work around this limitation by creating sequences with a step
interval equal to or greater than the number of nodes, then setting a different
offset on each node. Use the `INCREMENT BY` option for `CREATE SEQUENCE` or
`ALTER SEQUENCE`, and use `setval(...)` to set the start point.

### Triggers

No triggers are fired when applying changes on the subscriber.

Firing of `ENABLE REPLICA` and `ENABLE ALWAYS` triggers may be added by a later
version.

### PostgreSQL Version differences

pglogical can replicate across PostgreSQL major versions. Despite that, long
term cross-version replication is not considered a design target, though it may
often work. Issues where changes are valid on the provider but not on the
subscriber are more likely to arise when replicating across versions.

It is safer to replicate from an old version to a newer version since PostgreSQL
maintains solid backward compatibility but only limited forward compatibility.

Replicating between different minor versions makes no difference at all.

### Doesn't replicate DDL

Logical decoding doesn't decode catalog changes directly. So the plugin can't
just send a `CREATE TABLE` statement when a new table is added.

If the data being decoded is being applied to another PostgreSQL database then
its table definitions must be kept in sync via some means external to the logical
decoding plugin its self, such as:

* Event triggers using DDL deparse to capture DDL changes as they happen and write them to a table to be replicated and applied on the other end; or
* doing DDL management via tools that synchronise DDL on all nodes

## How does pglogical differ from BDR?

`pglogical` is based on technology developed for BDR and shares some code with
BDR. It's designed to be more flexible than BDR and to apply better to
single-master unidirectional replication, data-gather/merge, non-mesh
multimaster topologies, etc.

It omits some features found in BDR:

* Mesh multi-master. Limited multi-master support with conflict resolution exists,
  but mutual replication connections must be added individually.

* Distributed sequences. Use different sequence offsets on each node instead.

* DDL replication. Users must keep table definitions consistent themselves.
  pglogical provides queue functions to help with this.

* Global DDL locking. There's no DDL replication so no global locking is
  required.... but that introduces problems with mutual multi-master replication.
  See next point.

* Global flush-to-consistent-state. Part of BDR's DDL locking is a step
  where all nodes' queues are plugged by prevening new xacts from being
  committed, then flushed to the peer nodes. This ensures there are no xacts in
  the queue that can't be applied once table structure has changed. pglogical
  doesn't do this so multi-master replication (where nodes replicate to each
  other) is not yet supported.
  See "limitations".

See "limitations and restrictions" for more information.

It also adds some features:

* Flexible connections between nodes; topology is not restricted to
  a mesh configuration like BDR's. Cascading logical replication is possible.

* Loosely-coupled output plugin that's re-usable for other projects

* JSON output so queued transactions can be inspected

... but its main purpose is to provide a cleaner, simpler base that doesn't
require a patched PostgreSQL, with a pluggable and extensible design.

## Credits and Licence

pglogical has been designed, developed and tested by the 2ndQuadrant team
* Petr Jelinek
* Craig Ringer
* Simon Riggs
* Pallavi Sontakke
* Umair Shahid

pglogical licence is The PostgreSQL Licence

pglogical copyright is novated to PostgreSQL Global Development Group
