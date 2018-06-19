# `pglogical` Output Plugin

This is the [logical decoding](http://www.postgresql.org/docs/current/static/logicaldecoding.html)
[output plugin](http://www.postgresql.org/docs/current/static/logicaldecoding-output-plugin.html)
for `pglogical`. Its purpose is to extract a change stream from a PostgreSQL
database and send it to a client over a network connection using a
well-defined, efficient protocol that multiple different applications can
consume.

The primary purpose of `pglogical_output` is to supply data to logical
streaming replication solutions, but any application can potentially use its
data stream. The output stream is designed to be compact and fast to decode,
and the plugin supports upstream filtering of data (through hooks) so that only
the required information is sent.

Only one database is replicated, rather than the whole PostgreSQL install. A
subset of that database may be selected for replication, currently based on
table and on replication origin. Filtering by a WHERE clause can be supported
easily in future.

No triggers are required to collect the change stream and no external ticker or
other daemon is required. The stream of changes is accumulated using
[replication slots](http://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html#AEN66446),
as supported in PostgreSQL 9.4 or newer, and sent on top of the
[PostgreSQL streaming replication protocol](http://www.postgresql.org/docs/current/static/protocol-replication.html).

Unlike block-level ("physical") streaming replication, the change stream from
`pglogical_output` is compatible across different PostgreSQL
versions and can even be consumed by non-PostgreSQL clients.

Because logical decoding is used, only the changed rows are sent on the wire.
There's no index change data, no vacuum activity, etc transmitted.

The use of a replication slot means that the change stream is reliable and
crash-safe. If the client disconnects or crashes it can reconnect and resume
replay from the last message that client processed. Server-side changes that
occur while the client is disconnected are accumulated in the queue to be sent
when the client reconnects. This reliability also means that server-side
resources are consumed whether or not a client is connected.

# Why another output plugin?

See [`DESIGN.md`](DESIGN.md) for a discussion of why using one of the existing
generic logical decoding output plugins like `wal2json` to drive a logical
replication downstream isn't ideal. It's mostly about speed.

# Architecture and high level interaction

The output plugin is loaded by a PostgreSQL walsender process when a client
connects to PostgreSQL using the PostgreSQL wire protocol with connection
option `replication=database`, then uses
[the `CREATE_REPLICATION_SLOT ... LOGICAL ...` or `START_REPLICATION SLOT ... LOGICAL ...` commands](http://www.postgresql.org/docs/current/static/logicaldecoding-walsender.html) to start streaming changes. (It can also be used via
[SQL level functions](http://www.postgresql.org/docs/current/static/logicaldecoding-sql.html)
over a non-replication connection, but this is mainly for debugging purposes).

The client supplies parameters to the  `START_REPLICATION SLOT ... LOGICAL ...`
command to specify the version of the `pglogical` protocol it supports,
whether it wants binary format, etc.

The output plugin processes the connection parameters and the connection enters
streaming replication protocol mode, sometimes called "COPY BOTH" mode because
it's based on the protocol used for the `COPY` command.  PostgreSQL then calls
functions in this plugin to send it a stream of transactions to decode and
translate into network messages. This stream of changes continues until the
client disconnects.

The only client-to-server interaction after startup is the sending of periodic
feedback messages that allow the replication slot to discard no-longer-needed
change history. The client *must* send feedback, otherwise `pg_xlog` on the
server will eventually fill up and the server will stop working.


# Usage

The overall flow of client/server interaction is:

* Client makes PostgreSQL fe/be protocol connection to server
    * Connection options must include `replication=database` and `dbname=[...]` parameters
    * The PostgreSQL client library can be `libpq` or anything else that supports the replication sub-protocol
    * The same mechanisms are used for authentication and protocol encryption as for a normal non-replication connection
* Client issues `IDENTIFY_SYSTEM`
    * Server responds with a single row containing system identity info
* Client issues `CREATE_REPLICATION_SLOT slotname LOGICAL 'pglogical'` if it's setting up for the first time
    * Server responds with success info and a snapshot identifier
    * Client may at this point use the snapshot identifier on other connections while leaving this one idle
* Client issues `START_REPLICATION SLOT slotname LOGICAL 0/0 (...options...)` to start streaming, which loops:
    * Server emits `pglogical` message block encapsulated in a replication protocol `CopyData` message
    * Client receives and unwraps message, then decodes the `pglogical` message block
    * Client intermittently sends a standby status update message to server to confirm replay
* ... until client sends a graceful connection termination message on the fe/be protocol level or the connection is broken

 The details of `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT` and `START_REPLICATION` are discussed in the [replication protocol docs](http://www.postgresql.org/docs/current/static/protocol-replication.html) and will not be repeated here.

## Make a replication connection

To use the `pglogical` plugin you must first establish a PostgreSQL FE/BE
protocol connection using the client library of your choice, passing
`replication=database` as one of the connection parameters. `database` is a
literal string and is not replaced with the database name; instead the database
name is passed separately in the usual `dbname` parameter. Note that
`replication` is not a GUC (configuration parameter) and may not be passed in
the `options` parameter on the connection, it's a top-level parameter like
`user` or `dbname`.

Example connection string for `libpq`:

    'user=postgres replication=database sslmode=verify-full dbname=mydb'

The plug-in name to pass on logical slot creation is `'pglogical'`.

Details are in the replication protocol docs.

## Get system identity

If required you can use the `IDENTIFY_SYSTEM` command, which reports system
information:

	  systemid       | timeline |  xlogpos  | dbname | dboid
    ---------------------+----------+-----------+--------+-------
     6153224364663410513 |        1 | 0/C429C48 | testd  | 16385
    (1 row)

Details are in the replication protocol docs.

## Create the slot if required

If your application creates its own slots on first use and hasn't previously
connected to this database on this system you'll need to create a replication
slot. This keeps track of the client's replay state even while it's disconnected.

The slot name may be anything your application wants up to a limit of 63
characters in length. It's strongly advised that the slot name clearly identify
the application and the host it runs on.

Pass `pglogical` as the plugin name.

e.g.

    CREATE_REPLICATION_SLOT "reporting_host_42" LOGICAL "pglogical";

`CREATE_REPLICATION_SLOT` returns a snapshot identifier that may be used with
[`SET TRANSACTION SNAPSHOT`](http://www.postgresql.org/docs/current/static/sql-set-transaction.html)
to see the database's state as of the moment of the slot's creation. The first
change streamed from the slot will be the change immediately after this
snapshot was taken. The snapshot is useful when cloning the initial state of a
database being replicated. Applications that want to see the change stream
going forward, but don't care about the initial state, can ignore this. The
snapshot is only valid as long as the connection that issued the
`CREATE_REPLICATION_SLOT` remains open and has not run another command.

## Send replication parameters

The client now sends:

    START_REPLICATION SLOT "the_slot_name" LOGICAL (
	'Expected_encoding', 'UTF8',
	'Max_proto_major_version', '1',
	'Min_proto_major_version', '1',
	...moreparams...
    );

to start replication.

The parameters are very important for ensuring that the plugin accepts
the replication request and streams changes in the expected form. `pglogical`
parameters are discussed in the separate `pglogical` protocol documentation.

## Process the startup message

`pglogical_output` will send a `CopyData` message containing its
startup message as the first protocol message. This message contains a
set of key/value entries describing the capabilities of the upstream output
plugin, its version and the Pg version, the tuple format options selected,
etc.

The downstream client may choose to cleanly close the connection and disconnect
at this point if it doesn't like the reply. It might then inform the user
or reconnect with different parameters based on what it learned from the
first connection's startup message.

## Consume the change stream

`pglogical_output` now sends a continuous series of `CopyData`
protocol messages, each of which encapsulates a `pglogical` protocol message
as documented in the separate protocol docs.

These messages provide information about transaction boundaries, changed
rows, etc.

The stream continues until the client disconnects, the upstream server is
restarted, the upstream walsender is terminated by admin action, there's
a network issue, or the connection is otherwise broken.

The client should send periodic feedback messages to the server to acknowledge
that it's replayed to a given point and let the server release the resources
it's holding in case that change stream has to be replayed again. See
["Hot standby feedback message" in the replication protocol docs](http://www.postgresql.org/docs/current/static/protocol-replication.html)
for details.

## Disconnect gracefully

Disconnection works just like any normal client; you use your client library's
usual method for closing the connection. No special action is required before
disconnection, though it's usually a good idea to send a final standby status
message just before you disconnect.

# Tests

There are two sets of tests bundled with `pglogical_output`: the `pg_regress`
regression tests and some custom Python tests for the protocol.

The `pg_regress` tests check invalid parameter handling and basic
functionality.  They're intended for use by the buildfarm using an in-tree
`make check`, but may also be run with an out-of-tree PGXS build against an
existing PostgreSQL install using `make USE_PGXS=1 clean installcheck`.

The Python tests are more comprehensive, and examine the data sent by
the extension at the protocol level, validating the protocol structure,
order and contents. They can run using the SQL-level logical decoding
interface or, with a psycopg2 containing https://github.com/psycopg/psycopg2/pull/322,
with the walsender / streaming replication protocol. The Python-based tests
exercise the internal binary format support, too. See `test/README.md` for
details.

The tests may fail on installations that are not utf-8 encoded because the
payloads of the binary protocol output will have text in different encodings,
which aren't visible to psql as text to be decoded. Avoiding anything except
7-bit ascii in the tests *should* prevent the problem.

# Changeset forwarding

It's possible to use `pglogical_output` to cascade replication between multiple
PostgreSQL servers, in combination with an appropriate client to apply the
changes to the downstreams.

There are two forwarding modes:

* Forward everything. Transactions are replicated whether they were made directly
  on the immediate upstream or some other node upstream of it. This is the only
  option when running on 9.4. All rows from transactions are sent.

  Selected by not setting a row or transaction filter hook.

* Filtered forwarding. Transactions are replicated unless a client-supplied
  transaction filter hook says to skip this transaction. Row changes are
  replicated unless the client-supplied row filter hook (if provided) says to
  skip that row.

  Selected by installing a transaction and/or row filter hook (see "hooks").

If the upstream server is 9.5 or newer the server will enable changeset origin
information. It will set `forward_changeset_origins` to true in the startup
reply message to indicate this. It will then send changeset origin messages
after the `BEGIN` for each transaction, per the protocol documentation. Origin
messages are omitted for transactions originating directly on the immediate
upstream to save bandwidth.  If `forward_changeset_origins` is true then
transactions without an origin are always from the immediate upstream that’s
running the decoding plugin.

Note that 9.4 servers lack replication origin information and won't send it
on the wire. They also always pass zeroes to the hooks. So you can't filter
by origin in 9.4, and thus can't do mutual multi-master as it'll create an
infinite loop.

Clients may use this facility to form arbitrarily complex topologies when
combined with hooks to determine which transactions are forwarded. An obvious
case is bi-directional (mutual) replication.

# Selective replication

By specifying a row filter hook it's possible to filter the replication stream
server-side so that only a subset of changes is replicated.


# Hooks

`pglogical_output` exposes a number of extension points where applications can
modify or override its behaviour.

All hooks are called in a memory context that lasts for the duration of the
logical decoding session. They may switch to longer lived contexts if needed,
but are then responsible for their own cleanup.

## Hook setup function

The downstream must specify the fully-qualified name of a SQL-callable function
on the server as the value of the `hooks.setup_function` client parameter.
The SQL signature of this function is

    CREATE OR REPLACE FUNCTION funcname(hooks internal, memory_context internal)
    RETURNS void STABLE
    LANGUAGE c AS 'MODULE_PATHNAME';

Permissions are checked. This function must be callable by the user that the
output plugin is running as. The function name *must* be schema-qualified and is
parsed like any other qualified identifier.

The function receives a pointer to a newly allocated structure of hook function
pointers to populate as its first argument. The function must not free the
argument.

If the hooks need a private data area to store information across calls, the
setup function should get the `MemoryContext` pointer from the 2nd argument,
then `MemoryContextAlloc` a struct for the data in that memory context and
store the pointer to it in `hooks->hooks_private_data`. This will then be
accessible on future calls to hook functions. It need not be manually freed, as
the memory context used for logical decoding will free it when it's freed.
Don't put anything in it that needs manual cleanup.

Hooks other than the hook setup function and the startup hook are called in a
short-lived memory context. If they want to preserve anything they allocate
after the hook returns they must switch to the memory context that was passed
to the setup function and allocate it there.

Each hook has its own C signature (defined below) and the pointers must be
directly to the functions. Hooks that the client does not wish to set must be
left null.

An example is provided in `examples/hooks` and the argument structs are defined
in `pglogical_output/hooks.h`, which is installed into the PostgreSQL source
tree when the extension is installed.

Each hook that is enabled results in a new startup parameter being emitted in
the startup reply message. Clients must check for these and must not assume a
hook was successfully activated because no error is seen.

Hook functions are called in the context of the backend doing logical decoding.
Except for the startup hook, hooks see the catalog state as it was at the time
the transaction or row change being examined was made. Access to to non-catalog
tables is unsafe unless they have the `user_catalog_table` reloption set. Among
other things this means that it's not safe to invoke arbitrary functions,
user-defined procedures, etc, from hooks.


## Startup hook

The startup hook is called when logical decoding starts.

This hook can inspect the parameters passed by the client to the output
plugin as in_params. These parameters *must not* be modified.

It can add new parameters to the set to be returned to the client in the
startup parameters message, by appending to List out_params, which is
initially NIL. Each element must be a `DefElem` with the param name
as the `defname` and a `String` value as the arg, as created with
`makeDefElem(...)`. It and its contents must be allocated in the
logical decoding memory context.

For walsender based decoding the startup hook is called only once, and
cleanup might not be called at the end of the session.

Multiple decoding sessions, and thus multiple startup hook calls, may happen
in a session if the SQL interface for logical decoding is being used. In
that case it's guaranteed that the cleanup hook will be called between each
startup.

When successfully enabled, the output parameter `hooks.startup_hook_enabled` is
set to true in the startup reply message.

Unlike the other hooks, this hook sees a snapshot of the database's current
state, not a time-traveled catalog state. It is safe to access all tables from
this hook.

Also unlike other hooks, the startup hook is called in a memory context with
the same lifetime of the decoding session. It's called in the same context
as the one that's passed to the hook setup hook.

## Transaction filter hook

The transaction filter hook can exclude entire transactions from being decoded
and replicated based on the node they originated from.

It is passed a `const TxFilterHookArgs *` containing:

* The hook argument supplied by the client, if any
* The `RepOriginId` that this transaction originated from

and must return boolean, where true retains the transaction for sending to the
client and false discards it. (Note that this is the reverse sense of the low
level logical decoding transaction filter hook).

The hook function must *not* free the argument struct or modify its contents.

The transaction filter hook is only called on PostgreSQL 9.5 and above. It
is ignored on 9.4.

Note that individual changes within a transaction may have different origins to
the transaction as a whole; see "Origin filtering" for more details. If a
transaction is filtered out, all changes are filtered out even if their origins
differ from that of the transaction as a whole.

When successfully enabled, the output parameter
`hooks.transaction_filter_enabled` is set to true in the startup reply message.

Memory allocated in this hook is freed at the end of the call.

## Row filter hook

The row filter hook is called for each row. It is passed information about the
table, the transaction origin, and the row origin.

It is passed a `const RowFilterHookArgs*` containing:

* The hook argument supplied by the client, if any
* The `Relation` the change affects
* The change type - 'I'nsert, 'U'pdate or 'D'elete

It can return true to retain this row change, sending it to the client, or
false to discard it.

The function *must not* free the argument struct or modify its contents.

Note that it is more efficient to exclude whole transactions with the
transaction filter hook rather than filtering out individual rows.

When successfully enabled, the output parameter
`hooks.row_filter_enabled` is set to true in the startup reply message.

Memory allocated in this hook is freed at the end of the call.

## Shutdown hook

The shutdown hook is called when a decoding session ends. You can't rely on
this hook being invoked reliably, since a replication-protocol walsender-based
session might just terminate. It's mostly useful for cleanup to handle repeated
invocations under the SQL interface to logical decoding.

You don't need a hook to free memory you allocated, unless you explicitly
switched to a longer lived memory context like `TopMemoryContext`. Memory
allocated in the hook context will be automatically freed when the decoding
session shuts down.

# Limitations

The advantages of logical decoding in general and `pglogical_output` in
particular are discussed above. There are also some limitations that apply to
`pglogical_output`, and to Pg's logical decoding in general.

Notably:

## Mostly one-way communication

Per the protocol documentation, the downstream can't send anything except
replay progress messages to the upstream after replication begins, and can't
re-initialise replication without a disconnect.

To achieve downstream-to-upstream communication, clients can use a regular
libpq connection to the upstream then write to tables or call functions.
Alternately, a separate replication connection in the opposite direction can be
created by the application to carry information from downstream to upstream.

See "Protocol flow" in the protocol documentation for more information.

## Doesn't replicate global objects/shared catalog changes

PostgreSQL has a number of object types that exist across all databases, stored
in *shared catalogs*. These include:

* Roles (users/groups)
* Security labels on users and databases

Such objects cannot be replicated by `pglogical_output`. They're managed with DDL that
can't be captured within a single database and isn't decoded anyway.

Global object changes must be synchronized via some external means.

## Physical replica failover

Logical decoding cannot follow a physical replication failover because
replication slot state is not replicated to physical replicas. If you fail over
to a streaming replica you have to manually reconnect your logical replication
clients, creating new slots, etc. This is a core PostgreSQL limitation.

Also, there's no built-in way to guarantee that the logical replication slot
from the failed master hasn't replayed further than the physical streaming
replica you failed over to. You could receive changes on your logical decoding
stream from the old master that never made it to the physical streaming
replica. This is true (albeit very unlikely) *even if the physical streaming
replica is synchronous* because PostgreSQL sends the replication data anyway,
then just delays the commit's visibility on the master. Support for strictly
ordered standbys would be required in PostgreSQL to avoid this.

To achieve failover with logical replication you cannot mix in physical
standbys. The logical replication client has to take responsibility for
maintaining slots on logical replicas intended as failover candidates
and for ensuring that the furthest-ahead replica is promoted if there is
more than one.

## Can only replicate complete transactions

Logical decoding can only replicate a transaction after it has committed. This
usefully skips replication of rolled back transactions, but it also means that
very large transactions must be completed upstream before they can begin on the
downstream, adding to replication latency.

## Replicates only one transaction at a time

Logical decoding serializes transactions in commit order, so pglogical_output
cannot replay interleaved concurrent transactions. This can lead to high latencies
when big transactions are being replayed, since smaller transactions get queued
up behind them.

## Unique index required for inserts or updates

To replicate `INSERT`s or `UPDATE`s it is necessary to have a `PRIMARY KEY`
or a (non-partial, columns-only) `UNIQUE` index on the table, so the table
has a `REPLICA IDENTITY`. Without that `pglogical_output` doesn't know what
old key to send to allow the receiver to tell which tuple is being updated.

## UNLOGGED tables aren't replicated

Because `UNLOGGED` tables aren't written to WAL, they aren't replicated by
logical or physical replication. You can only replicate `UNLOGGED` tables
with trigger-based solutions.

## Unchanged fields are often sent in `UPDATE`

Because there's no tracking of dirty/clean fields when a tuple is updated,
logical decoding can't tell if a given field was changed by an update.
Unchanged fields can only by identified and omitted if they're a variable
length TOASTable type and are big enough to get stored out-of-line in
a TOAST table.

# Troubleshooting and debugging

## Non-destructively previewing pending data on a slot

Using the json mode of `pglogical_output` you can examine pending transactions
on a slot without consuming them, so they are still delivered to the usual
client application that created/owns this slot. This is best done using the SQL
interface to logical decoding, since it gives you finer control than using
`pg_recvlogical`.

You can only peek at a slot while there is no other client connected to that
slot.

Use `pg_logical_slot_peek_changes` to examine the change stream without
destructively consuming changes. This is extremely helpful when trying to
determine why an error occurs in a downstream, since you can examine a
transaction in json (rather than binary) format. It's necessary to supply a
minimal set of required parameters to the output plugin.

e.g. given setup:

    CREATE TABLE discard_test(blah text);
    SELECT 'init' FROM pg_create_logical_replication_slot('demo_slot', 'pglogical_output');
    INSERT INTO discard_test(blah) VALUES('one');
    INSERT INTO discard_test(blah) VALUES('two1'),('two2'),('two3');
    INSERT INTO discard_test(blah) VALUES('three1'),('three2');

you can peek at the change stream with:

     SELECT location, xid, data
     FROM pg_logical_slot_peek_changes('demo_slot', NULL, NULL,
              'min_proto_version', '1', 'max_proto_version', '1',
              'startup_params_format', '1', 'proto_format', 'json');

The two `NULL`s mean you don't want to stop decoding after any particular
LSN or any particular number of changes. Decoding will stop when there's nothing
left to decode or you cancel the query.

This will emit a key/value startup message then change data rows like:

     location  | xid  |                                            data
     0/4E8AAF0 | 5562 | {"action":"B", has_catalog_changes:"f", xid:"5562", first_lsn:"0/4E8AAF0", commit_time:"2015-11-13 14:26:21.404425+08"}
     0/4E8AAF0 | 5562 | {"action":"I","relation":["public","discard_test"],"newtuple":{"blah":"one"}}
     0/4E8AB70 | 5562 | {"action":"C", final_lsn:"0/4E8AB30", end_lsn:"0/4E8AB70"}
     0/4E8ABA8 | 5563 | {"action":"B", has_catalog_changes:"f", xid:"5563", first_lsn:"0/4E8ABA8", commit_time:"2015-11-13 14:26:32.015611+08"}
     0/4E8ABA8 | 5563 | {"action":"I","relation":["public","discard_test"],"newtuple":{"blah":"two1"}}
     0/4E8ABE8 | 5563 | {"action":"I","relation":["public","discard_test"],"newtuple":{"blah":"two2"}}
     0/4E8AC28 | 5563 | {"action":"I","relation":["public","discard_test"],"newtuple":{"blah":"two3"}}
     0/4E8ACA8 | 5563 | {"action":"C", final_lsn:"0/4E8AC68", end_lsn:"0/4E8ACA8"}
     ....

The output is the LSN (log sequence number) associated with a change, the top
level transaction ID that performed the change, and the change data as json.

You can see the transaction boundaries by xid changes and by the "B"egin and
"C"ommit messages, and you can see the individual row "I"nserts. Replication
origins, commit timestamps, etc will be shown if known.

See http://www.postgresql.org/docs/current/static/functions-admin.html for
information on the peek functions.

If you want the binary format you can get that with
`pg_logical_slot_peek_binary_changes` and the `native` protocol, but that's
generally much less useful.

# Manually discarding a change from a slot

Sometimes it's desirable to manually purge one or more changes from a
replication slot. This is usually an error recovery step when problems arise
with the downstream code that's replaying from the slot.

You can use the peek functions to determine the point in the stream you want to
discard up to, as identified by LSN (log sequence number). See
"non-destructively previewing pending data on a slot" above for details.

You can't control the point you start discarding from, it's always from the
current stream position up to a point you specify. If the peek shows that
there's data you still want to retain you must make sure that the downstream
replays up to the point you want to keep changes and sends replay confirmation.
In other words there's no way to cut a sequence of changes out of the middle of
the pending change stream.

Once you've peeked the stream and know the LSN you want to discard up to, you
can use `pg_logical_slot_get_changes`, specifying an `upto_lsn`, to consume
changes from the slot up to but not including that point. That will be the
point at which replay resumes.

For example, if you wanted to discard the first transaction in the example
from the section above, i.e. discard xact 5562 and start decoding at xact
5563 from its' BEGIN lsn `0/4E8ABA8`, you'd run:

      SELECT location, xid, data
      FROM pg_logical_slot_get_changes('demo_slot', '0/4E8ABA8', NULL,
               'min_proto_version', '1', 'max_proto_version', '1',
               'startup_params_format', '1', 'proto_format', 'json');

Note that `_get_changes` is used instead of `_peek_changes` and that
the `upto_lsn` is `'0/4E8ABA8'` instead of `NULL`.





