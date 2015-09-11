# `pg_logical` Output Plugin

This is the [logical decoding](http://www.postgresql.org/docs/current/static/logicaldecoding.html)
[output plugin](http://www.postgresql.org/docs/current/static/logicaldecoding-output-plugin.html)
for `pg_logical`. Its purpose is to extract a change stream from a PostgreSQL
database and send it to a client over a network connection using a
well-defined, efficient protocol that multiple different applications can
consume.

No triggers are required to collect the change stream and no external ticker or
other daemon is required. It's accumulated using
[replication slots](http://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html#AEN66446),
as supported in PostgreSQL 9.4 or newer, and sent on top of the
[PostgreSQL streaming replication protocol](http://www.postgresql.org/docs/current/static/protocol-replication.html).

Unlike block-level ("physical") streaming replication, the change stream from
the `pg_logical` output plugin is compatible across different PostgreSQL
versions and can even be consumed by non-PostgreSQL clients.

The use of a replication slot means that the change stream is reliable and
crash-safe. If the client disconnects or crashes it can reconnect and resume
replay from the last message that client processed. Server-side changes that
occur while the client is disconnected are accumulated in the queue to be sent
when the client reconnects. This reliabiliy also means that server-side
resources are consumed whether or not a client is connected.

## Architecture and high level interaction

The output plugin is loaded by a PostgreSQL walsender process when a client
connects to PostgreSQL using the PostgreSQL wire protocol with connection
option `replication=database`, then uses
[the `CREATE_REPLICATION_SLOT ... LOGICAL ...` or `START_REPLICATION SLOT ... LOGICAL ...` commands](http://www.postgresql.org/docs/current/static/logicaldecoding-walsender.html) to start streaming changes. (It can also be used via
[SQL level functions](http://www.postgresql.org/docs/current/static/logicaldecoding-sql.html)
over a non-replication connection, but this is mainly for debugging purposes).

The client supplies parameters to the  `START_REPLICATION SLOT ... LOGICAL ...`
command to specify the version of the `pg_logical` protocol it supports,
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


## Usage

The overall flow of client/server interaction is:

* Client makes PostgreSQL fe/be protocol connection to server
    * Connection options must include `replication=database` and `dbname=[...]` parameters
    * The PostgreSQL client library can be `libpq` or anything else that supports the replication sub-protocol
    * The same mechanisms are used for authentication and protocol encryption as for a normal non-replication connection
* [Client issues `IDENTIFY_SYSTEM`
    * Server responds with a single row containing system identity info
* Client issues `CREATE_REPLICATION_SLOT slotname LOGICAL 'pg_logical'` if it's setting up for the first time
    * Server responds with success info and a snapshot identifier
    * Client may at this point use the snapshot identifier on other connections while leaving this one idle
* Client issues `START_REPLICATION SLOT slotname LOGICAL 0/0 (...options...)` to start streaming, which loops:
    * Server emits `pg_logical` message block encapsulated in a replication protocol `CopyData` message
    * Client receives and unwraps message, then decodes the `pg_logical` message block
    * Client intermittently sends a standby status update message to server to confirm replay
* ... until client sends a graceful connection termination message on the fe/be protocol level or the connection is broken

 The details of `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT` and `START_REPLICATION` are discussed in the [replication protocol docs](http://www.postgresql.org/docs/current/static/protocol-replication.html) and will not be repeated here.

### Make a replication connection

To use the `pg_logical` plugin you must first establish a PostgreSQL FE/BE
protocol connection using the client library of your choice, passing
`replication=database` as one of the connection parameters. `database` is a
literal string and is not replaced with the database name; instead the database
name is passed separately in the usual `dbname` parameter. Note that
`replication` is not a GUC (configuration parameter) and may not be passed in
the `options` parameter on the connection, it's a top-level parameter like
`user` or `dbname`.

Example connection string for `libpq`:

    'user=postgres replication=database sslmode=verify-full dbname=mydb'

The plug-in name to pass on logical slot creation is `'pg_logical'`.

Details are in the replication protocol docs.

### Get system identity

If required you can use the `IDENTIFY_SYSTEM` command, which reports system
information:

	  systemid       | timeline |  xlogpos  | dbname | dboid
    ---------------------+----------+-----------+--------+-------
     6153224364663410513 |        1 | 0/C429C48 | testd  | 16385
    (1 row)

Details in the replication protocol docs.

### Create the slot if required

If your application creates its own slots on first use and hasn't previously
connected to this database on this system you'll need to create a replication
slot. This keeps track of the client's replay state even while it's disconnected.

The slot name may be anything your application wants up to a limit of 63
characters in length. It's strongly advised that the slot name clearly identify
the application and the host it runs on.

Pass `pg_logical` as the plugin name.

e.g.

    CREATE_REPLICATION_SLOT "reporting_host_42" LOGICAL "pg_logical";

`CREATE_REPLICATION_SLOT` returns a snapshot identifier that may be used with
[`SET TRANSACTION SNAPSHOT`](http://www.postgresql.org/docs/current/static/sql-set-transaction.html)
to see the database's state as of the moment of the slot's creation. The first
change streamed from the slot will be the change immediately after this
snapshot was taken. The snapshot is useful when cloning the initial state of a
database being replicted. Applications that want to see the change stream
going forward, but don't care about the initial state, can ignore this. The
snapshot is only valid as long as the connection that issued the
`CREATE_REPLICATION_SLOT` remains open and has not run another command.

### Send replication parameters

The client now sends:

    START_REPLICATION SLOT "the_slot_name" LOGICAL (
	'Expected_encoding', 'UTF8',
	'Max_proto_major_version', '1',
	'Min_proto_major_version', '1',
	...moreparams...
    );

to start replication.

The parameters are very important for ensuring that the plugin accepts
the replication request and streams changes in the expected form. `pg_logical`
parameters are discussed in the separate `pg_logical` protocol documentation.

### Consume the change stream



### Disconnect gracefully

Disconnection works just like any normal client; you use your client library's
usual method for closing the connection. No special action is required before
disconnection, though it's usually a good idea to send a final standby status
message just before you disconnect.
