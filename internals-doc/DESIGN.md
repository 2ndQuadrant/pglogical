# Design decisions

Explanations of why things are done the way they are.

## Why does pglogical_output exist when there's wal2json etc?

`pglogical_output` does plenty more than convert logical decoding change
messages to a wire format and send them to the client.

It handles format negotiations, sender-side filtering using pluggable hooks
(and the associated plugin handling), etc. The protocol its self is also
important, and incorporates elements like binary datum transfer that can't be
easily or efficiently achieved with json.

## Custom binary protocol

Why do we have a custom binary protocol inside the walsender / copy both protocol,
rather than using a json message representation?

Speed and compactness. It's expensive to create json, with lots of allocations.
It's expensive to decode it too. You can't represent raw binary in json, and must
encode it, which adds considerable overhead for some data types. Using the
obvious, easy to decode json representations also makes it difficult to do
later enhancements planned for the protocol and decoder, like caching row
metadata.

The protocol implementation is fairly well encapsulated, so in future it should
be possible to emit json instead for clients that request it. Right now that's
not the priority as tools like wal2json already exist for that.

## Column metadata

The output plugin sends metadata for columns - at minimum, the column names -
before each row that first refers to that relation.

The reason metadata must be sent is that the upstream and downstream table's
attnos don't necessarily correspond. The column names might, and their ordering
might even be the same, but any column drop or column type change will result
in a dropped column on one side. So at the user level the tables look the same,
but their attnos don't match, and if we rely on attno for replication we'll get
the wrong data in the wrong columns. Not pretty.

That could be avoided by requiring that the downstream table be strictly
maintained by DDL replication, but:

* We don't want to require DDL replication
* That won't work with multiple upstreams feeding into a table
* The initial table creation still won't be correct if the table has dropped
  columns, unless we (ab)use `pg_dump`'s `--binary-upgrade` support to emit
  tables with dropped columns, which we don't want to do.

So despite the bandwidth cost, we need to send metadata.

Support for type metadata is penciled in to the protocol so that clients that
don't have table definitions at all - like queueing engines - can decode the
data. That'll also permit type validation sanity checking on the apply side
with logical replication.

The upstream expects the client to cache this metadata and re-use it when data
is sent for the relation again. Cache size controls, an LRU and purge
notifications will be added later; for now the client must cache everything
indefinitely.

## Relation metadata cache size controls

The relation metadata cache will have downstream size control added. The
downstream will send a parameter indicating that it supports caching, and the
maximum cache size desired.

Since there is no downstream-to-upstream communication after the startup params
there's no easy way for the downstream to tell the upstream when it purges
cache entries. So the downstream cache is a slave cache that must depend
strictly on the upstream cache. The downstream tells the upstream how to manage
its cache and then after that it just follows orders.

To keep the caches in sync so the upstream never sends a row without knowing
the downstream has metadata for it cached the downstream must always cache
relation metadata when it receives it, and may not purge it from its cache
until it receives a purge message for that relation from the upstream. If a
new metadata message for the same relation arrives it *must* replace the old
entry in the cache.

The downstream does *not* have to promptly purge or invalidate cache entries
when it gets purge messages from the upstream. They are just notifications that
the upstream no longer expects the downstream to retain that cache entry and
will re-send it if it is required again later.

## Not an extension

There's no extension script for pglogical_output. That's by design. We've tried
really hard to avoid needing one, allowing applications using pglogical_output
to entirely define any SQL level catalogs they need and interact with them
using the hooks.

That way applications don't have to deal with some of their catalog data being
in pglogical_output extension catalogs and some being in their own.

There's no issue with dump and restore that way either. The app controls it
entirely and pglogical_output doesn't need any policy or tools for it.

pglogical_output is meant to be a re-usable component of other solutions. Users
shouldn't need to care about it directly.

## Hooks

Quite a bit of functionality that could be done directly in the output
plugin is instead delegated to pluggable hooks. Replication origin filtering
for example.

That's because pglogical_output tries hard not to know anything about the
topology of the replication cluster and leave that to applications using the
plugin. It doesn't 


## Hook entry point as a SQL function

The hooks entry point is a SQL function that populates a passed `internal`
struct with hook function pointers.

The reason for this is that hooks are specified by a remote peer over the
network. We can't just let the peer say "dlsym() this arbitrary function name
and call it with these arguments" for fairly obvious security reasons. At bare
minimum all replication using hooks would have to be superuser-only if we did
that.

The SQL entry point is only called once per decoding session and the rest of
the calls are plain C function pointers.

## The startup reply message

The protocol design choices available to `pg_logical` are constrained by being
contained in the copy-both protocol within the fe/be protocol, running as a
logical decoding plugin. The plugin has no direct access to the network socket
and can't send or receive messages whenever it wants, only under the control of
the walsender and logical decoding framework.

The only opportunity for the client to send data directly to the logical
decoding plugin is in the  `START_REPLICATION` parameters, and it can't send
anything to the client before that point.

This means there's no opportunity for a multi-way step negotiation between
client and server. We have to do all the negotiation we're going to in a single
exchange of messages - the setup parameters and then the replication start
message. All the client can do if it doesn't like the offer the server makes is
disconnect and try again with different parameters.

That's what the startup message is for. It reports the plugin's capabilities
and tells the client which requested options were honoured. This gives the
client a chance to decide if it's happy with the output plugin's decision
or if it wants to reconnect and try again with different options. Iterative
negotiation, effectively.

## Unrecognised parameters MUST be ignored by client and server

To ensure upward and downward compatibility, the output plugin must ignore
parameters set by the client if it doesn't recognise them, and the client
must ignore parameters it doesn't recognise in the server's startup reply
message.

This ensures that older clients can talk to newer servers and vice versa.

For this to work, the server must never enable new functionality such as
protocol message types, row formats, etc without the client explicitly
specifying via a startup parameter that it understands the new functionality.
Everything must be negotiated.

Similarly, a newer client talking to an older server may ask the server to
enable functionality, but it can't assume the server will actually honour that
request. It must check the server's startup reply message to see if the server
confirmed that it enabled the requested functionality. It might choose to
disconnect and report an error to the user if the server didn't do what it
asked. This can be important, e.g. when a security-significant hook is
specified.

## Support for transaction streaming

Presently logical decoding requires that a transaction has committed before it
can *begin* sending it to the client. This means long running xacts can take 2x
as long, since we can't start apply on the replica until the xact is committed
on the master.

Additionally, a big xact will cause large delays in apply of smaller
transactions because logical decoding reorders transactions into strict commit
order and replays them in that sequence. Small transactions that committed after
the big transaction cannot be replayed to the replica until the big transaction
is transferred over the wire, and we can't get a head start on that while it's
still running.

Finally, the accumulation of a big transaction in the reorder buffer means that
storage on the upstream must be sufficient to hold the entire transaction until
it can be streamed to the replica and discarded. That is in addition to the
copy in retained WAL, which cannot be purged until replay is confirmed past
commit for that xact. The temporary copy serves no data safety purpose; it can
be regenerated from retained WAL is just a spool file.

There are big upsides to waiting until commit. Rolled-back transactions and
subtransactions are never sent at all. The apply/downstream side is greatly
simplified by not needing to do transaction ordering, worry about
interdependencies and conflicts during apply. The commit timestamp is known
from the beginning of replay, allowing for smarter conflict resolution
behaviour in multi-master scenarios. Nonetheless sometimes we want to be able
to stream changes in advance of commit.

So we need the ability to start streaming a transaction from the upstream as
its changes are seen in WAL, either applying it immediately on the downstream
or spooling it on the downstream until it's committed. This requires changes
to the logical decoding facilities themselves, it isn't something pglogical_output
can do alone. However, we've left room in pglogical_output to support this
when support is added to logical decoding:

* Flags in most message types let us add fields if we need to, like a
  HAS_XID flag and an extra field for the transaction ID so we can
  differentiate between concurrent transactions when streaming. The space
  isn't wasted the rest of the time.

* The upstream isn't allowed to send new message types, etc, without a
  capability flag being set by the client. So for interleaved xacts we won't
  enable them in logical decoding unless the client tells us the client is
  prepared to cope with them by sending additional startup parameters.

Note that for consistency reasons we still have to commit things in the same
order on the downstream. The purpose of transaction streaming is to reduce the
latency between the commit of the last xact before a big one and the first xact
after the big one, minimising the duration of the stall in the flow of smaller
xacts perceptible on the downstream.

Transaction streaming also makes parallel apply on the downstream possible,
though it is not necessary to have parallel apply to benefit from transaction
streaming. Parallel apply has further complexities that are outside the scope
of the output plugin design.
