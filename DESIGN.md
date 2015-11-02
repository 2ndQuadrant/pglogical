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
