# Design decisions

Explanations of why things are done the way they are.

## Hooks as SQL-level functions

Hooks for things like replication set filtering use SQL-level functions that're
looked up via the syscache, permissions-checked, and executed via the fmgr. This
imposes fmgr call overheads for each invocation.

The reason for this is that hooks are specified by a remote peer over the
network. We can't just let the peer say "dlsym() this arbitrary function name
and call it with these arguments" for fairly obvious security reasons. At bare
minimum all replication using hooks would have to be superuser-only if we did
that.

fmgr calls are used for operators and all sorts of things, so it's not like
they're especially slow. The SPI isn't needed, we can DirectFunctionCall
the filter procs.

## Case for support-optional vs support-required parameters

Upward compatibility. We need the downstream client to have a way to tell the
upstream server "this is important, and if you don't know what I'm talking
about you need to be honest about it, don't just nod and smile and pretend you
understand me then ignore me." We also need the same in the other direction,
so we can add new messages, options, etc that might break BC with old clients
cleanly and without fearing data corruption.

Remember how old clients would mangle data when `bytea_output` was changed to
`hex`? That's why we have this.

It's done by a simple rule of upper-case vs lower-case first letter because
that seems good enough. There are few other choices when working with
a key/value parameter structure.

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
or if it wants to reconnect and try again with different options.
