CREATE FUNCTION pglogical.wait_for_subscription_sync_complete(subscription_name name)
RETURNS void RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_subscription_sync_complete';

CREATE FUNCTION pglogical.wait_for_table_sync_complete(subscription_name name, relation regclass)
RETURNS void RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_wait_for_table_sync_complete';

CREATE FUNCTION pglogical.xact_commit_timestamp_origin("xid" xid, OUT "timestamp" timestamptz, OUT "roident" oid)
RETURNS record RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_xact_commit_timestamp_origin';
