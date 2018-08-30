CREATE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
	    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure text = 'none',
	    synchronize_data boolean = true, forward_origins text[] = '{all}', apply_delay interval DEFAULT '0')
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

DROP FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[], synchronize_structure boolean,
    synchronize_data boolean, forward_origins text[], apply_delay interval);
