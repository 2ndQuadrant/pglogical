CREATE OR REPLACE FUNCTION pglogical.create_subscription(subscription_name name, provider_dsn text,
    replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = true,
    synchronize_data boolean = true, forward_origins text[] = '{all}')
RETURNS oid STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'pglogical_create_subscription';

SELECT * FROM pglogical.create_replication_set('ddl_sql', true, false, false, false);

UPDATE pglogical.subscription SET sub_replication_sets = array_append(sub_replication_sets, 'ddl_sql');

BEGIN;
	WITH applys AS (
		SELECT sub_name FROM pglogical.subscription WHERE sub_enabled
	),
	WITH disable AS (
		SELECT pglogical.alter_subscription_disable(sub_name, true) FROM applys
	)
	SELECT pglogical.alter_subscription_enable(sub_name, true) FROM applys;
COMMIT;
