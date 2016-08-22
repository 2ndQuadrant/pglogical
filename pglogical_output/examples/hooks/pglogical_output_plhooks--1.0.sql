\echo Use "CREATE EXTENSION pglogical_output_plhooks" to load this file. \quit

-- Use @extschema@ or leave search_path unchanged, don't use explicit schema

CREATE FUNCTION pglo_plhooks_setup_fn(internal)
RETURNS void
STABLE
LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pglo_plhooks_setup_fn(internal)
IS 'Register pglogical output pl hooks. See docs for how to specify functions';

--
-- Called as the startup hook.
--
-- There's no useful way to expose the private data segment, so you
-- just don't get to use that from pl hooks at this point. The C
-- wrapper will extract a startup param named pglo_plhooks.client_hook_arg
-- for you and pass it as client_hook_arg to all callbacks, though.
--
-- For implementation convenience, a null client_hook_arg is passed
-- as the empty string.
--
-- Must return the empty array, not NULL, if it has nothing to add.
--
CREATE FUNCTION pglo_plhooks_demo_startup(startup_params text[], client_hook_arg text)
RETURNS text[]
LANGUAGE plpgsql AS $$
DECLARE
    elem text;
	paramname text;
	paramvalue text;
BEGIN
	FOREACH elem IN ARRAY startup_params
	LOOP
		IF elem IS NULL THEN
				RAISE EXCEPTION 'Startup params may not be null';
		END IF;

		IF paramname IS NULL THEN
				paramname := elem;
		ELSIF paramvalue IS NULL THEN
				paramvalue := elem;
		ELSE
				RAISE NOTICE 'got param: % = %', paramname, paramvalue;
				paramname := NULL;
				paramvalue := NULL;
		END IF;
	END LOOP;

	RETURN ARRAY['pglo_plhooks_demo_startup_ran', 'true', 'otherparam', '42'];
END;
$$;

CREATE FUNCTION pglo_plhooks_demo_txn_filter(origin_id int, client_hook_arg text)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
		-- Not much to filter on really...
		RAISE NOTICE 'Got tx with origin %',origin_id;
		RETURN true;
END;
$$;

CREATE FUNCTION pglo_plhooks_demo_row_filter(affected_rel regclass, change_type "char", client_hook_arg text)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
		-- This is a totally absurd test, since it checks if the upstream user
		-- doing replication has rights to make modifications that have already
		-- been committed and are being decoded for replication. Still, it shows
		-- how the hook works...
		IF pg_catalog.has_table_privilege(current_user, affected_rel,
				CASE change_type WHEN 'I' THEN 'INSERT' WHEN 'U' THEN 'UPDATE' WHEN 'D' THEN 'DELETE' END)
		THEN
				RETURN true;
		ELSE
				RETURN false;
		END IF;
END;
$$;

CREATE FUNCTION pglo_plhooks_demo_shutdown(client_hook_arg text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
		RAISE NOTICE 'Decoding shutdown';
END;
$$
