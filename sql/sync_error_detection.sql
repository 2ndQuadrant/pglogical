/* Test for sync error handling in RM#2829 */

SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

\c :subscriber_dsn

-- Create a downstream-only table then try to resync it from the provider. This
-- will fail since the provider table does not exist.

CREATE TABLE public.test_sync_error(id serial primary key, data text);

SELECT * FROM pglogical.alter_subscription_resynchronize_table('test_subscription', 'test_sync_error');

DO $$
-- Wait until the sync has started
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pglogical.local_sync_status WHERE sync_status != 'i' AND sync_relname = 'test_sync_error') = 1 THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

-- sync_status will always be an in-progress state here, because the worker exits
-- without changing it, and a new worker will set it again.
SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('r', 'y') AS sync_ok, sync_status NOT IN ('y', 'r') AS sync_in_progress
FROM pglogical.local_sync_status
WHERE sync_relname = 'test_sync_error'
ORDER BY 2,3,4;

-- If we create the table on the upstream and ask to resync again it should work

\c :provider_dsn

CREATE TABLE public.test_sync_error(id serial primary key, data text);

INSERT INTO public.test_sync_error(data) VALUES ('awk awk awk');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

-- The new row must not have applied, since the table is in failed-sync state and not receiving changes
SELECT * FROM pglogical.test_sync_error;

-- But we can resync it now. Add some local content first to make sure it gets clobbered.
INSERT INTO public.test_sync_error(data) VALUES ('dead parrot');

SELECT * FROM pglogical.alter_subscription_resynchronize_table('test_subscription', 'test_sync_error');

DO $$
-- give it 10 seconds to syncrhonize the tabes
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pglogical.local_sync_status WHERE sync_status IN ('y', 'r') AND sync_relname = 'test_sync_error') = 1 THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status IN ('r', 'y') AS sync_ok
FROM pglogical.local_sync_status
WHERE sync_relname = 'test_sync_error'
ORDER BY 2,3,4;

\c :provider_dsn

\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_sync_error;
$$);
