
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
CREATE USER super2 SUPERUSER;

\c :subscriber_dsn
SELECT * FROM pglogical.alter_node_add_interface('test_provider', 'super2', (SELECT provider_dsn FROM pglogical_regress_variables()) || ' user=super2');

SELECT * FROM pglogical.alter_subscription_interface('test_subscription', 'super2');

DO $$
BEGIN
    FOR i IN 1..100 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status != 'down') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT pg_sleep(0.1);
SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status();

\c :provider_dsn
SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT usename FROM pg_stat_replication WHERE application_name = 'test_subscription';

\c :subscriber_dsn
SELECT * FROM pglogical.alter_subscription_interface('test_subscription', 'test_provider');

DO $$
BEGIN
    FOR i IN 1..100 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status != 'down') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$$;

SELECT pg_sleep(0.1);
SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status();

\c :provider_dsn
DROP USER super2;
SELECT plugin, slot_type, active FROM pg_replication_slots;
SELECT usename FROM pg_stat_replication WHERE application_name = 'test_subscription';
