--PRIMARY KEY
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

-- testing update of primary key
-- create  table with primary key and 3 other tables referencing it
SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.pk_users (
    id integer PRIMARY KEY,
    another_id integer unique not null,
    a_id integer,
    name text,
    address text
);

--pass
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'pk_users');

INSERT INTO pk_users VALUES(1,11,1,'User1', 'Address1');
INSERT INTO pk_users VALUES(2,12,1,'User2', 'Address2');
INSERT INTO pk_users VALUES(3,13,2,'User3', 'Address3');
INSERT INTO pk_users VALUES(4,14,2,'User4', 'Address4');

SELECT * FROM pk_users;

\d+ pk_users;

\c :subscriber_dsn

SELECT * FROM pk_users;

\c :provider_dsn

UPDATE pk_users SET address='UpdatedAddress1' WHERE id=1;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn

SELECT * FROM pk_users;

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
CREATE UNIQUE INDEX another_id_temp_idx ON public.pk_users (another_id);
ALTER TABLE public.pk_users DROP CONSTRAINT pk_users_pkey,
    ADD CONSTRAINT pk_users_pkey PRIMARY KEY USING INDEX another_id_temp_idx;

ALTER TABLE public.pk_users DROP CONSTRAINT pk_users_another_id_key;
$$);

\d+ pk_users;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

UPDATE pk_users SET address='UpdatedAddress2' WHERE id=2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn
\d+ pk_users;
SELECT * FROM pk_users;

\c :provider_dsn
UPDATE pk_users SET address='UpdatedAddress3' WHERE another_id=12;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn

SELECT * FROM pk_users;

\c :provider_dsn
UPDATE pk_users SET address='UpdatedAddress4' WHERE a_id=2;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn

INSERT INTO pk_users VALUES(4,15,2,'User5', 'Address5');
-- subscriber now has duplicated value in id field while provider does not
SELECT * FROM pk_users;

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
CREATE UNIQUE INDEX id_temp_idx ON public.pk_users (id);
ALTER TABLE public.pk_users DROP CONSTRAINT pk_users_pkey,
    ADD CONSTRAINT pk_users_pkey PRIMARY KEY USING INDEX id_temp_idx;
$$);

\d+ pk_users;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn
\d+ pk_users;

SELECT pglogical.alter_subscription_disable('test_subscription', true);

\c :provider_dsn

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pg_replication_slots WHERE active = false) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT data::json->'action' as action, CASE WHEN data::json->>'action' IN ('I', 'D', 'U') THEN json_extract_path(data::json, 'relation') END as data FROM pg_logical_slot_get_changes((SELECT slot_name FROM pg_replication_slots), NULL, 1, 'min_proto_version', '1', 'max_proto_version', '1', 'startup_params_format', '1', 'proto_format', 'json');
SELECT data::json->'action' as action, CASE WHEN data::json->>'action' IN ('I', 'D', 'U') THEN data END as data FROM pg_logical_slot_get_changes((SELECT slot_name FROM pg_replication_slots), NULL, 1, 'min_proto_version', '1', 'max_proto_version', '1', 'startup_params_format', '1', 'proto_format', 'json');

\c :subscriber_dsn

SELECT pglogical.alter_subscription_enable('test_subscription', true);
DELETE FROM pk_users WHERE id = 4;-- remove the offending entries.

\c :provider_dsn

DO $$
BEGIN
	FOR i IN 1..100 LOOP
		IF (SELECT count(1) FROM pg_replication_slots WHERE active = true) THEN
			RETURN;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;
UPDATE pk_users SET address='UpdatedAddress2' WHERE id=2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn
SELECT * FROM pk_users;

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
DROP TABLE public.pk_users CASCADE;
$$);
