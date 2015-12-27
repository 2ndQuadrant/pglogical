\echo Use "CREATE EXTENSION pglogical_origin" to load this file. \quit

DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') != 904 THEN
		RAISE EXCEPTION 'pglogical_origin can only be installed into PostgreSQL 9.4';
	END IF;
END;$$;

CREATE TABLE pglogical_origin.replication_origin (
	roident oid NOT NULL,
	roname text NOT NULL,
	roremote_lsn pg_lsn NOT NULL
);

CREATE UNIQUE INDEX replication_origin_roident_index ON pglogical_origin.replication_origin(roident);
CREATE UNIQUE INDEX replication_origin_roname_index ON pglogical_origin.replication_origin(roname);
