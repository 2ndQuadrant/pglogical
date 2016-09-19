DO $$
BEGIN
	IF (SELECT setting::integer/100 FROM pg_settings WHERE name = 'server_version_num') = 904 THEN
		CREATE EXTENSION IF NOT EXISTS pglogical_origin;
	END IF;
END;$$;
CREATE EXTENSION pglogical;

SELECT pglogical.pglogical_max_proto_version();

SELECT pglogical.pglogical_min_proto_version();

SELECT pglogical.pglogical_version() = extversion
FROM pg_extension
WHERE extname = 'pglogical';

DROP EXTENSION pglogical;
