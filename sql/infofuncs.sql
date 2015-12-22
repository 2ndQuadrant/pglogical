CREATE EXTENSION pglogical;

SELECT pglogical.pglogical_max_proto_version();

SELECT pglogical.pglogical_min_proto_version();

SELECT pglogical.pglogical_version() = extversion
FROM pg_extension
WHERE extname = 'pglogical';

DROP EXTENSION pglogical;
