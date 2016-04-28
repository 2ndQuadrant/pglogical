CREATE FUNCTION pglogical_output_version() RETURNS text
LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION pglogical_output_version_num() RETURNS integer
LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION pglogical_output_proto_version() RETURNS integer
LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION pglogical_output_min_proto_version() RETURNS integer
LANGUAGE c AS 'MODULE_PATHNAME';
