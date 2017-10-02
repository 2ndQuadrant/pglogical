SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.test_trg_data(id serial primary key, data text);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'test_trg_data');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

CREATE TABLE test_trg_hist(table_name text, action text, action_id serial, original_data text, new_data text);

CREATE FUNCTION test_trg_data_hist_fn() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        INSERT INTO test_trg_hist (table_name,action,original_data,new_data)
        VALUES (TG_TABLE_NAME::TEXT, substring(TG_OP,1,1), ROW(OLD.*), ROW(NEW.*));
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO test_trg_hist (table_name,action,original_data)
        VALUES (TG_TABLE_NAME::TEXT, substring(TG_OP,1,1), ROW(OLD.*));
        RETURN OLD;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO test_trg_hist (table_name,action,new_data)
        VALUES (TG_TABLE_NAME::TEXT, substring(TG_OP,1,1), ROW(NEW.*));
        RETURN NEW;
    ELSE
        RAISE WARNING 'Unknown action';
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER test_trg_data_hist_trg
AFTER INSERT OR UPDATE OR DELETE ON test_trg_data
FOR EACH ROW EXECUTE PROCEDURE test_trg_data_hist_fn();

\c :provider_dsn

INSERT INTO test_trg_data(data) VALUES ('no_history');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT * FROM test_trg_data;
SELECT * FROM test_trg_hist;

ALTER TABLE test_trg_data ENABLE REPLICA TRIGGER test_trg_data_hist_trg;

\c :provider_dsn

INSERT INTO test_trg_data(data) VALUES ('yes_history');
UPDATE test_trg_data SET data = 'yes_history';
DELETE FROM test_trg_data;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT * FROM test_trg_data;
SELECT * FROM test_trg_hist;

DROP TABLE test_trg_hist CASCADE;

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
        CREATE TABLE public.basic_dml (
                id serial primary key,
                other integer,
                data text,
                something interval
        );
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'basic_dml');

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
-- create row filter trigger
CREATE FUNCTION filter_basic_dml_fn() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP in ('UPDATE', 'INSERT')) THEN
-- treating 'DELETE' as pass-through
        IF (NEW.id > 1 AND NEW.data IS DISTINCT FROM 'baz' AND NEW.data IS DISTINCT FROM 'bbb') THEN
            RETURN NEW;
        ELSE
            RETURN NULL;
        END IF;
    ELSE
        RAISE WARNING 'Unknown action';
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER filter_basic_dml_trg
BEFORE INSERT OR UPDATE ON basic_dml
FOR EACH ROW EXECUTE PROCEDURE filter_basic_dml_fn();

\c :provider_dsn
-- insert into table at provider
\COPY basic_dml FROM STDIN WITH CSV
5000,1,aaa,1 hour
5001,2,bbb,2 years
5002,3,ccc,3 minutes
5003,4,ddd,4 days
\.

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
-- rows received at suscriber as trigger is not enabled yet.
SELECT * from basic_dml ORDER BY id;
-- Now enable trigger:
ALTER TABLE basic_dml ENABLE REPLICA TRIGGER filter_basic_dml_trg;

\c :provider_dsn
TRUNCATE basic_dml;
-- check basic insert replication
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
-- rows filtered at suscriber as trigger is enabled.
SELECT * from basic_dml ORDER BY id;

\set VERBOSITY terse
DROP FUNCTION test_trg_data_hist_fn() CASCADE;
DROP FUNCTION filter_basic_dml_fn() CASCADE;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_trg_data CASCADE;
	DROP TABLE public.basic_dml CASCADE;
$$);
