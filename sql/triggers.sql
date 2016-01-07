SELECT * FROM pglogical_regress_variables();
\gset

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.test_trg_data(id serial primary key, data text);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'test_trg_data');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn

CREATE TABLE test_trg_hist(table_name text, action text, action_ts timestamptz default now(), original_data text, new_data text);

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

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn

SELECT * FROM test_trg_data;
SELECT * FROM test_trg_hist;

ALTER TABLE test_trg_data ENABLE REPLICA TRIGGER test_trg_data_hist_trg;

\c :provider_dsn

INSERT INTO test_trg_data(data) VALUES ('yes_history');
UPDATE test_trg_data SET data = 'yes_history';
DELETE FROM test_trg_data;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :subscriber_dsn

SELECT * FROM test_trg_data;
SELECT * FROM test_trg_hist;

DROP TABLE test_trg_hist CASCADE;

\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.test_trg_data CASCADE;
$$);
