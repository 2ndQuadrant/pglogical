--PRIMARY KEY
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn

-- Test conflicts where a secondary unique constraint with a predicate exits,
-- ensuring we don't generate false conflicts.
SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.secondary_unique_pred (
    a integer PRIMARY KEY,
    b integer NOT NULL,
    check_unique boolean NOT NULL
);

CREATE UNIQUE INDEX ON public.secondary_unique_pred (b) WHERE (check_unique);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'secondary_unique_pred');

INSERT INTO secondary_unique_pred (a, b, check_unique) VALUES (1, 1, false);
INSERT INTO secondary_unique_pred (a, b, check_unique) VALUES (2, 1, false);
INSERT INTO secondary_unique_pred (a, b, check_unique) VALUES (3, 2, true);
-- must fail
INSERT INTO secondary_unique_pred (a, b, check_unique) VALUES (5, 2, true);

SELECT * FROM secondary_unique_pred ORDER BY a;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT * FROM secondary_unique_pred ORDER BY a;

\c :provider_dsn

-- This line doesn't conflict on the provider. On the subscriber
-- we must not detect a conflict on (b), since the existing local
-- row matches (check_unique) but the new remote row doesn't. So
-- this must get applied.
INSERT INTO secondary_unique_pred (a, b, check_unique) VALUES (4, 2, false);

SELECT * FROM secondary_unique_pred ORDER BY a;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn

SELECT * FROM secondary_unique_pred ORDER BY a;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.secondary_unique_pred CASCADE;
$$);
