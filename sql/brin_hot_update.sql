-- test UPDATEs that change only BRIN-indexed columns
--
-- Since PostgreSQL 16 such an UPDATE can be HOT, and the new tuple must then
-- be added to the BRIN index only.  If it is added to a B-tree as well, the
-- B-tree gets a second entry for the row, pointing at the heap-only tuple.
-- Once VACUUM has made the page all-visible, an index-only scan returns
-- that entry as a duplicate row.
SELECT * FROM pglogical_regress_variables()
\gset

\c :provider_dsn
SELECT pglogical.replicate_ddl_command($$
	CREATE TABLE public.brin_hot (
		id int PRIMARY KEY,
		b int NOT NULL,
		v int NOT NULL
	) WITH (autovacuum_enabled = off);
	CREATE INDEX brin_hot_b_idx ON public.brin_hot (b);
	CREATE INDEX brin_hot_v_idx ON public.brin_hot USING brin (v)
		WITH (pages_per_range = 1);
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'brin_hot');

INSERT INTO brin_hot SELECT g, g, g FROM generate_series(1, 10) g;

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
-- a local row for a replicated INSERT to conflict with
INSERT INTO brin_hot VALUES (11, 11, 11);

\c :provider_dsn
-- HOT update on PostgreSQL 16+ (only the BRIN index needs updating)
UPDATE brin_hot SET v = v + 100 WHERE id <= 5;

-- non-HOT update (all indexes need updating)
UPDATE brin_hot SET b = b + 100 WHERE id = 6;

-- conflicts with the local row, so it is applied as an UPDATE of v
INSERT INTO brin_hot VALUES (11, 11, 111);

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

\c :subscriber_dsn
SET enable_seqscan = off;

-- the BRIN index finds the new values
SET enable_indexscan = off;
SELECT id, v FROM brin_hot WHERE v > 100 ORDER BY id;
RESET enable_indexscan;

-- each B-tree has exactly one entry per row
VACUUM ANALYZE brin_hot;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM brin_hot WHERE id > 0;
SELECT id FROM brin_hot WHERE id > 0;
EXPLAIN (COSTS OFF) SELECT b FROM brin_hot WHERE b > 0;
SELECT b FROM brin_hot WHERE b > 0;
RESET enable_bitmapscan;
RESET enable_seqscan;

\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.brin_hot CASCADE;
$$);
