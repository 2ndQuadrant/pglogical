SET synchronous_commit = on;

-- Schema setup

CREATE TABLE demo (
	seq serial primary key,
	tx text,
	ts timestamp,
	jsb jsonb,
	js json,
	ba bytea
);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'pglogical_output');

-- Queue up some work to decode with a variety of types

INSERT INTO demo(tx) VALUES ('textval');
INSERT INTO demo(ba) VALUES (BYTEA '\xDEADBEEF0001');
INSERT INTO demo(ts, tx) VALUES (TIMESTAMP '2045-09-12 12:34:56.00', 'blah');
INSERT INTO demo(js, jsb) VALUES ('{"key":"value"}', '{"key":"value"}');

-- Rolled back txn
BEGIN;
DELETE FROM demo;
INSERT INTO demo(tx) VALUES ('blahblah');
ROLLBACK;

-- Multi-statement transaction with subxacts
BEGIN;
SAVEPOINT sp1;
INSERT INTO demo(tx) VALUES ('row1');
RELEASE SAVEPOINT sp1;
SAVEPOINT sp2;
UPDATE demo SET tx = 'update-rollback' WHERE tx = 'row1';
ROLLBACK TO SAVEPOINT sp2;
SAVEPOINT sp3;
INSERT INTO demo(tx) VALUES ('row2');
INSERT INTO demo(tx) VALUES ('row3');
RELEASE SAVEPOINT sp3;
SAVEPOINT sp4;
DELETE FROM demo WHERE tx = 'row2';
RELEASE SAVEPOINT sp4;
SAVEPOINT sp5;
UPDATE demo SET tx = 'updated' WHERE tx = 'row1';
COMMIT;


-- txn with catalog changes
BEGIN;
CREATE TABLE cat_test(id integer);
INSERT INTO cat_test(id) VALUES (42);
COMMIT;

-- Aborted subxact with catalog changes
BEGIN;
INSERT INTO demo(tx) VALUES ('1');
SAVEPOINT sp1;
ALTER TABLE demo DROP COLUMN tx;
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO demo(tx) VALUES ('2');
COMMIT;
