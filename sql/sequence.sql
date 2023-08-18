-- like bt_index_check('pglogical.sequence_state', true)
CREATE FUNCTION heapallindexed() RETURNS void AS $$
DECLARE
	count_seqscan int;
	count_idxscan int;
BEGIN
	count_seqscan := (SELECT count(*) FROM pglogical.sequence_state);
	SET enable_seqscan = off;
	count_idxscan := (SELECT count(*) FROM pglogical.sequence_state);
	RESET enable_seqscan;
	IF count_seqscan <> count_idxscan THEN
		RAISE 'seqscan found % rows, but idxscan found % rows',
			count_seqscan, count_idxscan;
	END IF;
END
$$ LANGUAGE plpgsql;

-- Replicate one sequence.
CREATE SEQUENCE stress;
SELECT * FROM pglogical.create_replication_set('stress_seq');
SELECT * FROM pglogical.replication_set_add_sequence('stress_seq', 'stress');
SELECT pglogical.synchronize_sequence('stress');
SELECT heapallindexed();

-- Sync it 400 times in one transaction, to cross a pglogical.sequence_state
-- page boundary and get a non-HOT update.
DO $$
BEGIN
  FOR i IN 1..400 LOOP
    PERFORM pglogical.synchronize_sequence('stress');
  END LOOP;
END;
$$;
SELECT heapallindexed();
