DO
LANGUAGE plpgsql
$$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'regression_slot')
	THEN
		PERFORM pg_drop_replication_slot('regression_slot');
	END IF;
END;
$$;
