CREATE FUNCTION
pglogical.wait_slot_confirm_lsn(slotname name, target pg_lsn)
RETURNS void LANGUAGE c AS 'pglogical','pglogical_wait_slot_confirm_lsn';

