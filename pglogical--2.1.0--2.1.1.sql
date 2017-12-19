ALTER TABLE pglogical.local_sync_status ADD COLUMN sync_statuslsn pg_lsn NULL;
UPDATE pglogical.local_sync_status SET sync_statuslsn = '0/0';
ALTER TABLE pglogical.local_sync_status ALTER COLUMN sync_statuslsn SET NOT NULL;
