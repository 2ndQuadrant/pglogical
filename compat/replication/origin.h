typedef uint16 RepOriginId;
#define InvalidRepOriginId 0

extern PGDLLIMPORT RepOriginId replorigin_session_origin;
extern PGDLLIMPORT XLogRecPtr replorigin_session_origin_lsn;
extern PGDLLIMPORT TimestampTz replorigin_session_origin_timestamp;

extern RepOriginId replorigin_create(char *name);
extern void replorigin_drop(RepOriginId roident);

extern RepOriginId replorigin_by_name(char *name, bool missing_ok);
extern void replorigin_session_setup(RepOriginId node);
extern void replorigin_session_reset(void);
extern XLogRecPtr replorigin_session_get_progress(bool flush);

extern void replorigin_advance(RepOriginId node,
				   XLogRecPtr remote_commit,
				   XLogRecPtr local_commit,
				   bool go_backward, bool wal_log);
