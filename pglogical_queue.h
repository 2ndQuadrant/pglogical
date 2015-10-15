/*-------------------------------------------------------------------------
 *
 * pglogical_node.h
 *		pglogical node and connection catalog manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_node.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_QUEUE_H
#define PGLOGICAL_QUEUE_H

#define QUEUE_COMMAND_TYPE_SQL	'Q'

extern void queue_command(Oid roleoid, char message_type, char *message);

extern char message_type_from_queued_tuple(HeapTuple queue_tup);
extern char *sql_from_queued_tuple(HeapTuple queue_tup, char **role);

extern Oid get_queue_table_oid(void);

#endif /* PGLOGICAL_NODE_H */

