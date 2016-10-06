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

#include "utils/jsonb.h"

#define QUEUE_COMMAND_TYPE_SQL			'Q'
#define QUEUE_COMMAND_TYPE_TRUNCATE		'T'
#define QUEUE_COMMAND_TYPE_TABLESYNC	'A'
#define QUEUE_COMMAND_TYPE_SEQUENCE		'S'

typedef struct QueuedMessage
{
	TimestampTz	queued_at;
	List	   *replication_sets;
	char	   *role;
	char		message_type;
	Jsonb	   *message;
} QueuedMessage;

extern void queue_message(List *replication_sets, Oid roleoid,
						  char message_type, char *message);

extern QueuedMessage *queued_message_from_tuple(HeapTuple queue_tup);

extern Oid get_queue_table_oid(void);

extern void create_truncate_trigger(Relation rel);

#endif /* PGLOGICAL_NODE_H */
