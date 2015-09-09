/*-------------------------------------------------------------------------
 *
 * pg_logical_node.h
 *		pg_logical node and connection catalog manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_node.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_NODE_H
#define PG_LOGICAL_NODE_H

typedef struct PGLogicalNode
{
	const char *name;
	char		role;
	char		status;
	const char *dsn;
	bool		valid;
} PGLogicalNode;

typedef struct PGLogicalConnection
{
	PGLogicalNode  *node;
	char		   *replication_sets;
} PGLogicalConnection;

extern void create_node(PGLogicalNode *node);
extern void alter_node(PGLogicalNode *node);
extern void drop_node(const char *nodename);

extern PGLogicalNode **get_nodes();
extern PGLogicalNode *get_local_node();
extern void set_node_status(const char *nodename, char status);

extern List *get_node_subscribers(const char *nodename);
extern List *get_node_publishers(const char *nodename);

extern void create_node_connection(const char *originname, const char *targetname);
extern void drop_node_connection(const char *originname, const char *targetname);

#endif /* PG_LOGICAL_NODE_H */

