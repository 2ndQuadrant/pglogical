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
	int			id;
	const char *name;
	char		role;
	char		status;
	const char *dsn;
	bool		valid;
} PGLogicalNode;

#define NODE_STATUS_INIT				'i'
#define NODE_STATUS_SYNC_SCHEMA			's'
#define NODE_STATUS_SYNC_DATA			'd'
#define NODE_STATUS_SYNC_CONSTRAINTS	'o'
#define NODE_STATUS_SLOTS				'l'
#define NODE_STATUS_CATCHUP				'c'
#define NODE_STATUS_CONNECT_BACK		'b'
#define NODE_STATUS_READY				'r'

#define NODE_ROLE_PUBLISHER		'p'
#define NODE_ROLE_SUBSCRIBER	's'
#define NODE_ROLE_FOWARDER		'f'

typedef struct PGLogicalConnection
{
	int				id;
	PGLogicalNode  *origin;
	PGLogicalNode  *target;
	char		   *replication_sets;
} PGLogicalConnection;

extern void create_node(PGLogicalNode *node);
extern void alter_node(PGLogicalNode *node);
extern void drop_node(int nodeid);

extern PGLogicalNode **get_nodes(void);
extern PGLogicalNode *get_node(int nodeid);
extern PGLogicalNode *get_local_node(void);
extern void set_node_status(int nodeid, char status);

extern List *get_node_subscribers(int nodeid);
extern List *get_node_publishers(int nodeid);

extern int get_node_connectionid(int originid, int targetid);
extern PGLogicalConnection *get_node_connection_by_id(int connid);
extern void create_node_connection(int originid, int targetid);
extern void drop_node_connection(int connid);

#endif /* PG_LOGICAL_NODE_H */

