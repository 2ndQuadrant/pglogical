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
#ifndef PGLOGICAL_NODE_H
#define PGLOGICAL_NODE_H

typedef struct PGLogicalNode
{
	int			id;
	const char *name;
	char		role;
	char		status;
	const char *dsn;
	const char *init_dsn;
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
	List		   *replication_sets;
	bool			default_set;
} PGLogicalConnection;

extern void create_node(PGLogicalNode *node);
extern void alter_node(PGLogicalNode *node);
extern void drop_node(int nodeid);

extern PGLogicalNode **get_nodes(void);
extern PGLogicalNode *get_node(int nodeid);
extern PGLogicalNode *get_local_node(void);
extern PGLogicalNode *get_node_by_name(const char *node_name, bool missing_ok);
extern void set_node_status(int nodeid, char status);

extern List *get_node_subscribers(int nodeid);
extern List *get_node_publishers(int nodeid);

extern int get_node_connectionid(int originid, int targetid);
extern PGLogicalConnection *get_node_connection(int connid);
extern PGLogicalConnection *find_node_connection(int originid, int targetid,
												 bool missing_ok);
extern int32 create_node_connection(int originid, int targetid,
									List *replication_sets);
extern void drop_node_connection(int connid);

#endif /* PGLOGICAL_NODE_H */

