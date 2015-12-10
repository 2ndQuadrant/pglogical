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
	Oid			id;
	char	   *name;
} PGLogicalNode;

typedef struct PGlogicalInterface
{
	Oid				id;
	const char	   *name;
	Oid				nodeid;
	const char	   *dsn;
} PGlogicalInterface;

typedef struct PGLogicalLocalNode
{
	PGLogicalNode	*node;
	PGlogicalInterface *interface;
} PGLogicalLocalNode;

typedef struct PGLogicalSubscription
{
	Oid			id;
	char	   *name;
	PGLogicalNode	   *origin;
   	PGLogicalNode	   *target;
	PGlogicalInterface *origin_if;
	PGlogicalInterface *target_if;
	bool		enabled;
	List	   *replication_sets;
	List	   *forward_origins;
	char	   *slot_name;
} PGLogicalSubscription;

extern void create_node(PGLogicalNode *node);
extern void drop_node(Oid nodeid);

extern PGLogicalNode *get_node(Oid nodeid);
extern PGLogicalNode *get_node_by_name(const char *name, bool missing_ok);

extern void create_node_interface(PGlogicalInterface *node);
extern void drop_node_interface(Oid ifid);
extern void drop_node_interfaces(Oid nodeid);
extern PGlogicalInterface *get_node_interface(Oid ifid);

extern void create_local_node(Oid nodeid, Oid ifid);
extern void drop_local_node(void);
extern PGLogicalLocalNode *get_local_node(bool missing_ok);

extern void create_subscription(PGLogicalSubscription *sub);
extern void alter_subscription(PGLogicalSubscription *sub);
extern void drop_subscription(Oid subid);

extern PGLogicalSubscription *get_subscription(Oid subid);
extern PGLogicalSubscription *get_subscription_by_name(const char *name, bool missing_ok);
extern List *get_node_subscriptions(Oid nodeid, bool origin);

#endif /* PGLOGICAL_NODE_H */
