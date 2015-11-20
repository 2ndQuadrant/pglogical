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

typedef struct PGLogicalProvider
{
	Oid			id;
	const char *name;
} PGLogicalProvider;

typedef struct PGLogicalSubscriber
{
	Oid			id;
	const char *name;
	bool		enabled;
	char		status;
	const char *provider_name;
	const char *provider_dsn;
	const char *local_dsn;
	List	   *replication_sets;
} PGLogicalSubscriber;

#define SUBSCRIBER_STATUS_INIT				'i'
#define SUBSCRIBER_STATUS_SYNC_SCHEMA		's'
#define SUBSCRIBER_STATUS_SYNC_DATA			'd'
#define SUBSCRIBER_STATUS_SYNC_CONSTRAINTS	'o'
#define SUBSCRIBER_STATUS_SLOTS				'l'
#define SUBSCRIBER_STATUS_CATCHUP			'c'
#define SUBSCRIBER_STATUS_READY				'r'

extern void create_provider(PGLogicalProvider *provider);
extern void drop_provider(Oid providerid);

extern void create_subscriber(PGLogicalSubscriber *subscriber);
extern void drop_subscriber(Oid subscriberid);

extern PGLogicalProvider *get_provider(Oid providerid);
extern PGLogicalProvider *get_provider_by_name(const char *name, bool missing_ok);
extern List *get_providers(void);

extern PGLogicalSubscriber *get_subscriber(Oid subscriberid);
extern PGLogicalSubscriber *get_subscriber_by_name(const char *name, bool missing_ok);
extern List *get_subscribers(void);
extern void set_subscriber_status(int subscriberid, char status);

#endif /* PGLOGICAL_NODE_H */
