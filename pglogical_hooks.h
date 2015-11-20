#ifndef PGLOGICAL_HOOKS_H
#define PGLOGICAL_HOOKS_H

#include "replication/reorderbuffer.h"

/* public interface for hooks */
#include "pglogical_output/hooks.h"
#include "pglogical_output.h"

extern void load_hooks(PGLogicalOutputData *data);

extern void call_startup_hook(PGLogicalOutputData *data, List *plugin_params);

extern void call_shutdown_hook(PGLogicalOutputData *data);

extern bool call_row_filter_hook(PGLogicalOutputData *data,
		ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);

extern bool call_txn_filter_hook(PGLogicalOutputData *data,
		RepOriginId txn_origin);


#endif
