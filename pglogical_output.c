/*-------------------------------------------------------------------------
 *
 * pglogical_output.c
 *		  Logical Replication output plugin which just loads and forwards
 *		  the call to the pglogical.
 *
 *		  This exists for backwards compatibility.
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/logical.h"

PG_MODULE_MAGIC;

extern void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	LogicalOutputPluginInit plugin_init;

	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	plugin_init = (LogicalOutputPluginInit)
		load_external_function("pglogical", "_PG_output_plugin_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "could not load pglogical output plugin");

	plugin_init(cb);
}
