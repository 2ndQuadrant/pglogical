# contrib/pglogical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

DATA = pglogical--1.0.sql

OBJS = pglogical_apply.o pglogical_conflict.o pglogical_manager.o \
	   pglogical_node.o pglogical_proto.o pglogical_relcache.o \
	   pglogical.o pglogical_init_replica.o pglogical_repset.o \
	   pglogical_rpc.o pglogical_functions.o pglogical_queue.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

REGRESS = init basic extended toasted

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pglogical
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
@installcheck: ;

check: regresscheck ;

regresscheck:
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-config ./postgresql.conf \
	    --temp-instance=./tmp_check \
	    --outputdir=./regression_output \
	    $(REGRESS)

.PHONY: regresscheck
