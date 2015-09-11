# contrib/pg_logical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

DATA = pglogical--1.0.sql

OBJS = pg_logical_apply.o pg_logical_conflict.o pg_logical_manager.o \
	   pg_logical_node.o pg_logical_proto.o pg_logical_relcache.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_logical_output
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

