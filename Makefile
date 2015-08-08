# contrib/pg_logical_output/Makefile

MODULE_big = pg_logical_output
PGFILEDESC = "pg_logical_output - logical replication output plugin"

OBJS = pg_logical_output.o pg_logical_proto.o

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
