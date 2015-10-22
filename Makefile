MODULE_big = pglogical_output
PGFILEDESC = "pglogical_output - logical replication output plugin"

OBJS = pglogical_output.o pglogical_proto.o pglogical_config.o pglogical_hooks.o

ifdef USE_PGXS
# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes
# Usual recipe
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pglogical_output
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
