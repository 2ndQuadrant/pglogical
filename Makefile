MODULE_big = pglogical_output
PGFILEDESC = "pglogical_output - logical replication output plugin"

OBJS = pglogical_output.o pglogical_proto.o pglogical_config.o pglogical_hooks.o

REGRESS = params basic hooks


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

# These don't do anything yet, since temp install is disabled
EXTRA_INSTALL += ./examples/hooks
REGRESS_OPTS += --temp-config=regression.conf

plhooks:
	make -C examples/hooks USE_PGXS=1 clean install

installcheck: plhooks

else

subdir = contrib/pglogical_output
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

# 'make installcheck' disabled when building in-tree because these tests
# require "wal_level=logical", which typical installcheck users do not have
# (e.g. buildfarm clients).
installcheck:
	;

EXTRA_INSTALL += $(subdir)/examples/hooks
EXTRA_REGRESS_OPTS += --temp-config=./regression.conf

endif

install: all
	$(MKDIR_P) '$(DESTDIR)$(includedir)'/pglogical_output
	$(INSTALL_DATA) pglogical_output/compat.h '$(DESTDIR)$(includedir)'/pglogical_output
	$(INSTALL_DATA) pglogical_output/hooks.h '$(DESTDIR)$(includedir)'/pglogical_output
