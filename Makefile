# contrib/pglogical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

DATA = pglogical--1.0.sql

OBJS = pglogical_apply.o pglogical_conflict.o pglogical_manager.o \
	   pglogical_node.o pglogical_proto.o pglogical_relcache.o \
	   pglogical.o pglogical_repset.o pglogical_rpc.o \
	   pglogical_functions.o pglogical_queue.o \
	   pglogical_worker.o pglogical_hooks.o pglogical_sync.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

REGRESS = init basic extended toasted replication_set add_table

ifdef USE_PGXS

# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes

PG_CPPFLAGS += -Ipglogical_output

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# We can't do a normal 'make check' because PGXS doesn't support
# creating a temp install. We don't want to use a normal PGXS
# 'installcheck' though, because it's a pain to set up a temp install
# manually, with the config overrides needed.
#
# We compromise by using the install we're building against, installing
# pglogical_output (from a submodule) and pglogical into it, then making
# a temp instance. This means that 'check' affects the target DB
# install. Nobody with any sense runs 'make check' under a user with
# write permissions to their production PostgreSQL install (right?)
# but this is still not ideal.
regresscheck:
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-config ./regress-postgresql.conf \
	    --temp-instance=./tmp_check \
	    --outputdir=./regression_output \
	    --create-role=logical \
	    $(REGRESS)

pglogical_output:
	$(MAKE) -C pglogical_output clean all

pglogical_output_install: pglogical_output
	$(MAKE) -C pglogical_output install

check: pglogical_output_install install regresscheck ;

else

# In-tree builds only
subdir = contrib/pglogical
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
@installcheck: ;

EXTRA_INSTALL += $(top_srcdir)/contrib/pglogical_output
EXTRA_REGRESS_OPTS += $(top_srcdir)/contrib/regress-postgresql.conf

endif

.PHONY: regresscheck pglogical_output pglogical_output_install
