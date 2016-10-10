MODULE_big = pglogical_output
PGFILEDESC = "pglogical_output - logical replication output plugin"

OBJS = pglogical_output.o pglogical_hooks.o pglogical_config.o \
	   pglogical_proto.o pglogical_proto_native.o \
	   pglogical_proto_json.o pglogical_relmetacache.o \
	   pglogical_infofuncs.o

REGRESS = prep params_native basic_native hooks_native basic_json hooks_json encoding_json extension cleanup

EXTENSION = pglogical_output
DATA = pglogical_output--1.1.0.sql

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
