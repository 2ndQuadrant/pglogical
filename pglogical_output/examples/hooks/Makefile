MODULES = pglogical_output_plhooks
EXTENSION = pglogical_output_plhooks
DATA = pglogical_output_plhooks--1.0.sql
DOCS = README.pglogical_output_plhooks

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_CPPFLAGS = -I../..
include $(PGXS)
else
subdir = contrib/pglogical_output/examples/hooks
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

# Allow the hook plugin to see the pglogical_output headers
# Necessary because !PGXS builds don't respect PG_CPPFLAGS
override CPPFLAGS := $(CPPFLAGS) -I../..
endif
