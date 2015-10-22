MODULES = pglogical_output_plhooks
EXTENSION = pglogical_output_plhooks
DATA = pglogical_output_plhooks--1.0.sql
DOCS = README.pglogical_output_plhooks

ifdef INTREE
PG_CPPFLAGS = -I../..
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
