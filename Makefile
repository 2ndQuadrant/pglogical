MODULE_big = pglogical_output
PGFILEDESC = "pglogical_output - logical replication output plugin"

OBJS = pglogical_output.o pglogical_hooks.o pglogical_config.o \
	   pglogical_proto.o pglogical_proto_native.o \
	   pglogical_proto_json.o pglogical_relmetacache.o \
	   pglogical_infofuncs.o

REGRESS = prep params_native basic_native hooks_native basic_json hooks_json encoding_json extension cleanup

EXTENSION = pglogical_output
DATA = pglogical_output--1.0.0.sql
EXTRA_CLEAN += pglogical_output.control


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

# The # in #define is taken as a comment, per https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=142043
# so it must be escaped. The $ placeholders in awk must be doubled too.
pglogical_output_version=$(shell awk '/\#define PGLOGICAL_OUTPUT_VERSION[ \t]+\".*\"/ { print substr($$3,2,length($$3)-2) }' pglogical_output.h )

distdir=pglogical-output-$(pglogical_output_version)

all: pglogical_output.control

pglogical_output.control: pglogical_output.control.in pglogical_output.h
	sed 's/__PGLOGICAL_OUTPUT_VERSION__/$(pglogical_output_version)/' pglogical_output.control.in > pglogical_output.control

install: header_install

header_install: pglogical_output/compat.h pglogical_output/hooks.h
	$(INSTALL_DATA) pglogical_output/compat.h '$(DESTDIR)$(includedir)'/pglogical_output
	$(INSTALL_DATA) pglogical_output/hooks.h '$(DESTDIR)$(includedir)'/pglogical_output

GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)

git-dist: clean
	rm -f .distgitrev .distgittag
	if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	echo $(GITHASH) > .distgitrev
	git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	git ls-tree -r -t --full-tree HEAD --name-only |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" -T - \
	    .distgitrev .distgittag
	echo >&2 "Prepared ${distdir}.tar.bz2 for rev=`cat .distgitrev`, tag=`cat .distgittag`"
	rm -f .distgitrev .distgittag
