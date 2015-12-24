# contrib/pglogical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

DATA = pglogical--1.0.0.sql

OBJS = pglogical_apply.o pglogical_conflict.o pglogical_manager.o \
	   pglogical_node.o pglogical_proto.o pglogical_relcache.o \
	   pglogical.o pglogical_repset.o pglogical_rpc.o \
	   pglogical_functions.o pglogical_queue.o pglogical_fe.o \
	   pglogical_worker.o pglogical_hooks.o pglogical_sync.o

SCRIPTS_built = pglogical_create_subscriber


PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

REGRESS = preseed infofuncs init_fail init preseed_check basic extended toasted replication_set add_table matview bidirectional primary_key foreign_key functions copy drop

EXTRA_CLEAN += pglogical.control

# The # in #define is taken as a comment, per https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=142043
# so it must be escaped. The $ placeholders in awk must be doubled too.
pglogical_version=$(shell awk '/\#define PGLOGICAL_VERSION[ \t]+\".*\"/ { print substr($$3,2,length($$3)-2) }' pglogical.h )

ifdef USE_PGXS


# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes

PG_CONFIG = pg_config

#PG_CPPFLAGS += -Ipglogical_output

PGVER := $(shell $(PG_CONFIG) --version | sed 's/[^0-9\.]//g' | awk -F . '{ print $$1$$2 }')

ifeq ($(PGVER),94)
PG_CPPFLAGS += -Icompat
OBJS += compat/pglogical_compat.o
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

pglogical_create_subscriber: pglogical_create_subscriber.o pglogical_fe.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)

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

ifeq ($(PGVER),94)
regresscheck: ;
else
regresscheck:
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-config ./regress-postgresql.conf \
	    --temp-instance=./tmp_check \
	    --outputdir=./regression_output \
	    --create-role=logical \
	    $(REGRESS)

check: install regresscheck ;

endif

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

pglogical.control: pglogical.control.in pglogical.h
	sed 's/__PGLOGICAL_VERSION__/$(pglogical_version)/' pglogical.control.in > pglogical.control

all: pglogical.control

GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)

dist-common: clean
	@if test "$(wanttag)" -eq 1 -a "`git name-rev --tags --name-only $(GITHASH)`" = "undefined"; then echo "cannot 'make dist' on untagged tree; tag it or use make git-dist"; exit 1; fi
	@rm -f .distgitrev .distgittag
	@if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	@if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	@echo $(GITHASH) > .distgitrev
	@git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	@git ls-tree -r -t --full-tree HEAD --name-only |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" -T - \
	    .distgitrev .distgittag
	@echo >&2 "Prepared ${distdir}.tar.bz2 for rev=`cat .distgitrev`, tag=`cat .distgittag`"
	@rm -f .distgitrev .distgittag
	@md5sum "${distdir}.tar.bz2" > "${distdir}.tar.bz2.md5"
	@if test -n "$(GPGSIGNKEYS)"; then gpg -q -a -b $(shell for x in $(GPGSIGNKEYS); do echo -u $$x; done) "${distdir}.tar.bz2"; else echo "No GPGSIGNKEYS passed, not signing tarball. Pass space separated keyid list as make var to sign."; fi

dist: distdir=pglogical-$(pglogical_version)
dist: wanttag=1
dist: dist-common

git-dist: distdir=pglogical-$(pglogical_version)_git$(GITHASH)
git-dist: wanttag=0
git-dist: dist-common


.PHONY: regresscheck
