# contrib/pglogical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

SUBDIRS = pglogical_output

DATA = pglogical--1.0.0.sql pglogical--1.0.0--1.0.1.sql \
	   pglogical--1.0.1--1.1.0.sql \
	   pglogical--1.1.0--1.1.1.sql pglogical--1.1.1--1.1.2.sql \
	   pglogical--1.1.2--1.2.0.sql \
	   pglogical--1.2.0.sql pglogical--1.2.0--1.2.1.sql \
	   pglogical--1.2.1.sql

OBJS = pglogical_apply.o pglogical_conflict.o pglogical_manager.o \
	   pglogical_node.o pglogical_proto.o pglogical_relcache.o \
	   pglogical.o pglogical_repset.o pglogical_rpc.o \
	   pglogical_functions.o pglogical_queue.o pglogical_fe.o \
	   pglogical_worker.o pglogical_hooks.o pglogical_sync.o \
	   pglogical_sequences.o

# 9.4 needs SCRIPTS set to do anything, even if SCRIPTS_built is set
SCRIPTS = pglogical_create_subscriber

SCRIPTS_built = pglogical_create_subscriber

REGRESS = preseed infofuncs init_fail init preseed_check basic extended \
		  toasted replication_set add_table matview bidirectional primary_key \
		  interfaces foreign_key functions copy triggers parallel drop

EXTRA_CLEAN += pglogical.control compat94/pglogical_compat.o \
			   compat95/pglogical_compat.o pglogical_create_subscriber.o

# The # in #define is taken as a comment, per https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=142043
# so it must be escaped. The $ placeholders in awk must be doubled too.
pglogical_version=$(shell awk '/\#define PGLOGICAL_VERSION[ \t]+\".*\"/ { print substr($$3,2,length($$3)-2) }' $(realpath $(srcdir)/pglogical.h) )

# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes

PG_CONFIG ?= pg_config

PG_CPPFLAGS += -I$(libpq_srcdir) $(addprefix -I,$(realpath $(srcdir)/pglogical_output/))
SHLIB_LINK += $(libpq)

PGVER := $(shell $(PG_CONFIG) --version | sed 's/[^0-9\.]//g' | awk -F . '{ print $$1$$2 }')

ifeq ($(PGVER),94)
PG_CPPFLAGS += $(addprefix -I,$(realpath $(srcdir)/compat94))
OBJS += $(srcdir)/compat94/pglogical_compat.o
DATA += compat94/pglogical_origin.control compat94/pglogical_origin--1.0.0.sql
REGRESS = preseed infofuncs init_fail init preseed_check basic extended \
		  toasted replication_set add_table matview primary_key foreign_key \
		  functions copy triggers parallel drop
REGRESS += --dbname=regression
SCRIPTS_built += pglogical_dump/pglogical_dump
SCRIPTS += pglogical_dump/pglogical_dump
requires = requires=pglogical_origin
else
DATA += pglogical_origin.control pglogical_origin--1.0.0.sql
requires =
endif

ifeq ($(PGVER),95)
PG_CPPFLAGS += $(addprefix -I,$(realpath $(srcdir)/compat95))
OBJS += $(srcdir)/compat95/pglogical_compat.o
endif

PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


ifeq ($(PGVER),94)
regresscheck: ;
check: ;

$(srcdir)/pglogical_dump/pg_dump.c:
	$(warning pglogical_dump empty, trying to fetch as submodule)
	git submodule init
	git submodule update

pglogical_dump/pglogical_dump: pglogical_dump/pg_dump.c

SUBDIRS += pglogical_dump

else
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

check: install regresscheck

endif

pglogical_create_subscriber: pglogical_create_subscriber.o pglogical_fe.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)


pglogical.control: pglogical.control.in pglogical.h
	sed 's/__PGLOGICAL_VERSION__/$(pglogical_version)/;s/__REQUIRES__/$(requires)/' $(realpath $(srcdir)/pglogical.control.in) > pglogical.control

all: pglogical.control

GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)

dist-common: clean
	@if test "$(wanttag)" -eq 1 -a "`git name-rev --tags --name-only $(GITHASH)`" = "undefined"; then echo "cannot 'make dist' on untagged tree; tag it or use make git-dist"; exit 1; fi
	@rm -f .distgitrev .distgittag
	@if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	@if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	@echo $(GITHASH) > .distgitrev
	@git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	@(git ls-tree -r -t --full-tree HEAD --name-only \
	  && cd pglogical_dump\
	  && git ls-tree -r -t --full-tree HEAD --name-only | sed 's/^/pglogical_dump\//'\
	 ) |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" --no-recursion \
	    -T - .distgitrev .distgittag
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


# runs TAP tests

# PGXS doesn't support TAP tests yet.
# Copy perl modules in postgresql_srcdir/src/test/perl
# to postgresql_installdir/lib/pgxs/src/test/perl

check_prove:
	$(prove_check)

.PHONY: all check regresscheck

$(recurse)
