# contrib/pglogical/Makefile

MODULE_big = pglogical
EXTENSION = pglogical
PGFILEDESC = "pglogical - logical replication"

MODULES = pglogical_output

DATA = pglogical--1.0.0.sql pglogical--1.0.0--1.0.1.sql \
	   pglogical--1.0.1--1.1.0.sql \
	   pglogical--1.1.0--1.1.1.sql pglogical--1.1.1--1.1.2.sql \
	   pglogical--1.1.2--1.2.0.sql \
	   pglogical--1.2.0--1.2.1.sql pglogical--1.2.1--1.2.2.sql \
	   pglogical--1.2.2--2.0.0.sql \
	   pglogical--2.0.0--2.0.1.sql \
	   pglogical--2.0.0--2.1.0.sql pglogical--2.0.1--2.1.0.sql \
	   pglogical--2.1.0--2.1.1.sql pglogical--2.1.1--2.2.0.sql \
	   pglogical--2.2.0.sql \
	   pglogical--2.2.0--2.2.1.sql pglogical--2.2.1.sql \
	   pglogical--2.2.1--2.2.2.sql pglogical--2.2.2.sql \
	   pglogical--2.2.2--2.3.0.sql \
	   pglogical--2.2.2--2.3.1.sql \
	   pglogical--2.3.0.sql \
	   pglogical--2.3.0--2.3.1.sql \
	   pglogical--2.3.1.sql \
	   pglogical--2.3.1--2.3.2.sql \
	   pglogical--2.3.2.sql \
	   pglogical--2.3.2--2.3.3.sql \
	   pglogical--2.3.3.sql \
	   pglogical--2.3.3--2.3.4.sql \
	   pglogical--2.3.4.sql

OBJS = pglogical_apply.o pglogical_conflict.o pglogical_manager.o \
	   pglogical.o pglogical_node.o pglogical_relcache.o \
	   pglogical_repset.o pglogical_rpc.o pglogical_functions.o \
	   pglogical_queue.o pglogical_fe.o pglogical_worker.o \
	   pglogical_sync.o pglogical_sequences.o pglogical_executor.o \
	   pglogical_dependency.o pglogical_apply_heap.o pglogical_apply_spi.o \
	   pglogical_output_config.o pglogical_output_plugin.o \
	   pglogical_output_proto.o pglogical_proto_json.o \
	   pglogical_proto_native.o pglogical_monitoring.o

SCRIPTS_built = pglogical_create_subscriber

REGRESS = preseed infofuncs init_fail init preseed_check basic extended conflict_secondary_unique \
		  toasted replication_set add_table matview bidirectional primary_key \
		  interfaces foreign_key functions copy triggers parallel row_filter \
		  row_filter_sampling att_list column_filter apply_delay multiple_upstreams \
		  node_origin_cascade drop

EXTRA_CLEAN += compat94/pglogical_compat.o compat95/pglogical_compat.o \
			   compat96/pglogical_compat.o compat10/pglogical_compat.o \
			   pglogical_create_subscriber.o

# The # in #define is taken as a comment, per https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=142043
# so it must be escaped. The $ placeholders in awk must be doubled too.
pglogical_version=$(shell awk '/\#define PGLOGICAL_VERSION[ \t]+\".*\"/ { print substr($$3,2,length($$3)-2) }' $(realpath $(srcdir)/pglogical.h) )

# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes

PG_CONFIG ?= pg_config

PGVER := $(shell $(PG_CONFIG) --version | sed 's/[^0-9]//g' | cut -c 1-2)

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(realpath $(srcdir)/compat$(PGVER)) -Werror=implicit-function-declaration
SHLIB_LINK += $(libpq) $(filter -lintl, $(LIBS))

OBJS += $(srcdir)/compat$(PGVER)/pglogical_compat.o

ifeq ($(PGVER),94)
DATA += compat94/pglogical_origin.control compat94/pglogical_origin--1.0.0.sql
REGRESS = preseed infofuncs init preseed_check basic extended \
		  toasted replication_set add_table matview primary_key \
		  interfaces foreign_key functions copy triggers parallel \
		  att_list column_filter apply_delay multiple_upstreams \
		  node_origin_cascade drop

REGRESS += --dbname=regression
SCRIPTS_built += pglogical_dump/pglogical_dump
SCRIPTS += pglogical_dump/pglogical_dump
requires = requires=pglogical_origin
control_path = $(abspath $(abs_top_builddir))/pglogical.control
else
DATA += pglogical_origin.control pglogical_origin--1.0.0.sql
requires =
control_path = $(abspath $(srcdir))/pglogical.control
endif

EXTRA_CLEAN += $(control_path)


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
# glogical into it, then making a temp instance. This means that 'check'
# affects the target DB install. Nobody with any sense runs 'make check'
# under a user with write permissions to their production PostgreSQL
# install (right?)
# But this is still not ideal.
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
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(filter-out -lreadline, $(LIBS)) -o $@$(X)


pglogical.control: pglogical.control.in pglogical.h
	sed 's/__PGLOGICAL_VERSION__/$(pglogical_version)/;s/__REQUIRES__/$(requires)/' $(realpath $(srcdir)/pglogical.control.in) > $(control_path)

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


define prove_check
rm -rf $(CURDIR)/tmp_check/log
cd $(srcdir) && TESTDIR='$(CURDIR)' $(with_temp_install) PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) --verbose $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(or $(PROVE_TESTS),t/*.pl)
endef

check_prove:
	$(prove_check)

.PHONY: all check regresscheck pglogical.control

define _pgl_create_recursive_target
.PHONY: $(1)-$(2)-recurse
$(1): $(1)-$(2)-recurse
$(1)-$(2)-recurse: $(if $(filter check, $(3)), temp-install)
	$(MKDIR_P) $(2)
	$$(MAKE) -C $(2) -f $(abspath $(srcdir))/$(2)/Makefile VPATH=$(abspath $(srcdir))/$(2) $(3)
endef

$(foreach target,$(if $1,$1,$(standard_targets)),$(foreach subdir,$(if $2,$2,$(SUBDIRS)),$(eval $(call _pgl_create_recursive_target,$(target),$(subdir),$(if $3,$3,$(target))))))


#
# The following hideous hack works around pg_regress's inability to inject
# prefix commands by using a wrapper 'postgres' that finds the real postgres.
#

define VALGRIND_WRAPPER
#!/bin/bash

set -e -u -x

# May also want --expensive-definedness-checks=yes
#
# Quicker runs without --track-origins=yes --read-var-info=yes
#
# If you don't want leak checking, use --leak-check=no
#
# When just doing leak checking and not looking for detailed memory error reports you don't need:
# 	--track-origins=yes --read-var-info=yes --malloc-fill=8f --free-fill=9f 
#
SUPP=$(POSTGRES_SRC)/src/tools/valgrind.supp

# Pop top two elements from path; the first is added by pg_regress
# and the next is us.
function join_by { local IFS="$$1"; shift; echo "$$*"; }
IFS=':' read -r -a PATHA <<< "$$PATH"
export PATH=$$(join_by ":" "$${PATHA[@]:2}")

NEXT_POSTGRES=$$(which postgres)
if [ "$${NEXT_POSTGRES}" -ef "./valgrind/postgres" ]; then
    echo "ERROR: attempt to execute self"
    exit 1
fi

echo "Running $${NEXT_POSTGRES} under Valgrind"

valgrind --leak-check=full --show-leak-kinds=definite,possible,reachable --gen-suppressions=all \
	--suppressions="$${SUPP}" --suppressions=`pwd`/pglogical.supp --verbose \
	--time-stamp=yes  --log-file=valgrind-$$$$-%p.log --trace-children=yes \
	--track-origins=yes --read-var-info=yes --malloc-fill=8f --free-fill=9f \
	--num-callers=30 \
	postgres "$$@"

endef

export VALGRIND_WRAPPER

valgrind-check:
	$(if $(POSTGRES_SRC),,$(error set Make variable POSTGRES_SRC to postgres source dir to find valgrind.supp))
	$(if $(wildcard $(POSTGRES_SRC)/src/tools/valgrind.supp),,$(error missing valgrind suppressions at $(POSTGRES_SRC)/src/tools/valgrind.supp))
	mkdir -p valgrind/
	echo "$$VALGRIND_WRAPPER" > valgrind/postgres
	chmod a+x valgrind/postgres
	PATH=./valgrind/:$(PATH) $(MAKE) check
	rm valgrind/postgres
