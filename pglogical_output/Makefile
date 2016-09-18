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

install: header_install

header_install: pglogical_output/compat.h pglogical_output/hooks.h
	$(MKDIR_P) '$(DESTDIR)$(includedir)'/pglogical_output
	$(INSTALL_DATA) pglogical_output/compat.h '$(DESTDIR)$(includedir)'/pglogical_output
	$(INSTALL_DATA) pglogical_output/hooks.h '$(DESTDIR)$(includedir)'/pglogical_output

GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)

dist-common: clean
	@if test "$(wanttag)" -eq 1 -a "`git name-rev --tags --name-only $(GITHASH)`" = "undefined"; then echo "cannot 'make dist' on untagged tree; tag it or use make git-dist"; exit 1; fi
	@rm -f .distgitrev .distgittag
	@if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	@if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	@echo $(GITHASH) > .distgitrev
	@git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	@(git ls-tree -r -t --full-tree HEAD --name-only ; \
	  echo .distgitrev ; echo .distgittag) | grep -v '^test' |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" -T -
	@echo >&2 "Prepared ${distdir}.tar.bz2 for rev=`cat .distgitrev`, tag=`cat .distgittag`"
	@rm -f .distgitrev .distgittag
	@md5sum "${distdir}.tar.bz2" > "${distdir}.tar.bz2.md5"
	@if test -n "$(GPGSIGNKEYS)"; then gpg -q -a -b $(shell for x in $(GPGSIGNKEYS); do echo -u $$x; done) "${distdir}.tar.bz2"; else echo "No GPGSIGNKEYS passed, not signing tarball. Pass space separated keyid list as make var to sign."; fi

dist: distdir=pglogical-output-$(pglogical_output_version)
dist: wanttag=1
dist: dist-common

git-dist: distdir=pglogical-output-$(pglogical_output_version)_git$(GITHASH)
git-dist: wanttag=0
git-dist: dist-common
