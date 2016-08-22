What are these tests?
---

These tests exersise the pglogical protocol, parameter validation, hooks and
filters, and the overall behaviour of the extension. They are *not* the tests
run by `make check` or `make installcheck` on the top level source directory;
those are the `pg_regress` tests discussed in the "tests" section of the
top-level `README.md`.

QUICK START
---

To run these tests:

* Install the output plugin into your PostgreSQL instance, e.g.

        make USE_PGXS=1 install

  Use the same options, environment variables, etc as used for compiling,
  most notably the `PATH` to ensure the same `pg_config` is used.

* Create a temporary PostgreSQL datadir at any location of your choosing:

        initdb -A trust -D tmp_install

* Start the temporary PostgreSQL instance with:

        PGPORT=5142 postgres -D tmp_install -c max_replication_slots=5 -c wal_level=logical -c max_wal_senders=10 -c track_commit_timestamp=on

  (leave out `track_commit_timestamp=on` for 9.4)

* In another session, in the test directory:

        PGPORT=5142 make

RUNNING JUST ONE TEST
---

To run just one test, specify the class-qualified method name.

    PGPORT=5142 python test/test_filter.py FilterTest.test_filter

WALSENDER VS SQL MODE
---

By default the tests use the SQL interface for logical decoding.

You can instead use the walsender interface, i.e. the streaming replication
protocol. However, this requires a patched psycopg2 at time of writing. You
can get the branch from https://github.com/zalando/psycopg2/tree/feature/replication-protocol

You should uninstall your existing `psycopg2` packages, then:

    git clone https://github.com/zalando/psycopg2.git
    git checkout feature/replication-protocol
    PATH=/path/to/pg/bin:$PATH python setup.py build
    sudo PATH=/path/to/pg/bin:$PATH python setup.py install

Now run the tests with the extra enviroment variable PGLOGICALTEST_USEWALSENDER=1
set, e.g.

    PGLOGICALTEST_USEWALSENDER=1 PGPORT=5142 make

At time of writing the walsender tests may not always be passing, as the
SQL tests are the authorative ones.

DETAILED LOGGING
---

You can get more detailed info about what's being done by setting the env var
`PGLOGICALTEST_LOGLEVEL=DEBUG`

TROUBLESHOOTING
---

No module named psycopg2
===

If you get an error like:

    ImportError: No module named psycopg2

you need to install `psycopg2` for your local Python install. It'll be
available as a package via the same channel you installed Python its self from.

could not access file "pglogical_output": No such file or directory
===

You forgot to install the output plugin before running the tests, or
the tests are connecting to a different PostgreSQL instance than the
one you installed the plugin in.
