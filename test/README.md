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

        PGPORT=5142 postgres -D tmp_install -c max_replication_slots=5 -c wal_level=logical -c max_wal_senders=10

* In another session, in the test directory:

        PGPORT=5142 make


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
