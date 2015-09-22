QUICK START
---

To run these tests:

* Create a temporary install at any location of your choosing:

        initdb -A trust -D tmp_install

* Start the install with:

        PGPORT=5142 postgres -D tmp_install -c max_replication_slots=5 -c wal_level=logical

* In another session, in the test directory:

        PGPORT=5142 make


TROUBLESHOOTING
---

If you get an error like:

    ImportError: No module named psycopg2

you need to install `psycopg2` for your local Python install. It'll be
available as a package via the same channel you installed Python its self from.
