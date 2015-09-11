To run these tests:

* Create a temporary install:

        initdb -A trust -D tmp_install

* Start the install with:

        PGPORT=5142 postgres -D tmp_install -c max_replication_slots=5 -c wal_level=logical

* In another session, in the test directory:

        PGPORT=5142 make
