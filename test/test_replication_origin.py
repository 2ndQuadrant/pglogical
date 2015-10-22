import random
import string
import unittest
from base import PGLogicalOutputTest

class ReplicationOriginTest(PGLogicalOutputTest):
    """
    Tests for handling of changeset forwarding and replication origins.

    These tests have to deal with a bit of a wrinkle: if the decoding plugin
    is running in PostgreSQL 9.4 the lack of replication origins support means
    that we cannot send replication origin information, and we always forward
    transactions from other peer nodes. So it produces output you can't get
    from 9.5+: all changesets forwarded, but without origin information.

    9.4 also lacks the functions for setting up replication origins, so we
    have to special-case that.
    """

    fake_upstream_origin_name = "pglogical_test_fake_upstream";
    fake_xact_lsn = '14/abcdef0'
    fake_xact_timestamp = '2015-10-08 12:13:14.1234'

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_origin;")
        if self.conn.server_version/100 != 904:
            cur.execute("""
                SELECT pg_replication_origin_drop(%s)
                FROM pg_replication_origin
                WHERE roname = %s;
                """,
                (self.fake_upstream_origin_name, self.fake_upstream_origin_name))
        self.conn.commit()

        cur.execute("CREATE TABLE test_origin (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
        self.conn.commit()

        self.is95plus = self.conn.server_version/100 > 904

        if self.is95plus:
            # Create the replication origin for the fake remote node
            cur.execute("SELECT pg_replication_origin_create(%s);", (self.fake_upstream_origin_name,))
            self.conn.commit()

            # Ensure that commit timestamps are enabled.
            cur.execute("SHOW track_commit_timestamp");
            if cur.fetchone()[0] != 'on':
                raise ValueError("This test requires track_commit_timestamp to be on")
            self.conn.commit()

        self.connect_decoding()

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_origin;")
        self.conn.commit()
        self.teardown_replication_session_origin(cur);
        self.conn.commit()
        PGLogicalOutputTest.tearDown(self)

    def setup_replication_session_origin(self, cur):
        """Sets session-level replication origin info. Ignored on 9.4."""
        if self.conn.get_transaction_status() != 0:
            raise ValueError("Transaction open or aborted, expected no open transaction")

        if self.is95plus:
            # Set our session up so it appears to be replaying from the nonexistent remote node
            cur.execute("SELECT pg_replication_origin_session_setup(%s);", (self.fake_upstream_origin_name,))
            self.conn.commit()

    def setup_xact_origin(self, cur, origin_lsn, origin_commit_timestamp):
        """Sets transaction-level replication origin info. Ignored on 9.4. Implicitly begins a tx."""
        if self.conn.get_transaction_status() != 0:
            raise ValueError("Transaction open or aborted, expected no open transaction")

        if self.is95plus:
            # Run transactions that seem to come from the remote node
            cur.execute("SELECT pg_replication_origin_xact_setup(%s, %s);", (origin_lsn, origin_commit_timestamp))

    def reset_replication_session_origin(self, cur):
        """
        Reset session's replication origin setup to defaults.

        Always executes an empty transaction on 9.5+; does nothing
        on 9.4.
        """
        if self.conn.get_transaction_status() != 0:
            raise ValueError("Transaction open or aborted, expected no open transaction")
        if self.is95plus:
            cur.execute("SELECT pg_replication_origin_session_reset();")
            self.conn.commit()

    def teardown_replication_session_origin(self, cur):
        if self.conn.get_transaction_status() != 0:
            raise ValueError("Transaction open or aborted, expected no open transaction")
        if self.is95plus:
            cur.execute("SELECT pg_replication_origin_session_is_setup()")
            if cur.fetchone()[0] == 't':
                cur.execute("SELECT pg_replication_origin_session_reset();")
            self.conn.commit()
            cur.execute("SELECT pg_replication_origin_drop(%s);", (self.fake_upstream_origin_name,))
            self.conn.commit()

    def expect_origin_progress(self, cur, lsn):
        if self.is95plus:
            initialtxstate = self.conn.get_transaction_status()
            if initialtxstate not in (0,2):
                raise ValueError("Expected open valid tx or no tx")
            cur.execute("SELECT local_id, external_id, remote_lsn FROM pg_show_replication_origin_status()")
            if lsn is not None:
                (local_id, external_id, remote_lsn) = cur.fetchone()
                self.assertEquals(local_id, 1)
                self.assertEquals(external_id, self.fake_upstream_origin_name)
                self.assertEquals(remote_lsn.lower(), lsn.lower())
            self.assertIsNone(cur.fetchone(), msg="Expected only one replication origin to exist")
            if initialtxstate == 0:
                self.conn.commit()

    def run_test_transactions(self, cur):
        """
        Run a set of transactions with and without a replication origin set.

        This simulates a mix of local transactions and remotely-originated
        transactions being applied by a pglogical downstream or some other
        replication-origin aware replication agent.

        On 9.5+ the with-origin transaction simulates what the apply side of a
        logical replication downstream would do by setting replication origin
        information on the session and transaction. So to the server it's just
        like this transaction was forwarded from another node.

        On 9.4 it runs like a locally originated tx because 9.4 lacks origins
        support.

        All the tests will decode the same series of transactions, but with
        different connection settings.
        """

        self.expect_origin_progress(cur, None)

        # Some locally originated tx's for data we'll then modify
        cur.execute("INSERT INTO test_origin(colb, colc) VALUES(%s, %s)", ('2015-10-08', 'foobar'))
        self.assertEquals(cur.rowcount, 1)
        cur.execute("INSERT INTO test_origin(colb, colc) VALUES(%s, %s)", ('2015-10-08', 'bazbar'))
        self.assertEquals(cur.rowcount, 1)
        self.conn.commit()

        self.expect_origin_progress(cur, None)

        # Now the fake remotely-originated tx
        self.setup_replication_session_origin(cur)
        self.setup_xact_origin(cur, self.fake_xact_lsn, self.fake_xact_timestamp)
        # Some remotely originated inserts
        cur.execute("INSERT INTO test_origin(colb, colc) VALUES(%s, %s)", ('2016-10-08', 'fakeor'))
        self.assertEquals(cur.rowcount, 1)
        cur.execute("INSERT INTO test_origin(colb, colc) VALUES(%s, %s)", ('2016-10-08', 'igin'))
        self.assertEquals(cur.rowcount, 1)
        # Delete a tuple that was inserted locally
        cur.execute("DELETE FROM test_origin WHERE colb = '2015-10-08' and colc = 'foobar'")
        self.assertEquals(cur.rowcount, 1)
        # modify a tuple that was inserted locally
        cur.execute("UPDATE test_origin SET colb = '2016-10-08' where colc = 'bazbar'")
        self.assertEquals(cur.rowcount, 1)
        self.conn.commit()

        self.expect_origin_progress(cur, self.fake_xact_lsn)

        # Reset replication origin to return to locally originated tx's
        self.reset_replication_session_origin(cur)

        self.expect_origin_progress(cur, self.fake_xact_lsn)

        # and finally use a local tx to modify remotely originated transactions
        # Delete and modify remotely originated tuples
        cur.execute("DELETE FROM test_origin WHERE colc = 'fakeor'")
        self.assertEquals(cur.rowcount, 1)
        cur.execute("UPDATE test_origin SET colb = '2015-10-08' WHERE colc = 'igin'")
        self.assertEquals(cur.rowcount, 1)
        # and insert a new row mainly to verify that the origin reset was respected
        cur.execute("INSERT INTO test_origin(colb, colc) VALUES (%s, %s)", ('2017-10-08', 'blahblah'))
        self.assertEquals(cur.rowcount, 1)
        self.conn.commit()

        self.expect_origin_progress(cur, self.fake_xact_lsn)

    def decode_test_transactions(self, messages, expect_origins, expect_forwarding):
        """
        Decode the transactions from run_test_transactions, varying the
        expected output based on whether we've been told we should be getting
        origin messages, and whether we should be getting forwarded transaction
        data.

        This intentionally doesn't use maybe_expect_origin() to make sure it's
        testing what the unit test specifies, not what the server sent in the
        startup message.
        """
        # two inserts in one locally originated tx. No forwarding. Local tx's
        # never get origins.
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        messages.expect_row_meta()
        m = messages.expect_insert()
        messages.expect_commit()

        # The remotely originated transaction is still replayed when forwarding
        # is off, but on 9.5+ the data from it is omitted.
        #
        # An origin message will be received only if on 9.5+.
        if expect_forwarding:
            messages.expect_begin()
            if expect_origins:
                messages.expect_origin()
            # 9.4 forwards unconditionally
            m = messages.expect_row_meta()
            m = messages.expect_insert()
            m = messages.expect_row_meta()
            m = messages.expect_insert()
            m = messages.expect_row_meta()
            m = messages.expect_delete()
            m = messages.expect_row_meta()
            m = messages.expect_update()
            messages.expect_commit()

        # The second locally originated tx modifies the remotely
        # originated tuples. It's locally originated so no origin
        # message is sent.
        messages.expect_begin()
        m = messages.expect_row_meta()
        m = messages.expect_delete()
        m = messages.expect_row_meta()
        m = messages.expect_update()
        m = messages.expect_row_meta()
        m = messages.expect_insert()
        messages.expect_commit()

    def test_forwarding_not_requested_95plus(self):
        """
        For this test case we don't ask for forwarding to be enabled.

        For 9.5+ we should get only transactions originated locally, so any transaction
        with an origin set will be ignored. No origin info messages will be sent.
        """
        cur = self.conn.cursor()

        if not self.is95plus:
            self.skipTest("Cannot run forwarding-off origins-off test on a PostgreSQL 9.4 server")

        self.run_test_transactions(cur)

        # Forwarding not requested.
        messages = self.get_changes({'forward_changesets':'f'})

        # Startup msg
        (m, params) = messages.expect_startup()

        # We should not get origins, ever
        self.assertEquals(params['forward_changeset_origins'], 'f')
        # Changeset forwarding off respected by 9.5+
        self.assertEquals(params['forward_changesets'], 'f')

        # decode, expecting no origins
        self.decode_test_transactions(messages, False, False)

    # Upstream doesn't send origin correctly yet
    @unittest.skip("Doesn't work yet")
    def test_forwarding_requested_95plus(self):
        """
        In this test we request that forwarding be enabled. We'll get
        forwarded transactions and origin messages for them.
        """
        cur = self.conn.cursor()

        if not self.is95plus:
            self.skipTest("Cannot run forwarding-on with-origins test on a PostgreSQL 9.4 server")

        self.run_test_transactions(cur)

        #client requests to forward changesets
        messages = self.get_changes({'forward_changesets': 't'})

        # Startup msg
        (m, params) = messages.expect_startup()

        # Changeset forwarding is forced on by 9.4 and was requested
        # for 9.5+ so should always be on.
        self.assertEquals(params['forward_changesets'], 't')
        # 9.5+ will always forward origins if cset forwarding is
        # requested.
        self.assertEquals(params['forward_changeset_origins'], 't')

        # Decode, expecting forwarding, and expecting origins unless 9.4
        self.decode_test_transactions(messages, self.is95plus, True)

    def test_forwarding_not_requested_94(self):
        """
        For this test case we don't ask for forwarding to be enabled.

        For 9.4, we should get all transactions, even those that were originated "remotely".
        9.4 doesn't support replication identifiers so we couldn't tell the server that the
        tx's were applied from a remote node, and it'd have to way to store that info anyway.
        """
        cur = self.conn.cursor()

        if self.is95plus:
            self.skipTest("9.4-specific test doesn't make sense on this server version")

        self.run_test_transactions(cur)

        # Forwarding not requested.
        messages = self.get_changes({'forward_changesets':'f'})

        # Startup msg
        (m, params) = messages.expect_startup()

        # We should not get origins, ever
        self.assertEquals(params['forward_changeset_origins'], 'f')
        # Changeset forwarding is forced on by 9.4
        self.assertEquals(params['forward_changesets'], 't')

        # decode, expecting no origins, and forwarding
        self.decode_test_transactions(messages, False, True)

if __name__ == '__main__':
    unittest.main()
