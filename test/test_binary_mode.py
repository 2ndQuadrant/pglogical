import random
import string
import unittest
from base import PGLogicalOutputTest

class BinaryModeTest(PGLogicalOutputTest):

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_binary;")
        cur.execute("CREATE TABLE test_binary (colv bytea, colts timestamp);")
        self.conn.commit()
        self.connect_decoding()

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_binary;")
        self.conn.commit()
        PGLogicalOutputTest.tearDown(self)

    def probe_for_server_params(self):
        cur = self.conn.cursor()

        # Execute a dummy transaction so we have something to decode
        cur.execute("INSERT INTO test_binary values (decode('ff','hex'), NULL);")
        self.conn.commit()

        # Make a connection to decode the dummy tx. We're just doing this
        # so we can capture the startup response from the server, then
        # we'll disconnect and reconnect with the binary settings captured
        # from the server to ensure we make a request that'll get binary
        # mode enabled.
        messages = self.get_changes()

        (m, params) = messages.expect_startup()

        # Check we got everything we expected from the startup params
        expected_params = ['pg_version_num', 'binary.bigendian',
                'binary.sizeof_datum', 'binary.sizeof_int',
                'binary.sizeof_long', 'binary.float4_byval',
                'binary.float8_byval', 'binary.integer_datetimes',
                'binary.internal_basetypes', 'binary.binary_basetypes',
                'binary.basetypes_major_version']
        for pn in expected_params:
            self.assertTrue(pn in params, msg="Expected startup msg param binary.basetypes_major_version absent")

        self.assertEquals(int(params['pg_version_num'])/100,
                int(params['binary.basetypes_major_version']),
                msg="pg_version_num/100 <> binary.basetypes_major_version")

        # We didn't ask for it, so binary and send/recv must be disabled
        self.assertEquals(params['binary.internal_basetypes'], 'f')
        self.assertEquals(params['binary.binary_basetypes'], 'f')

        # Read and discard the fields of our dummy tx
        messages.expect_begin()
        messages.expect_row_meta()
        messages.expect_insert()
        messages.expect_commit()

        cur.close()

        # We have to disconnect and reconnect if using walsender so that
        # we can start a new replication session
        self.logger.debug("before: Interface is %s", self.interface)
        self.reconnect_decoding()
        self.logger.debug("after: Interface is %s", self.interface)

        return params

    def test_binary_mode(self):
        params = self.probe_for_server_params()
        major_version = int(params['pg_version_num'])/100

        cur = self.conn.cursor()

        # Now that we know the server's parameters, do another transaction and
        # decode it in binary mode using the format we know the server speaks.
        cur.execute("INSERT INTO test_binary values (decode('aa','hex'), TIMESTAMP '2000-01-02 12:34:56');")
        self.conn.commit()

        messages = self.get_changes({
            'binary.want_internal_basetypes': 't',
            'binary.want_binary_basetypes': 't',
            'binary.basetypes_major_version': str(major_version),
            'binary.bigendian' : params['binary.bigendian'],
            'binary.sizeof_datum' : params['binary.sizeof_datum'],
            'binary.sizeof_int' : params['binary.sizeof_int'],
            'binary.sizeof_long' : params['binary.sizeof_long'],
            'binary.float4_byval' : params['binary.float4_byval'],
            'binary.float8_byval' : params['binary.float8_byval'],
            'binary.integer_datetimes' : params['binary.integer_datetimes']
            })

        # Decode the startup message
        (m, params) = messages.expect_startup()
        # Binary mode should be enabled since we sent the params the server wants
        self.assertEquals(params['binary.internal_basetypes'], 't')
        # send/recv mode is implied by binary mode
        self.assertEquals(params['binary.binary_basetypes'], 't')

        # Decode the transaction we sent
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()

        # and verify that the message fields are in the expected binary representation
        self.assertEqual(m.message['newtup'][0], '\x05\xaa')
        # FIXME this is probably wrong on bigendian
        self.assertEqual(m.message['newtup'][1], '\x00\x7c\xb1\xa9\x1e\x00\x00\x00')

        messages.expect_commit()

    def test_sendrecv_mode(self):
        params = self.probe_for_server_params()
        major_version = int(params['pg_version_num'])/100

        cur = self.conn.cursor()

        # Now that we know the server's parameters, do another transaction and
        # decode it in binary mode using the format we know the server speaks.
        cur.execute("INSERT INTO test_binary values (decode('aa','hex'), TIMESTAMP '2000-01-02 12:34:56');")
        self.conn.commit()

        # Send options that don't match the server's binary mode, so it falls
        # back to send/recv even though we requested binary too.
        if int(params['binary.sizeof_long']) == 8:
            want_sizeof_long = 4
        elif int(params['binary.sizeof_long']) == 4:
            want_sizeof_long = 8
        else:
            self.fail("What platform has sizeof(long) == %s anyway?" % params['binary.sizeof_long'])

        messages = self.get_changes({
            # Request binary even though we know we won't get it
            'binary.want_internal_basetypes': 't',
            # and expect to fall back to send/recv
            'binary.want_binary_basetypes': 't',
            'binary.basetypes_major_version': str(major_version),
            'binary.bigendian' : params['binary.bigendian'],
            'binary.sizeof_datum' : params['binary.sizeof_datum'],
            'binary.sizeof_int' : params['binary.sizeof_int'],
            'binary.sizeof_long' : want_sizeof_long,
            'binary.float4_byval' : params['binary.float4_byval'],
            'binary.float8_byval' : params['binary.float8_byval'],
            'binary.integer_datetimes' : params['binary.integer_datetimes']
            })

        # Decode the startup message
        (m, params) = messages.expect_startup()
        # Binary mode should be disabled because we aren't compatible
        self.assertEquals(params['binary.internal_basetypes'], 'f')
        # send/recv mode should be on, since we're compatible with the same
        # major version.
        self.assertEquals(params['binary.binary_basetypes'], 't')

        # Decode the transaction we sent
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()

        # and verify that the message fields are in the expected send/recv representation
        # The text field lacks the length prefix
        self.assertEqual(m.message['newtup'][0], '\xaa')
        # and the timestamp is in network byte order
        self.assertEqual(m.message['newtup'][1], '\x00\x00\x00\x1e\xa9\xb1\x7c\x00')

        messages.expect_commit()

if __name__ == '__main__':
    unittest.main()
