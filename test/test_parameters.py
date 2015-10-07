import random
import string
import unittest
from base import PGLogicalOutputTest
import psycopg2

class ParametersTest(PGLogicalOutputTest):

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        self.cur.execute("DROP TABLE IF EXISTS blah;")
        self.cur.execute("CREATE TABLE blah(id integer);")
        self.conn.commit()
        self.connect_decoding()

    def tearDown(self):
        self.cur.execute("DROP TABLE blah;")
        self.conn.commit()
        PGLogicalOutputTest.tearDown(self)

    def test_protoversion(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'startup_params_format': 'borkbork'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'startup_params_format': '2'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'startup_params_format': None}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'max_proto_version': None}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'min_proto_version': None}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'min_proto_version': '2'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'max_proto_version': '0'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'max_proto_version': 'borkbork'}))

    def test_unknown_params(self):
        # Should get ignored
        self.do_dummy_tx()
        self.get_startup_msg(self.get_changes({'unknown_parameter': 'unknown'}))

    def test_unknown_params(self):
        # Should get ignored
        self.do_dummy_tx()
        self.get_startup_msg(self.get_changes({'unknown.some_param': 'unknown'}))

    def test_encoding_missing(self):
        # Should be ignored, server should send reply params
        messages = self.get_changes({'expected_encoding': None})

    def test_encoding_bogus(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'expected_encoding': 'gobblegobble'}))

    def test_encoding_differs(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'expected_encoding': 'LATIN-1'}))

    def do_dummy_tx(self):
        """force a dummy tx so there's something to decode"""
        self.cur.execute("INSERT INTO blah(id) VALUES (1)")
        self.conn.commit()

    def get_startup_msg(self, messages):
        """Read and return the startup message"""
        m = messages.next()
        self.assertEqual(m.message_type, 'S')
        return m

if __name__ == '__main__':
    unittest.main()
