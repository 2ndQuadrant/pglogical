import random
import string
import unittest
from base import PGLogicalOutputTest
import psycopg2

class ParametersTest(PGLogicalOutputTest):

    def setUp(self):
        PGLogicalOutputTest.setUp(self)

    def tearDown(self):
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
        # Should all get ignored
        list(self.get_changes({'unknown_required_parameter': 'unknown'}))
        list(self.get_changes({'unknown.some_param': 'unknown'}))
        list(self.get_changes({'Unknown.some_param': 'unknown'}))
        list(self.get_changes({'Unknown.Some_param': 'unknown'}))

    def test_encoding_missing(self):
        # Should be ignored, server should send reply params
        list(self.get_changes({'expected_encoding': None}))

    def test_encoding_bogus(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'expected_encoding': 'gobblegobble'}))

    def test_encoding_differs(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'expected_encoding': 'LATIN-1'}))


if __name__ == '__main__':
    unittest.main()
