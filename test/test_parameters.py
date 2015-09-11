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
            list(self.get_changes({'Max_proto_version': None}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Min_proto_version': None}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Min_proto_version': '2'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Max_proto_version': '0'}))

        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Max_proto_version': 'borkbork'}))

    def test_ignored_unknown_params(self):
        list(self.get_changes({'unknown_some_param': 'ignored'}))
        list(self.get_changes({'unknown.some_param': 'ignored'}))

    def test_required_unknown_params(self):
        # TODO Failure expected for now, needs fix
        # Should throw on this required parameter
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Unknown_required_parameter': 'error'}))

        # TODO Should ERROR
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Unknown.some_param': 'error'}))

    def test_encoding_missing(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Expected_encoding': None}))

    def test_encoding_bogus(self):
        with self.assertRaises(psycopg2.DatabaseError):
            list(self.get_changes({'Expected_encoding': 'gobblegobble'}))


if __name__ == '__main__':
    unittest.main()
