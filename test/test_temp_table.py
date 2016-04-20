import random
import string
import unittest
from base import PGLogicalOutputTest

class TempTableTest(PGLogicalOutputTest):

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        cur = self.conn.cursor()
        cur.execute("CREATE TEMP TABLE test_temp (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
        self.conn.commit()
        self.connect_decoding()

    def tearDown(self):
        PGLogicalOutputTest.tearDown(self)

    def test_temp_changes(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO test_temp(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'foobar'))
        cur.execute("INSERT INTO test_temp(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'bazbar'))
        self.conn.commit()

        cur.execute("DELETE FROM test_temp WHERE cola = 1")
        cur.execute("UPDATE test_temp SET colc = 'foobar' WHERE cola = 2")
        self.conn.commit()

        messages = self.get_changes()
        # verify - no startup message seen
        with self.assertRaises(StopIteration):
            messages.expect_startup()
        # verify - no ReplicationMessage for TEMP table
        with self.assertRaises(StopIteration):
            messages.message_generator.next()

if __name__ == '__main__':
    unittest.main()
