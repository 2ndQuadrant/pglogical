import random
import string
import unittest
from base import PGLogicalOutputTest

class BasicTest(PGLogicalOutputTest):
    def rand_string(self, length):
        return ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(length)])

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE test_changes (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
        self.conn.commit()

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE test_changes;")
        self.conn.commit()
        PGLogicalOutputTest.tearDown(self)

    def test_changes(self):

        cur = self.conn.cursor()
        cur.execute("INSERT INTO test_changes(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'foobar'))
        cur.execute("INSERT INTO test_changes(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'bazbar'))
        self.conn.commit()

        cur.execute("DELETE FROM test_changes WHERE cola = 1")
        cur.execute("UPDATE test_changes SET colc = 'foobar' WHERE cola = 2")
        self.conn.commit()

        messages = self.get_changes()

        # CREATE TABLE produced empty TX
        m = messages.next()
        self.assertEqual(m.mesage_type, 'B')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'C')

        # two inserts in one tx
        m = messages.next()
        self.assertEqual(m.mesage_type, 'B')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'R')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'I')
        self.assertEqual(m.message['newtup'][2], 'foobar')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'R')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'I')
        self.assertEqual(m.message['newtup'][2], 'bazbar')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'C')

        # delete and update in one tx
        m = messages.next()
        self.assertEqual(m.mesage_type, 'B')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'R')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'D')
        self.assertEqual(m.message['keytup'][0], '1')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'R')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'U')
        self.assertEqual(m.message['newtup'][0], '2')
        self.assertEqual(m.message['newtup'][2], 'foobar')
        m = messages.next()
        self.assertEqual(m.mesage_type, 'C')

if __name__ == '__main__':
    unittest.main()
