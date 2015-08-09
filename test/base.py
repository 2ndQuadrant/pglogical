import unittest
import psycopg2

from pg_logical_proto import ReplicationMessage

SLOT_NAME = 'test'

class PGLogicalOutputTest(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect("dbname=postgres host=localhost")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, 'pg_logical_output')", (SLOT_NAME,))
        cur.close()
        self.conn.commit()

    def tearDown(self):
        self.conn.rollback()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM pg_drop_replication_slot(%s)", (SLOT_NAME,))
        cur.close()
        self.conn.commit()

    def get_changes(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM pg_logical_slot_get_binary_changes(%s, NULL, NULL, 'client_encoding', 'UTF8')", (SLOT_NAME,));
        for row in cur:
            m = ReplicationMessage(row)
            yield m
