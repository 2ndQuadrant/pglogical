import unittest
import psycopg2

from pg_logical_proto import ReplicationMessage

SLOT_NAME = 'test'

class PGLogicalOutputTest(unittest.TestCase):

    conn = None

    def setUp(self):
        self.conn = psycopg2.connect("dbname=postgres host=localhost")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, 'pg_logical_output')", (SLOT_NAME,))
        cur.close()
        self.conn.commit()

        if hasattr(self, 'set_up'):
            self.set_up()

    def tearDown(self):
        if hasattr(self, 'tear_down'):
            self.tear_down()

    def doCleanups(self):
        if (self.conn == None):
            return

        self.conn.rollback()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM pg_drop_replication_slot(%s)", (SLOT_NAME,))
        cur.close()
        self.conn.commit()

    def get_changes(self, kwargs = {}):
        cur = self.conn.cursor()
        params_dict = {
                'Expected_encoding': 'UTF8',
                'Min_proto_version': '1',
                'Max_proto_version': '1'
                }
        params_dict.update(kwargs)
        params = [i for k, v in params_dict.items() for i in [k,v]]
        try:
            cur.execute("SELECT * FROM pg_logical_slot_get_binary_changes(%s, NULL, NULL" + (", %s" * len(params)) + ")",
                    [SLOT_NAME] + params);
        finally:
            self.conn.commit()
        for row in cur:
            m = ReplicationMessage(row)
            yield m
