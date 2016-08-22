import random
import string
import unittest
import pprint
from base import PGLogicalOutputTest

class FilterTest(PGLogicalOutputTest):
    def rand_string(self, length):
        return ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(length)])

    def set_up(self):
        cur = self.conn.cursor()
        cur.execute("DROP EXTENSION IF EXISTS pglogical_output_plhooks;")
        cur.execute("DROP TABLE IF EXISTS test_changes, test_changes_filter;")
        cur.execute("DROP FUNCTION IF EXISTS test_filter(regclass, \"char\", text)")
        cur.execute("DROP FUNCTION IF EXISTS test_action_filter(regclass, \"char\", text)")
        cur.execute("CREATE TABLE test_changes (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
        cur.execute("CREATE TABLE test_changes_filter (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
        cur.execute("CREATE EXTENSION pglogical_output_plhooks;")


        # Filter function that filters out (removes) all changes
        # in tables named *_filter*
        cur.execute("""
            CREATE FUNCTION test_filter(relid regclass, action "char", nodeid text)
            returns bool stable language plpgsql AS $$
            BEGIN
                IF nodeid <> 'foo' THEN
                    RAISE EXCEPTION 'Expected nodeid <foo>, got <%>',nodeid;
                END IF;
                RETURN relid::regclass::text NOT LIKE '%_filter%';
            END
            $$;
            """)

        # function to filter out Deletes and Updates - Only Inserts pass through
        cur.execute("""
            CREATE FUNCTION test_action_filter(relid regclass, action "char", nodeid text)
            returns bool stable language plpgsql AS $$
            BEGIN
                RETURN action NOT IN ('U', 'D');
            END
            $$;
            """)

        self.conn.commit()
        self.connect_decoding()

    def tear_down(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE test_changes, test_changes_filter;")
        cur.execute("DROP FUNCTION test_filter(regclass, \"char\", text)");
        cur.execute("DROP FUNCTION test_action_filter(regclass, \"char\", text)");
        cur.execute("DROP EXTENSION pglogical_output_plhooks;")
        self.conn.commit()

    def exec_changes(self):
        """Execute a stream of changes we can process via various filters"""
        cur = self.conn.cursor()
        cur.execute("INSERT INTO test_changes(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'foobar'))
        cur.execute("INSERT INTO test_changes_filter(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'foobar'))
        cur.execute("INSERT INTO test_changes(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'bazbar'))
        self.conn.commit()

        cur.execute("INSERT INTO test_changes_filter(colb, colc) VALUES(%s, %s)", ('2015-08-08', 'bazbar'))
        self.conn.commit()

        cur.execute("UPDATE test_changes set colc = 'oobar' where cola=1")
        cur.execute("UPDATE test_changes_filter set colc = 'oobar' where cola=1")
        self.conn.commit()

        cur.execute("DELETE FROM test_changes where cola=2")
        cur.execute("DELETE FROM test_changes_filter where cola=2")
        self.conn.commit()


    def test_filter(self):
        self.exec_changes();

        params = {
                'hooks.setup_function': 'public.pglo_plhooks_setup_fn',
                'pglo_plhooks.row_filter_hook': 'public.test_filter',
                'pglo_plhooks.client_hook_arg': 'foo'
                }

        messages = self.get_changes(params)

        (m, params) = messages.expect_startup()

        self.assertIn('hooks.row_filter_enabled', m.message['params'])
        self.assertEquals(m.message['params']['hooks.row_filter_enabled'], 't')

        # two inserts into test_changes, the test_changes_filter insert is filtered out
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'foobar\0')
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'bazbar\0')
        messages.expect_commit()

        # just an empty tx as the  test_changes_filter insert is filtered out
        messages.expect_begin()
        messages.expect_commit()

        # 1 update each into test_changes and test_changes_filter
        # update of test_changes_filter is filtered out
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_update()
        self.assertEqual(m.message['newtup'][0], '1\0')
        self.assertEqual(m.message['newtup'][2], 'oobar\0')
        messages.expect_commit()

        # 1 delete each into test_changes and test_changes_filter
        # delete of test_changes_filter is filtered out
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_delete()
        self.assertEqual(m.message['keytup'][0], '2\0')
        messages.expect_commit()

    def test_action_filter(self):
        self.exec_changes();

        params = {
                'hooks.setup_function': 'public.pglo_plhooks_setup_fn',
                'pglo_plhooks.row_filter_hook': 'public.test_action_filter'
                }

        messages = self.get_changes(params)

        (m, params) = messages.expect_startup()

        self.assertIn('hooks.row_filter_enabled', params)
        self.assertEquals(params['hooks.row_filter_enabled'], 't')

        # two inserts into test_changes, the test_changes_filter insert is filtered out
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'foobar\0')
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'foobar\0')
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'bazbar\0')
        messages.expect_commit()

        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][2], 'bazbar\0')
        messages.expect_commit()

        # just empty tx as updates are filtered out
        messages.expect_begin()
        messages.expect_commit()

        # just empty tx as deletes are filtered out
        messages.expect_begin()
        messages.expect_commit()

#       def test_hooks(self):
#           params = {
#                   'hooks.setup_function', 'public.pglo_plhooks_setup_fn',
#                   'pglo_plhooks.startup_hook', 'pglo_plhooks_demo_startup',
#                   'pglo_plhooks.row_filter_hook', 'pglo_plhooks_demo_row_filter',
#                   'pglo_plhooks.txn_filter_hook', 'pglo_plhooks_demo_txn_filter',
#                   'pglo_plhooks.shutdown_hook', 'pglo_plhooks_demo_shutdown',
#                   'pglo_plhooks.client_hook_arg', 'test data'
#                   }
#
    def test_validation(self):
        with self.assertRaises(Exception):
            self.get_changes({'hooks.row_filter': 'public.foobar'}).next()


if __name__ == '__main__':
    unittest.main()
