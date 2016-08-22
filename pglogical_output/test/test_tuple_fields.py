import random
import string
import unittest
from pglogical_proto import UnchangedField
from base import PGLogicalOutputTest

class TupleFieldsTest(PGLogicalOutputTest):

    def setUp(self):
        PGLogicalOutputTest.setUp(self)
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_tuplen;")
	#teardown after test
        cur.execute("DROP TABLE IF EXISTS test_text;")
        cur.execute("DROP TABLE IF EXISTS test_binary;")
        cur.execute("DROP TABLE IF EXISTS toasttest;")

        cur.execute("CREATE TABLE test_tuplen (cola serial PRIMARY KEY, colb timestamptz default now(), colc text);")
	cur.execute("CREATE TABLE toasttest(descr text, cnt int DEFAULT 0, f1 text, f2 text);")
        cur.execute("CREATE TABLE test_text (cola text, colb text);")
        cur.execute("CREATE TABLE test_binary (colv bytea);")

        self.conn.commit()
        self.connect_decoding()

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_tuplen;")
	#teardown after test
        cur.execute("DROP TABLE IF EXISTS test_text;")
        cur.execute("DROP TABLE IF EXISTS test_binary;")
        cur.execute("DROP TABLE toasttest;")
        self.conn.commit()

        PGLogicalOutputTest.tearDown(self)

    def test_null_tuple_field(self):
        """Make sure null in tuple fields is sent as a 'n' row-value message"""
        cur = self.conn.cursor()

        cur.execute("INSERT INTO test_tuplen(colb, colc) VALUES('2015-08-08', null)")
        # testing 'n'ull fields
        self.conn.commit()

        messages = self.get_changes()

        messages.expect_startup()

        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        # 'n'ull is reported as None by the test tuple reader
        self.assertEqual(m.message['newtup'][2], None)
        messages.expect_commit()

    def test_unchanged_toasted_tuple_field(self):
        """
        Large TOASTed fields are sent as 'u'nchanged if they're from an UPDATE
        and the UPDATE didn't change the TOASTed field, just other fields in the
        same tuple.
        """

        # TODO: A future version should let us force unpacking of TOASTed fields,
        # see bug #19

        cur = self.conn.cursor()

        cur.execute("INSERT INTO toasttest(descr, f1, f2) VALUES('one-toasted', repeat('1234567890',30000), 'atext');")
        self.conn.commit()

        # testing 'u'nchanged tuples
        cur.execute("UPDATE toasttest SET cnt = 2 WHERE descr = 'one-toasted'")
        self.conn.commit()

        # but make sure they're replicated when actually changed
        cur.execute("UPDATE toasttest SET cnt = 3, f1 = repeat('0987654321',25000) WHERE descr = 'one-toasted';")
        self.conn.commit()


        messages = self.get_changes()

        # Startup msg
        messages.expect_startup()

        # consume the insert
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][0], 'one-toasted\0')
        self.assertEqual(m.message['newtup'][1], '0\0') # default of cnt field
        self.assertEqual(m.message['newtup'][2], '1234567890'*30000 + '\0')
        self.assertEqual(m.message['newtup'][3], 'atext\0')
        messages.expect_commit()


        # First UPDATE
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_update()
        self.assertEqual(m.message['newtup'][0], 'one-toasted\0')
        self.assertEqual(m.message['newtup'][1], '2\0')
        # The big value is TOASTed, and since we didn't change it, it's sent
        # as an unchanged field marker.
        self.assertIsInstance(m.message['newtup'][2], UnchangedField)
        # While unchanged, 'atext' is small enough that it's not stored out of line
        # so it's written despite being unchanged.
        self.assertEqual(m.message['newtup'][3], 'atext\0')
        messages.expect_commit()



        # Second UPDATE
        messages.expect_begin()
        messages.expect_row_meta()
        m = messages.expect_update()
        self.assertEqual(m.message['newtup'][0], 'one-toasted\0')
        self.assertEqual(m.message['newtup'][1], '3\0')
        # this time we changed the TOASTed field, so it's been sent
        self.assertEqual(m.message['newtup'][2], '0987654321'*25000 + '\0')
        self.assertEqual(m.message['newtup'][3], 'atext\0')
        messages.expect_commit()



    def test_default_modes(self):
        cur = self.conn.cursor()

        cur.execute("INSERT INTO test_text(cola, colb) VALUES('sample1', E'sam\\160le2\\n')")
        cur.execute("INSERT INTO test_binary values (decode('ff','hex'));")
        self.conn.commit()

        messages = self.get_changes()

        messages.expect_startup()

        # consume the insert
        messages.expect_begin()
        messages.expect_row_meta()

        # The values of the two text fields will be the original text,
        # returned unchanged, but the escapes in the second one will
        # have been decoded, so it'll have a literal newline in it
        # and the octal escape decoded.
        m = messages.expect_insert()
        self.assertEqual(m.message['newtup'][0], 'sample1\0')
        self.assertEqual(m.message['newtup'][1], 'sample2\n\0')

        messages.expect_row_meta()
        m = messages.expect_insert()

	# While this is a bytea field, we didn't negotiate binary or send/recv
        # mode with the server, so what we'll receive is the hex-encoded text
        # representation of the bytea value as as text-format literal.
        self.assertEqual(m.message['newtup'][0], '\\xff\0')

        messages.expect_commit()

if __name__ == '__main__':
    unittest.main()
