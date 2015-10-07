import unittest
import psycopg2
import psycopg2.extras;
import cStringIO
import logging
import pprint
import psycopg2.extensions
import select
import time
import sys
import os
from pglogical_protoreader import ProtocolReader

from pglogical_proto import ReplicationMessage

SLOT_NAME = 'test'

class BaseDecodingInterface(object):
    """Helper for handling the different decoding interfaces"""

    conn = None
    cur = None

    def __init__(self, connstring, logger):
        # Establish base connection, which we use in walsender mode too
        self.logger = logger
        self.connstring = connstring
        self.conn = psycopg2.connect(self.connstring)
        self.logger.debug("Acquired connection with pid %s", self.conn.get_backend_pid())
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def slot_exists(self):
        self.cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT_NAME,))
        return self.cur.rowcount == 1

    def drop_slot_when_inactive(self):
        self.logger.debug("Dropping slot %s", SLOT_NAME)
        try:
            # We can't use the walsender protocol connection to drop
            # the slot because we have no way to exit COPY BOTH mode
            # so close the connection (above) and drop from SQL.
            if self.cur is not None:
                # There's a race between walsender disconnect and the slot becoming
                # free. We should use a DO block, but this will do for now.
                #
                # this is only an issue in walsender mode, but might as well do
                # it anyway.
                self.cur.execute("""
                DO
                LANGUAGE plpgsql
                $$
                DECLARE
                    timeleft float := 5.0;
                    _slotname name := %s;
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = _slotname)
                    THEN
                        RETURN;
                    END IF;
                    WHILE (SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = _slotname) AND timeleft > 0
                    LOOP
                        PERFORM pg_sleep(0.1);
                        timeleft := timeleft - 0.1;
                    END LOOP;
                    IF timeleft > 0 THEN
                        PERFORM pg_drop_replication_slot(_slotname);
                    ELSE
                        RAISE EXCEPTION 'Timed out waiting for slot to become unused';
                    END IF;
                END;
                $$
                """, (SLOT_NAME,))
        except psycopg2.ProgrammingError, ex:
            self.logger.exception("Attempt to DROP slot %s failed", SLOT_NAME)
        self.logger.debug("Dropped slot %s", SLOT_NAME)

    def cleanup(self):
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()

    def _get_changes_params(self, kwargs):
        params_dict = {
                'expected_encoding': 'UTF8',
                'min_proto_version': '1',
                'max_proto_version': '1',
                'startup_params_format': '1'
                }
        params_dict.update(kwargs)
        return params_dict



class SQLDecodingInterface(BaseDecodingInterface):
    """Use the SQL level logical decoding interfaces"""

    def __init__(self, connstring, parentlogger=logging.getLogger('base')):
        BaseDecodingInterface.__init__(self, connstring, logger=parentlogger.getChild('sqldecoding:%s' % hex(id(self))))

        # cleanup old slot
        if self.slot_exists():
            self.cur.execute("SELECT * FROM pg_drop_replication_slot(%s)", (SLOT_NAME,))

        # Create slot to use in testing
        self.cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, 'pglogical_output')", (SLOT_NAME,))

    def cleanup(self):
        self.logger.debug("Closing sql decoding connection")
        self.drop_slot_when_inactive()
        BaseDecodingInterface.cleanup(self)
        self.logger.debug("Closed sql decoding connection")

    def get_changes(self, kwargs = {}):
        params_dict = self._get_changes_params(kwargs)

        # Filter out entries with value None
        params = [i for k, v in params_dict.items() for i in [k,str(v)] if v is not None]
        # and create the slot
        try:
            self.cur.execute("SELECT * FROM pg_logical_slot_get_binary_changes(%s, NULL, NULL" + (", %s" * len(params)) + ")",
                    [SLOT_NAME] + params);
        finally:
            self.conn.commit()

        for row in self.cur:
            yield ReplicationMessage(row)



class WalsenderDecodingInterface(BaseDecodingInterface):
    """Use the replication protocol interfaces"""

    walcur = None
    walconn = None
    select_timeout = 1
    replication_started = False

    def __init__(self, connstring, parentlogger=logging.getLogger('base')):
        BaseDecodingInterface.__init__(self, connstring, logger=parentlogger.getChild('waldecoding:%s' % hex(id(self))))

        # Establish an async logical replication connection
        self.walconn = psycopg2.connect(self.connstring,
                connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.logger.debug("Acquired replication connection with pid %s", self.walconn.get_backend_pid())
        self.walcur = self.walconn.cursor()

        # clean up old slot
        if self.slot_exists():
                self.walcur.drop_replication_slot(SLOT_NAME)

        # Create slot to use in testing
        self.walcur.create_replication_slot(SLOT_NAME, output_plugin='pglogical_output')
        slotinfo = self.walcur.fetchone()
        self.logger.debug("Got slot info %s", slotinfo)


    def cleanup(self):
        self.logger.debug("Closing walsender connection")

        if self.walcur is not None:
            self.walcur.close()
        if self.walconn is not None:
            self.walconn.close()

        self.replication_started = False

        self.drop_slot_when_inactive()
        BaseDecodingInterface.cleanup(self)
        self.logger.debug("Closed walsender connection")

    def get_changes(self, kwargs = {}):
        params_dict = self._get_changes_params(kwargs)

        if not self.replication_started:
            self.walcur.start_replication(slot_name=SLOT_NAME,
                    options={k: v for k, v in params_dict.iteritems() if v is not None})
            self.replication_started = True
        try:
            while True:
                # There's never any "done" or "last message", so just keep
                # reading as long as the caller asks. If select times out,
                # a normal client would send feedback. We'll treat it as a
                # failure instead, since the caller asked for a message we
                # are apparently not going to receive.
                message = self.walcur.read_replication_message(decode=False)
                if message is None:
                    self.logger.debug("No message pending, select()ing with timeout %s", self.select_timeout)
                    sel = select.select([self.walcur], [], [], self.select_timeout)
                    if not sel[0]:
                        raise IOError("Server didn't send an expected message before timeout")
                else:
                    if len(message.payload) < 200:
                        self.logger.debug("payload: %s", repr(message.payload))
                    else:
                        self.logger.debug("payload (truncated): %s...", repr(message.payload)[:200])
                    yield ReplicationMessage((message.data_start, None, message.payload))
        except psycopg2.InternalError, ex:
            self.logger.debug("While retrieving a message: sqlstate=%s", ex.pgcode, exc_info=True)




class PGLogicalOutputTest(unittest.TestCase):

    connstring = "dbname=postgres host=localhost"
    interface = None

    def setUp(self):
        # A counter we can increment each time we reconnet with decoding,
        # for logging purposes.
        self.decoding_generation = 0

        # Set up our logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.loghandler = logging.StreamHandler()
        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)
        self.logger.addHandler(self.loghandler)
        self.logger.setLevel(os.environ.get('PGLOGICALTEST_LOGLEVEL', 'INFO'))

        # Get connections for test classes to use to run SQL
        self.conn = psycopg2.connect(self.connstring, connection_factory=psycopg2.extras.LoggingConnection)
        self.conn.initialize(self.logger.getChild('sql'))
        self.cur = self.conn.cursor()

        if hasattr(self, 'set_up'):
            self.set_up()

    def tearDown(self):
        if hasattr(self, 'tear_down'):
            self.tear_down()

        if self.conn is not None:
            self.conn.close()

    def doCleanups(self):
        if self.interface:
            self.interface.cleanup()

    def reconnect_decoding(self):
        """
        Close the logical decoding connection and re-establish it.

        This is useful when we want to restart decoding with different parameters,
        since in walsender mode there's no way to end a decoding session once
        begun.
        """
        if self.interface is not None:
            self.logger.debug("Disconnecting old decoding session and forcing reconnect")
            self.interface.cleanup()

        self.connect_decoding()

    def connect_decoding(self):
        """
        Make a slot and establish a decoding connection.

        Prior to this changes are not recorded, which is useful for setup.
        """
        self.decoding_generation += 1
        fmt = logging.Formatter('%%(name)-50s w=%s %%(message)s' % (self.decoding_generation,))
        self.loghandler.setFormatter(fmt)

        if os.environ.get("PGLOGICALTEST_USEWALSENDER", None):
            self.interface = WalsenderDecodingInterface(self.connstring, parentlogger=self.logger)
        else:
            self.interface = SQLDecodingInterface(self.connstring, parentlogger=self.logger)


    def get_changes(self, kwargs = {}):
        """
        Get a stream of messages as a generator that may be read from
        to fetch a new message each call. Messages are instances of
        class ReplicationMessage .

        The generator has helper methods for decoding particular
        types of message, for validation, etc.
        """
        if self.interface is None:
            raise ValueError("No logical decoding connection. Call connect_decoding()")

        msg_gen = self.interface.get_changes(kwargs)
        return ProtocolReader(msg_gen, tester=self, parentlogger=self.logger)
