import collections
import logging

class ProtocolReader(collections.Iterator):
    """
    A protocol generator wrapper that can validate a message before returning
    it and has helpers for reading different message types.

    The underlying message generator is stored as the message_generator
    member, but you shouldn't consume any messages from it directly, since
    that'll break validation if enabled.
    """

    startup_params = None

    def __init__(self, message_generator, validator=None, tester=None, parentlogger=logging.getLogger('base')):
        """
        Build a protocol reader to wrap the passed message_generator, which
        must return a ReplicationMessage instance when next() is called.

        A validator class may be provided. If supplied, it must have
        a validate(...) method taking a ReplicationMessage instance as
        an argument. It should throw exceptions if it sees things it
        doesn't like.

        A tester class may be provided. This class should be an instance
        of unittest.TestCase. If provided, unittest assertions are used
        to check message types, etc.
        """
        self.logger = parentlogger.getChild(self.__class__.__name__)
        self.message_generator = message_generator
        self.validator = validator
        self.tester = tester

    def next(self):
        """Validating for generator"""
        msg = self.message_generator.next()
        if self.validator:
            self.validator.validate(msg)
        return msg

    def expect_msg(self, msgtype):
        """Read a message and check it's type char is as specified"""
        m = self.next()
        # this is ugly, better suggestions welcome:
        try:
            if self.tester:
                self.tester.assertEqual(m.message_type, msgtype)
            elif m.message_type <> msgtype:
                raise ValueError("Expected message %s but got %s", msgtype, m.message_type)
        except Exception, ex:
            self.logger.debug("Expecting %s msg, got %s, unexpected message was: %s", msgtype, m.message_type, m)
            raise
        return m

    def expect_startup(self):
        """Get startup message and return the message and params objects"""
        m = self.expect_msg('S')
        # this is ugly, better suggestions welcome:
        if self.tester:
            self.tester.assertEquals(m.message['startup_msg_version'], 1)
        elif m.message['startup_msg_version'] <> 1:
            raise ValueError("Expected startup_msg_version 1, got %s", m.message['startup_msg_version'])
        self.startup_params = m.message['params']
        return (m, self.startup_params)

    def expect_begin(self):
        """Read a message and ensure it's a begin"""
        return self.expect_msg('B')

    def expect_row_meta(self):
        """Read a message and ensure it's a rowmeta message"""
        return self.expect_msg('R')

    def expect_commit(self):
        """Read a message and ensure it's a commit"""
        return self.expect_msg('C')

    def expect_insert(self):
        """Read a message and ensure it's an insert"""
        return self.expect_msg('I')

    def expect_update(self):
        """Read a message and ensure it's an update"""
        return self.expect_msg('U')

    def expect_delete(self):
        """Read a message and ensure it's a delete"""
        return self.expect_msg('D')

    def expect_origin(self):
        """
        Read a message and ensure it's a replication origin message.
        """
        return self.expect_msg('O')

    def maybe_expect_origin(self):
        """
        If the upstream sends replication origins, read one, otherwise
        do nothing and return None.

        If the upstream is 9.4 then it'll always send replication origin
        messages. For other upstreams they're sent only if enabled.

        Requires that the startup message was read with expect_startup(..)
        """
        if self.startup_params is None:
            raise ValueError("Startup message was not read with expect_startup()")
        if self.startup_params['forward_changeset_origins'] == 't':
            return self.expect_origin()
        else:
            return None
