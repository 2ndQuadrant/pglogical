from StringIO import StringIO
import struct
import datetime

class UnchangedField(object):
    """Opaque placeholder object for a TOASTed field that didn't change"""
    pass

def readcstr(f):
    buf = bytearray()
    while True:
        b = f.read(1)
        if b is None or len(b) == 0:
            if len(buf) == 0:
                return None
            else:
                raise ValueError("non-terminated string at EOF")
        elif b is '\0':
            return str(buf)
        else:
            buf.append(b)

class ReplicationMessage(object):
    def __new__(cls, msg):
        msgtype = msg[2][0]
        if msgtype == "S":
            cls = StartupMessage
        elif msgtype == "B":
            cls = BeginMessage
        elif msgtype == "C":
            cls = CommitMessage
        elif msgtype == "O":
            cls = OriginMessage
        elif msgtype == "R":
            cls = RelationMessage
        elif msgtype == "I":
            cls = InsertMessage
        elif msgtype == "U":
            cls = UpdateMessage
        elif msgtype == "D":
            cls = DeleteMessage
        else:
            raise Exception("Unknown message type %s", msgtype)

        return super(ReplicationMessage, cls).__new__(cls)

    def __init__(self, row):
        self.lsn = row[0]
        self.xid = row[1]
        self.msg = row[2]

    @property
    def message_type(self):
        return self.msg[0]

    @property
    def message(self):
        return None

    def __repr__(self):
        return repr(self.message)

    def parse_tuple(self, msg):
        assert msg.read(1) == "T"
        numcols = struct.unpack("!H", msg.read(2))[0]

        cols = []
        for i in xrange(0, numcols):
            typ = msg.read(1)
            if typ == 'n':
                cols.append(None)
            elif typ == 'u':
                cols.append(UnchangedField())
            else:
                assert typ in ('i','b','t') #typ should be 'i'nternal-binary, 'b'inary, 't'ext
                datalen = struct.unpack("!I", msg.read(4))[0]
                cols.append(msg.read(datalen))

        return cols

class ChangeMessage(ReplicationMessage):
    pass

class TransactionMessage(ReplicationMessage):
    pass

class StartupMessage(ReplicationMessage):
    @property
    def message(self):
        res = {"type": "S"}

        msg = StringIO(self.msg)
        msg.read(1) # 'S'
        res['startup_msg_version'] = struct.unpack("b", msg.read(1))[0]
        # Now split the null-terminated k/v strings
        # and store as a dict, since we don't care about order.
        params = {}
        while True:
            k = readcstr(msg)
            if k is None:
                break;
            v = readcstr(msg)
            if (v is None):
                raise ValueError("Value for key %s missing, read key as last entry" % k)
            params[k] = v
        res['params'] = params

        return res

class BeginMessage(TransactionMessage):
    @property
    def message(self):
        res = {"type": "B"}

        msg = StringIO(self.msg)
        msg.read(1) # 'B'
        msg.read(1) # flags

        lsn, time, xid = struct.unpack("!QQI", msg.read(20))
        res['final_lsn'] = lsn
        res['timestamp'] = datetime.datetime.fromtimestamp(time)
        res['xid'] = xid

        return res

class OriginMessage(ReplicationMessage):
    @property
    def message(self):
        res = {"type": "O"}

        msg = StringIO(self.msg)
        msg.read(1) # 'O'
        msg.read(1) # flags

        origin_lsn, namelen = struct.unpack("!QB", msg.read(9))
        res['origin_lsn'] = origin_lsn
        res['origin_name'] = msg.read(namelen)

        return res

class RelationMessage(ReplicationMessage):
    @property
    def message(self):
        res = {"type": "R"}

        msg = StringIO(self.msg)
        msg.read(1) # 'R'
        msg.read(1) # flags

        relid, namelen = struct.unpack("!IB", msg.read(5))
        res['relid'] = relid
        res['namespace'] = msg.read(namelen)
        namelen = struct.unpack("B", msg.read(1))[0]
        res['relation'] = msg.read(namelen)

        assert msg.read(1) == "A" # attributes
        numcols = struct.unpack("!H", msg.read(2))[0]

        cols = []
        for i in xrange(0, numcols):
            assert msg.read(1) == "C" # column
            msg.read(1) # flags
            assert msg.read(1) == "N" # name

            namelen = struct.unpack("!H", msg.read(2))[0]
            cols.append(msg.read(namelen))

        res["columns"] = cols

        return res

class CommitMessage(TransactionMessage):
    @property
    def message(self):
        res = {"type": "C"}

        msg = StringIO(self.msg)
        msg.read(1) # 'C'
        msg.read(1) # flags

        commit_lsn, end_lsn, time = struct.unpack("!QQQ", msg.read(24))
        res['commit_lsn'] = commit_lsn
        res['end_lsn'] = end_lsn
        res['timestamp'] = datetime.datetime.fromtimestamp(time)

        return res

class InsertMessage(ChangeMessage):
    @property
    def message(self):
        res = {"type": "I"}

        msg = StringIO(self.msg)
        msg.read(1) # 'I'
        msg.read(1) # flags

        res["relid"] = struct.unpack("!I", msg.read(4))[0]

        assert msg.read(1) == "N"
        res["newtup"] = self.parse_tuple(msg)

        return res

class UpdateMessage(ChangeMessage):
    @property
    def message(self):
        res = {"type": "U"}

        msg = StringIO(self.msg)
        msg.read(1) # 'I'
        msg.read(1) # flags

        res["relid"] = struct.unpack("!I", msg.read(4))[0]

        tuptyp = msg.read(1)
        if tuptyp == "K":
            res["keytup"] = self.parse_tuple(msg)
            tuptyp = msg.read(1)

        tuptyp == "N"
        res["newtup"] = self.parse_tuple(msg)

        return res

class DeleteMessage(ChangeMessage):
    @property
    def message(self):
        res = {"type": "D"}

        msg = StringIO(self.msg)
        msg.read(1) # 'I'
        msg.read(1) # flags

        res["relid"] = struct.unpack("!I", msg.read(4))[0]

        assert msg.read(1) == "K"
        res["keytup"] = self.parse_tuple(msg)

        return res

