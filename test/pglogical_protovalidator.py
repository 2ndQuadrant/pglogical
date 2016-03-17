from pglogical_proto import StartupMessage, BeginMessage, RelationMessage
from pglogical_proto import CommitMessage, ChangeMessage, OriginMessage


class ProtocolState:

    RELATION_IDENTFIER = 'relid'

    def __init__(self, nextState):
        self.nextState = None

    def transition(self, replicationMsg):
        pass

    """
    populate relids received from RelationMessage into a list
    if relid has changed from previous, save it as recentRelid for further validations.
    """
    def populateRelids(self, replicationMsg):
        currRelid = replicationMsg.message[self.RELATION_IDENTFIER]
        if currRelid not in self.relids:
            self.recentRelid = currRelid
            self.validateRecentRelidFlag = 1
        self.relids.append(currRelid)

    def raiseUnexpectedMessageType(self, replicationMsg):
        raise ProtocolValidationException('Got unexpected Message type %s in current state %s'
                                          %(replicationMsg.message_type(),
                                            self.__class__.__name__) )

    """- TxStates and allowed Message types in them:
    InitState    [S]
    TxReadyState [B R]
    IntoTxState  [O R I U D C]

- State transitions happen as:
    - InitState    [S]  --> TxReadyState
    - TxReadyState [B]  --> IntoTxState
    - IntoTxState  [C]  --> TxReadyState"""

""" Before any message processing has started """
class InitState(ProtocolState):

    def __init__(self, nextState):
        self.nextState = nextState

    def transition(self, replicationMsg):
        if isinstance(replicationMsg, StartupMessage):
            return self.nextState
        else:
            self.raiseUnexpectedMessageType(replicationMsg)

""" Ready for Transaction state """
class TxReadyState(ProtocolState):

    def __init__(self, nextState):
        self.nextState = None

    def transition(self, replicationMsg):
        if isinstance(replicationMsg, BeginMessage):
            return IntoTxState(self)
        elif isinstance(replicationMsg, RelationMessage):
            self.populateRelids(replicationMsg)
            return self
        else:
            self.raiseUnexpectedMessageType(replicationMsg)

""" Into Transaction state """
class IntoTxState(ProtocolState):
    """For Origin message [O], validate that:
         - It should be the first message after 'B'egin. e.g. we can start a counter in
           IntoTxState and check that it has to be 0 when 'O'rigin message comes in.
    """
    counter = 0

    def __init__(self, nextState):
        """ Instance wise nextState is the same as previous one """
        self.nextState = nextState
        self.counter = 0
        self.relids = []
        """ Previous transactions relids to be cleared at this point"""
        self.recentRelid = None
        """ Setting flag to false """
        self.validateRecentRelidFlag = 0


    """  - For each RowChangeMessage coming in [I, U, D]:
        -table meta for its relidentifier should be received sometime since connection,
        since the current transaction started.
    """
    def validateRelationId(self, replicationMsg):
        currRelid = replicationMsg.message[self.RELATION_IDENTFIER]
        if currRelid not in self.relids:
                raise ProtocolValidationException('Relation Message not received for relid:  %s'
                                          %(currRelid))
        self.validateRecentRelationId(currRelid)

    """ - For each RowChangeMessage coming in [I, U, D]:
           - if relidentifier recentRelid was just set from RelationMessage,
           validate that RowChange is for the same Relid.
    """
    def validateRecentRelationId(self, currRelid):
        if (self.validateRecentRelidFlag):
                self.validateRecentRelidFlag = 0
                """ flag reset """
                if currRelid != self.recentRelid:
                    raise ProtocolValidationException('Relation Message recent relid:  %s is '
                    ' different from Row Change message relid: %s'
                    %(self.recentRelid, currRelid))

    def validateOriginMessagePosition(self):
        if (self.counter > 1):
            """ O"""
            raise ProtocolValidationException('OriginMessage came at %s place'
            ' after start of transaction' % (self.counter))

    def transition(self, replicationMsg):
        self.counter += 1
        returnState = self

        if isinstance(replicationMsg, ChangeMessage):
            """ I, U, D"""
            self.validateRelationId(replicationMsg)
        elif isinstance(replicationMsg, RelationMessage):
            """ R"""
            self.populateRelids(replicationMsg)

        elif isinstance(replicationMsg, CommitMessage):
            """ C """
            returnState = TxReadyState(None)

        elif isinstance(replicationMsg, OriginMessage):
            self.validateOriginMessagePosition()

        else:
            self.raiseUnexpectedMessageType(replicationMsg)

        return returnState


""" Typed Exception for Protocol Validation """
class ProtocolValidationException(Exception):
    pass

class ProtocolValidator(object):

    """
    InitState and TxReadyState are once created and used, while IntoTxState can
    is created each time a new tx starts.
    """

    def __init__(self):

        self.txReadyState = TxReadyState(None)
        self.currentState = InitState(self.txReadyState)

    """ replicationMsg is ReplicationMessage """
    def validate(self, replicationMsg):
        """ Throws exception when invalid things are seen"""
        self.currentState = self.currentState.transition(replicationMsg)
