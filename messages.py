import big.all as big
from dataclasses import dataclass
import msgpack
import packraft


def decode(msg_bytes):
    return packraft.Message.deserialize(msg_bytes)


@dataclass
class Request(packraft.Message):
    pass

@dataclass
class Response(packraft.Message):
    success: bool



log_request_class_id_to_cls = {}

def register_log_request(id):
    def register_log_request(cls):
        assert isinstance(cls, type)
        cls.log_request_class_id = id
        log_request_class_id_to_cls[id] = cls
        return cls
    return register_log_request

@dataclass
class ClientRequest(Request):
    id: str # GUID

    @big.pure_virtual()
    def log_serialize(self):
        ...

    @staticmethod
    def log_deserialize(l):
        class_id = l.pop()
        cls = log_request_class_id_to_cls[class_id]
        return cls(*l)

@dataclass
class LoggedClientRequest(ClientRequest):
    # inherit from this if this request should be entered in the raft Log.

    @big.pure_virtual()
    def log_serialize(self):
        ...

class ClientResponse(Response):
    id: str # GUID from ClientRequest

@dataclass
class ClientRedirectResponse(ClientResponse):
    leader_id: int


@dataclass
class ClientPingRequest(ClientRequest):
    text: str

@dataclass
class ClientPingResponse(ClientResponse):
    text: str

@register_log_request(64)
@dataclass
class ClientGetRequest(ClientRequest):
    key: str

    # def log_serialize(self):
    #     return [
    #         self.id,
    #         self.key,
    #         self.log_request_class_id
    #         ]

@dataclass
class ClientGetResponse(ClientResponse):
    value: str

@register_log_request(64)
@dataclass
class ClientPutRequest(LoggedClientRequest):
    key: str
    value: str

    def log_serialize(self):
        return [
            self.id,
            self.key,
            self.value,
            self.log_request_class_id,
            ]

@dataclass
class ClientPutResponse(ClientResponse):
    pass



@register_log_request(65)
@dataclass
class ClientNoOpRequest(LoggedClientRequest):
    # used to prevent the tragedy
    # of the Raft paper's dreaded "Figure 8"
    def log_serialize(self):
        return [
            self.id,
            self.log_request_class_id,
            ]



@dataclass
class ServerRequest(Request):
    pass

@dataclass
class ServerResponse(Response):
    pass


@dataclass
class AppendEntriesRequest(ServerRequest):
    term: int
    leader_id: int
    previous_log_index: int
    previous_log_term: int
    entries: list
    leader_commit_index: int

@dataclass
class AppendEntriesResponse(ServerResponse):
    term: int
    log_index: int

@dataclass
class RequestVoteRequest(ServerRequest):
    term: int
    candidate_id: int # id of the server sending this request
    last_log_index: int
    last_log_term: int

@dataclass
class RequestVoteResponse(ServerResponse):
    term: int
    vote_granted: bool


@dataclass
class ServerRequestEnvelope(packraft.Message):
    request: ServerRequest
    transaction_id: str
    requestor: int

@dataclass
class ServerResponseEnvelope(packraft.Message):
    response: ServerResponse
    transaction_id: str

