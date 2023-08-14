import packraft
from dataclasses import dataclass


def decode(msg_bytes):
    return packraft.Message.deserialize(msg_bytes)


@dataclass
class Request(packraft.Message):
    pass

@dataclass
class Response(packraft.Message):
    success: bool


class ClientRequest(Request):
    pass

class ClientResponse(Response):
    pass

@dataclass
class ClientPingRequest(ClientRequest):
    text: str

@dataclass
class ClientPingResponse(ClientResponse):
    text: str

@dataclass
class ClientGetRequest(ClientRequest):
    key: str

@dataclass
class ClientGetResponse(ClientResponse):
    value: str

@dataclass
class ClientPutRequest(ClientRequest):
    key: str
    value: str

@dataclass
class ClientPutResponse(ClientResponse):
    pass

@dataclass
class ClientRedirectResponse(ClientResponse):
    leader_id: int

@dataclass
class ClientNoOpRequest(ClientRequest):
    # used to prevent the tragedy
    # of the Raft paper's dreaded "Figure 8"
    pass



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

