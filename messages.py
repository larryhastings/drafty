#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


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


# not technically a "message",
# this is the object stored in the Log
# to represent one log entry.

@dataclass
class LogEntry(packraft.Message):
    term: int
    request: LoggedClientRequest

    def log_serialize(self):
        l = self.request.log_serialize()
        l.append(self.term)
        return l

    @classmethod
    def log_deserialize(cls, l):
        term = l.pop()
        request = ClientRequest.log_deserialize(l)
        return cls(term, request)
