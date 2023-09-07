#!/usr/bin/env python3

import application
from dataclasses import dataclass, field
from messages import *

sentinel = object()

@dataclass
class KeyValueStore(application.Application):

    store: dict = field(default_factory=dict, init=False)

    def on_request(self, request: ClientRequest):
        if isinstance(request, ClientGetRequest):
            value = self.store.get(request.key, sentinel)
            if value == sentinel:
                return ClientGetResponse(success=False, value=None)
            return ClientGetResponse(success=True, value=self.store[request.key])
        if isinstance(request, ClientPutRequest):
            self.store[request.key] = request.value
            return ClientPutResponse(success=True)
        raise ValueError(f"unhandled request {request}")

    def is_logged(self, request: ClientRequest):
        return isinstance(request, ClientPutRequest)
