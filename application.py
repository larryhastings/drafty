#!/usr/bin/env python3

import big.all as big
from messages import *

class Application:
    @big.pure_virtual()
    def on_request(self, request: ClientRequest):
        ...

    @big.pure_virtual()
    def is_logged(self, request: ClientRequest):
        ...

class MockApplication:
    def request(self, request: ClientRequest):
        if on_request(request, ClientGetRequest):
            return ClientGetResponse(success=True, value='xyz')
        if isinstance(request, ClientPutRequest):
            return ClientPutResponse(success=True)
        raise ValueError(f"unrecognized client request type, {client_request=}")

    def is_logged(self, request: ClientRequest):
        return isinstance(request, ClientPutRequest)

