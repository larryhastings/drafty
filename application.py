#!/usr/bin/env python3

import big.all as big
from messages import *

class Application:
    @big.pure_virtual()
    def on_request(self, request: ClientRequest):
        """
        Handle a request from the client.
        """
        ...

    @big.pure_virtual()
    def on_handled_request(self, request: ClientRequest):
        """
        Construct the correct reply to a repeated,
        but already known to be handled, request from the client.
        This can happen due to servers / networks crashing.

        Consider this scenario:
        The client submits a request, the request is handled
        correctly, but the client doesn't get informed so
        resubmits the request.  The server and log know that
        the request has been handled, but they don't know how
        to create the appropriate response to the client.
        The server calls this API to construct the appropriate
        "sucess" response.
        """
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

    def on_repeated_request(self, request: ClientRequest):
        return self.on_request(request)

    def is_logged(self, request: ClientRequest):
        return isinstance(request, ClientPutRequest)
