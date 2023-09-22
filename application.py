#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

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
