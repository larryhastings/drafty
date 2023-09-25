#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import big.all as big
import dataclasses
from dataclasses import dataclass, field
from messages import *
import uuid


def serial_number_generator(*, prefix='', width=0, tuple=False):
    """
    Flexible serial number generator.
    """

    i = 1
    # yield prefix + base64.b32hexencode(i.to_bytes(5, 'big')).decode('ascii').lower().lstrip('0').rjust(3, '0')

    if tuple:
        # if prefix isn't a conventional iterable
        if not isinstance(prefix, (builtins.tuple, list)):
            while True:
                # yield 2-tuple
                yield (prefix, i)
                i += 1
        # yield n-tuple starting with prefix and appending i
        prefix = list(prefix)
        prefix.append(0)
        while True:
            prefix[-1] = i
            yield tuple(prefix)
            i += 1

    if width:
        while True:
            yield f"{prefix}{i:0{width}}"
            i += 1

    while True:
        yield f"{prefix}{i}"
        i += 1


# def local_request_luid(id):
#     i = 0
#     blowup = "local-request-{id}-1"
#     while True:
#         i += 1
#         s = f"local-request-{id}-{i}"
#         if s == blowup:
#             raise RuntimeError("die, die, die")
#         yield s

class Driver:
    def __init__(self):
        self.requests = {} # maps luid -> tuple((request_envelope, id_of_other_server))

    def __repr__(self):
        return f"<{type(self).__name__}>"

    def run(self, server):
        id = server.id
        self.local_request_luid = serial_number_generator(prefix=f"local-request-{id}-").__next__
        self.local_response_luid = serial_number_generator(prefix=f"local-response-{id}-").__next__
        self.remote_request_luid = serial_number_generator(prefix=f"remote-request-{id}-").__next__
        self.remote_response_luid = serial_number_generator(prefix=f"remote-response-{id}-").__next__
        self.client_luid = serial_number_generator(prefix=f"client-{id}-").__next__
        self.timer_luid = serial_number_generator(prefix=f"timer-{id}-").__next__
        self.heartbeat_timer_luid = serial_number_generator(prefix=f"heartbeat-timer-{id}-").__next__
        self.election_timeout_timer_luid = serial_number_generator(prefix=f"election-timeout-timer-{id}-").__next__
        self.server = server
        server.start(self)

    def on_client_recv(self, message, context, *, luid):
        """
        Called by the driver when it receives a message from a client.
        Base class implementation just handles some bookkeeping.
        Only allows ClientRequest messages.
        """
        if not isinstance(message, ClientRequest):
            raise RuntimeError(f"unrecognized message {message}")

        # we'll assign a transaction id here
        self.requests[luid] = (message, context)
        self.server.on_client_request(message, luid)

    def on_server_recv(self, message):
        """
        Called by the driver when it receives a message from a server.
        Base class implementation just handles some bookkeeping.
        Only allows two types of messages:
            ServerRequestEnvelope
            ServerResponseEnvelope
        """
        if isinstance(message, ServerRequestEnvelope):
            luid = self.remote_request_luid()
            request_envelope = message
            self.requests[luid] = (request_envelope, None)
            result = self.on_request(request_envelope.request, luid)
            if result is not None:
                self.send_server_response(result, luid)
            return luid
        if isinstance(message, ServerResponseEnvelope):
            luid = self.remote_response_luid()
            response_envelope = message
            request_luid = response_envelope.transaction_id
            request_envelope, destination = self.requests[request_luid]
            self.on_response(response_envelope.response, request_envelope.request, destination, request_luid)
            return luid
        raise RuntimeError(f"unrecognized message {message}")

    # Call this base function from send_server_request to make the actual
    # message you send over the wire, then the caller does the actual sending.
    def package_server_request(self, request, destination, *, luid=None):
        assert isinstance(request, ServerRequest)
        if luid == None:
            luid = self.local_request_luid()
        envelope = ServerRequestEnvelope(
            request,
            luid,
            requestor = self.server.id,
            )
        self.requests[luid] = (envelope, destination)
        return envelope, luid

    @big.pure_virtual()
    def send_server_request(self, request, destination):
        """
        Called by the abstracted server to send a request
        to another abstracted server.  (When using a real network
        driver, sends the request over the network.)

        request_luid is the luid assigned to the request
        this response is responding to.

        response_luid is the luid being assigned *now* to
        *this* response.  You can specify your own if you
        want, otherwise send_server_request will assign one for you.

        Returns a "luid", a hashable object that represents
        this transaction.
        """
        ...

    def package_server_response(self, response, request_luid, *, response_luid=None):
        if response_luid == None:
            response_luid = self.local_response_luid()
        assert isinstance(response, ServerResponse)
        request_envelope, recv_context = self.requests[request_luid]
        envelope = ServerResponseEnvelope(
            response,
            request_envelope.transaction_id,
            )
        return envelope, response_luid, request_envelope, recv_context

    @big.pure_virtual()
    def send_client_response(self, response, luid):
        """
        Called by the abstracted server to send a response
        to the client. (When using a real network driver,
        sends the message over the network.)  The context
        should be the context passed in to on_request() when
        this request was received.  (The "request_luid".)

        Returns nothing.
        """
        ...

    @big.pure_virtual()
    def send_server_response(self, response, luid):
        """
        Called by the abstracted server to send a response
        to another abstracted server, or the client.
        (When using a real network driver, sends the
        message over the network.)  The context should
        be the context passed in to on_request() when
        this request was received.  (The "request_luid".)

        Returns nothing.
        """
        ...

    def on_request(self, request, luid):
        return self.server.on_request(request, luid)

    def on_response(self, response, request, destination, request_luid): # destination is the person we sent the request to
        return self.server.on_response(response, request, destination, request_luid)


    @big.pure_virtual()
    def load_state(self):
        """
        Loads this node's persisted state dict from disk.
        Returns the current state, as a dict.
        If no state has been persisted yet, returns a dict with default values.
        """
        ...

    @big.pure_virtual()
    def save_state(self, d):
        """
        Saves this node's current state dict to disk.
        """
        ...

    @big.pure_virtual()
    def load_log(self):
        """
        Loads this node's persisted log entries from disk.
        Returns all the current entries, as a list.
        If no log entries have been persisted yet, returns an empty list.
        """
        ...

    @big.pure_virtual()
    def save_log(self, log, start, end):
        """
        Saves some or all of this node's log entries to disk.
        All entries written by this function are *appended* to any existing
        persisted log entries.  (You *cannot* overwrite previous entries
        using this API.)
        """
        ...




    @big.pure_virtual()
    def time(self):
        """
        Returns the current time in seconds (float or int).
        Useful for debug printfs, not used in the actual protocol.
        """
        ...

    @big.pure_virtual()
    def set_timer(self, interval, callback, luid):
        """
        Sets a timer.  If, after interval (float or int)
        seconds, the timer is not canceled, the driver will
        call callback().

        Returns an object that represents the timer
        (a timer_luid).

        To cancel the timer, call cancel_timer and pass
        in that object.
        """
        ...

    @big.pure_virtual()
    def cancel_timer(self, timer):
        ...

    @big.BoundInnerClass
    class Timer:
        """
        Convenient wrapper around the driver's
        set_timer and cancel_timer methods.
        timer = driver.Timer(interval, callback)
        """
        def __init__(self, driver, interval, callback, luid):
            self.driver = driver
            self.callback = callback
            self.timer = driver.set_timer(interval, self.on_timer, luid=luid)

        def on_timer(self):
            self.timer = None
            self.callback()

        def cancel(self):
            if self.timer:
                t = self.timer
                self.timer = None
                self.driver.cancel_timer(t)
