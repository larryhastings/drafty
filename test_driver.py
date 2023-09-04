#!/usr/bin/env python3


import driver
from log import LogEntry
import math
from messages import *
import os
import perky
import pprint
import raftconfig
import random


class TestDriver(driver.Driver):

    def __init__(self):
        super().__init__()
        self.server = None
        self._time = 0
        self._timers = []
        self._luid_to_timer = {}
        self.requests = {}
        self.reset_outgoing()

    def reset_outgoing(self):
        self.server_outgoing_messages = []
        for id in range(len(raftconfig.servers)):
            self.server_outgoing_messages.append([])

        self.client_responses = []

    def run(self, server):
        super().run(server)

    def send_server_request(self, request, destination):
        envelope, luid = self.package_server_request(request, destination)
        self.server_outgoing_messages[destination].append(envelope)
        return luid

    def send_server_response(self, response, luid):
        envelope, luid, request_envelope, recv_context = self.package_server_response(response, luid)
        self.server_outgoing_messages[request_envelope.requestor].append(envelope)

    def send_client_response(self, response, luid):
        self.client_responses.append(response)

    def _sort_timers(self):
        self._timers.sort(key=lambda timer:timer[0])

    def time(self):
        return self._time

    def set_timer(self, interval, callback, *, luid=None):
        time = self._time + interval
        if luid is not None:
            luid = self.timer_luid()
        entry = (time, luid, callback)
        self._timers.append(entry)
        self._luid_to_timer[luid] = entry
        self._sort_timers()
        return luid

    def cancel_timer(self, timer):
        luid = timer
        entry = self._luid_to_timer.pop(luid)
        self._timers.remove(entry)
        self._sort_timers()

    def advance_time(self, increment):
        final_time  = self._time + increment
        while self._timers:
            event = self._timers[0]
            time, luid, callback = event
            if time > final_time:
                break
            self._timers.pop(0)
            self._time = time
            # print(f"CALLING {callback=}")
            callback()
        self._time = final_time

    def waiting_requests(self):
        for q in self.server_outgoing_messages:
            while q:
                message = q.pop(0)
                yield message

    def reply_to_heartbeat_with_success(self, heartbeats):
        for request_envelope in heartbeats:
            assert isinstance(request_envelope, ServerRequestEnvelope)
            request = request_envelope.request
            assert isinstance(request, AppendEntriesRequest), f"expected AppendEntriesRequest but got {request}"

            # fake reply--since we're using the information
            # from our leader, everything will be pleasantly current
            response = AppendEntriesResponse(
                term=self.server.term,
                success=success,
                log_index=len(self.server.log) - 1,
                )
            response_envelope, response_luid, request_envelope, recv_context = self.package_server_response(response, request_envelope.transaction_id)
            self.on_server_recv(response_envelope)

    def append_entries_test(self, request, expected_response):
        assert isinstance(self.server.state, self.server.Follower), "please put server into Follower state for this test"
        self.reset_outgoing()

        request_envelope, request_luid = self.package_server_request(request, self.server.id)
        self.on_server_recv(request_envelope)

        messages = list(self.waiting_requests())
        assert len(messages) == 1
        response_envelope = messages.pop()
        assert isinstance(response_envelope, ServerResponseEnvelope)
        assert response_envelope.response == expected_response

    def client_request_test(self, request, expected_response, *, expect_heartbeats):
        """
        Test driver utility function.
        Pass in a Request, and the expected Response,
        and I'll pump the Request through the server
        and see if the Response is correct.
        """
        # assert isinstance(self.server.state, self.server.Leader), "please put server into Leader state for this test"
        self.reset_outgoing()

        context = "aba daba honeymoon"
        client_test_luid = "client-1"
        self.on_client_recv(request, context, luid=client_test_luid)

        def check_response():
            assert len(self.client_responses) == 1, f"expected one client response, got {self.client_responses}"
            response = self.client_responses.pop()
            assert response == expected_response, f"expected {expected_response}\n\ngot {response}"

        # there should be four messages waiting
        heartbeats = list(self.waiting_requests())
        if not expect_heartbeats:
            assert not heartbeats
            check_response()
            return

        assert len(heartbeats) == 2, f"expected two heartbeats, got {len(heartbeats)}\n\n{heartbeats}"

        for i, request in enumerate(heartbeats):
            self.reply_to_heartbeat_with_success([request])
            if i != 0:
                assert not self.client_messages
                continue
            check_response()



if __name__ == "__main__":
    tests_passed = 0

    def success():
        global tests_passed
        tests_passed += 1

    from server import Server

    HEARTBEAT_INTERVAL = 0.5

    import raftconfig
    server = Server(
        application=None,
        election_timeout_interval_start=HEARTBEAT_INTERVAL * 10,
        election_timeout_interval_range=HEARTBEAT_INTERVAL * 5,
        heartbeat_interval=HEARTBEAT_INTERVAL,
        id=0,
        servers=raftconfig.servers,
        )
    tp = TestDriver()

    tp.run(server)

    # simulate AppendEntries coming in!
    request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        previous_log_index = -1,
        previous_log_term = 0,
        entries = [],
        leader_commit_index = 0)

    expected_response =  AppendEntriesResponse(
        success=True,
        term=0,
        log_index=-1,
        )

    tp.append_entries_test(request, expected_response)
    success()

    server.state.leader_id = 3
    my_entries = [
            LogEntry(1, ClientPutRequest('z', 'q')),
            LogEntry(1, ClientPutRequest('3', '4')),
        ]

    request = AppendEntriesRequest(
        term=0,
        leader_id=3,
        previous_log_index = -1,
        previous_log_term = 0,
        entries = my_entries,
        leader_commit_index = 0)

    expected_response =  AppendEntriesResponse(
        success=True,
        term=0,
        log_index=1,
        )
    tp.append_entries_test(request, expected_response)
    success()

    # send a request to a *follower*
    request = ClientGetRequest("anything")
    expected_response = ClientRedirectResponse(success=False, leader_id=3)
    tp.client_request_test(request, expected_response, expect_heartbeats=False)
    success()

    called = None
    def timer_callback():
        global called
        called = tp.time()

    tp.Timer(5, timer_callback, tp.timer_luid())
    assert called == None
    tp.advance_time(11)
    assert called == 5, f"{called=} != 5"
    success()

    # leaving Leader state should cancel heartbeat timer
    tp.reset_outgoing()
    tp.server.state = tp.server.Leader()
    heartbeat_luid = tp.server.state.heartbeat_timer.timer
    assert heartbeat_luid is not None
    heartbeat_event = tp._luid_to_timer[heartbeat_luid]
    assert heartbeat_event is not None
    tp.server.state = tp.server.Follower()
    assert heartbeat_luid not in tp._luid_to_timer
    assert heartbeat_event not in tp._timers
    success()

    tp.reset_outgoing()
    tp.server.state = tp.server.Leader()
    tp.advance_time(HEARTBEAT_INTERVAL * 1.5)
    # pprint.pprint(tp.server_outgoing_messages[1])
    for i in server.others:
        assert len(tp.server_outgoing_messages[i]) == 2, f"{len(tp.server_outgoing_messages[i])=} should both be 2"
    for destination in tp.server_outgoing_messages:
        for message in destination:
            assert isinstance(message, ServerRequestEnvelope)
            assert isinstance(message.request, AppendEntriesRequest)
            assert message.request.entries == my_entries, f"{message.request.entries=} != {my_entries=}"
    success()


    # serialization experiment
    d = {
        'log': packraft.packed(tp.server.log),
        'term': 3,
        'candidate': 3,
    }
    try:
        perky.dump("0.pky", d)
        d2 = perky.load("0.pky")
        log = packraft.unpacked(d2['log'])
        assert log == tp.server.log
        success()
    finally:
        os.unlink("0.pky")


    # write test for replicated log here.
    # tp.server.state = tp.server.Leader()

    print(f"All {tests_passed} driver tests passed.")
