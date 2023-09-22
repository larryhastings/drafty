#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import application
import big.all as big
from big.all import BoundInnerClass
from dataclasses import dataclass, field
import keyvalue
import log
from log import Log, CommittedState
from messages import *
import pathlib
import perky
import raftconfig
import random
from typing import Callable


def manufactured_field(cls, **kwargs):
    return field(init=False, default_factory=cls, **kwargs)


def counter(start=0):
    def counter(start=0):
        while True:
            yield start
            start += 1
    return counter().__next__

waiting_room_counter = counter()

@dataclass
class PersistentState(packraft.Message):
    log: Log
    term: int
    candidate: int

# This needs reworking, but this is a quick and dirty way to turn off debug
# printing.
def no_print(self, s):
    pass

@dataclass
@big.accessor()
class Server:
    """
    Abstracted Raft server.
    All external messages come through the Driver.
    """
    application: application.Application
    servers: list
    id: int
    heartbeat_interval: float
    committed: CommittedState = manufactured_field(CommittedState)
    election_timeout_interval_start: float
    election_timeout_interval_range: float
    leader_id: int = None

    print_debug: Callable = field(init=False, default=print)

    # because we're 0-based,
    # our "next_index" == len(self.log)

    # can't use log.Log, it's occluded by the local "log" attribute
    log: Log = None

    def __post_init__(self):
        if self.application == None:
            self.application = keyvalue.KeyValueStore()
        self.directory = pathlib.Path(str(self.id))
        self.directory.mkdir(exist_ok=True)
        self.state_path = self.directory / "state.pky"
        self.cached_persistent_dict = None
        self.load_persistent_state()
        self.log = Log(self.directory)

    # automatically reset voted_for whenever term changes
    voted_for: int = -1

    _term: int = field(default=-1, init=False)
    @property
    def term(self):
        return self._term

    @term.setter
    def term(self, value):
        if self._term != value:
            self._term = value
            self.voted_for = -1

    # initialized by calling server.start()
    # (which should be called by driver.run())
    driver = None

    def start(self, driver):
        self.driver = driver
        self.state_manager = big.StateManager(self.Follower())

        # array of ints of all server ids EXCEPT US
        self.others = []
        # 2-tuples: (server_id, server_id_is_us)
        self.ids = []
        for id in range(len(raftconfig.servers)):
            is_us = id == self.id
            self.ids.append((id, is_us))
            if not is_us:
                self.others.append(id)

    def on_request(self, request, luid):
        if isinstance(request, AppendEntriesRequest):
            result = self.on_append_entries(request, luid)
            return result
        if isinstance(request, RequestVoteRequest):
            return self.on_request_vote(request, luid)
        raise ValueError(f"unrecognized request type {request} (luid={luid})")

    def on_response(self, response, request, destination, request_luid): # destination is the person we sent the request to
        if isinstance(response, AppendEntriesResponse):
            return self.on_append_entries_response(response, request, destination, request_luid)
        if isinstance(response, RequestVoteResponse):
            return self.on_request_vote_response(response, request, destination, request_luid)
        raise ValueError(f"unrecognized response type {response} (request={request} destination={destination} request_luid={request_luid})")

    @big.dispatch()
    def on_append_entries(self, request, luid):
        ...

    @big.dispatch()
    def on_request_vote(self, request, luid):
        ...

    @big.dispatch()
    def on_client_request(self, request, luid):
        ...

    @big.dispatch()
    def on_append_entries_response(self, response, request, destination, request_luid):
        ...

    @big.dispatch()
    def on_request_vote_response(self, response, request, destination, request_luid):
        ...

    SAME_TERM = "same term"
    NEW_TERM  = "new term!"
    OLD_TERM  = "old term"

    def analyze_received_term(self, term):
        if self.term == term:
            self.print_debug(f"analyze_received_term: {self.term=} -- {term=} -> SAME_TERM")
            return self.SAME_TERM

        if term < self.term:
            # term we received was less than our term,
            # ignore it completely.
            self.print_debug(f"analyze_received_term: {self.term=} -- {term=} -> OLD_TERM")
            return self.OLD_TERM

        # term we received was greater than our term!
        # a new term! we must go to Follower.
        # forced transition to follower
        assert term > self.term
        self.term = term
        self.print_debug(f"analyze_received_term: {self.term=} -- {term=} -> NEW_TERM")
        return self.NEW_TERM

    def change_to_follower_on_new_term(self, term):
        what_happened = self.analyze_received_term(term)
        if what_happened == self.NEW_TERM:
            # a new term! we must go to Follower.
            # forced transition to follower
            if not isinstance(self.state, self.Follower):
                self.print_debug("[!] term changed us to follower!")
                self.state = self.Follower()
        return what_happened

    def ignore_request_in_this_state(self, request, luid):
        what_happened = self.change_to_follower_on_new_term(request.term)
        if what_happened == self.NEW_TERM:
            self.on_request(request, luid)
        return what_happened != self.SAME_TERM

    def ignore_response_in_this_state(self, response, request, destination, request_luid):
        what_happened = self.change_to_follower_on_new_term(response.term)
        if what_happened == self.NEW_TERM:
            self.on_response(response, request, destination, request_luid)
            return True
        return what_happened != self.SAME_TERM

    def load_persistent_state(self, *, to_index=None):
        if self.state_path.exists():
            d = perky.load(self.state_path)
            self.term = int(d['term'])
            self.voted_for = int(d['voted for'])
            self.cached_persistent_dict = d

    def save_persistent_state(self, *, to_index=None):
        persistent_dict = {
            'term': str(self.term),
            'voted for': str(self.voted_for),
            }
        if self.cached_persistent_dict != persistent_dict:
            perky.dump(self.state_path, persistent_dict)
            self.cached_persistent_dict = persistent_dict
        self.log.serialize(to_index=to_index)

    @BoundInnerClass
    @dataclass
    class State(big.State):
        server: "Server"
        election_timeout_timer = None

        def on_enter(self):
            self.server.print_debug(f">> entered state {type(self).__name__} -- in term {self.server.term}")

        def on_exit(self):
            self.server.print_debug(f"<<  exited state {type(self).__name__} -- in term {self.server.term}")

        @big.pure_virtual()
        def on_append_entries(self, request, luid):
            ...

        @big.pure_virtual()
        def on_request_vote(self, request, luid):
            ...

        @big.pure_virtual()
        def on_client_request(self, request, luid):
            ...

        @big.pure_virtual()
        def on_append_entries_response(self, response, request, destination, request_luid):
            ...

        @big.pure_virtual()
        def on_request_vote_response(self, response, request, destination, request_luid):
            ...

    @BoundInnerClass
    @dataclass
    class FollowerOrCandidate(State.cls):
        # stuff that's the same for both Follower and Candidate
        election_timeout_timer = None

        def cancel_election_timeout_timer(self):
            if self.election_timeout_timer:
                self.election_timeout_timer.cancel()

        def reset_election_timeout_timer(self):
            self.cancel_election_timeout_timer()
            interval = self.server.election_timeout_interval_start + (random.random() * self.server.election_timeout_interval_range)
            self.election_timeout_timer = self.server.driver.Timer(interval, self.on_election_timeout, self.server.driver.election_timeout_timer_luid())

        def on_enter(self):
            super().on_enter()
            self.reset_election_timeout_timer()

        def on_exit(self):
            # STAY FRESH, CHEESE BAGS
            self.cancel_election_timeout_timer()
            super().on_exit()

        ##
        ## handle transitioning to candidate
        ##
        def on_election_timeout(self):
            # hot DIGGITY, we is becoming a CANDY-DATE!
            self.server.print_debug(">> election timeout timer timed out time timed timety-time time timeout.")
            self.server.state = self.server.Candidate()

        def on_client_request(self, request, luid):
            # We're not the leader,
            # tell the client to contact the old leader.
            #
            # is this what we want for a Candidate?  yeah.
            # maybe network bifurcation means I can't talk
            # to the old leader but they can.
            # if the leader is truly down, maybe the
            # client will try, time out, retry, and
            # by then we'll have a new leader elected.
            self.server.driver.send_client_response(
                ClientRedirectResponse(
                    success=False,
                    leader_id=self.server.leader_id,
                    ),
                luid,
            )


    @BoundInnerClass
    @dataclass
    class Follower(FollowerOrCandidate.cls):

        ##
        ## handle basic follower work
        ##
        def on_append_entries(self, request, luid):
            self.reset_election_timeout_timer()

            if self.server.analyze_received_term(request.term) == self.server.OLD_TERM:
                success = False
            else:
                success = self.server.log.append_entries(request.previous_log_index, request.previous_log_term, request.entries)
                if success:
                    self.server.leader_id = request.leader_id
                    # remember: request.leader_commit_index and self.server.committed.index
                    # are both the index of the highest committed request.
                    # if we've only committed log[0], they are 0.
                    committed_max_index = max(request.leader_commit_index, self.server.committed.index) + 1
                    # so this expression --------------------------vvvvvvvvvvvvvvvvvvvvvvvv
                    committed_max_index = min(committed_max_index, len(self.server.log) - 1)
                    # is the highest index we can commit, expressed in the same way.

                    if self.server.committed.index < committed_max_index:
                        # persist
                        self.server.save_persistent_state(to_index=committed_max_index)
                        # and commit
                        for index in range(self.server.committed.index + 1, committed_max_index + 1):
                            log_entry = self.server.log[index]
                            request = log_entry.request
                            if not isinstance(request, ClientNoOpRequest):
                                response = self.server.application.on_request(request)
                                assert response.success
                            self.server.committed.index = index

            return AppendEntriesResponse(
                term=self.server.term,
                success=success,
                log_index=len(self.server.log) - 1,
                )

        def on_append_entries_response(self, response, request, destination, request_luid):
            # Maybe we used to be leader?  Anyway, we can ignore it.
            pass

        def on_request_vote(self, request, luid):
            self.server.print_debug(">> Follower.on_request_vote")

            if self.server.analyze_received_term(request.term) == self.server.OLD_TERM:
                self.server.print_debug(">> old term! fail and don't grant vote.")
                success = False
                vote_granted = False
            else:
                success = True

                last_log_index = len(self.server.log) - 1
                self.server.print_debug(f">>    {last_log_index=} = {len(self.server.log)=} - 1")

                last_log_term = self.server.log[-1].term if bool(self.server.log) else -1
                self.server.print_debug(f">>    {last_log_term=} = {self.server.log[-1].term if bool(self.server.log) else 'self.server.log[-1].term'=} if {bool(self.server.log)=} else -1")

                vote_granted_1 = vote_granted_2 = vote_granted_3 = vote_granted_4 = False

                vote_granted_1 = request.term >= self.server.term
                self.server.print_debug(f">>    {vote_granted_1=} = {request.term=} >= {self.server.term=}")

                if vote_granted_1:
                    vote_granted_2 = (self.server.voted_for == -1) or (self.server.voted_for == request.candidate_id)
                    self.server.print_debug(f">>    {vote_granted_2=} = ({self.server.voted_for=} == -1) or ({self.server.voted_for=} == {request.candidate_id=})")

                if vote_granted_2:
                    vote_granted_3 = request.last_log_index >= last_log_index
                    self.server.print_debug(f">>    {vote_granted_3=} = {request.last_log_index=} >= {last_log_index=}")

                if vote_granted_3:
                    vote_granted_4 = request.last_log_term >= last_log_term
                    self.server.print_debug(f">>    {vote_granted_4=} = {request.last_log_term=} >= {last_log_term=}")

                vote_granted = (
                    vote_granted_1
                    and vote_granted_2
                    and vote_granted_3
                    and vote_granted_4
                    )
                self.server.print_debug(f">>      {vote_granted=}")

            if vote_granted:
                self.voted_for = request.candidate_id
                # raft.pdf says!
                self.reset_election_timeout_timer()

            return RequestVoteResponse(
                success = success,
                term = self.server.term,
                vote_granted = vote_granted)

        def on_request_vote_response(self, response, request, destination, request_luid):
            self.server.print_debug(">> Follower.on_request_vote_response, I ignore RequestVote responses.")
            pass

    @BoundInnerClass
    @dataclass
    class Candidate(FollowerOrCandidate.cls):
        # we automatically vote for ourselves.
        votes: int = 1

        def on_enter(self):
            super().on_enter()

            self.server.term += 1
            self.server.print_debug(f">> Candidate incremented term to {self.server.term}")

            # if we have N servers, we need M votes:
            #            0                  ?
            #            1                  1
            #            2                  2
            #            3                  2
            #            4                  3
            #            5                  3
            self.votes_needed = (len(self.server.ids) // 2) + 1

            request = RequestVoteRequest(
                term = self.server.term,
                candidate_id = self.server.id,
                last_log_index = len(self.server.log) - 1,
                last_log_term = self.server.log[-1].term if self.server.log else -1,
                )

            self.server.print_debug(f"[##] Hello, I'm Bob Johnson, and I want to be your Leader.")
            for id in self.server.others:
                self.server.driver.send_server_request(request, id)
            self.server.print_debug(f"[##] sent {len(self.server.others)} vote requests:")
            self.server.print_debug(f"         {type(request)}")
            self.server.print_debug(f"         {request.term=}")
            self.server.print_debug(f"         {request.last_log_index=}")
            self.server.print_debug(f"         {request.last_log_term=}")

        def on_request_vote(self, request, luid):
            self.server.print_debug(f">> Candidate.on_request_vote {request.term}")
            if self.server.ignore_request_in_this_state(request, luid):
                return
            self.server.print_debug(f">> I'm a candidate: vote is never granted!  Sending success=True, term={self.server.term}, vote_granted=False")
            return RequestVoteResponse(
                success = True,
                term = self.server.term,
                vote_granted = False,
                )

        def on_request_vote_response(self, response, request, destination, request_luid):
            self.server.print_debug(">> Candidate.on_request_vote_response")
            if self.server.ignore_response_in_this_state(response, request, destination, request_luid):
                return

            self.server.print_debug(f"[##] Candidate.on_request_vote_response: {response.vote_granted=}")
            self.votes += int(response.vote_granted)

            self.server.print_debug(f"[##] {self.votes=} -- {self.votes_needed=} -- {len(self.server.ids)=}")
            if self.votes == self.votes_needed:
                # HOT DIGGITY DOG we iz a LEE-DUR
                self.server.state = self.server.Leader()

        def on_append_entries(self, request, luid):
            if self.server.ignore_request_in_this_state(request, luid):
                return

        def on_append_entries_response(self, response, request, destination, request_luid):
            if self.server.ignore_response_in_this_state(response, request, destination, request_luid):
                return


    @BoundInnerClass
    @dataclass
    class Leader(State.cls):
        followers: list[CommittedState] = None
        heartbeat_timer = None
        current_waiting_room: "WaitingRoom" = None
        waiting_rooms: dict = None
        # maps log_index to a 2-tuple: (client_request, luid)
        log_index_to_client_requests: dict = manufactured_field(dict)

        @BoundInnerClass
        @dataclass
        class WaitingRoom:
            state: "State"
            request: AppendEntriesRequest = None
            # Map of luids to ClientRequests
            # waiting for this heartbeat to be committed.
            client_requests: dict = manufactured_field(dict)
            # Map of luids to AppendEntryRequests
            # for this heartbeat.
            follower_requests: dict = manufactured_field(dict)
            id: int = manufactured_field(waiting_room_counter)
            log_index: int = -1
            committed: bool = False
            append_entries_response_counter: int = 0
            no_op_luid: str = b'totally invalid luid'

            def __repr__(self):
                return f"<WaitingRoom {self.id} state={type(self.state)} request={self.request} client_requests={list(self.client_requests)} follower_requests={list(self.follower_requests)} committed={self.committed}>"

            def add_client_request(self, request, luid):
                # This is now only used for unlogged requests.
                # Requests that go into the log are stored in
                # log_index_to_client_requests in the Leader.
                print(f"[WR {self.id}] add client request {luid=} {request=}")
                self.client_requests[luid] = request

            def add_follower_request(self, request, luid):
                print(f"[WR {self.id}] add follower request {luid=} {request=}")
                self.follower_requests[luid] = request

            def on_append_entries_response(self, response, request, destination, request_luid):
                """
                Returns True if *this* response means the request is
                "committed"--we've received resposes from a majority
                of servers.
                """
                debug_print = False
                debug_print = True
                self.append_entries_response_counter += 1
                counter = self.append_entries_response_counter
                if debug_print:
                    print(f"[WR {self.id}] ENTER on_append_entries_response #{counter}")
                    print(f"[WR {self.id}] In case you're curious, {self.state.server.committed.index=}")
                assert self.state.current_waiting_room != self, f"weirdly, {self.state.current_waiting_room} == {self}"
                if not request_luid in self.follower_requests:
                    # arrived twice?
                    # it is not our lot in life to understand what
                    # the network does or why.
                    # anyway we already handled it.
                    if debug_print:
                        print(f"[WR {self.id}] response for {request_luid=} was delivered twice?  ignoring this completely.")
                        print(f"[WR {self.id}] EXIT  on_append_entries_response #{counter}")
                    return False

                # unlink
                del self.follower_requests[request_luid]

                if self.committed:
                    if debug_print:
                        print(f"[WR {self.id}] waiting room already committed.")
                        print(f"[WR {self.id}] EXIT  on_append_entries_response #{counter}")
                    return False

                server_count = len(self.state.server.servers)
                luids_received = server_count - len(self.follower_requests)
                # 4 servers, (4 // 2) + 1 -> 2 + 1 -> 3
                # 5 servers, (5 // 2) + 1 -> 2 + 1 -> 3
                # 6 servers, (6 // 2) + 1 -> 3 + 1 -> 4
                consensus_minimum = (server_count // 2) + 1

                self.committed = luids_received >= consensus_minimum
                if debug_print:
                    print(f"[WR {self.id}] we ready to commit?  self.committed = ({luids_received} >= {consensus_minimum}), which is {self.committed}")
                if not self.committed:
                    if debug_print:
                        print(f"[WR {self.id}] we are not.  until we meet again--farewell, my brother.")
                        print(f"[WR {self.id}] EXIT  on_append_entries_response #{counter}")
                    return False

                if debug_print:
                    print(f"[WR {self.id}] yes we are! let's commit!")

                self.state.server.save_persistent_state()
                print(f"[WR server state has been persisted.]")

                responses = {}

                if debug_print:
                    print(f"[WR {self.id}] committing indices {self.state.server.committed.index + 1} to {self.log_index} inclusive")
                for index in range(self.state.server.committed.index + 1, self.log_index + 1):
                    if debug_print:
                        print(f"[WR {self.id}] committing index {index}, log is len {len(self.state.server.log)}")
                    log_entry = self.state.server.log[index]
                    request = log_entry.request
                    if isinstance(request, ClientNoOpRequest):
                        self.state.server.committed.index = index
                        continue
                    response = self.state.server.application.on_request(request)
                    responses[index] = response
                    if debug_print:
                        print(f"[WR {self.id}] application responds {response=}")
                    self.state.server.committed.index = index

                    requests_and_luids = self.state.log_index_to_client_requests.get(index, None)
                    if requests_and_luids:
                        for cr, luid in requests_and_luids:
                            response.id = cr.id
                            self.state.server.driver.send_client_response(response, luid)
                        del self.state.log_index_to_client_requests[index]

                if not self.client_requests:
                    if debug_print:
                        print(f"[WR {self.id}] we don't have any client requests, which is fine if we're a heartbeat.")
                    return

                response = ...

                if debug_print:
                    print(f"[WR {self.id}] received consensus from {luids_received=} {self.state.server.committed.index=}")
                    print(f"{self.client_requests=}")
                for luid, client_request in self.client_requests.items():
                    if debug_print:
                        print(f"[WR {self.id}] unlogged client request {luid=}")
                    if luid == self.no_op_luid:
                        continue
                    response = self.state.server.application.on_request(client_request)
                    if debug_print:
                        print(f"[WR {self.id}] sending client response {response=} {luid=}")
                    assert response != ..., "work log indices miscalculated!"
                    response.id = client_request.id
                    self.state.server.driver.send_client_response(response, luid)

                self.client_requests.clear()
                if debug_print:
                    print(f"[WR {self.id}] EXIT  on_append_entries_response #{counter}")
                return True


        def on_enter(self):
            super().on_enter()
            debug_print = False
            self.server.server_id = self.server.id # that'll show 'em!
            c = self.server.committed
            if debug_print:
                print(f">> Leader.on_enter  howdy I'm server {self.server.id}")
                print(f">> Leader.on_enter {self.server.ids=}")
            self.followers = [c if is_us else c.copy() for (id, is_us) in self.server.ids]
            if debug_print:
                print(f">> Leader.on_enter {self.followers=}")
                print(f">> Leader.on_enter {list((o, o is c) for o in self.followers) = }")
            self.current_waiting_room = self.WaitingRoom()
            self.waiting_rooms = {}

            # used to prevent the tragedy
            # of the Raft paper's dreaded "Figure 8"
            no_op = ClientNoOpRequest(f'{self.server.id}-{self.server.term}'.encode('ascii'))
            self.current_waiting_room.no_op_luid = f"no_op-term-{self.server.term}"
            # this will kick off a heartbeat, etc.
            self.on_client_request(no_op, self.current_waiting_room.no_op_luid)

        def on_exit(self):
            self.cancel_heartbeat()
            super().on_exit()

        def send_append_entries_request(self, id):
            assert id != self.server.id
            committed_state = self.followers[id]
            if committed_state.index == -1:
                entries = list(self.server.log)
                # Cannot be None for packraft serialization.
                previous_log_term = -1
            else:
                entries = self.server.log[committed_state.index + 1:]
                previous_log_term = self.server.log[committed_state.index].term

            request = AppendEntriesRequest(
                term=self.server.term,
                leader_id=self.server.id,
                previous_log_index=committed_state.index,
                previous_log_term=previous_log_term,
                entries=entries,
                leader_commit_index=self.server.committed.index,
                )
            luid = self.server.driver.send_server_request(request, id)
            return request, luid

        def send_heartbeat(self):
            self.reset_heartbeat()
            for id in self.server.others:
                r2, luid = self.send_append_entries_request(id)
                self.current_waiting_room.add_follower_request(r2, luid)
                self.waiting_rooms[luid] = self.current_waiting_room
            self.current_waiting_room.log_index = max(self.current_waiting_room.log_index, self.server.committed.index)
            # self.server.print_debug(f"[{self.server.driver.time():03}] heartbeat sent! {list(self.current_waiting_room.follower_requests)}")
            self.current_waiting_room = self.WaitingRoom()

        def cancel_heartbeat(self):
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
                self.heartbeat_timer = None

        def reset_heartbeat(self):
            self.cancel_heartbeat()
            self.heartbeat_timer = self.server.driver.Timer(self.server.heartbeat_interval, self.send_heartbeat, self.server.driver.heartbeat_timer_luid())
            # self.server.print_debug(f"[{self.server.driver.time():03}] new heartbeat timer set for +{HEARTBEAT_INTERVAL}s, gonna call {self.heartbeat_timer.callback}")

        def on_append_entries(self, request, luid):
            if self.server.ignore_request_in_this_state(request, luid):
                return

        def on_append_entries_response(self, response, request, destination, request_luid):
            debug_print = False
            if debug_print:
                print(f">> Leader.on_append_entries_response({response=}, {request=}, {destination=}, {request_luid=})")
            if self.server.ignore_response_in_this_state(response, request, destination, request_luid):
                return

            if response.success:
                assert destination != self.server.id, "we shouldn't be getting responses from... ourselves?!"
                if debug_print:
                    print("It's follower index updatin' time:")
                    print(f"{self.followers = }")
                    print(f"setting followers[{destination=}].index to {response.log_index=}")
                    print(f"followers[{destination=}].index is {self.followers[destination]=}")
                    print(f"Does that happen to be {self.server.committed=}?")
                    print(f"{self.server.committed == self.followers[destination] = }")
                self.followers[destination].index = response.log_index
            else:
                # wellsah!  this follower is out-of-date!
                # start probing the follower to find where their
                # log diverged, so we can backfill.
                self.followers[destination].index -= 1
                self.send_append_entries_request(destination)

            # self.server.print_debug(f'deleting {request_luid=}')
            # for _luid, _waiting_room in self.waiting_rooms.items():
            #     self.server.print_debug(f'{_luid=}')
            #     self.server.print_debug(f'  client_requests={list(_waiting_room.client_requests)}')
            #     self.server.print_debug(f'  follower_requests={list(_waiting_room.follower_requests)}')
            # self.server.print_debug()

            waiting_room = self.waiting_rooms.get(request_luid)
            if not waiting_room:
                # AppendEntries log backfill requests don't go into
                # a waiting room.
                # Let's not worry about updating our committed index
                # just now--we'll just wait until the next heartbeat.
                return

            del self.waiting_rooms[request_luid]
            if not waiting_room.on_append_entries_response(response, request, destination, request_luid):
                return

        def on_request_vote(self, request, luid):
            if self.server.ignore_request_in_this_state(request, luid):
                return

        def on_request_vote_response(self, response, request, destination, request_luid):
            if self.server.ignore_response_in_this_state(response, request, destination, request_luid):
                return

        def on_client_request(self, request, luid):
            # self.requests.append((request, luid))
            debug_print = False
            debug_print = True
            if debug_print:
                print(f"[Leader.on_client_request -- 1 --] Oh my!  I, the Leader, have received {request=} {luid=}.")
                print(f"[Leader.on_client_request -- 1 --] In case you're curious, {self.server.committed.index = }")
            if debug_print:
                print(f"[Leader.on_client_request -- 2 --]")
            if self.server.application.is_logged(request):
                # log_index is the position we wrote this entry into.
                # log[log_index] == log_entry
                if debug_print:
                    print(f"[Leader.on_client_request -- 3 --]")
                log_index = self.server.log.guid_cache.get(request.id)
                if log_index:
                    # We've seen this request before.  Has it been committed?
                    if log_index <= self.server.committed.index:
                        # it has! reply with success.
                        if debug_print:
                            print(f"[Leader.on_client_request -- 3a -- replying with success to already-handled request]")
                        response = self.server.application.on_handled_request(request)
                        self.state.server.driver.send_client_response(response, luid)
                        return
                    # it hasn't.  fall through, add it to log_index_to_client_requests
                    if debug_print:
                        print(f"[Leader.on_client_request -- 3b -- redundant already-submitted request, is in log but not committed]")
                else:
                    log_entry = log.LogEntry(self.server.term, request)
                    log_index = self.server.log.append(log_entry)
                    assert log_index > self.server.committed.index
                    if debug_print:
                        print(f"[Leader.on_client_request -- 3c -- new never-before-seen request, {log_index=} {log_entry=}]")

                if not log_index in self.log_index_to_client_requests:
                    self.log_index_to_client_requests[log_index] = []
                self.log_index_to_client_requests[log_index].append((request, luid))

                self.current_waiting_room.log_index = max(self.current_waiting_room.log_index, log_index)

                if debug_print:
                    print(f"[Leader.on_client_request -- 4 --] {log_index=}")
                if debug_print:
                    print(f"[Leader.on_client_request -- 5 --] Log is now {self.server.log=}")
                    print(f"[Leader.on_client_request -- 6 --]  {self.server.committed.index=} != {log_index=} -> {self.server.committed.index != log_index}")
                if debug_print:
                    print(f"[Leader.on_client_request -- 7 --] Request gets logged.")
            else:
                if debug_print:
                    print(f"[Leader.on_client_request -- 8 --] No logging for you!")
                self.current_waiting_room.add_client_request(request, luid)
            if debug_print:
                print(f"[Leader.on_client_request -- 9 --] In case you're curious, {self.server.committed.index = }")
                print(f"[Leader.on_client_request -- 9 --] I, the Leader, now choose to be done processing this client's request... FOR NOW.")
            self.send_heartbeat()
