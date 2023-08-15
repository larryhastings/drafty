import sys
import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Any
import datetime
import collections
import pprint

import aioconsole

import asyncio_rpc
import raftconfig
import messages

# The sans i/o raft server.
from server import Server

# Driver is our base class.
from driver import Driver


# Keeps track of when the object itself was created.
@dataclass
class PrettyTimeNow:
    _tm: datetime.datetime = field(default_factory=datetime.datetime.now)

    def time_since(self):
        return datetime.datetime.now() - self._tm

    def __repr__(self):
        return str(self._tm)


@dataclass
class Statistics:
    start_time: PrettyTimeNow = field(default_factory=PrettyTimeNow)
    client_message_reads: int = 0
    client_message_writes: int = 0
    server_message_reads: int = 0
    server_message_writes: int = 0


@dataclass
class RaftPeer:
    peer_num: int
    peer_address: tuple[str, int]

    async def send(self, msg: messages.Response):
        try:
            reader, writer = await asyncio.open_connection(
                self.peer_address.host, self.peer_address.port
            )
        except ConnectionRefusedError:
            # Can't connect, can't send the message.  Such is life.
            return

        await asyncio_rpc.send(writer, msg.serialize())

        # Close the connection.
        writer.close()
        await writer.wait_closed()


@dataclass
class Timer:
    duration: float
    callback: Callable
    luid: str  # Used just for debugging.
    when_done: Callable
    handle_pending_items: Callable
    cancelled: bool = False

    # Always initialize start_time, because it might be referenced before run()
    # is called.
    start_time: float = field(default_factory=time.time)

    async def run(self):
        # Catch and show the exception here because we're not awaiting on this
        # task anywhere.
        try:
            # If we've already been cancelled before we have a chance to run, just
            # exit.  It happens.
            if self.cancelled:
                return

            self.start_time = time.time()
            await asyncio.sleep(self.duration)
            self.when_done(self.luid)
            if not self.cancelled:
                self.callback()

                # Items might have been scheduled during the callback, so this runs
                # AsyncioDriver.handle_pending_items.
                await self.handle_pending_items()

        except Exception as ex:
            print(f"timer {self.luid} exception {ex}")
            asyncio.current_task().print_stack()

    def cancel(self):
        self.cancelled = True

    def __repr__(self):
        # Time remaining is an approximation, because asyncio.sleep()'s timer
        # might be different from time.time(), but this is good enough for
        # debugging.
        return f"{type(self).__qualname__}(duration={self.duration:.4f}, luid={self.luid}, cancelled={self.cancelled}, remaining={self.duration-(time.time()-self.start_time):.4f})"


@dataclass
class AsyncioDriver(Driver):
    name: str
    peers: dict[int, RaftPeer]
    server: Server
    client_port: int
    raft_server_port: int

    pending_messages: list = field(default_factory=collections.deque, init=False)
    pending_timers: list = field(default_factory=collections.deque, init=False)
    client_luid_map: dict = field(default_factory=dict, init=False)
    stats: Statistics = field(default_factory=Statistics, init=False)

    # The list of running timers.  This is used to cancel them, indexed by ???
    timers: dict = field(default_factory=dict, init=False)

    debug_print_client_messages: int = False
    debug_print_server_messages: int = False

    def __post_init__(self):
        super().__init__()

    async def run(self):
        super().run(self.server)

        # Start listening.
        raft_coro = asyncio.start_server(
            self.raft_connection_made, "", self.raft_server_port
        )
        client_coro = asyncio.start_server(
            self.client_connection_made, "", self.client_port
        )

        await self.handle_pending_items()

        return raft_coro, client_coro

    def assert_pending_queues_empty(self):
        if len(self.pending_timers) != 0 or len(self.pending_messages) != 0:
            print(
                f"expecting empty pending queues, got {len(self.pending_timers)} timers and {len(self.pending_timers)} messages"
            )
            return
        # The pending queues should be empty.
        assert len(self.pending_timers) == 0, repr(self.pending_timers)
        assert len(self.pending_messages) == 0, repr(self.pending_messages)

    # A message came in from a client.  It needs to get a response.
    async def process_client_request_and_wait_for_response(self, request_msg):
        # Create a queue that we're going to wait on.  This becomes the
        # context that's returned when we receive a response to
        # this.

        queue = asyncio.Queue(maxsize=1)

        # We're getting ready to call into the server, there should be no
        # pending work items.
        self.assert_pending_queues_empty()

        # Yes: process the client message, and eventually generate a response,
        # which will get put on the queue.

        # We need to allocate the luid because on_client_recv is going to
        # directly send a redirect back, if need be.  And client_luid_map needs
        # to already be populated before then.
        luid = self.client_luid()
        self.client_luid_map[luid] = queue
        self.on_client_recv(request_msg, queue, luid=luid)

        # Items might have been scheduled.
        await self.handle_pending_items()

        # Wait on the response to show up on the queue.  This is where Raft
        # makes sure the message is committed.
        response_msg = await queue.get()

        # We don't need the queue any more.
        del self.client_luid_map[luid]

        return response_msg

    async def client_connection_made(self, reader, writer):
        # An incoming message from a client.  This is the callback from the
        # server's client_socket.listen() (conceptually).

        # HACK: Set the task name.  There's no way to do this before actually
        # getting a connection.
        asyncio.current_task().set_name("client listen")

        # Read a message from the TCP connection.
        request = messages.decode(await asyncio_rpc.receive(reader))

        # We don't need peer_addr, except for logging.
        peer_addr = writer.get_extra_info("peername")

        if self.debug_print_client_messages:
            print(f"incoming client message {request}")
        self.stats.client_message_reads += 1

        # Process the client message, get the response.
        response = await self.process_client_request_and_wait_for_response(request)

        # Write the response back on the same TCP connection.
        if self.debug_print_client_messages:
            print(f"outgoing client message {response}")
        self.stats.client_message_writes += 1
        await asyncio_rpc.send(writer, response.serialize())

        # We're now done with the connection.
        writer.close()
        await writer.wait_closed()

    async def raft_connection_made(self, reader, writer):
        # An incoming message from a peer server.  This is the callback from
        # the server's socket.listen() (conceptually).

        # HACK: Set the task name.  There's no way to do this before actually
        # getting a connection.
        asyncio.current_task().set_name("raft listen")

        # Read a message from the TCP connection.
        message = messages.decode(await asyncio_rpc.receive(reader))

        # We don't need peer_addr, except for logging.
        peer_addr = writer.get_extra_info("peername")
        if self.debug_print_server_messages:
            print(f"incoming server message {message}")
        self.stats.server_message_reads += 1

        # This is a message from a peer server.  Since there's no response that
        # needs to be sent over the same TCP connection, close the connection.
        # Any actual response to this message can just be sent on a different
        # connection.  (Or the connections can be cached, someday.)

        writer.close()
        await writer.wait_closed()

        # We're getting ready to call into the server, there should be no
        # pending work items.
        self.assert_pending_queues_empty()

        self.on_server_recv(message)

        # Items might have been scheduled.
        await self.handle_pending_items()

    def send_client_response(self, response, request_luid):
        # print(f"queuing client response {response=} {request_luid}")
        client_wait_queue = self.client_luid_map[request_luid]
        # print(f"client queue {client_wait_queue=}")
        self.pending_messages.append((response, client_wait_queue, request_luid))
        return client_wait_queue

    def send_server_request(self, request, destination):
        envelope_msg, luid = self.package_server_request(request, destination)
        self.pending_messages.append((envelope_msg, destination, luid))
        return luid

    def send_server_response(self, response, request_luid):
        (
            envelope_msg,
            response_luid,
            request_envelope,
            recv_context,
        ) = self.package_server_response(response, request_luid)
        self.pending_messages.append(
            (envelope_msg, request_envelope.requestor, response_luid)
        )
        return response_luid

    async def handle_pending_items(self):
        await self.create_pending_timers()
        await self.send_pending_messages()
        # await asyncio.gather(self.send_pending_messages(), self.create_pending_timers())

        self.assert_pending_queues_empty()

    def remove_expired_timer(self, luid):
        del self.timers[luid]

    async def create_pending_timers(self):
        # Actually create the pending timers.

        # Popping off the deque isn'r really needed because we don't await
        # anything, but it's done this way to be similar to
        # send_pending_messages.

        while True:
            try:
                interval, callback, luid = self.pending_timers.popleft()
            except IndexError:
                break

            timer = Timer(
                interval,
                callback,
                luid,
                when_done=self.remove_expired_timer,
                handle_pending_items=self.handle_pending_items,
            )
            self.timers[luid] = timer

            asyncio.create_task(timer.run(), name=luid)

    async def send_pending_messages(self):
        # Actually send all of our pending messages.

        # Iterate by popping off the deque, because new entries can be added
        # when we await.

        msg_awaitables = []

        while True:
            try:
                envelope_msg, peer_id, luid = self.pending_messages.popleft()
            except IndexError:
                break

            if isinstance(envelope_msg, messages.ClientResponse):
                # peer_id is the queue that the client is waiting on.  Put the
                # message into that queue, which will cause it to be sent back
                # to the client.
                if self.debug_print_client_messages:
                    print(f"outgoing client message {luid} {peer_id} {envelope_msg}")
                queue = peer_id  # Ugh.
                msg_awaitables.append(queue.put(envelope_msg))
            else:
                assert isinstance(
                    envelope_msg,
                    (messages.ServerRequestEnvelope, messages.ServerResponseEnvelope),
                ), f"expected server message, got {envelope_msg=}"
                if self.debug_print_server_messages:
                    print(f"outgoing server message {luid=} {peer_id=} {envelope_msg=}")

                peer = self.peers[peer_id]
                self.stats.server_message_writes += 1

                msg_awaitables.append(peer.send(envelope_msg))

        # Send all of the messages in parallel.
        await asyncio.gather(*msg_awaitables)

    def time(self):
        return time.time()

    def set_timer(self, interval, callback, luid):
        self.pending_timers.append((interval, callback, luid))
        return luid

    def cancel_timer(self, luid):
        timer = self.timers.get(luid)
        if timer is None:
            return

        timer.cancel()


@dataclass
class Console:
    driver: object
    exit: bool = field(init=False, default=False)

    async def main(self):
        def state_name():
            # Ick.
            return (
                str(type(self.driver.server.state)).rpartition(".")[2].partition("'")[0]
            )

        apl = appeal.Appeal()

        @apl.command()
        def quit():
            self.exit = True

        @apl.command()
        def uptime():
            print(self.driver.stats.start_time.time_since())

        @apl.command()
        def log():
            print(f"{len(self.driver.server.log.entries)} log entries:")
            for idx, entry in enumerate(self.driver.server.log.entries):
                print(f"{idx:3} {entry}")

        @apl.command()
        def tasks():
            for idx, task in enumerate(asyncio.all_tasks()):
                print(f"{idx:<3}: {task.get_name()}")
                print(task)

        @apl.command(name='client-msgs')
        def client_msgs(enable: appeal.validate("on", "off") = "on"):
            self.driver.debug_print_client_messages = True if enable == "on" else False

        @apl.command(name='server-msgs')
        def server_msgs(enable: appeal.validate("on", "off") = "on"):
            self.driver.debug_print_server_messages = True if enable == "on" else False

        @apl.command(name='client-queue')
        def client_queue():
            pprint.pprint(self.driver.client_luid_map)

        @apl.command()
        def pend():
            print(f"{len(self.driver.pending_messages)} pending messages:")
            for idx, o in enumerate(self.driver.pending_messages):
                print(f"{idx:3} {o}")
            print()
            print(f"{len(self.driver.pending_timers)} pending timers:")
            for idx, o in enumerate(self.driver.pending_timers):
                print(f"{idx:3} {o}")

        @apl.command()
        def timers():
            for idx, timer in enumerate(self.driver.timers.values()):
                print(f"{idx:3}: {timer}")

        @apl.command()
        def stats():
            pprint.pprint(self.driver.stats)

        @apl.command()
        def app():
            pprint.pprint(self.driver.server.application)

        @apl.command(name='new-state')
        # Heavy-handedly set the next state.
        def new_state(
            next_state: appeal.validate("follower", "candidate", "leader") = None
        ):
            if next_state:
                # Figure out the next state based on the passed in string.
                if next_state == "follower":
                    next = self.driver.server.Follower
                elif next_state == "candidate":
                    next = self.driver.server.Candidate
                elif next_state == "leader":
                    next = self.driver.server.Leader
                else:
                    raise ValueError(f"unknown next_state {next_state}")
            else:
                # Figure out the next state based on the current state.  See
                # the raft paper, firgure 4 for why this order was chosen.
                if isinstance(self.driver.server.state, self.driver.server.Follower):
                    next = self.driver.server.Candidate
                elif isinstance(self.driver.server.state, self.driver.server.Candidate):
                    next = self.driver.server.Leader
                elif isinstance(self.driver.server.state, self.driver.server.Leader):
                    next = self.driver.server.Follower
                else:
                    raise ValueError(f"unknown state {type(self.driver.server.state)}")
            self.driver.server.state = next()

        while not self.exit:
            prompt = (
                f"[{self.driver.name}:{state_name()}:{self.driver.server.term}] cmd: "
            )
            cmd = (await aioconsole.ainput(prompt)).strip()
            if not cmd:
                # They just pressed enter, loop around.
                continue

            args = cmd.split()

            try:
                apl.main(args)
            except SystemExit as ex:
                print(ex)
                continue

            print()


async def amain(
    nodenum: int,
    server_msgs,
    client_msgs,
    heartbeat_interval,
    election_timeout_interval_start,
    election_timeout_interval_range,
):
    server_addr = raftconfig.servers[nodenum]

    # Start up the application and server.  We implement the driver above.

    server = Server(
        None,
        servers=raftconfig.servers,
        id=nodenum,
        heartbeat_interval=heartbeat_interval,
        election_timeout_interval_start=election_timeout_interval_start,
        election_timeout_interval_range=election_timeout_interval_range,
    )

    asyncio_driver = AsyncioDriver(
        name=f"server {nodenum}",
        peers={
            idx: RaftPeer(idx, addr)
            for idx, addr in enumerate(raftconfig.servers)
            if idx != nodenum
        },
        server=server,
        raft_server_port=server_addr.port,
        client_port=server_addr.client_port,
    )

    # Set any command line options before we start running.
    asyncio_driver.debug_print_server_messages = server_msgs
    asyncio_driver.debug_print_client_messages = client_msgs

    raft_coro, client_coro = await asyncio_driver.run()

    console_task = asyncio.create_task(Console(asyncio_driver).main(), name="Console")

    # Await the known tasks.
    await asyncio.gather(console_task, raft_coro, client_coro)


import appeal

app = appeal.Appeal()


@app.global_command()
def main(
    nodenum: int,
    *,
    server_msgs=False,
    client_msgs=False,
    heartbeat_interval=5,
    election_timeout_interval_start=6,
    election_timeout_interval_range=1,
):
    asyncio.run(
        amain(
            nodenum,
            server_msgs,
            client_msgs,
            heartbeat_interval,
            election_timeout_interval_start,
            election_timeout_interval_range,
        )
    )


app.main()
