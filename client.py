#!/usr/bin/env python3

import appeal
import asyncio
import asyncio_rpc
import base64
import messages
import raftconfig
import random
import sys
import uuid


class TooManyRedirectsError(Exception):
    pass


async def send_msg_to_server(server_id, msg, timeout):
    server_addr = raftconfig.servers[server_id]
    reader, writer = await asyncio.open_connection(server_addr.host, server_addr.client_port)

    await asyncio_rpc.send(writer, msg.serialize())

    msg = messages.decode(await asyncio_rpc.receive(reader, timeout=timeout))

    writer.close()
    await writer.wait_closed()

    return msg


# Put some limit on the number of redirects we'll follow.  The leader could be
# changing while we're making our request, but at some point something must be
# wrong.

MAX_REDIRECTS = 10


async def send_msg_follow_redirects(server_id, msg, timeout):
    num_redirects = 0
    while True:
        response = await send_msg_to_server(server_id, msg, timeout)

        # Was this a redirect?
        if isinstance(response, messages.ClientRedirectResponse):
            num_redirects += 1
            if num_redirects > MAX_REDIRECTS:
                raise TooManyRedirectsError(f"{num_redirects}: raft error?")

            server_id = response.leader_id
            if server_id == None:
                sys.exit("giving up, no servers available.  (is your network down?)")
            print(f"redirecting to {server_id}")
            continue

        return response


async def send_receive(server_id, request, timeout):
    try:
        return await send_msg_follow_redirects(server_id, request, timeout)
    except asyncio.TimeoutError:
        return "timeout"


app = appeal.Appeal()

DEFAULT_TIMEOUT = 60

def generate_guid():
    """
    Returns a bytes object of length 16,
    containing effectively-random and
    hopefully-unique bytes.
    """
    # Is uuid1 what we want?
    # I usually use uuid4, but it turns out
    # that one is *totally random*.  uuid1
    # incorporates like local MAC address and stuff
    # and seems to have a higher chance of being
    # genuinely globally unique.
    guid = uuid.uuid1()
    guid = str(uuid.uuid1())
    guid = guid.replace('-', '')
    guid = base64.b16decode(guid, casefold=True)
    return guid

def run(server_id, request, timeout):
    return asyncio.run(send_receive(server_id, request, timeout))


@app.command()
def ping(server_id: int, *, guid='', timeout=DEFAULT_TIMEOUT):
    guid = guid or generate_guid()
    print(run(server_id, messages.ClientPingRequest(guid, "hello"), timeout))


@app.command()
def put(server_id: int, key, value, *, guid='', timeout=DEFAULT_TIMEOUT):
    guid = guid or generate_guid()
    print(run(server_id, messages.ClientPutRequest(guid, key, value), timeout))


@app.command()
def putrnd(server_id: int, *, guid='', timeout=DEFAULT_TIMEOUT):
    letters = "bcdfghjklmnpqrstvwxz"
    vowels = "aeiouy"
    key = "".join((random.choice(letters), random.choice(vowels), random.choice(letters),))
    value = str(int(random.random() * 1000))
    print(f"put {key}={value}")
    guid = guid or generate_guid()
    print(run(server_id, messages.ClientPutRequest(guid, key, value), timeout))

@app.command()
def get(server_id: int, key, *, guid='', timeout=DEFAULT_TIMEOUT):
    guid = guid or generate_guid()
    print(run(server_id, messages.ClientGetRequest(guid, key), timeout))


app.main()
