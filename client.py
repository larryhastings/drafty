import sys
import asyncio

import asyncio_rpc
import messages
import raftconfig

import appeal


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


def run(server_id, request, timeout):
    return asyncio.run(send_receive(server_id, request, timeout))


@app.command()
def ping(server_id: int, *, timeout=DEFAULT_TIMEOUT):
    print(run(server_id, messages.ClientPingRequest("hello"), timeout))


@app.command()
def put(server_id: int, key, value, *, timeout=DEFAULT_TIMEOUT):
    print(run(server_id, messages.ClientPutRequest(key, value), timeout))


@app.command()
def get(server_id: int, key, *, timeout=DEFAULT_TIMEOUT):
    print(run(server_id, messages.ClientGetRequest(key), timeout))


app.main()
