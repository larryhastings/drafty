import asyncio

async def send(writer, msg):
    # Pre-pend the message length for framing.
    # TODO: error checking on the message length.
    writer.write(b"%8d" % len(msg) + msg)
    await writer.drain()

async def receive(reader, *, timeout=None):
    # For framing, read the length, then the message body.
    async with asyncio.timeout(timeout):
        n_str = await reader.readexactly(8)
        msg = await reader.readexactly(int(n_str.decode()))

    return msg
