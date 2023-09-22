#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

"""
PackRaft
========

    Copyright 2023 by Larry Hastings

    A gloriously simple serialization library for RPC messages.

Quickstart
==========

    from dataclasses import dataclass
    import packraft
    import socket

    @dataclass # important!
    class MyMessage(packraft.Message):
        message: str
        request_id: int

    r, w = socket.socketpair()
    msg = MyMessage("Hello, networked chum", 33)
    msg.send(w)

    msg2 = packraft.Message.recv(r)
    assert msg2 == msg


Overview
========

    PackRaft is a serialization library for networked
    messages.  It allows you to seamlessly serialize a
    Python object and send it over a socket (or store
    it in a file, etc), then deserialize it and recreate
    an identical object.

    Unlike pickle, PackRaft doesn't sidestep normal
    object creation, and it doesn't root around in your
    modules to find your classes.  Instead, there's
    an automatic (but explicit) registration mechanism
    for classes that can be serialized and unserialized.

    Note that PackRaft requires the serializer and
    deserializer to have *identical* registered classes.

    The actual serialization is done using MessagePack.
    PackRaft first serializes your object into a
    MessagePack-supported object, usually a dict. This is
    called "packing" the object.  PackRaft then serializes
    that object using MessagePack, prefixes the resulting
    bytes object with metadata (protocol id, length)
    so it can be correctly read on the receiving side,
    and writes it to the file-like object.

    On the receiving side, once PackRaft has received
    a complete message, it unserializes the bytes object
    with MessagePack, then "unpacks" that object using
    the various PackRaft-registered classes, and finally
    returns the recreated object to you.

    PackRaft 1.0 doesn't try very hard to reduce the
    size of its messages.  It's really just a toy,
    written for use in David Beazley's "Rafting Trip"
    class.

Using PackRaft
==============

    There are just a couple of rules.

    * All the objects you serialize should either be
      supported directly by MessagePack, or should be
      subclasses of a class that inherits from
      packraft.Packable, or serialized with a
      packraft.Packer class.
        * Every class that inherits from either
          packraft.Packable or packraft.Packer must
          have a globally unique name.
    * Most of your high-level messages should be subclasses
      of packraft.Message, which handles packing and
      unpacking for you.  *Every* subclass of
      packraft.Message *must* be decorated with @dataclass,
      and every attribute defined on the subclass should
      have an annotation with the type of the value.
      Annotations should be either
        * one of the types natively supported by
          MessagePack:
            * None
            * bool
            * int
            * float
            * str
            * bytes
            * dict
        * a subclass of Packable,
        * a type registered as being handled by a Packer,
        * or list[X] or tuple[X] where X is one of
          the previous three options.
    * If you don't want to use @dataclass, you should instead
      subclass packraft.Packable.  You'll have to implement
      your own pack() and unpack() methods.
    * If you want to add support for packing and unpacking
      a third-party type, you can do that by creating a
      subclass of packraft.Packer with custom pack() and
      unpack() methods, and registering it as a handler for
      that third-party type with the @packraft.packs() decorator.
    * Most of the time, you should send a message by
      calling its "send" method, and you should receive
      messages by calling the packraft.Message class
      method "recv". Both take a file-like object
      (e.g. a socket), both are blocking.

"""


##
## To test coverage:
##     % coverage run --parallel-mode packraft.py ; coverage run --parallel-mode packraft.py -v ; coverage combine ; coverage html -i
##
## Then examine htmlcov/index.html:
##     % xdg-open htmlcov/index.html
##

import abc
import dataclasses
from dataclasses import dataclass
import datetime
import inspect
import msgpack
import packraft
import socket
import sys
import time
import types


__all__ = []


def export(o):
    __all__.append(o.__name__)
    return o


##
## low-level wire protocol stuff
##

PROTOCOL_IDENTIFIER = b"\n01\n"


@export
class UnsupportedProtocolError(ValueError):
    pass


#
# The PackRaft wire format
# ------------------------
#
# Let's say we want to send an object o, which represents a message,
# using the PackRaft wire format.  We proceed as follows:
#
# First, we need a "protocol".   This is a bytes string that will
#    appear at the beginning of *every* message.  We'll call our
#    bytes string PROTOCOL.
#
#    The protocol string should be at least four bytes, though really
#    anything will work.  If PackRaft gets a message that doesn't
#    start with the correct protocol, it throws an exception,
#    which will probably (hopefully!) stop processing.
#
# Next we serialize o using MessagePack, let's call the resulting bytes object B.
#    If o doesn't serialize properly, MessagePack will throw an exception.
#    (The rest of this file handles converting Python objects into
#    MessagePack-friendly equivalents, a process called "packing".)
#
# At last we're ready to build our wire format message.  There are actually
# two different formats; one is used for small messages, the other for larger
# messages.
#
# If len(B) <= 127, the wire format for this message is
#    PROTOCOL
#    len(B) (a single unsigned byte)
#    B
# and the total length of the message is len(PROTOCOL) + 1 + len(B).
#
# If len(B) >= 128, then:
#    We serialize len(B) using MessagePack, let's call the resulting bytes object L.
#    The wire format for this message is
#        PROTOCOL
#        len(L) | 0x80 (a single unsigned byte),
#        L
#        B
# and the total length of the message is len(PROTOCOL) + 1 + len(L) + len(B).
#
# I regret to inform you that this protocol has a maximum message
# length of
#    702,223,880,805,592,151,456,759,840,151,962,786,569,522,
#        257,399,338,504,974,336,254,522,393,264,865,238,137,
#        237,142,489,540,654,437,582,500,444,843,247,630,303,
#        354,647,534,431,314,931,612,685,275,935,445,798,350,
#        655,833,690,880,801,860,555,545,317,367,555,154,113,
#        605,281,582,053,784,524,026,102,900,245,630,757,473,
#        088,050,106,395,169,337,932,361,665,227,499,793,929,
#        447,186,391,815,763,110,662,594,625,536 bytes.
#
# (That's 256**127 bytes.)
#
# If you attempt to encode a message that's too large,
# you'll get a ValueError.
#
# p.s. just kidding, MessagePack would blow up first.
# But!  In the far-flung future, When MessagePack supports
# 1024-bit pointers, sadly you'll finally hit this limit.
#

@export
def compute_length_header(b):
    length = len(b)
    length_msg = msgpack.dumps(length)
    if length < 0x80:
        return length_msg
    length_length = len(length_msg)
    assert length_length <= 0x7f
    length_length_msg = bytes((length_length | 0x80,)) + length_msg
    return length_length_msg

@export
def length_header_from_bytes(b):
    length = b[0]
    if not (length & 0x80):
        return length, b[1:]

    length_end = (length & 0x7F) + 1
    length_msg = b[1:length_end]
    length = msgpack.loads(length_msg)
    return length, b[length_end:]

@export
def length_header_from_stream(f):
    length = f.read(1)[0]
    if length < 0x80:
        return length
    length_msg = f.read(length & 0x7F)
    length = msgpack.loads(length_msg)
    return length

@export
def serialize(o, protocol=None):
    """
    Convert o into the PackRaft wire format
    representation of o.

    o should be a object that MessagePack can serialize.
    If the object can't be serialized using MessagePack,
    serialize raises an error.

    protocol should be a bytes object or None.
    If None, defaults to PROTOCOL_IDENTIFIER.
    """
    if protocol is None:
        protocol = PROTOCOL_IDENTIFIER
    msg = msgpack.dumps(o)
    length_msg = compute_length_header(msg)
    return protocol + length_msg + msg


@export
def deserialize(b, *, protocol=None):
    """ """
    if protocol is None:
        protocol = PROTOCOL_IDENTIFIER

    def recv(l):
        nonlocal b
        result = b[:l]
        b = b[l:]
        return result

    received_protocol = recv(len(protocol))
    if received_protocol != protocol:
        raise UnsupportedProtocolError(
            f"wrong protocol bytes {received_protocol!r}, don't match {protocol!r}"
        )

    length, b = length_header_from_bytes(b)
    msg = recv(length)
    d = msgpack.loads(msg)
    # assert isinstance(d, dict)
    return d


@export
def send(f, o, *, protocol=None):
    """
    Write o to file-like object f.
    o is serialized using the PackRaft wire protocol.

    o should be a object that MessagePack can serialize.
    If the object can't be serialized using MessagePack,
    send raises an error.

    protocol should be a bytes object or None.
    If None, defaults to PROTOCOL_IDENTIFIER.
    """
    f.send(serialize(o, protocol))


@export
def recv(f, *, protocol=None):
    """
    Reads a MessagePack-serialized object o from
    the file-like object f.  Deserializes and returns o.

    protocol should be a bytes object or None.
    If None, defaults to PROTOCOL_IDENTIFIER.

    If the data read from f doesn't start with
    the protocol bytes, or the object can't be
    read or deserialized, recv raises an error.
    """
    if protocol is None:
        protocol = PROTOCOL_IDENTIFIER
    received_protocol = f.recv(len(protocol))
    if received_protocol != protocol:
        raise UnsupportedProtocolError(
            f"wrong protocol bytes {received_protocol!r}, don't match {protocol!r}"
        )

    length = length_header_from_stream(f)
    msg = f.recv(length)
    d = msgpack.loads(msg)
    # assert isinstance(d, dict)
    return d


##
## the rest of the file is high-level Packable / Packer stuff
##


@export
class PackingError(ValueError):
    pass


# whitelist of types we'll let msgpack encode.
# if a type isn't on this list, and it's not in
# _cls_to_packer or a subclass of Packable,
# we'll raise early so you can deal with it.
msgpack_types = {
    type(None),
    bool,
    int,
    float,
    str,
    bytes,
    dict,
}


_cls_to_packer = {}
_packable_cls_to_name = {}
_packable_name_to_cls = {}


@export
def packs(cls):
    """
    Decorator for subclasses of Packer.
    Pass in the type that your packer packs.

    e.g.
        @packs(complex)
        class ComplexPacker(Packer):
            ...
    """

    def packs(packer):
        if not issubclass(packer, Packer):
            raise RuntimeError(
                f"can't decorate {packer} with @packs, it's not a subclass of Packer"
            )
        _cls_to_packer[cls] = packer
        return cls

    return packs


@export
def is_packable(o):
    """
    Returns True if we know how to pack object o, False otherwise.

    Note that o still may not be packable, if it's a container object
    and one of the objects inside isn't packable.  (is_packable
    only examines o itself, not the objects inside o.)
    """
    return isinstance(o, Packable) or (type(o) in _cls_to_packer)


@export
def packed(o):
    """
    Helper function that packs o as necessary.
    Returns a "packed" version of the value o.
    A "packed" value is a value that can be successfully
    serialized by MessagePack.
    If o is already of an acceptable type, returns o unchanged.
    """
    if isinstance(o, Packable):
        # print(f">> pack {o=} {o.pack()=}")
        return o.pack()
    t = type(o)
    packer = _cls_to_packer.get(t)
    if packer:
        # print(f">> packer {o=} {packer=} {packer.pack(o)=}")
        return packer.pack(o)
    if not t in msgpack_types:
        raise TypeError(f"don't know how to pack object of type {type(o)}, o={o!r}")
    # print(f">> pack native {o=}")
    return o


@export
def is_unpackable(o):
    """
    Returns True if we know specifically how to unpack object o,
    False otherwise.  An object o is considered unpackable if it's
    a dict, and o['class'] is the name of a registered Packable
    or Packer class.

    Note that o still may not be unpackable, if it represents a
    container object and one of the objects inside isn't unpackable.
    (is_unpackable only examines o itself, not the objects
    potentially stored inside o.)
    """
    return isinstance(o, dict) and ("class" in o)


@export
def unpacked(o, converter=None):
    """
    Helper function that unpacks a value as necessary.
    Returns o, or potentially an unpacked copy of o.

    If o is a packed object, returns the unpacked version of o.
    If o is a list, returns a new list where every element is unpacked.
    Otherwise returns o unchanged.
    """
    if isinstance(o, dict) and ("class" in o):
        cls_name = o["class"]
        cls = _packable_name_to_cls.get(cls_name)
        if not cls:
            raise PackingError(
                f"Can't unpack dict, class '{cls_name}' is unregistered, dict={o}"
            )
        return cls.unpack(o)
    elif isinstance(o, list):
        return [unpacked(v) for v in o]
    if converter:
        # I admit, it's a slight hack.
        if (converter in {int, float, bool, str}) and (o == None):
            return None
        return converter(o)
    return o


class RegisteredPackableMeta(type):
    """
    Base class for Packable and Packed.
    Handles registering new subclasses
    so the packing machinery knows how to
    unpack those objects by name.
    """

    def __new__(cls, name, parents, dct):
        c = super().__new__(cls, name, parents, dct)
        if name in _packable_name_to_cls:
            raise RuntimeError(
                f"can't register a second Packable class {c} called '{name}', we already have {_packable_name_to_cls[name]}"
            )
        assert c not in _packable_cls_to_name
        _packable_cls_to_name[c] = name
        _packable_name_to_cls[name] = c
        return c


@export
class Packable(metaclass=RegisteredPackableMeta):
    """
    Abstract base class for Packable objects.
    Packable objects know how to pack and unpack
    themselves.

    The verbs "pack" and "unpack" here specifically
    mean conversion to and from a Python object that
    can be serialized using MessagePack.

    Let's say T is a subclass of Packable,
    and O is an instance of T.  First, you pack O
    by calling O.pack().  This returns an object O2.
    You can then call msgpack.dumps(O2), which will
    return a bytes string B, suitable for writing
    to a socket.

    Later, perhaps in another process, you call
    msgpack.loads(B), which returns (an identical
    copy of) O2. You can now call T.unpack(O2),
    which returns (an identical copy of) O.

    Instead of calling these pack and unpack
    methods directly, you probably want to call
    packraft.packed(O) and packraft.unpacked(O2).
    These helper functions handle looking up T
    automatically as well as other details.
    """

    @abc.abstractmethod
    def pack(self):
        """
        Return a packed version of self.
        """
        raise RuntimeError(
            f"called abstract Packable.pack, self={self}, cls={type(self)}"
        )

    @classmethod
    @abc.abstractmethod
    def unpack(cls, d):
        """
        Class method.  Return a new instance of cls initialized using the values in d.
        """
        raise RuntimeError(f"called abstract Packable.unpack, cls={cls}, d={d!r}")


@export
class Packer(metaclass=RegisteredPackableMeta):
    """
    Abstract base class for Packer classes.
    A Packer class knows how to pack and unpack
    some other kind of object.  For example,
    you might write a Packer for the Python
    'complex' type.  'complex' objects don't know
    how to "pack" and "unpack" themselves; your
    subclass of Packer would handle packing and
    unpacking them.

    The verbs "pack" and "unpack" here specifically
    mean conversion to and from a Python object that
    can be serialized using MessagePack.

    Let's say T is a subclass of Packer, and O is
    an instance of a type S that T knows how to pack
    and unpack.  When you define class T, you decorate it
    with @packraft.packs(S).

    To write O to a socket, first you pack O by calling
    T.pack(O).  This returns an object O2. You then
    call msgpack.dumps(O2), which will return a
    bytes string B, suitable for writing to a socket.

    Later, perhaps in another process, you call
    msgpack.loads(B), which returns (an identical
    copy of) O2.  You can now call T.unpack(O2),
    which returns (an identical copy of) O.

    Instead of calling these pack and unpack
    methods directly, you probably want to call
    packraft.packed(O) and packraft.unpacked(O2).
    These helper functions handle looking up T
    automatically as well as other details.
    """

    @classmethod
    @abc.abstractmethod
    def pack(cls, o):
        """
        Class method.  Return a packed version of o.
        """
        raise RuntimeError(f"called abstract Packer.pack, cls={cls}, o={o!r}")

    @classmethod
    @abc.abstractmethod
    def unpack(cls, d):
        """
        Class method.  Return a new instance of some type initialized using the values in d.
        """
        raise RuntimeError(f"called abstract Packer.unpack, cls={cls}, d={d!r}")


@packs(tuple)
class PackedTuple(Packer):
    """
    Packer for tuple objects.

    MessagePack doesn't differentiate between list and tuple.
    If you use MessagePack to serialize a tuple,
    then deserialize that bytes object, you'll actually get back a list.
    This Packer ensures that a tuple emerges from the roundtrip as a tuple.
    """

    @classmethod
    def pack(cls, o):
        return {"class": cls.__name__, "value": [packed(v) for v in o]}

    @classmethod
    def unpack(cls, d):
        value = d["value"]
        return tuple(unpacked(v) for v in value)


# I do this just so I don't have to special-case packing/unpacking
# lists in unpacked().  This way was way easier.
@packs(list)
class PackedList(Packer):
    """
    Packer for list objects.

    Note that unpacking lists isn't done here,
    it's done in packraft.unpacked().
    """

    @classmethod
    def pack(cls, o):
        return [packed(v) for v in o]

    @classmethod
    def unpack(cls, d):
        # this can never get called.
        # we don't return a dict with a 'class' key in our pack method.
        # instead, unpacking lists is a special case in unpacked().
        raise RuntimeError("unreachable code")


@export
class Timestamp(Packable):
    """
    A high-precision timestamp object that can be packed/unpacked.
    Supports rich comparisons.

    Supports simple time math, using either two TimeStamps,
    or one TimeStamp and one int representing a delta:
        ts - ts2   -> delta
        ts - delta -> ts2
        ts + delta -> ts2
    """

    def __init__(self, t=None):
        if t == None:
            t = time.time_ns()
        self.t = t

    def __repr__(self):
        one_billion = 1_000_000_000
        s = self.t // one_billion
        ns = self.t % one_billion
        t = datetime.datetime.fromtimestamp(s)
        repr = t.strftime("%Y/%m/%d %H:%M:%S.") + str(ns).rjust(9, "0")
        return f'<TimeStamp "{repr}">'

    def pack(self):
        one_billion = 1_000_000_000
        s = self.t // one_billion
        ns = self.t % one_billion
        return {"class": type(self).__name__, "s": s, "ns": ns}

    @classmethod
    def unpack(self, d):
        one_billion = 1_000_000_000
        return Timestamp(d["s"] * one_billion) + d["ns"]

    def __lt__(self, other):
        if not isinstance(other, Timestamp):
            raise TypeError(
                f"'<' not supported between instances of 'Timestamp' and '{type(other)}'"
            )
        return self.t < other.t

    def __le__(self, other):
        if not isinstance(other, Timestamp):
            raise TypeError(
                f"'<=' not supported between instances of 'Timestamp' and '{type(other)}'"
            )
        return self.t <= other.t

    def __eq__(self, other):
        return isinstance(other, Timestamp) and (self.t == other.t)

    def __ge__(self, other):
        if not isinstance(other, Timestamp):
            raise TypeError(
                f"'>=' not supported between instances of 'Timestamp' and '{type(other)}'"
            )
        return self.t >= other.t

    def __gt__(self, other):
        if not isinstance(other, Timestamp):
            raise TypeError(
                f"'>' not supported between instances of 'Timestamp' and '{type(other)}'"
            )
        return self.t > other.t

    def __ne__(self, other):
        return not (isinstance(other, Timestamp) and (self.t == other.t))

    def __add__(self, other):
        if isinstance(other, int):
            return Timestamp(self.t + other)
        raise TypeError(
            f"'-' not supported between instances of 'Timestamp' and '{type(other)}'"
        )

    def __sub__(self, other):
        if isinstance(other, Timestamp):
            return self.t - other.t
        if isinstance(other, int):
            return Timestamp(self.t - other)
        raise TypeError(
            f"'-' not supported between instances of 'Timestamp' and '{type(other)}'"
        )


@export
@dataclass
class Message(Packable):
    """
    Base class for all wire messages.
    Knows how to pack and unpack itself automatically.
    Every subclass *must* be @dataclass.
    """

    def pack(self):
        cls = type(self)

        # undocumented and unsupported, but consistent
        if not "__dataclass_fields__" in cls.__dict__:
            raise TypeError(f"Message subclass {cls} is not an @dataclass")

        d = {"class": cls.__name__}

        for field in dataclasses.fields(self):
            name = field.name
            value = getattr(self, name)
            value = packed(value)
            d[name] = value

        return d

    @classmethod
    def unpack(cls, d):
        annotations = inspect.get_annotations(cls)
        def unchanged(o):
            return o
        def annotation(name):
            return annotations.get(name, unchanged)
        d2 = {key: unpacked(value, annotation(key)) for key, value in d.items() if key != "class"}
        o = cls(**d2)
        return o

    def send(self, f):
        send(f, self.pack())

    @classmethod
    def recv(cls, f):
        d = recv(f)
        assert is_unpackable(d)
        return unpacked(d)

    def serialize(self):
        return serialize(packed(self))

    @classmethod
    def deserialize(cls, b):
        return unpacked(deserialize(b))


if __name__ == "__main__":  # pragma: nocover
    verbose = bool(set(sys.argv) & set(("-v", "--verbose")))

    r = w = None

    def new_sockets():
        global r
        global w
        r, w = socket.socketpair()

    new_sockets()

    if verbose:
        import pprint

    test_count = 0

    ##
    ## tests for low-level wire format stuff
    ##

    def passed():
        global test_count
        test_count += 1
        if verbose:
            print()
        else:
            print(".", end="", flush=True)

    def test(
        d, *, read_protocol=PROTOCOL_IDENTIFIER, write_protocol=PROTOCOL_IDENTIFIER
    ):
        global test_count
        if verbose:
            pprint.pprint(d)
            print(serialize(d, protocol=write_protocol))
        send(w, d, protocol=write_protocol)
        d2 = recv(r, protocol=read_protocol)
        assert d2 == d
        passed()

    def fail(
        d, *, read_protocol=PROTOCOL_IDENTIFIER, write_protocol=PROTOCOL_IDENTIFIER
    ):
        global test_count
        try:
            test(
                {"a": "howdy"},
                read_protocol=read_protocol,
                write_protocol=write_protocol,
            )
            raise RuntimeError("shouldn't get here!")  # pragma: no cover
        except UnsupportedProtocolError as e:
            if verbose:
                print(f"correctly handled unsupported protocol error {e}")
            passed()
            new_sockets()

    test({"a": 3, "b": 44.0})
    test({"a": 3, "b": 44.0, "c": "Hello, nurse!"})
    test({"a": 3, "b": 44.0, "c": "Hello, nurse!", "d": "0123456789abcdef" * 16})
    fail({"a": "howdy"}, read_protocol=b"zqxy")
    fail({"a": "howdy"}, write_protocol=b"zqxy")
    test({"a": "howdy"}, write_protocol=b"zqxy", read_protocol=b"zqxy")

    ##
    ## tests for the high-level Packable / Packed stuff
    ##

    def passed():
        global test_count
        test_count += 1
        if verbose:
            print()
        else:
            print(".", end="", flush=True)

    def test(msg):
        if verbose:
            print(msg)
            d = msg.pack()
            pprint.pprint(d)
            print(serialize(d, PROTOCOL_IDENTIFIER))
        msg.send(w)
        original = msg
        roundtripped = Message.recv(r)
        assert roundtripped == original, f"{roundtripped=} != {original=}"
        passed()

    def fail(msg, exception):
        global test_count
        try:
            test(msg)
            RuntimeError("shouldn't get here!")  # pragma: no cover
        except exception as e:
            if verbose:
                print(f"correctly handled exception {exception} '{str(e)}'")
            passed()

    @dataclass
    class RunElection(Message):
        id: int
        t: Timestamp = dataclasses.field(default_factory=Timestamp)

    test(RunElection(1))

    @dataclass
    class Update(Message):
        key: str
        value: str

    test(Update("koogle", "chocolate"))

    SERVER_TYPE_LEADER = "type:leader"
    SERVER_TYPE_FOLLOWER = "type:follower"

    @dataclass
    class Server(Message):
        name: str
        type: str

    @dataclass
    class ElectionResult(Message):
        id: int
        winner: Server

    test(ElectionResult(15, Server("colony", SERVER_TYPE_LEADER)))
    test(ElectionResult(16, None))

    @dataclass
    class ElectionResult2(Message):
        id: int
        winner: Server
        followers: tuple[Server]

    test(
        ElectionResult2(
            33,
            Server("colony", SERVER_TYPE_LEADER),
            (
                Server("zikzak", SERVER_TYPE_FOLLOWER),
                Server("kewpie", SERVER_TYPE_FOLLOWER),
                Server("tinsel", SERVER_TYPE_FOLLOWER),
                Server("monolith", SERVER_TYPE_FOLLOWER),
                Server("odyssey", SERVER_TYPE_FOLLOWER),
            ),
        )
    )

    test(
        ElectionResult2(
            33,
            Server("colony", SERVER_TYPE_LEADER),
            [
                Server("zikzak", SERVER_TYPE_FOLLOWER),
                Server("kewpie", SERVER_TYPE_FOLLOWER),
                Server("tinsel", SERVER_TYPE_FOLLOWER),
                Server("monolith", SERVER_TYPE_FOLLOWER),
                Server("odyssey", SERVER_TYPE_FOLLOWER),
            ],
        )
    )

    @dataclass
    class UnpackableMessage(Message):
        id: int
        inscrutable_thingy: complex

    fail(UnpackableMessage(88, 2 + 1j), TypeError)

    @packs(complex)
    class PackedComplex(Packer):
        @classmethod
        def pack(cls, o):
            return {"class": cls.__name__, "real": o.real, "imag": o.imag}

        @classmethod
        def unpack(cls, d):
            return complex(d["real"], d["imag"])

    # it should now work!
    test(UnpackableMessage(88, 2 + 1j))

    # we can't define a second Packable class
    # with the same name as an existing class.
    try:

        class PackableComplex(Packable):
            pass

    except RuntimeError:
        passed()

    # WHOOPS WE FORGOT TO DECORATE WITH @dataclass
    # I WONDER WHAT WILL HAPPEN
    class MalformedMessage(UnpackableMessage):
        new_attribute: float = dataclasses.field(default=0.0)

    fail(MalformedMessage(99, 3), TypeError)

    ###########################

    if not verbose:
        print()
    plural = "s" if test_count != 1 else ""
    print(f"All {test_count} test{plural} passed.")
