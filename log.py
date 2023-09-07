#!/usr/bin/env python3

import dataclasses
from dataclasses import dataclass, field
import messages
import msgpack
import packraft
import pathlib
import sys
import zlib


def _next_id():
    i = 0
    while True:
        yield i
        i += 1

next_id = _next_id().__next__

@dataclass
class LogEntry(packraft.Message):
    term: int
    request: messages.ClientRequest

    def log_serialize(self):
        l = self.request.log_serialize()
        l.append(self.term)
        return l

    @classmethod
    def log_deserialize(cls, l):
        term = l.pop()
        request = messages.ClientRequest.log_deserialize(l)
        return cls(term, request)


NETWORK_BYTE_ORDER = 'big' # also the name of a wonderful PyPI package! check it out!

@dataclass
class Log:
    directory: pathlib.Path
    entries: list[LogEntry] = field(default_factory=list)
    # lock: threading.Lock

    def __post_init__(self):
        self.log_path = self.directory / "log.data"
        self.deserialize()

    def __getitem__(self, i):
        return self.entries[i]

    def __len__(self):
        return len(self.entries)

    def append(self, entry):
        """
        This "append" call is used by the Leader to append
        individual client requests to its log.  See
        "append_entries" for the function used by a Follower
        to respond to AppendEntries RPC messages.
        """
        self.entries.append(entry)

    def serialize(self):
        with self.log_path.open("wb") as f:
            blobs = []
            length = 0
            for i, entry in enumerate(self.entries):
                o = entry.log_serialize()
                b = msgpack.dumps(o)

                crc32 = zlib.crc32(b)
                network_crc32 = crc32.to_bytes(4, NETWORK_BYTE_ORDER)

                length_header = packraft.compute_length_header(b)

                blobs.append(network_crc32)
                length += len(network_crc32)
                blobs.append(length_header)
                length += len(length_header)
                blobs.append(b)
                length += len(b)

            serialized_log = b''.join(blobs)
            print(f"serialized_log = {serialized_log!r}")
            f.write(serialized_log)

    def deserialize(self):
        if not self.log_path.exists():
            return
        self.entries.clear()
        with self.log_path.open("rb") as f:
            print("just opened", self.log_path)
            while True:
                network_crc32 = f.read(4)
                if not network_crc32:
                    break
                print(f"{network_crc32=}")
                stored_crc32 = int.from_bytes(network_crc32, NETWORK_BYTE_ORDER)
                length = packraft.length_header_from_stream(f)
                b = f.read(length)
                computed_crc32 = zlib.crc32(b)
                if computed_crc32 != stored_crc32:
                    sys.exit(f"Log corrupt: Entry {len(self.entries)} has mismatching CRC32 (want {hex(stored_crc32)[2:]}, got {hex(computed_crc32)[2:]})")
                o = msgpack.loads(b)
                print("LOADED", o, "FROM", repr(b))
                entry = LogEntry.log_deserialize(o)
                self.entries.append(entry)

    def append_entries(self, previous_index, previous_term, entries):
        """
        This "append_entries" is used by a Follower
        to respond to AppendEntries RPC messages.
        See "append" for the function used by the Leader
        to append individual client requests to its log.
        """
        length = len(self.entries)
        if previous_index == -1:
            # if we have entries,
            # we're about to increment,
            # so this means we'll start checking at 0
            pass
        elif ((length > previous_index) and (self.entries[previous_index].term == previous_term)):
            pass
        else:
            return False

        # it worked, we are returning true.
        # incorporate entries starting at entries[previous_index + 1].
        for entry in entries:
            previous_index += 1
            if length > previous_index:
                previous_entry = self.entries[previous_index]
                if previous_entry.term == entry.term:
                    assert previous_entry == entry, f'{previous_entry=}!={entry=}'
                    continue
                # mismatch!  truncate list here and start appending.
                self.entries = self.entries[:previous_index]
                length = -1
            self.entries.append(entry)

        return True


class CommittedState:
    # _index:int = field(default=-1, init=False)  # index into Log
    _index = -1

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    def __init__(self, index=-1):
        self.index = index

    def copy(self):
        return CommittedState(self.index)
