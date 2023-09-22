#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import collections
import dataclasses
from dataclasses import dataclass, field
from lru import LRU
import messages
import msgpack
import packraft
import pathlib
import sys
import zlib


MAX_LOG_SIZE = 1<<20 # one megabyte
# fake tiny size to test using multiple files
# MAX_LOG_SIZE = 48

GUID_CACHE_SIZE = 1<<20

def manufactured_field(cls, **kwargs):
    return field(init=False, default_factory=cls, **kwargs)



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


@dataclass
class Log:
    directory: pathlib.Path
    entries: list[LogEntry] = manufactured_field(list)
    # maps request.id (which is a GUID) -> log index of request
    # Note that all entries in the log get guid_cache cached,
    # whether they're committed or not.
    guid_cache: LRU = None

    def __post_init__(self):
        self.log_path_format = str(self.directory / "log.{i}.data")
        self.highest_log = 0
        self.highest_serialized = -1
        self.guid_cache = LRU(GUID_CACHE_SIZE)
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
        self.guid_cache[entry.request.id] = len(self.entries)
        index = len(self.entries)
        self.entries.append(entry)
        return index

    def serialize(self, to_index=None):
        if to_index == None:
            to_index = len(self.entries)
        else:
            assert to_index <= len(self.entries)
            to_index += 1

        # print(f"log.serialize: starting, have {len(self.entries)} entries, will serialize up to index {to_index}.")

        queue = collections.deque()

        fields = []
        length = 0
        for entry in self.entries[self.highest_serialized + 1:to_index]:
            o = entry.log_serialize()
            b = msgpack.dumps(o)

            crc32 = zlib.crc32(b)
            network_crc32 = crc32.to_bytes(4, NETWORK_BYTE_ORDER)

            length_header = packraft.compute_length_header(b)

            fields.append(network_crc32)
            fields.append(length_header)
            fields.append(b)

            queue.append(b''.join(fields))
            # print(f"log.serialize: queued serialized entry, {len(queue[-1])} bytes")

        entries = []
        length = 0
        log_path = None

        def flush():
            nonlocal entries
            nonlocal length
            if not entries:
                return

            # print(f"log.serialize: flushing {len(entries)} entries, total length {length}, to {log_path=}")
            with log_path.open('ab') as f:
                f.write(b''.join(entries))
            self.highest_serialized += len(entries)
            entries.clear()
            length = 0

        while queue:
            log_path = pathlib.Path(self.log_path_format.format(i=self.highest_log))
            if log_path.exists():
                stat = log_path.stat()
                log_size = stat.st_size
            else:
                log_size = 0
            log_remaining = MAX_LOG_SIZE - log_size
            # print(f"log.serialize: can write {log_remaining} bytes to {log_path}")

            # we use unforced to force writing at least one
            # log entry to a fresh log file.  in testing,
            # MAX_LOG_SIZE was 64.  if we got an entry that
            # serialized to 83 bytes, it would never fit, right?
            # the unforced flag ensures that, every time we
            # open a fresh log file, we always write at least
            # one log entry to it.
            unforced = (log_remaining != MAX_LOG_SIZE)
            while queue:
                entry = queue[0]
                new_length = length + len(entry)
                if unforced and (new_length > log_remaining):
                    break
                entries.append(entry)
                length = new_length
                queue.popleft()
                unforced = False

            flush()
            if not queue:
                break

            self.highest_log += 1

        # print(f"log.serialize: done.")

    def deserialize(self):
        self.entries.clear()
        while True:
            log_path = pathlib.Path(self.log_path_format.format(i=self.highest_log))
            if not log_path.exists():
                if self.highest_log > 0:
                    self.highest_log -= 1
                break
            with log_path.open("rb") as f:
                # print("just opened", log_path)
                while True:
                    network_crc32 = f.read(4)
                    if not network_crc32:
                        break
                    # print(f"{network_crc32=}")
                    stored_crc32 = int.from_bytes(network_crc32, NETWORK_BYTE_ORDER)
                    length = packraft.length_header_from_stream(f)
                    b = f.read(length)
                    computed_crc32 = zlib.crc32(b)
                    if computed_crc32 != stored_crc32:
                        sys.exit(f"Log corrupt: Entry {len(self.entries)} has mismatching CRC32 (want {hex(stored_crc32)[2:]}, got {hex(computed_crc32)[2:]})")
                    o = msgpack.loads(b)
                    # print("LOADED", o, "FROM", repr(b))
                    entry = LogEntry.log_deserialize(o)
                    self.guid_cache[entry.request.id] = len(self.entries)
                    self.entries.append(entry)
                    self.highest_serialized += 1
            self.highest_log += 1

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
            self.append(entry)

        return True

