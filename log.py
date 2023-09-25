#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from dataclasses import dataclass, field
from driver import Driver
from lru import LRU
import math
import messages
import packraft


GUID_CACHE_SIZE = 1<<20

def manufactured_field(cls, **kwargs):
    return field(init=False, default_factory=cls, **kwargs)




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
    driver: Driver
    entries: list[messages.LogEntry] = manufactured_field(list)
    # maps request.id (which is a GUID) -> log index of request
    # Note that all entries in the log get guid_cache cached,
    # whether they're committed or not.
    guid_cache: LRU = None

    def __post_init__(self):
        self.guid_cache = LRU(GUID_CACHE_SIZE)
        self.entries = self.driver.load_log()
        length = len(self.entries)
        if length:
            width = int(math.log10(length) + 1)
        else:
            width = 1
        for index, entry in enumerate(self.entries):
            if 1:
                print(f"persistent log {index:{width}}: {entry}")
            self.guid_cache[entry.request.id] = index
        self.unserialized_start = len(self.entries)

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
            end = len(self.entries)
        else:
            assert to_index <= len(self.entries)
            end = to_index + 1

        assert self.unserialized_start <= end, f"can't serialize from {self.unserialized_start=} to {end=}, THEY'S BACKWARDS"
        if self.unserialized_start == end:
            return

        self.driver.save_log(self.entries, self.unserialized_start, end)
        self.unserialized_start = end

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

