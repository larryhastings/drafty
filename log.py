#!/usr/bin/env python3


import dataclasses
from dataclasses import dataclass, field
import messages
import packraft


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


@dataclass
class Log(packraft.Message):
    entries: list[LogEntry] = field(default_factory=list)
    # lock: threading.Lock

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
