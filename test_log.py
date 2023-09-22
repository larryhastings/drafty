#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from log import *
from messages import *

tests_passed = 0

def success():
    global tests_passed
    tests_passed += 1


log = Log()
entries = []

assert len(log) == 0
success()

def test_heartbeat():
    # first set of parameters should pass
    # the rest should fail.
    parameters = []

    if not log:
        parameters.append((None, None))
        parameters.append((0, 3))
        parameters.append((1, 1))
    else:
        previous_index = len(log) - 1
        previous_term = log[previous_index].term

        parameters.append((previous_index, previous_term))
        parameters.append((previous_index + 1, previous_term))
        parameters.append((previous_index    , previous_term + 1))
        parameters.append((previous_index + 1, previous_term + 1))

    expected_result = True
    for p in parameters:
        result = log.append_entries(*p, [])
        assert result == expected_result, f"{p=} -> {expected_result=} != {result=}"
        expected_result = False
        success()


test_heartbeat()


# fail to append
failure_entry = LogEntry(2, None)
result = log.append_entries(1, 1, [failure_entry])
assert not result
success()
assert log.entries == entries
success()

test_heartbeat()

# this append works, entries is now 1
entry = LogEntry(1, None)
entries.append(entry)
result = log.append_entries(None, 0, [entry])

assert result
success()
assert len(log) == 1
success()
assert log.entries == entries
success()

test_heartbeat()

# idempotent test: redo this append, entries is still 1
result = log.append_entries(None, 0, [entry])

assert result
success()
assert len(log) == 1
success()
assert log.entries == entries
success()

test_heartbeat()

# this one fails again
failure_entry = LogEntry(2, None)
result = log.append_entries(2, 0, [failure_entry])
assert not result
success()
assert log.entries == entries
success()

# this one works again, entries is now 2
entry = LogEntry(1, ClientPutRequest('x', 'y'))
entries.append(entry)
result = log.append_entries(0, 1, [entry])

assert result
success()
assert log.entries == entries
success()

test_heartbeat()

# test idempotency:
# overwrite both entries
result = log.append_entries(None, None, list(entries))

assert result
success()
assert log.entries == entries
success()

# overwrite first entry
result = log.append_entries(None, None, [entries[0]])

assert result
success()
assert log.entries == entries
success()

# overwrite second entry
result = log.append_entries(0, 1, [entry])

assert result
success()
assert log.entries == entries
success()


if 0:
    # white box testing:
    # append garbage ("uncommitted") to the end of log
    log.entries.extend(
        [
        LogEntry(2, ClientPutRequest('z', 'q')),
        LogEntry(2, ClientPutRequest('3', '4')),
        ],
        )
    # this append should fail
    entry = LogEntry(4, ClientPutRequest('r', 's'))
    result = log.append_entries(2, 4, [entry])

    assert not result
    success()

    test_heartbeat()

    # this append should work, and truncate the log back to only 2 entries
    result = log.append_entries(1, 1, [entry])
    entries.append(entry)

    assert result
    success()
    assert log.entries == entries
    success()

    test_heartbeat()

print(f"All {tests_passed} log tests passed.")