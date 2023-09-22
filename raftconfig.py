#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import perky
from dataclasses import dataclass

@dataclass
class ServerAddress:
    host: str
    port: int
    client_port: int

_pky = perky.load("raftconfig.pky")

for d in _pky['servers']:
    client_port = d['client port']
    del d['client port']
    d['client_port'] = client_port

servers = [ServerAddress(**d) for d in _pky['servers']]

del _pky
del ServerAddress
del perky
del client_port
