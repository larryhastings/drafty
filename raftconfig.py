#!/usr/bin/env python3

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
