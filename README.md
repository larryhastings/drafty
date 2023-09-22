# Drafty

### A toy Raft implementation by Eric V. Smith and Larry Hastings

## Overview

Drafty is a toy implementation of Raft, written as coursework
for a class on the Raft algorithm.  We made it open source
so everybody can read it, but we don't expect it to actually
be useful, except as perhaps example code for other students,
or as a metaphorical signpost on the road to perdition.

Features:

* Uses asyncio.
* Single-threaded.
* Implements a simple key-value store.
* Implements the following Raft features:

    * Elections.
    * Heartbeats.
    * Log replication.
    * Log serialization / deserialization.
    * Repeated client requests are not committed twice.

Misfeatures:

* Minimal test suite.
* Custom RPC protocol (see `packraft.py`).

Not implemented (yet?):

* Snapshotting.

## Running drafty yourself

Here's the best way to play with drafty.

* Install all the dependencies: `python3 -m pip install -r requirements.txt`
* Open four terminal / command windows.  The first three
  will be servers, the fourth will be your client.
* In the first one run:  `python3 asyncio_driver.py 0`
* In the second one run:  `python3 asyncio_driver.py 1`
* In the third one run:  `python3 asyncio_driver.py 2`
* In the fourth one run: `python3 client.py 0 putrnd`

Run the command in the fourth window as often as you like.
You can kill and restart any of the servers, and as long
as two are running your Raft service should continue to work.

## Notes

We use 0-based indexing for the log.  We use `-1` as
the magic value when the log is empty.
