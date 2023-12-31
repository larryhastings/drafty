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
* Raft implementation has only high-level
  [sans-I/O](https://sans-io.readthedocs.io/how-to-sans-io.html)
  interfaces; all interactions with the local platform
  go through an abstracted platform driver.
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
  are going to run three servers, the fourth will be your client.
* In the first one run:  `python3 asyncio_driver.py 0`
* In the second one run:  `python3 asyncio_driver.py 1`
* In the third one run:  `python3 asyncio_driver.py 2`
* At this point your three servers should be running, and one of the servers should have become elected Leader.  If you ran them in order, the leader will be node 0.  Now, in the fourth one run: `python3 client.py 0 putrnd`

Run client requests in the fourth window as often as you like.
You can kill and restart any of the servers as often as you like, and as long
as two are running your Raft service should continue to work.

## Architecture

drafty was implemented in a modular way.  There are strictly
defined subsystems in drafty, and during its development we
kept the code honest about where functionality should live.
This had only minor downsides; for such a small project,
it has a surprising amount of architecture.  But we felt this
lent structure to the project, and we always knew both where
code should live in the project, as well as where the fault
would lie when there was a mysterious bug.

### The Abstracted Server

Here's an awkward ASCII-art diagram of drafty's subsystems.

    +-------------+
    | Application |
    +-------------+
          ^
          |
          |   +-----------+
          |   |    Log    |
          |   +-----------+
          |         ^
          |         |
    +-------------------+                      +----------------+
    |                   |<-----RPC calls/      |                |
    |      Server       |      responses,      |     Driver     |
    |                   |      timers,         |                |
    |                   |      file access---->|                |
    +-------------------+                      +----------------+
             |                                          |
             v                                          v
        +-----------+ (Leader,                       Platform
        |   State   |  Candidate,                     |    |
        +-----------+  Follower)                      v    |
             |                                     Network |
             v                                             v
        +-----------+                                  Filesystem
        |WaitingRoom|  (Leader state only)
        +-----------+

For now let's just consider two modules: the Server and the
Driver.  The Server, and its subsidiary modules (Log, State,
Leader, Candidate, Follower, and WaitingRoom) implement the
Raft protocol and server-side logic.  The Driver handles all
interaction with the local platform: it contains the "main"
function for the project.  When you start a drafty instance,
you start a Driver, and the Driver starts the Server.

The most important detail about this design: the Server
never directly talks to the local platform.  It's
a conceptually "pure" system.  It doesn't directly talk to
network sockets or files; it doesn't have any concept of time.
The Driver virtualizes all that functionality; all network
traffic, filesystem access, and time functions the Server uses
are provided by the Driver.  To differentiate this Server
module from other things one might call "the server"--the
project as a whole, a running server process--we called this
"the abstracted server".

Not only does this mean the Driver can choose whatever
implementation strategy it likes--our main Driver uses
[asyncio,](https://docs.python.org/3/library/asyncio.html)
but it could just as easily use threads or 
[Twisted](https://twisted.org/) or
[Trio](https://github.com/python-trio/trio)--it makes
testing much easier.  All state originates from the
Driver, so it's no work at all to use mock objects
for network requests, timer events, and even the
state of the Raft log.

### Application

The Application is what the Raft paper calls the
"state machine".  This is the actual application that
the Raft server architecture proposes to make
high-reliability.  In our case it's a simple toy
key/value store.

### State, Leader, Candidate, Follower

Network-handling code is best implemented as a state
machine, and Raft is no exception.  Most of the actual Raft
algorithm is implemented in three state classes: Follower,
Candidate, and Leader.  These all inherit from a State base
class; the current state of the Server is stored in Server.state.

When a network message arrives, the Driver sends it to the
Server, and the Server dispatches it to the current State.
So, if the node receives a Raft "AppendEntries" request, the
code that handles it is the `on_append_entries_request` function
in the current State (Follower, Candidate, Leader).  All incoming
network requests and responses are handled in this way.

drafty uses 
['StateManager'](https://github.com/larryhastings/big#statemanagerstate--on_enteron_enter-on_exiton_exit-state_classnone)
from Larry's 
[**big**](https://github.com/larryhastings/big)
library to manage the state.  Originally we used a
local precursor of this library called `interstate`;
this was removed in favor of the cleaned-up version that
went into **big**, but you might still see it if you
examine older revisions.  (`interstate` and `big.state`
work almost identically, differing only in details that
are irrelevant to drafty.)


### WaitingRoom

One of the more complicated aspects of implementing Raft
has to do with maintaining multiple simultaneous
client requests.  Unless you rate-limit the Leader sending
out AppendEntries requests (which drafty currently does not),
you can easily have multiple AppendEntries requests
simultaneously in process.  The WaitingRoom class helps
manage all this state.

A WaitingRoom represents one round of AppendEntries requests.
Whenever the Leader wants to commit new entries to the Log,
it needs to send out an AppendEntries request to each of its
Followers, and only commit the entries when more than 50% of
the Followers have responded with success.

In the final analysis WaitingRoom is probably overkill
for the problem we were solving.  But we were worried
about the complexity of Raft, so we erred on the side
of too much architecture instead of not enough.  The
WaitingRoom approach worked great, and it was easy to
modify as we understood the algorithm better and
adjusted our implementation to match.

### The downside of this approach

The downside of drafty's architecture-heavy approach is
that you can lose track of "where you are".  In `server.py`
there are three possible subsystems you might be coding
on at any particular time: the `Server`, the `State`,
and the `WaitingRoom`.
If you need to refer to another subsystem, e.g. the `Log`,
each of these must follow a different dotted path to that
object.  For example, a method on the `WaitingRoom` would
refer to the `Log` object via `self.state.server.log`,
wheras the `Server` object could find it with just `self.log`.
If you're shuffling code from one area to another, and it
happens to be between subsystems and you don't notice, now
you have an `AttributeError` waiting to happen.
Combine this with the fact that our `asyncio` design likes
to hide exceptions and you have a constant stumbling block
during development.


### The "luid"

One design choice that we felt turned out well was something
we called a "luid".  A "GUID" is a Globally Unique IDentifier;
naturally, a "luid" is a Locally Unique IDentifier.  LUIDs are
names associated with data coming from or going out over the
network.  They're allocated locally, and are only guaranteed
to be unique on the node where they were allocated.  Thus,
if you send a luid to another server, e.g. as metadata as part
of a request, the other server probably shouldn't use that luid
as a key in a dictionary, as it's not guaranteed to be unique
in their namespace.  If you get a luid as part of a network
message, you should include it as part of your response, and
otherwise you shouldn't do anything with it.

One aspect of our design is that the various server nodes
don't maintain permanent open connections to each other.
If node 1 sends an AppendEntries request to node 2, it
opens a socket to node 2, dumps the bytes down the socket,
and closes the connection.  When node 2 wants to send its
response, it opens a new socket to node 1, dumps the
response bytes down that socket, and closes.  In a
conventional, state-ful keep-open-the-connection style of
networked server design, you'd associate the request with
the open socket.  Since we can't do that, we needed to
associate it with some other ID; the luid became that id.

Luids are used on both sides of a server-side RPC call.
The sending server uses a luid as the `transaction_id`
in the "envelope" containing the RPC call, which is
returned as part of the response.  And when a network
message comes in off the wire from a remote server, 
the server assigns it its *own* luid, to track it locally.

Note that the "abstracted server" never deals with luids.
All luids are assigned and managed in the driver.

It sounds a bit complicated but it really isn't.  And actually
it paid off in one really lovely way: internally we have a
generator function that makes it easy to create an iterator
that returns successive luid values, assembled from a readable
text string and a monotonically increasing number.  This was
a boon during debugging; instead of a message associated
with a conventional GUID--a meaningless string of 32 hex
digits with some dashes--our "luids" communicated information
about the object with which they were associated,
like `'remote-request-5'`.

One final note: the client uses real GUIDs, not luids,
when making requests to the server.  This was necessary
to support the "repeated requests don't commit twice to
the log" feature.  To save space, these GUIDs are losslessly
compressed, from 36 bytes to 16 bytes, by throwing away
the dashes and merging each pair of hex characters into
one byte.

## Notes on indexing

Raft calls for 1-based indexing; that is, the first entry
in an array is index 1.  W instead use 0-based
indexing for the log, like a Python array.  We therefore
use `-1` as the magic value for "leaderCommittedIndex"
when the log is empty.

The Raft choice of using 1-based indexing is a bit
perplexing--did they initially write it in Pascal?
Apart from changing to 0-indexing, we more or less
adhered to sending values in messages in the spirit
of what the Raft protocol requires.  This required
a certain amount of "adding 1 here" and
"subtracting 1 there", which is confusing.  Doing it
this way felt more sane in the context of the weeklong
class where we wrote this project, as we felt fidelity
to what the Raft paper dictates would be less confusing
overall.  It's remotely possible we'll revisit this
decision in the future, slightly altering our wire
protocol so our logic can have fewer +1s and -1s for
computing indices.

(In particular, we note that 1-based "the index of the
most recent committed entry" is the same number as
"the count of committed entries".  So we *could* send
the latter, and by happy coincidence it'd be the same
number Raft wants us to send, and everybody's happy.)
