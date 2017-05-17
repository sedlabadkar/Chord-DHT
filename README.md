Chord DHT
========================

## What is it?
Chord protocol implementation

The specification of the Chord protocol can be found in the paper Chord:

A Scalable Peer-to-peer Lookup Service for Internet Applications by Ion Stoica, Robert Morris, David Karger, M. Frans Kaashoek, Hari Balakrishnan.
https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf.

You can also refer to the Wikipedia page: https://en.wikipedia.org/wiki/Chord (peer-to-peer)

The goal of this project is to implement in Scala using the actor model the Chord protocol and a simple object access service to prove its usefulness.

Implements chord APIs as described in the paper. 

---

## How to use?

Project can be executed as follows: 

  * `scalac chord <numNodes> <numRequests>`
  * `example: scalac chord 10 10`

