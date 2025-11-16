# Distributed Systems Labs

## 2a) +3a)

2a) +3a) Out of order delivery and dropped messages present a challenge to
consistency. E.g. if a propagation message from the central node to one
other node is dropped, that node never applies that update/addition/deletion
and diverges from the other nodes which apply it. But also simple reordering
of messages can cause inconsistencies: E.g. if two updates to an entry are
propagated out of order to a peripheral node, the first update (delivered out
of order) will override the first, while on nodes where the messages are
delivered in order, the second update will override the first. Leading to an
inconsistency.

Our solution uses a ReliableMessenger that guarantees eventual in-order
delivery of the messages even over an unreliable transport. 

Another solution would be to assign an id at the original node making the request, infinitely
resending the add_entry request. But only apply them at the other nodes,
if the id does not already exists. This will eventually be consistent.

## 2b) + 3b) 
Our solution correctly solves the medium and hard test scenarios.

## 4

We implemented update and delete functionality.

It was a challenge to remember the fields of the messages. This was solved
by splitting the screen an having the message creation code open on one screen
at all times. (What do you expect here?)

Another forseen challenge was that differences in the way the requests were
applied to the central and the peripheral nodes could result in inconsistencies.
This was solved by using the same method to update the board in all nodes.


## ReliableMessenger

ReliableMessenger is inspired by TCP.
ReliableMessenger creates a Connection for every other node it communicates
with.
When sending a message the Connection sends it including the sequence number
of the message and at the same time stores it
in a buffer. If it receives an acknowledgment of the reception of the message
it removes the message from the buffer. If after a timeout no acknowledgment
is received, 
If a Connection receives a message it stores it in its own inbound buffer and
delivers all messages in the buffer that can be delivered in order. And sends
an acknowledment for the last of these messages (and thereby implicit for all
previous messages). 

In our implementation inbound and outbound buffer have a fixed number of slots
(this number is window_size).
