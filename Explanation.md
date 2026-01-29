### Ricart-Argrawala Algorithm

Building on previous labs, we use Events as our transactions.

AppMessage encapsulates the needs of Ricart-Agrawala Algorithm. 
It usese three different message types.

Node uses three states. : Idle, Requesting and InCriticalSection.

By default it is in Idle. If a user creates, updates or deletes an entry, an
event is created and in the next update the node goes into the Requesting state
and sends a "Request" message to all other nodes. 

When all other nodes replied with a "Reply" message, the node enters the
critical section applies the event and sends a "Propagate" message to all
other nodes. 

The node returns to "Idle" if all other nodes have confirmed that they have
recieved the "Propagate" message.

In accordance with the Ricart-Agrawal algorithm:
If a node recieves a request message it returns a reply message if,
 * it is in the "Idle" state.
 * it is in the "Requesting" state and the other request has an earlier
   logical timestamp than the active own request. (With the event Id as a
   tiebreaker)
   
Otherwiese the request is queued and replied to when the node reenters the "Idle" state.



### Blockchain

A Node implementation for the use of a blockchain was implemented in
node_blockchain.py. 

Again events were used as our implementation of Transactions. 

A node collects all events recieved since its last created block and forges a
block of them. Different from the template, but in line with real world
implementations, node then checks if the last (binary) digits of the hash of
the block are zero and if so, accepts the block as valid and sends it to the
other nodes.

If two nodes concurrently forge a block, the blockchain has a side chain of
equal length and some nodes pick one or the other chain leading to
inconsistencies. However when a new block if forged, the forging node picks
one chain and extends it. As soon as the new block is propagated to all other
nodes, one of the chains is longer and all nodes agree now on the longer
chain, leading to consistency again. 

Other than Ricart & Agravala the approach does not enter a guaranteed
exclusive critical section, but instead randomly enters a critical section,
with the chance being low enough that it is unlikely that two nodes enter the
critical section concurrently. Also different from Agravala, nodes only send a
single message to propagate a new block instead of requireing 4 sequential
messages. (If the messages needed to guarantee delivery are ignored).
