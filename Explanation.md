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
