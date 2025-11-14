"""

"""
import random
import logging
from typing import List,Any
from transport import NetworkMessage,MessageQueue, Transport, UnreliableTransport

class OutOfResourceError(Exception):
    pass

class ProtocollError(Exception):
    pass

logger = logging.getLogger(__name__)

WINDOW_SIZE = 10
TIMEOUT = 10
__all__ = ["MessageQueue", "Transport", "UnreliableTransport", "Messenger", "ReliableMessenger"]

class MessengerMessage:
    """
    Message wrapping a higher level message.
    """
    @staticmethod
    def from_dictionary(dictionary):
        return MessengerMessage(
            dictionary["source"],
            dictionary["destination"],
            dictionary["content"]
        )
    
    
    def __init__(self, source, destination, content):
        self.source = source
        self.destination = destination
        self.content = content

    def as_dictionary(self):
        return {
            "source":self.source,
            "destination":self.destination,
            "content":self.content
            }
        
    

class Messenger:
    """
    Handles delivery of a message to a specific output queue.
    """
    def __init__(self, own_id, num_out: int):
        self.own_id = own_id
        self.in_queue = MessageQueue()
        self.out_queues = {i: MessageQueue() for i in range(num_out) }
    
    def send(self, destination, content: Any): 
        assert (destination in self.out_queues)
        msg =MessengerMessage(self.own_id, destination, content)
        self.out_queues[destination].put(NetworkMessage(msg.as_dictionary()))

    def has_message(self) -> bool:
        return not self.in_queue.empty()

    def receive(self) -> List[tuple[int,Any]]:
        contents = []
        while not self.in_queue.empty():
            logger.info("Messenger {} received message".format(self.own_id))
            msg = MessengerMessage.from_dictionary(self.in_queue.get().get_content())
            contents.append((msg.source, msg.content))
        return contents


class OutboundBuffer:
    """
    Buffer for ReliableMessenger to hold outbound messages.

    The Buffer holds a fixed amount (capacity)  values however it indexes them from the beginning of all values ever inserted 
    """
    class EmptySentinel:
        def __repr__(self):
            return "<empty>"

        def __str__(self):
            return "empty"
        
    empty = EmptySentinel() # special object, denoting empty slot.

    def __init__(self, capacity):
        self._capacity = capacity
        self._values = [self.empty for i in range(self._capacity)]
        self._start = 0
        self._size = 0

    @property
    def capacity(self):
        return self._capacity

    @property
    def start(self):
        """Index of the first value currently in the buffer.

        0-indexed
        """
        return self._start

    @property
    def size(self):
        """Number of values currently in the buffer"""
        return self._size

    @property
    def end(self):
        """One after the last message currently in the buffer."""
        return self._start + self._size
    
    def __getitem__(self, i):
        """
        Access value in the buffer by index.

        throws an IndexError if value not currently in the buffer.
        """
        if i < self._start:
            raise IndexError()
        elif i > self._start + self._size:
            raise IndexError()
        else:
            return self._values[( i) % self._capacity]

    def __setitem__(self, i, value):
        """
        Access value in the buffer by index.

        throws an IndexError if value not currently in the buffer.
        """
        if i < self._start:
            raise IndexError()
        elif i > self._start + self._size:
            raise IndexError()
        else:
            self._values[( i) % self._capacity] = value
            
    def drop(self):
        """Remove and return the value with the lowest index from the buffer."""
        value = self._values[(self._start) % self._capacity]
        self._values[(self._start) % self._capacity] = self.empty
        self._start += 1
        self._size -= 1
        return value

    def append(self, value):
        """
        Append another value to the buffer.

        Throws an Out of OutOfResourceError if not enough slots in the buffer.
        """
        if self._size >= self._capacity:
            raise OutOfResourceError("Buffer full")
        self._values[(self._start + self._size) % self._capacity] = value
        self._size += 1

    def create_empty_slot(self):
        self.append(self.empty)
        
        
class ConnectionMessage:
    @staticmethod
    def from_dictionary(dictionary):
        return ConnectionMessage(
            dictionary["typ"],
            dictionary["value"],
            dictionary["content"]
        )
    
    
    def __init__(self, typ, value, content):
        self.typ = typ
        self.value = value
        self.content = content

    def as_dictionary(self):
        return {
            "typ":self.typ,
            "value":self.value,
            "content":self.content
            }

    
class Connection:        
    """
    Handles the connection of one node to a specific other node in a ReliableMessenger.
    """
    
    def __init__(self, own_id, remote_id, out_queue, messenger_message_factory, window_size=WINDOW_SIZE, timeout=TIMEOUT): 
        self.own_id = own_id
        self.remote_id = remote_id
        self.out_queue = out_queue
        self._out_buffer = OutboundBuffer(window_size)
        self._in_buffer = OutboundBuffer(window_size)
        self._timeout = timeout
        self._messenger_message_factory = messenger_message_factory

    def send(self, content:Any, time:float):
        """
        Send the content in a message.

        Content must be serilizable.
        """
        try:
            self._out_buffer.append((time+self._timeout,content))
        except IndexError:
            raise OutOfResourceError("No space in the out_buffer. Try again later")
        else:
            self._send_content(self._out_buffer.end, content)

    def receive(self, message_dict, time)->List[Any]:
        """
        Handle receiving a message.

        message_dict must be a dictionary that can be loaded into a ConnectionMessage.
        time is the current time.
        """
        message = ConnectionMessage.from_dictionary(message_dict)
        if message.typ == "content":
            if not( self._in_buffer.size == 0 or self._in_buffer[self._in_buffer.start] is self._in_buffer.empty):
                pass
            if message.value > self._in_buffer.end:
                for _ in range(self._in_buffer.end, message.value):
                    self._in_buffer.create_empty_slot()
            assert message.value > 0
            try:
                self._in_buffer[message.value-1] = message.content
            except IndexError:
                pass
            contents = []
            next_to_send = self._in_buffer.start
            for pos in range(self._in_buffer.start,self._in_buffer.end):
                if self._in_buffer[pos] is not self._in_buffer.empty:
                    next_to_send = pos + 1
                    content = self._in_buffer.drop()
                    contents.append(content)
                else:
                    break
            self._send_ack(next_to_send)
            return contents
        elif message.typ == "ack":
            self._receive_handle_ack(message.value,time)
            return []
        else:
            raise Exception(f"unkown connection message type:{message.typ}")
        
    def wake_up(self, time):
        """
        To be called regularly.

        t is the current time.
        
        - resends messages if timeout is reached.
        """
        if self._out_buffer.size == 0:
            #if no messages are waiting to be delivered, nothing to do.
            return
        else:
            for i in range(self._out_buffer.start,self._out_buffer.end):
                timeout_time,content = self._out_buffer[i]
                if timeout_time <= time:
                    
                    self._send_content(i+1,
                                       content)
                    self._out_buffer[i] = (time+self._timeout,content)

    def _receive_handle_ack(self, ack, t):
        logger.debug(f"received ack={ack}, at time {t}")
        if (ack <= self._out_buffer.start):
            pass
        elif ack > self._out_buffer.end:
            #TODO: think if we can recover from this.
            # Probably not, this is a major Protocol violation. We have no idea what happened on the other side.  
            raise ProtocolError("acknowledged unsend message")
        else:
            i = 0
            while ack > self._out_buffer.start:
                i+=1
                self._out_buffer.drop()

    def _send_content(self, content_id,content):
        typ = "content"
        msg = self._messenger_message_factory(
            ConnectionMessage(
                "content",
                content_id,
                content
            ).as_dictionary())
        logger.debug(f"Connection: content send:{msg.as_dictionary()}")
        self.out_queue.put(NetworkMessage(msg.as_dictionary()))

    def _send_ack(self, value):
        typ = "ack"
        content = None
        msg = self._messenger_message_factory(
            ConnectionMessage(
                typ,
                value,
                content
            ).as_dictionary())
        logger.debug(f"Connection: ack send:{msg.as_dictionary()}")
        self.out_queue.put(NetworkMessage(msg.as_dictionary()))
    
class ReliableMessenger:
    def __init__(self, own_id, num_out:int, timeout=TIMEOUT, window_size=WINDOW_SIZE):
        connections, out_queues = self._create_connections(own_id, num_out, timeout, window_size)
        self._connections = connections
        self._shortcut_buffer = [] #for those messages that are send to self.
        self._own_id = own_id
        self.out_queues = out_queues
        self.in_queue = MessageQueue()
        

    def _create_connections(self, own_id, num_out, timeout, window_size):
        connections = {}
        out_queues = {}
        for i in range(num_out):
            out_queues[i] = MessageQueue()
            if i == own_id:
                continue
            def make_messenger_message_factory(own_id,i):
                def make_messenger_message( content):
                    return MessengerMessage(own_id, i , content)
                return make_messenger_message
            connections[i] = Connection(own_id, i, out_queues[i], make_messenger_message_factory(own_id, i ), timeout=timeout, window_size=window_size)
        return connections, out_queues

    def send(self, destination, content: Any, time:float):
        """
        Send content in a message.

        content must be serializable
        time is the current time.
        """
        if destination == self._own_id:
            self._shortcut_buffer.append((self._own_id,content))
        else:
            self._connections[destination].send(content, time)

    def receive(self, t:float)-> List[tuple[int,Any]]:
        """
        Check for any messages recieved and return their content as a tuple (source, content).

        t is the current time.
        """
        # as there is only one in_queue for all remote nodes,
        #  ReliableMessenger receives it
        #  and then dispatches it to the relevant connection.
        #  connection then handles acknowledgments, out of order deliveries, missing messages.
        #  This also means that a MessengerMessage needs to wrap a ConnectionMessage, not the other way around.
        #  Which is why the Messenger inject a messenger_message_factory into Connection.
        contents = self._shortcut_buffer
        self._shortcut_buffer = []
        while not self.in_queue.empty():
            logger.info("Messenger {} received message".format(self._own_id))
            messenger_message = MessengerMessage.from_dictionary(self.in_queue.get().get_content())
            messages = self._connections[messenger_message.source].receive(messenger_message.content, t)
            contents.extend([(messenger_message.source, content) for content in messages])
        for connection in self._connections.values():
            connection.wake_up(t)
        return contents
