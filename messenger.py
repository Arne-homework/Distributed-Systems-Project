import random
from typing import List,Any
from transport import NetworkMessage,MessageQueue, Transport, UnreliableTransport

class OutOfResourceError(Exception):
    pass

class ProtocollError(Exception):
    pass

WINDOW_SIZE = 10
TIMEOUT = 10

class MessengerMessage:
    """
    Message containing the id of the source and destination.
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
            print("Messenger {} received message".format(self.own_id))
            msg = MessengerMessage.from_dictionary(self.in_queue.get().get_content())
            contents.append((msg.source, msg.content))
        return contents


class OutboundBuffer:
    class EmptySentinel:
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
        return self._start

    @property
    def size(self):
        return self._size

    @property
    def end(self):
        return self._start + self._size
    
    def __getitem__(self, i):
        if i < self._start:
            raise IndexError()
        elif i > self._start + self._size:
            raise IndexError()
        else:
            return self._values[( i) % self._capacity]
            
    def drop(self):
        value = self._values[(self._start) % self._capacity]
        self._values[(self._start) % self._capacity] = self.empty
        self._start += 1
        self._size -= 1
        return value

    def append(self, value):
        if self._size >= self._capacity:
            raise IndexError()
        self._values[(self._start + self._size) % self._capacity] = value
        self._size += 1

    
        
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
            
    def __init__(self, own_id, remote_id, out_queue, messenger_message_factory, window_size=WINDOW_SIZE, timeout=TIMEOUT):
        self.own_id = own_id
        self.remote_id = remote_id
        self.out_queue = out_queue
        self._out_buffer = OutboundBuffer(window_size)
        self._last_received = 0
        self._last_ack_time = None
        self._timeout = timeout
        self._messenger_message_factory = messenger_message_factory

    def _send_content(self, content_id,content):
        typ = "content"
        msg = self._messenger_message_factory(
            ConnectionMessage(
                "content",
                content_id,
                content
            ).as_dictionary())
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
        self.out_queue.put(NetworkMessage(msg.as_dictionary()))
    
    def send(self, content):
        try:
            self._out_buffer.append(content)
        except IndexError:
            raise OutOfResourceError("No space in the out_buffer. Try again later")
        else:
            self._send_content(self._out_buffer.end, content)

    def _receive_handle_ack(self, ack, t):
        if (ack <= self._out_buffer.start):
            pass
        elif ack > self._out_buffer.end:
            # just throw.
            #TODO: think if we can recover from this.
            raise ProtocolError("acknowledged unsend message")
        else:
            i = 0
            while ack > self._out_buffer.start:
                i+=1
                self._out_buffer.drop()

            if self._out_buffer.size == 0:
                self._last_ack_time = None
            else:
                self._last_ack_time = t

    def receive(self, message_dict, t):
        message = ConnectionMessage.from_dictionary(message_dict)
        if message.typ == "content":
            if message.value != self._last_received + 1:
                #message has arrived out of order
                return[]
            self._last_received += 1
            self._send_ack(self._last_received)
            return [message.content]
        elif message.typ == "ack":
            self._receive_handle_ack(message.value,t)
            return []
        else:
            raise Exception(f"unkown connection message type:{message.typ}")
        
    def wake_up(self, t):
        """ to be called regularly

        resends messages if timeout is reached
        """
        if self._out_buffer.size == 0:
            #if no messages are waiting to be delivered, nothing to do.
            return
        elif self._last_ack_time is None:
            # if a message was send out since :
            # set the current time as delivery time.
            # because send doesn't give us a time.
            self._last_ack_time = t
            return
        elif self._last_ack_time + self._timeout > t:
            #if timeout not yet reached: do nothing.
            return
        elif self._last_ack_time + self._timeout <= t:
            #timeout reached, resend all buffered messages.
            for i in range(self._out_buffer.size):
                self._send_content(self._out_buffer.start+i+1,
                                   self._out_buffer[self._out_buffer.start+i])

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

    def send(self, destination, content: Any):
        if destination == self._own_id:
            self._shortcut_buffer.append((self._own_id,content))
        else:
            self._connections[destination].send(content)

    def receive(self, t:float)-> List[tuple[int,Any]]:
        contents = self._shortcut_buffer
        self._shortcut_buffer = []
        while not self.in_queue.empty():
            print("Messenger {} received message".format(self._own_id))
            messenger_message = MessengerMessage.from_dictionary(self.in_queue.get().get_content())
            messages = self._connections[messenger_message.source].receive(messenger_message.content, t)
            contents.extend([(messenger_message.source, content) for content in messages])
        for connection in self._connections.values():
            connection.wake_up(t)
        return contents
