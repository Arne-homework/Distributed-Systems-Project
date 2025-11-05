import json
import os
import queue
import random
from typing import List

DROP_RATE = float(os.getenv('DROP_RATE')) if os.getenv('DROP_RATE') else 0.0
MIN_DELAY = float(os.getenv('MIN_DELAY')) if os.getenv('MIN_DELAY') else 0.0
MAX_DELAY = float(os.getenv('MAX_DELAY')) if os.getenv('MAX_DELAY') else 0.0


class Message:
    def __init__(self, content):
        self.content = json.dumps(content)  # store as JSON string so it remains immutable!
        self.len = len(self.content)

    def __str__(self):
        return f'{self.content}'

    def get_content(self):
        return json.loads(self.content)


class MessageQueue(queue.SimpleQueue[Message]):
    pass

def create_unreliable_transport(
        in_queue:MessageQueue,
        out_queue:MessageQueue,
        r:random.Random):
        return UnreliableTransport(in_queue, out_queue, r, DROP_RATE, MIN_DELAY, MAX_DELAY )


class MessageRecord:
    def __init__(self, msg:Message, min_delivery_time:float, max_delivery_time):
        self.msg=msg
        self.min_delivery_time=min_delivery_time
        self.max_delivery_time=max_delivery_time



class QueueWithPeek():
    """ A queue that supports seeing the first element, without removing it from the queue.
    """
    #uses a simple linked list implementation.
    class _Node():
        def __init__(self, value, next):
            self.value = value
            self.next = next
            
    def __init__(self):
        self._head = None  # head=None signals an empty queue.
        
    def put(self, value):
        self._head = self._Node(value, self._head)

    def empty(self):
        return self._head is None

    def peek(self):
        if self._head is None:
            raise queue.Empty()
        else:
            return self._head.value

    def get(self):
        if self._head is None:
            raise queue.Empty()
        else:
            value = self._head.value
            self._head = self._head.next
            return value


class Transport:
    def __init__(self, in_queue: MessageQueue, out_queue: MessageQueue, r: random.Random):
        self.in_queue = in_queue
        self.out_queue = out_queue

    def deliver(self, t: float):
        # use the time parameter to ensure replayability
        while not self.in_queue.empty():
            msg = self.in_queue.get()
            print("Delivering message at time {}: {}".format(t, msg))
            self.out_queue.put(msg)

def drop_message(out_queue:MessageQueue, t:float,msg_record:MessageRecord):
    pass


class UnreliableTransport(Transport):
    """simulates an ureliable transport mechanism

    messages can be dropped or delayed but stay inorder.
    """
    def __init__(self,
                  in_queue:MessageQueue,
                  out_queue:MessageQueue,
                  r:random.Random,
                  drop_rate:float,
                  min_delay:float,
                  max_delay:float,
                  overdelay_resolution_strategy=drop_message
                  #we can't guarantee for any message, that it is delivered within the delay window.
                  #If the message is more delayed than the delay window allows we need to deal with that.
                  #   the basic choices are: drop it. which increases the drop rate over the requested rate.
                  #   or send it which results in a delay > max_delay.
                  ):
        super().__init__(in_queue, out_queue, r)
        self._delay_queue=QueueWithPeek()
        self._overdelay_resolution_strategy=overdelay_resolution_strategy
        self.set_random_generator(r)
        self.set_drop_rate(drop_rate)
        self.set_delay(min_delay,max_delay)
        
    def set_random_generator(self, r: random.Random):  # use for replayability
        self._random_generator=r

    def set_drop_rate(self, drop_rate: float):
        assert 0 <= drop_rate <= 1.0
        self._drop_rate=drop_rate

    def set_delay(self, min_delay: float, max_delay: float):
        assert 0 <= min_delay <= max_delay
        self._min_delay=min_delay
        self._max_delay=max_delay



    def _should_message_be_dropped(self) :
        return (self._random_generator.uniform(0,1) < self._drop_rate)

    def _is_message_overdelayed(self, msg_record, current_time):
        return current_time > msg_record.max_delivery_time

    def deliver(self,t):
        """handles the messages of the in_qeueue (empties the in_queue) and delivers messages that are not to be dropped or futher delayed to the out_queue
        """
        while not self.in_queue.empty():
            msg = self.in_queue.get()
            if(not self._should_message_be_dropped() ):
                self._delay_queue.put(MessageRecord(msg,t+self._min_delay, t+self._max_delay))

        while not self._delay_queue.empty():
            msg_record = self._delay_queue.peek()
            if self._is_message_overdelayed(msg_record, t):
                msg_record = self._delay_queue.get()
                self._overdelay_resolution_strategy(out_queue, t, msg_record)
            elif self._random_generator.uniform(msg_record.min_delivery_time,msg_record.max_delivery_time) < t:
                msg = self._delay_queue.get().msg
                print(f"Delivering message at time {t}: {msg}")
                self.out_queue.put(msg)
            else:
                break
            
                
class Messenger:
    def __init__(self, own_id, num_out: int):
        self.own_id = own_id
        self.in_queue = MessageQueue()
        self.out_queues = {i: MessageQueue() for i in range(num_out)}

    def send(self, destination, msg: Message): # TODO: Maybe add from and to fields to Message?
        assert (destination in self.out_queues)
        self.out_queues[destination].put(msg)

    def has_message(self) -> bool:
        return not self.in_queue.empty()

    def receive(self) -> List[Message]:
        msgs = []
        while not self.in_queue.empty():
            print("Messenger {} received message".format(self.own_id))
            msgs.append(self.in_queue.get())
        return msgs


