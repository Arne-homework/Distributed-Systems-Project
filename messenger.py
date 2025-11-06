import json
from dataclasses import dataclass,field
from typing import Any
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

@dataclass(order=True)
class MessageRecord:
    min_delivery_time:float
    max_delivery_time:float = field(compare=False)
    msg : Message = field(compare=False)
    
class DelayQueue(queue.PriorityQueue[MessageRecord]):
    pass



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
        self._delay_queue=DelayQueue()
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
        rand = self._random_generator.uniform(0,1)
#        print(f"Should message be droppend? {rand} <{self._drop_rate}")
        return (rand < self._drop_rate)

    def _is_message_overdelayed(self, msg_record, current_time):
        return current_time > msg_record.max_delivery_time

    def _should_message_be_delivered_now(self, msg_record, t):
        return msg_record.min_delivery_time < t

    def _create_message_record(self, msg, t):
        rand = self._random_generator.uniform(self._min_delay,self._max_delay) 
        return MessageRecord(t + rand, t + self._max_delay, msg)
            
    def deliver(self,t):
        """handles the messages of the in_qeueue (empties the in_queue) and delivers messages that are not to be dropped or futher delayed to the out_queue
        """
        while not self.in_queue.empty():
            msg = self.in_queue.get()
            if(not self._should_message_be_dropped() ):
                self._delay_queue.put(self._create_message_record(msg,t))

        while not self._delay_queue.empty():
            msg_record = self._delay_queue.get()
            if self._is_message_overdelayed(msg_record, t):
                self._overdelay_resolution_strategy(self.out_queue, t, msg_record)
            elif self._should_message_be_delivered_now(msg_record,t):
                msg = msg_record.msg
                print(f"Delivering message at time {t}: {msg}")
                self.out_queue.put(msg)
            else:
                self._delay_queue.put(msg_record)
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


