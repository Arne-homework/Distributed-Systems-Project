import json
import queue
import random
from typing import List


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


class UnreliableTransport(Transport):
    pass  # TODO: implement unreliable transport, i.e., messages being lost or delayed

    def set_random_generator(self, r: random.Random):  # use for replayability
        pass

    def set_drop_rate(self, drop_rate: float):
        pass

    def set_delay(self, min_delay: float, max_delay: float):
        pass

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


