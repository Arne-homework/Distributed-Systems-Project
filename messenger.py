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
    """
    Unreliable transport that simulates network conditions:
    - Message delays (random delay within a range)
    - Message drops (probabilistic packet loss)
    """

    def __init__(self, in_queue: MessageQueue, out_queue: MessageQueue, r: random.Random):
        super().__init__(in_queue, out_queue, r)
        self.r = r
        self.min_delay = 0.0
        self.max_delay = 0.0
        self.drop_rate = 0.0
        self.buffered_messages = []  # List of (delivery_time, message) tuples

    def set_random_generator(self, r: random.Random):
        """Set the random number generator for reproducibility"""
        self.r = r

    def set_drop_rate(self, drop_rate: float):
        """Set the probability of dropping a message (0.0 to 1.0)"""
        self.drop_rate = drop_rate

    def set_delay(self, min_delay: float, max_delay: float):
        """Set the range of random delays for message delivery"""
        self.min_delay = min_delay
        self.max_delay = max_delay

    def deliver(self, t: float):
        """
        Deliver messages with delays and drops.

        Process:
        1. Pull new messages from in_queue
        2. Decide whether to drop each message
        3. Add surviving messages to buffer with delivery time
        4. Deliver buffered messages whose time has come
        """
        # Process new incoming messages
        while not self.in_queue.empty():
            msg = self.in_queue.get()

            # Decide whether to drop this message
            if self.r.random() < self.drop_rate:
                print(f"Dropping message at time {t}: {msg}")
                continue  # Message is lost

            # Calculate random delay for this message
            delay = self.r.uniform(self.min_delay, self.max_delay)
            delivery_time = t + delay

            # Add to buffer with delivery time
            self.buffered_messages.append((delivery_time, msg))

        # Deliver messages whose time has come
        # We need to check all buffered messages and deliver those ready
        remaining_messages = []
        for delivery_time, msg in self.buffered_messages:
            if t >= delivery_time:
                # Time to deliver this message
                print(f"Delivering message at time {t}: {msg}")
                self.out_queue.put(msg)
            else:
                # Not ready yet, keep in buffer
                remaining_messages.append((delivery_time, msg))

        # Update buffer with only the messages that haven't been delivered yet
        self.buffered_messages = remaining_messages

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


