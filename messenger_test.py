import unittest as ut
from unittest.mock import Mock
import random
from messenger import Messenger,MessengerMessage,ReliableMessenger,Connection, UnreliableTransport,MessageQueue

class Object():
    pass

class TestConnection(ut.TestCase):
    def make_messenger_message_factory(self, own_id,i):
        def make_messenger_message( content):
            return MessengerMessage(own_id, i , content)
        return make_messenger_message

    def test_simple_send(self):
        
        out_queue = MessageQueue()
        conn = Connection(
            0,
            1,
            out_queue,
            self.make_messenger_message_factory(0,1)
        )
        conn.send("Hello World")
        self.assertFalse(out_queue.empty())
        message = out_queue.get()
        self.assertEqual(message.get_content(), {"source": 0, "destination": 1, "content": {"typ": "content", "value": 1, "content": "Hello World"}})
        

        
class TestMessenger(ut.TestCase):
    def test_delivery_with_unreliable_transport3(self):
    
        messenger_0 = ReliableMessenger(0,2)
        messenger_1 = ReliableMessenger(1,2)

        randomizer = random.Random(42)
        transports = [
            UnreliableTransport(messenger_0.out_queues[1], messenger_1.in_queue,randomizer),
            UnreliableTransport(messenger_1.out_queues[0], messenger_0.in_queue,randomizer)
            ]
        transports[0].set_drop_rate(0.5)
        transports[1].set_drop_rate(0.5)
        transports[0].set_delay(1.0,5.0)
        transports[1].set_delay(1.0,5.0)

        contents = [f"Hello {i}" for i in range(10)]
        for content in contents:
            messenger_0.send(1,content)

        out_contents = []
        time_limit, time_step = 100,0.02
        for i in range(int(time_limit/time_step)):
            t = i*time_step
            for transport in transports:
                transport.deliver(t)

            contents0_out = messenger_0.receive(t)
            self.assertEqual(len(contents0_out),0)
            contents1_out = messenger_1.receive(t)
            out_contents.extend([y for (x,y) in contents1_out])

        self.assertEqual(contents, out_contents)

    def _make_unreliable_transport(self, out_queue, in_queue, randomizer):
        return UnreliableTransport(out_queue, in_queue, randomizer) 

    def test_delivery_with_unreliable_transport1(self):
        """ if the original message is dropped it is still delivered"""
        messenger_0 = ReliableMessenger(0,2, timeout=1, window_size=10)
        messenger_1 = ReliableMessenger(1,2, timeout=1, window_size=10)

        randomizer_0 = Object()
        randomizer_0.random = Mock(side_effect=[0.0,1.0])
        randomizer_0.uniform = Mock(side_effect=[0.0,0.0])

        randomizer_1 = Object()
        randomizer_1.random = Mock(side_effect=[0.0,1.0])
        randomizer_1.uniform = Mock(side_effect=[0.0,0.0])

        #Unreliable Transport is by default in fact reliable.
        transports = [
            self._make_unreliable_transport(messenger_0.out_queues[1], messenger_1.in_queue,randomizer_0),
            self._make_unreliable_transport(messenger_1.out_queues[0], messenger_0.in_queue,randomizer_1)
            ]
        transports[0].set_drop_rate(0.5)
        content = "Hello 1"
        messenger_0.send(1, content)

        # first the message is dropped.
        for transport in transports:
            transport.deliver(0.0)

        contents0_out_0 = messenger_0.receive(0.0)
        contents1_out_0 = messenger_1.receive(0.0)
        self.assertEqual(len(contents0_out_0),0)
        self.assertEqual(len(contents1_out_0),0)

        # calling receive after the timeout prompts a new message delivery (unseen from the outside)
        
        for transport in transports:
            transport.deliver(1.0) #deliver does nothing at this time
        contents0_out_1 = messenger_0.receive(1.0)
        contents1_out_1 = messenger_1.receive(1.0)
        self.assertEqual(len(contents0_out_1),0)
        self.assertEqual(len(contents1_out_1),0)

        # which is now finally delivered.
        for transport in transports:
            transport.deliver(1.1)
        contents0_out_2 = messenger_0.receive(1.1)
        contents1_out_2 = messenger_1.receive(1.1)
        self.assertEqual(len(contents0_out_2),0)
        self.assertEqual(len(contents1_out_2),1)

        self.assertEqual(content, contents1_out_2[0][1])










