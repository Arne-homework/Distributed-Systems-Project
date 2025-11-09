import unittest as ut
from unittest.mock import Mock
import random
from messenger import Messenger, UnreliableTransport

class Object():
    pass

class TestMessenger(ut.TestCase):
    def test_with_reliable_transport(self):
    
        messenger_0 = Messenger(0,2)
        messenger_1 = Messenger(1,2)

        #Unreliable Transport is by default in fact reliable.
        transports = [
            UnreliableTransport(messenger_0.out_queues[1], messenger_1.in_queue,random.Random(42)),
            UnreliableTransport(messenger_1.out_queues[0], messenger_0.in_queue,random.Random(42))
            ]

        content = "Hello from 0"
        messenger_0.send(1, content)

        for transport in transports:
            transport.deliver(0.0)

        contents_out = messenger_1.receive()
        self.assertEqual(len(contents_out),1)
        origin_out, content_out = contents_out[0]
        self.assertEqual(0, origin_out)
        self.assertEqual(content, content_out)


    def test_with_unreliable_transport(self):
    
        messenger_0 = Messenger(0,2)
        messenger_1 = Messenger(1,2)

        
        randomizer_0 = Object()
        randomizer_0.random = Mock(side_effect=[0.0,1.0])
        randomizer_0.uniform = Mock(side_effect=[0.0,0.0])

        randomizer_1 = Object()
        randomizer_1.random = Mock(side_effect=[0.0,1.0])
        randomizer_1.uniform = Mock(side_effect=[0.0,0.0])

        #Unreliable Transport is by default in fact reliable.
        transports = [
            UnreliableTransport(messenger_0.out_queues[1], messenger_1.in_queue,randomizer_0),
            UnreliableTransport(messenger_1.out_queues[0], messenger_0.in_queue,randomizer_1)
            ]
        transports[0].set_drop_rate(0.5)
        content = "Hello 1"
        messenger_0.send(1, content)

        for transport in transports:
            transport.deliver(0.0)

        contents_out = messenger_1.receive()
        self.assertEqual(len(contents_out),0)
#        self.assertEqual(content, contents_out[0][1])










