import unittest as ut
from unittest.mock import Mock

from messenger import UnreliableTransport, MessageQueue, Message, drop_message

class Object:
    pass

class TestUnreliableTransport(ut.TestCase):
    
    def help_create_message_queue(self):
        return MessageQueue()

    def help_create_message(self, msg):
        return Message(msg)
    
    def test_reliable_configuration(self):
        """if drop rate and delays are configured as 0,
        UnreliableTransport behaves equal to Transport
        """

        in_queue = self.help_create_message_queue()
        out_queue = self.help_create_message_queue()
        random_generator =  Object()
        random_generator.uniform = Mock(side_effect=[0.0,0.0])
        transport = UnreliableTransport(
            in_queue,
            out_queue,
            random_generator,
            drop_rate=0,
            min_delay=0,
            max_delay=0,
            overdelay_resolution_strategy=drop_message)
        

        msg = self.help_create_message("Hello")
        in_queue.put(msg)
        transport.deliver(42.42)
        self.assertTrue(in_queue.empty())
        self.assertFalse(out_queue.empty())
        out_msg = out_queue.get()
        self.assertIs(msg,out_msg)

    def test_delayed_message(self):
        """sometimes Messages are delayed
        """
        
        in_queue = self.help_create_message_queue()
        out_queue = self.help_create_message_queue()
        random_generator =  Object()
        random_generator.uniform = Mock(side_effect=[0.35,3.0])
        transport = UnreliableTransport(
            in_queue,
            out_queue,
            random_generator,
            drop_rate=0.3,
            min_delay=2.0,
            max_delay=8.0,
            overdelay_resolution_strategy=drop_message)


        msg = self.help_create_message("Hello")
        in_queue.put(msg)
        transport.deliver(1.0)
        self.assertTrue(in_queue.empty())
        self.assertTrue(out_queue.empty())
        transport.deliver(3.0)
        self.assertTrue(in_queue.empty())
        self.assertTrue(out_queue.empty())
        transport.deliver(4.0)
        self.assertTrue(in_queue.empty())
        self.assertFalse(out_queue.empty())
        out_msg = out_queue.get()
        self.assertIs(msg,out_msg)
        

    def test_dropped_message(self):
        """sometimes Messages are dropped (never arrive)
        """
        
        in_queue = self.help_create_message_queue()
        out_queue = self.help_create_message_queue()
        random_generator =  Object()
        random_generator.uniform = Mock(side_effect=[0.29,3.0])
        transport = UnreliableTransport(
            in_queue,
            out_queue,
            random_generator,
            drop_rate=0.3,
            min_delay=2.0,
            max_delay=8.0,
            overdelay_resolution_strategy=drop_message)


        msg = self.help_create_message("Hello")
        in_queue.put(msg)
        for i in range(1,100):
            transport.deliver(float(i))
            self.assertTrue(in_queue.empty())
            self.assertTrue(out_queue.empty())

    def test_out_of_order_message(self):
        """sometimes Messages arrive out of order
        """

        delay_1 = 7.0
        delay_2 = 3.0
        t1 = 1.0
        t2 = 1.5
        in_queue = self.help_create_message_queue()
        out_queue = self.help_create_message_queue()
        random_generator =  Object()
        random_generator.uniform = Mock(side_effect=[0.4,delay_1,
                                                     0.4,delay_2])
        transport = UnreliableTransport(
            in_queue,
            out_queue,
            random_generator,
            drop_rate=0.3,
            min_delay=2.0,
            max_delay=8.0,
            overdelay_resolution_strategy=drop_message)


        msg1 = self.help_create_message("Hello 1")
        msg2 = self.help_create_message("Hello 2")

        in_queue.put(msg1)
        transport.deliver(t1) # will be delivered at t1+delay_1 =8.0
        self.assertTrue(in_queue.empty())
        self.assertTrue(out_queue.empty())

        in_queue.put(msg2)
        transport.deliver(t2)# will be delivered at t2+delay_2=4.5
        self.assertTrue(in_queue.empty())
        self.assertTrue(out_queue.empty())

        transport.deliver(t2+delay_2)
        self.assertFalse(out_queue.empty())
        msg_out1 = out_queue.get()
        self.assertIs(msg2,msg_out1)

        transport.deliver(t1+delay_1)
        self.assertFalse(out_queue.empty())
        msg_out2 = out_queue.get()
        self.assertIs(msg1,msg_out2)
        
    
