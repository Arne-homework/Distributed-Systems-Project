"""
testing for some specific edge cases
"""
import unittest as ut
import logging
import sys
import random
from clock import clock_server,ExternalDeterminedClock
from transport import UnreliableTransport 
from messenger import ReliableMessenger 
from node import Node

logging.basicConfig( stream=sys.stdout,level=logging.ERROR, force=True)


class TestNodeCommunication(ut.TestCase):
    def setUp(self):
        clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    def _create_transports(self, nodes, rand, delay, drop_rate):
        num_nodes = len(nodes)
        transports = {}
        min_delay, max_delay = delay
        for from_id in range(num_nodes):
            for to_id in range(num_nodes):
                transport = UnreliableTransport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    rand,
                )
                transport.set_delay(min_delay, max_delay)
                transport.set_drop_rate(drop_rate)
                
                transports[(from_id, to_id)] = transport
        return transports
    
    def test_out_of_order_update_delivery(self):
        """
        in this test a node recieves an update before it recieved the creation event.
        The nodes can derive state from inconsistent histories without error (or Exception).
        """
        rand = random.Random(100)
        num_nodes = 3
        nodes = [Node(ReliableMessenger(i, num_nodes, timeout=2.0), i, num_nodes, rand) for i in range(num_nodes)]
        transports = self._create_transports(nodes, rand, [0.0,0.0], 0.0)
        current_time = 0.0

        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)
        
        nodes[0].create_entry("Initial_Entry")
        entry_id = nodes[0].board.get_ordered_entries()[0].id

        # creation event is delivered only to Node 1.
        for connection in [ (0,1)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        self.assertEqual(nodes[1].board.get_ordered_entries()[0].id, entry_id)

        current_time += 1.0
        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)
        nodes[1].update_entry(entry_id, "Updated_Entry")

        # we deliver the update to Node 2.
        for connection in [(1,2)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        # node 2 has the update but not the creation event.
        # Therefore it shows nothing. 
        self.assertEqual(len(nodes[2].board.get_ordered_entries()), 0)
        # node 0 has the creation event but not the update
        self.assertEqual(nodes[0].board.indexed_entries[entry_id].value, "Initial_Entry")
        # node 1 has the creation event and the update
        self.assertEqual(nodes[1].board.indexed_entries[entry_id].value, "Updated_Entry")
        
        current_time += 1.0
        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)

        # propagate the missing events
        for connection in [(1,0),(0,2)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        self.assertEqual(nodes[0].board.indexed_entries[entry_id].value, "Updated_Entry")
        self.assertEqual(nodes[1].board.indexed_entries[entry_id].value, "Updated_Entry")
        self.assertEqual(nodes[2].board.indexed_entries[entry_id].value, "Updated_Entry")


    def test_out_of_order_deletion_delivery(self):
        """
        in this test a node recieves an deletion event before it recieved the creation event.
        The nodes can derive state from inconsistent histories without error (or Exception).
        """
        rand = random.Random(100)
        num_nodes = 3
        nodes = [Node(ReliableMessenger(i, num_nodes, timeout=2.0), i, num_nodes, rand) for i in range(num_nodes)]
        transports = self._create_transports(nodes, rand, [0.0,0.0], 0.0)
        current_time = 0.0

        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)
        
        nodes[0].create_entry("Initial_Entry")
        entry_id = nodes[0].board.get_ordered_entries()[0].id

        # creation event is delivered only to Node 1.
        for connection in [ (0,1)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        self.assertEqual(nodes[1].board.indexed_entries[entry_id].value,"Initial_Entry")

        current_time += 1.0
        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)
        nodes[1].delete_entry(entry_id)

        # we deliver the deletion to Node 2.
        for connection in [(1,2)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        # node 2 has the deletion but not the creation event.
        # Therefore it shows nothing. 
        self.assertEqual(len(nodes[2].board.get_ordered_entries()), 0)
        # node 0 has the creation event but not the deletion
        self.assertEqual(nodes[0].board.indexed_entries[entry_id].value, "Initial_Entry")
        # node 1 has the creation event and the deletion
        self.assertEqual(len(nodes[2].board.get_ordered_entries()), 0)
        
        current_time += 1.0
        for _,clock in clock_server.all_clocks():
            clock.set_time(current_time)

        # propagate the missing events
        for connection in [(1,0),(0,2)]:
            transports[connection].deliver(current_time)
        for node in nodes:
            node.update()
        # all nodes know of the deletion
        self.assertEqual(len(nodes[0].board.get_ordered_entries()), 0)
        self.assertEqual(len(nodes[1].board.get_ordered_entries()), 0)
        self.assertEqual(len(nodes[2].board.get_ordered_entries()), 0)

