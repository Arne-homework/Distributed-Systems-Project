import unittest as ut
from clock import clock_server, ExternalDeterminedClock
from node import Node
from vector_clock import VectorTimestamp
from messenger import ReliableMessenger
from transport import Transport
import random
import logging
import sys


class TimestampTest(ut.TestCase):
    def setUp(self):
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.ERROR,
            force=True)
        clock_server.set_clock_factory(
            lambda n: ExternalDeterminedClock())

    def _create_transports(self, nodes):
        num_nodes = len(nodes)
        r = random.Random(42)
        transports = {}
        for from_id in range(num_nodes):
            for to_id in range(num_nodes):
                transports[(from_id, to_id)] = Transport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    r
                )
        return transports

    def _create_nodes(self, num_nodes):
        r = random.Random(43)
        nodes = [Node(ReliableMessenger(i, num_nodes, timeout=2.0),
                      i, num_nodes, r)
                 for i in range(num_nodes)]
        return nodes

    def test_causal_consistency_all_nodes(self):
        """
        Entries appear in causal order on all nodes.
        """
        time_offset = 3.0
        time_step = 0.1
        nodes = self._create_nodes(2)
        transports = self._create_transports(nodes)
        number_of_iterations = 100

        # Scenario: in a ping-pong fashion, we create events on the nodes.
        # between the creations we propagate the creation events such that
        #  the creations are causally linked.
        
        time = 0.0
        for i in range(number_of_iterations):
            time += time_step
            clock_server.get_clock_for_node(0).set_time(time_offset + time)
            clock_server.get_clock_for_node(1).set_time(time)

            nodes[0].create_entry(f"Entry {i}-0")
            for transport in transports.values():
                transport.deliver(time)
            for node in nodes:
                node.update()

            entries0 = nodes[0].get_entries()
            entries1 = nodes[1].get_entries()
            self.assertEqual(len(entries0), i*2+1)
            self.assertEqual(len(entries1), i*2+1)
            self.assertEqual(entries0[-1]["value"], f"Entry {i}-0")
            self.assertEqual(entries1[-1]["value"], f"Entry {i}-0")

            time += time_step
            clock_server.get_clock_for_node(0).set_time(time_offset + time)
            clock_server.get_clock_for_node(1).set_time(time)

            nodes[1].create_entry(f"Entry {i}-1")
            for transport in transports.values():
                transport.deliver(time)
            for node in nodes:
                node.update()

            entries0 = nodes[0].get_entries()
            entries1 = nodes[1].get_entries()
            self.assertEqual(len(entries0), i*2+2)
            self.assertEqual(len(entries1), i*2+2)
            self.assertEqual(entries0[-2]["value"], f"Entry {i}-0")
            self.assertEqual(entries1[-2]["value"], f"Entry {i}-0")
            self.assertEqual(entries0[-1]["value"], f"Entry {i}-1")
            self.assertEqual(entries1[-1]["value"], f"Entry {i}-1")

    def test_update_on_send(self):
        """
        the VectorTimestamp updated according to the rules of the vector clock
        whenever an event is created or recieved.
        """
        time = 0.0
        number_of_nodes = 2
        nodes = self._create_nodes(number_of_nodes)
        transports = self._create_transports(nodes)
        self.assertEqual(nodes[0].get_current_vector_timestamp(),
                         VectorTimestamp([0 for _ in range(number_of_nodes)]))
        self.assertEqual(nodes[1].get_current_vector_timestamp(),
                         VectorTimestamp([0 for _ in range(number_of_nodes)]))
        # creating an entry updates the counter
        nodes[0].create_entry("Entry 0")

        self.assertEqual(nodes[0].get_current_vector_timestamp(),
                         VectorTimestamp([1, 0]))

        # we deliver an event with VectorTimestamp([1, 0])
        # to nodes[1]. THis updates the current timestamp.
        for transport in transports.values():
            transport.deliver(time)
        for node in nodes:
            node.update()
        self.assertEqual(nodes[1].get_current_vector_timestamp(),
                         VectorTimestamp([1, 1]))
        
        nodes[1].create_entry("Entry 1")

        self.assertEqual(nodes[1].get_current_vector_timestamp(),
                         VectorTimestamp([1, 2]))

        # we deliver an event with VectorTimestamp([1, 2])
        # to nodes[0]. THis updates the that nodes timestamp.
        for transport in transports.values():
            transport.deliver(time)
        for node in nodes:
            node.update()
        self.assertEqual(nodes[0].get_current_vector_timestamp(),
                         VectorTimestamp([2, 2]))

    def test_concurrent_consistency_all_nodes(self):
        """
        Concurrent entries appear in the same order on all nodes.
        """
        time_offset_step = 11.0
        time_step = 0.1
        number_of_nodes = 10
        nodes = self._create_nodes(number_of_nodes)
        transports = self._create_transports(nodes)
        number_of_iterations = 5

        # Scenario: In each timestep we create a differnt event on each node.
        #  then we propage the creation events.
        time = 0.0
        for i in range(number_of_iterations):
            time += time_step
            for iclock_node, clock in clock_server.all_clocks():
                clock.set_time(time_offset_step*iclock_node + time)

            for inode, node in enumerate(nodes):
                node.create_entry(f"Entry {i}-{inode}")
            for transport in transports.values():
                transport.deliver(time)
            for node in nodes:
                node.update()

            for inode in range(number_of_nodes):
                for jnode in range(inode):
                    entries0 = nodes[jnode].get_entries()
                    entries1 = nodes[inode].get_entries()
                    self.assertEqual(len(entries0), (i+1)*number_of_nodes)
                    self.assertEqual(len(entries1), (i+1)*number_of_nodes)

                    self.assertEqual(
                        [entry["value"] for entry in entries0],
                        [entry["value"] for entry in entries1],
                        msg=f"Deviation between nodes {jnode}-{inode}")
