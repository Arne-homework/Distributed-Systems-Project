import unittest as ut

import sys
import random
from messenger import ReliableMessenger
from transport import UnreliableTransport
import logging
from clock import clock_server, ExternalDeterminedClock
from node_blockchain import Node

logging.basicConfig(stream=sys.stdout, level=logging.ERROR, force=True)


class TestBlockchainNode(ut.TestCase):
    def setUp(self):
        clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    def _make_transport(self, out_queue, in_queue, randomizer):
        return UnreliableTransport(out_queue, in_queue, randomizer)

    def _make_reliable_messengers(self, num_messengers):
        messengers = []
        for i in range(num_messengers):
            messengers.append(ReliableMessenger(i, num_messengers))
        return messengers

    def _deliver_messages(self, transports, time):
        for transport in transports:
            transport.deliver(time)


    def test_synchronizing_single_entry(self):
        """
        A single entry is synchronized between nodes.
        """
        messengers = self._make_reliable_messengers(2)
        randomizer = random.Random(42)

        transports = [
            self._make_transport(
                messengers[0].out_queues[1],
                messengers[1].in_queue,
                randomizer),
            self._make_transport(
                messengers[1].out_queues[0],
                messengers[0].in_queue,
                randomizer),
            ]
        nodes = [Node(messengers[0], 0, 2, 2, randomizer),
                 Node(messengers[1], 1, 2, 2, randomizer)]

        nodes[0].create_entry("Just an Entry")
        timestep = 0.1
        for i in range(200):
            time = i * timestep
            self._deliver_messages(transports, time)
            for _, clock in clock_server.all_clocks():
                clock.set_time(time)

            for node in nodes:
                if not node.is_crashed():
                    node.update()
        self.assertEqual(len(nodes[0].get_entries()), 1)
        self.assertEqual(nodes[0].get_entries()[0]["value"], "Just an Entry")
        self.assertEqual(len(nodes[1].get_entries()), 1)
        self.assertEqual(nodes[1].get_entries()[0]["value"], "Just an Entry")
