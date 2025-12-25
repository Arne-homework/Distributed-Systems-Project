import unittest as ut
from clock import clock_server, ExternalDeterminedClock
from node import Node, BloomClockSorter, VectorClockSorter, LamportClockSorter
from vector_clock import VectorTimestamp
from messenger import ReliableMessenger
from transport import Transport
import random
import logging
import sys


logging.basicConfig(
    stream=sys.stdout,
    level=logging.ERROR,
    force=True)


def create_transports(nodes):
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


def create_nodes(num_nodes, sorter=None):
    r = random.Random(43)
    nodes = [Node(ReliableMessenger(i, num_nodes, timeout=2.0),
                  i, num_nodes, r, sorter)
             for i in range(num_nodes)]
    return nodes


def check_performance(sorter, iterations):
    num_before = 0
    num_after = 0
    for i in range(iterations):
        clock_server.set_clock_factory(
            lambda n: ExternalDeterminedClock())
        clock_server.get_clock_for_node(0).set_time(0.0)
        clock_server.get_clock_for_node(1).set_time(0.0)
        nodes = create_nodes(2, sorter)
        transports = create_transports(nodes)
        for j in range(10):
            nodes[0].create_entry(f"Entry 0-{j}")
        assert nodes[0].get_entries()[-1]["value"] == "Entry 0-9"
        entry_id0 = nodes[0].get_entries()[-1]["id"]
        nodes[1].create_entry("Entry 1-0")
        assert nodes[1].get_entries()[-1]["value"] == "Entry 1-0"
        entry_id1 = nodes[1].get_entries()[-1]["id"]
        for transport in transports.values():
            transport.deliver(0.0)
        for node in nodes:
            node.update()
        all_entries = nodes[1].get_entries()
        for entry in all_entries:
            if entry["id"] == entry_id0:
                num_before += 1
                break
            elif entry["id"] == entry_id1:
                num_after += 1
                break
    return num_before, num_after


if __name__ == "__main__":
    iterations = 100

    print("Testing Vector Clock Sorter:")
    num_before, num_after = check_performance(VectorClockSorter(), iterations)
    print(f"{num_before}/{iterations}")
    
    print("\nTesting Bloom Clock Sorter:")
    num_before, num_after = check_performance(BloomClockSorter(), iterations)
    print(f"{num_before}/{iterations}")

    print("\nTesting Lamport Clock Sorter:")
    num_before, num_after = check_performance(LamportClockSorter(), iterations)
    print(f"{num_before}/{iterations}")

