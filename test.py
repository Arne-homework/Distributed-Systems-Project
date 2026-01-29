#!/usr/bin/env python3
"""
Test file for Lab 3: Eventual Consistency with Vector Clocks

Modify the parameters below to test different scenarios:
- NUM_ENTRIES: Number of entries to create
- NUM_SERVERS: Number of nodes
- SCENARIO: 'easy' (no failures), 'medium' (delays), 'hard' (delays + packet loss)
"""

import random
import sys
import time
import logging
from transport import Transport, UnreliableTransport
from messenger import ReliableMessenger
from clock import clock_server, ExternalDeterminedClock
from node import Node
import node_blockchain as nb

logging.basicConfig(stream=sys.stdout, level=logging.ERROR, force=True)

# ============================================================
# TEST CONFIGURATION
# ============================================================
NUM_ENTRIES = 1
NUM_SERVERS = 10
SCENARIO = "hard"  # Options: 'easy', 'medium', 'hard'
SIM_DURATION =55.0


# ============================================================
def create_transports(nodes, scenario, r):
    transports = {}
    num_nodes = len(nodes)

    for from_id in range(num_nodes):
        for to_id in range(num_nodes):
            if scenario == "easy":
                transport = Transport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    r,
                )
            else:
                transport = UnreliableTransport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    r,
                )
                if scenario == "medium":
                    transport.set_delay(0.25, 0.5)
                    transport.set_drop_rate(0.0)
                elif scenario == "hard":
                    transport.set_delay(0.25, 0.5)
                    transport.set_drop_rate(0.1)

            transports[(from_id, to_id)] = transport
    return transports


def run_simulation(nodes, transports, actions=None, duration_seconds=5.0, time_step=0.01, start_time=0.0):
    inputs = [] if actions is None else actions
    t = start_time
    iterations = int(duration_seconds / time_step)

    for i in range(iterations):
        if i<len(inputs):
            inputs[i](nodes)
        for transport in transports.values():
            transport.deliver(t)

        for _, clock in clock_server.all_clocks():
            clock.set_time(t)

        for node in nodes:
            if not node.is_crashed():
                node.update()

        t += time_step
        time.sleep(0.001)

    return t


def pass_fn(nodes):
    return


def check_consistency(nodes, expected_count):
    print("\n--- Consistency Check ---")
    reference_entries = None
    all_consistent = True

    for node in nodes:
        if not node.is_crashed():
            reference_entries = node.get_entries()
            break

    if reference_entries is None:
        print("All nodes are crashed! Cannot check consistency.")
        return False

    for node in nodes:
        if node.is_crashed():
            print(f"Node {node.own_id}: CRASHED (Skipping check)")
            continue

        entries = node.get_entries()
        print(f"Node {node.own_id}: {len(entries)} entries")

        if len(entries) != expected_count:
            print(f"  [FAIL] Expected {expected_count} entries, got {len(entries)}")
            all_consistent = False
        elif entries != reference_entries:
            print("  [FAIL] Entries differ from Reference Node")
            all_consistent = False
        else:
            print("  [OK] Consistent")

    return all_consistent


################################################
### scenarios


def test_critical_section():
    print("\n" + "=" * 60)
    print("TASK 2: Test Critical Section with Static Coordinator")
    print("=" * 60)

    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    r = random.Random(100)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=1.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)

    current_time = 0.0
    print("1. Creating Entries Node 0")

    for i in range(NUM_ENTRIES):
        nodes[0].create_entry(f"Entry {i}")
    current_time = run_simulation(
        nodes,
        transports,
        actions=[],
        duration_seconds=SIM_DURATION,
        start_time=current_time)

    if check_consistency(nodes, NUM_ENTRIES):
        print("\n>>>  CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>>  CONSISTENCY TEST FAILED <<<")


def test_ricart_agravala():
    print("\n" + "=" * 60)
    print("TASK 3: Test Ricart & Agravala Mutual Exclusion")
    print("=" * 60)

    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    r = random.Random(100)
    rand = random.Random(77)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)

    current_time = 0.0
    print("1. Creating Entries on all Nodes in parallel...")
    entry_ids = []
    for inode, node in enumerate(nodes):
        for ientry in range(NUM_ENTRIES):
            node.create_entry(f"Entry {ientry}-{inode}")
    current_time = run_simulation(
        nodes,
        transports,
        duration_seconds=SIM_DURATION,
        start_time=current_time)


    if (check_consistency(nodes, NUM_ENTRIES*NUM_SERVERS)):
        print("\n>>>  CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>>  CONSISTENCY TEST FAILED <<<")


def test_critical_section_blockchain():
    print("\n" + "=" * 60)
    print("TASK 4: Blockchain: Probabilistic Access ")
    print("=" * 60)

    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    r = random.Random(100)
    log_difficulty = 4
    nodes = [
        nb.Node(
            ReliableMessenger(
                i,
                NUM_SERVERS,
                timeout=1.0),
            i,
            NUM_SERVERS,
            log_difficulty,
            r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)

    current_time = 0.0
    print("1. Creating Entries Node 0")

    for i in range(NUM_ENTRIES):
        nodes[0].create_entry(f"Entry {i}")
    current_time = run_simulation(
        nodes,
        transports,
        actions=[],
        duration_seconds=SIM_DURATION,
        start_time=current_time)

    if check_consistency(nodes, NUM_ENTRIES):
        print("\n>>>  CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>>  CONSISTENCY TEST FAILED <<<")


def test_blockchain():
    print("\n" + "=" * 60)
    print("TASK 4b: The blockchain")
    print("=" * 60)

    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    log_difficulty = 10
    r = random.Random(100)
    nodes = [nb.Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, log_difficulty, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)

    current_time = 0.0
    print("1. Creating Entries on all Nodes in parallel...")
    entry_ids = []
    for inode, node in enumerate(nodes):
        for ientry in range(NUM_ENTRIES):
            node.create_entry(f"Entry {ientry}-{inode}")
    current_time = run_simulation(
        nodes,
        transports,
        duration_seconds=SIM_DURATION,
        start_time=current_time)

    
    if (check_consistency(nodes, len(nodes[0].get_entries()))):
        print("\n>>>  CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>>  CONSISTENCY TEST FAILED <<<")



if __name__ == "__main__":
    start_real_time = time.time()

    print("=" * 60)
    print(f"RUNNING SYSTEM TESTS")
    print(f"SCENARIO:           {SCENARIO.upper()}")
    print(f"Nodes:              {NUM_SERVERS}")
    print(f"Entries per Node:   {NUM_ENTRIES}")
    print(f"Simulated Duration: {SIM_DURATION}s")
    print("=" * 60)

#    test_critical_section()
#    test_ricart_agravala()
#    test_critical_section_blockchain()
    test_blockchain()
    end_real_time = time.time()
    total_real_time = end_real_time - start_real_time

    print("=" * 60)
    print(f"TEST SUITE FINISHED")
    print(f"Total Real-World Time: {total_real_time:.2f} seconds")
    print("=" * 60)



"""
TODO (Task 1): Test Transactions

Verify that transactions are correctly created and stored for later propagation.
"""


"""
TODO (Task 2): Test Critical Section with Static Coordinator
"""


"""
TODO (Task 3): Test Ricart & Agrawala Mutual Exclusion
"""


"""
TODO (Task 4): Test Blockchain Probabilistic Access
"""


"""
TODO (Task 5): Test Longest Chain Resolution

Verify that network partitions and resulting inconsistencies are resolved
using the longest chain rule.
"""
