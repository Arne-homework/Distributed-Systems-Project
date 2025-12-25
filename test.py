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

logging.basicConfig( stream=sys.stdout,level=logging.ERROR, force=True)

# ============================================================
# TEST CONFIGURATION
# ============================================================
NUM_ENTRIES = 5
NUM_SERVERS = 10
SCENARIO = "medium"  # Options: 'easy', 'medium', 'hard'

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


def run_simulation(nodes, transports, duration_seconds=5.0, time_step=0.01, start_time=0.0):
    t = start_time
    iterations = int(duration_seconds / time_step)

    for _ in range(iterations):
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


def test_check_eventual_consistency_vc():
    print("\n" + "=" * 60)
    print("TASK 3: Eventual Consistency")
    print("=" * 60)

    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    r = random.Random(100)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)

    current_time = 0.0
    print("1. Creating Entries on all Nodes in parallel...")

    for inode, node in enumerate(nodes):
        node.create_entry(f"Entry 0-{inode}")
    current_time = run_simulation(
        nodes, transports,
        duration_seconds=2.0,
        start_time=current_time)

    if check_consistency(nodes, NUM_SERVERS):
        print("\n>>> EVENTUAL CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>> EVENTUAL CONSISTENCY TEST FAILED <<<")


def test_conflict_resolution():
    print("\n" + "=" * 60)
    print("TASK 4: Conflict Resolution")
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
        node.create_entry(f"Entry 0-{inode}")
        entry_ids.append(node.get_entries()[0]["id"] )
    current_time = run_simulation(
        nodes, transports,
        duration_seconds=2.0,
        start_time=current_time)
    rand.shuffle(entry_ids)
    modified_entry_id = entry_ids[0]

    print(f"2. Updating/Deleting  entry {modified_entry_id} in parallel")

    def update_entry(node, inode):
        node.update_entry(modified_entry_id, f"Entry 1-{inode}")

    def delete_entry(node, inode):
        node.delete_entry(modified_entry_id)

    for inode, node in enumerate(nodes):
        rand.choice([update_entry, delete_entry])(node, inode)
    current_time = run_simulation(
        nodes, transports,
        duration_seconds=2.0,
        start_time=current_time)

    # We don't know if an update_entry or a delete_entry wins.
    #  THis would be dependend on the event_id.
    if (check_consistency(nodes, nodes[0].board.get_number_of_entries())):
        print("\n>>> EVENTUAL CONSISTENCY TEST PASSED <<<")
    else:
        print("\n>>> EVENTUAL CONSISTENCY TEST FAILED <<<")


def test_crash_recovery():
    print("\n" + "=" * 60)
    print("TASK 3a: CRASH AND RECOVERY TEST")
    print("=" * 60)


    r = random.Random(200)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, SCENARIO, r)
    current_time = 0.0

    print("1. ")
    for inode, node in enumerate(nodes):
        node.create_entry(f"Entry 0-{inode}")
    current_time = run_simulation(nodes, transports, duration_seconds=2.0, start_time=current_time)



    current_time = run_simulation(nodes, transports, duration_seconds=10.0, start_time=current_time)

    if check_consistency(nodes, 2):
        print("\n>>> CRASH TEST PASSED <<<")
    else:
        print("\n>>> CRASH TEST FAILED <<<")


"""
TODO (Task 2): Test VectorClock implementation here if you didn't create tests in vector_clock.py

The tests are in vector_clock_test.py

On a sensible OS, use
>python3 -m unittest vector_clock_test.py
use at least python3.11
"""


"""
TODO (Task 3): Test TimeStamp ordering

Implement tests for:
1. Causal ordering (when one VC happened before another)
2. Tie-breaking (when VCs are concurrent, use tie_breaker)

The tests  are implemented in ordering_test.py.

On a sensible OS, use
>python3 -m unittest ordering_test.py
use at least python3.11
"""


"""
TODO (Task 3): Test causal consistency

Implement tests to verify:
1. Entries appear in causal order on all nodes
2. Concurrent entries are ordered deterministically (tie-breaker)
3. Vector clocks are updated correctly on send/receive

The tests  are implemented in ordering_test.py and in here  (test_check_eventual_consistency_vc)

On a sensible OS, use
>python3 -m unittest ordering_test.py
use at least python3.11
"""


"""
TODO (Task 4): Test conflict resolution (update_entry, delete_entry)

After implementing update_entry() and delete_entry(), test:
1. Concurrent modifications - deterministic resolution
2. Concurrent deletions
3. Delete vs modify conflicts

The tests  are implemented in here  (test_conflict_resolution)

"""


if __name__ == "__main__":
    iterations = 100

    test_check_eventual_consistency_vc()
    test_conflict_resolution()

