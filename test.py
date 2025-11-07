#!/usr/bin/env python3
"""
Test file for Lab 1: Centralized Consistency with Coordinator Pattern

Modify the parameters below to test different scenarios:
- NUM_ENTRIES: Number of entries to create
- NUM_SERVERS: Number of nodes
- SCENARIO: 'easy' (no failures), 'medium' (delays), 'hard' (delays + packet loss)
"""

import random
import time
from messenger import Message, Messenger, Transport, UnreliableTransport
from node import Node

# ============================================================
# TEST CONFIGURATION
# ============================================================
NUM_ENTRIES = 10
NUM_SERVERS = 4
SCENARIO = 'hard'  # Options: 'easy', 'medium', 'hard'

# ============================================================

def create_transports(nodes, scenario, r):
    """
    Create transports between nodes based on the scenario.

    Scenarios:
    - 'easy': No delays, no packet loss (reliable)
    - 'medium': Delays (0.5-1.5s), no packet loss
    - 'hard': Delays (0.5-1.5s) + 10% packet loss
    """
    transports = {}
    num_nodes = len(nodes)

    for from_id in range(num_nodes):
        for to_id in range(num_nodes):
            if scenario == 'easy':
                # Reliable transport
                transport = Transport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    r
                )
            else:
                # Unreliable transport
                transport = UnreliableTransport(
                    nodes[from_id].messenger.out_queues[to_id],
                    nodes[to_id].messenger.in_queue,
                    r
                )

                if scenario == 'medium':
                    transport.set_delay(0.5, 1.5)  # Delays only
                    transport.set_drop_rate(0.0)   # No packet loss
                elif scenario == 'hard':
                    transport.set_delay(0.5, 1.5)  # Delays
                    transport.set_drop_rate(0.1)   # 10% packet loss

            transports[(from_id, to_id)] = transport

    return transports


def run_simulation(nodes, transports, duration_seconds=5.0, time_step=0.01):
    """
    Run the distributed system simulation for a specified duration.
    Delivers messages and updates nodes at each time step.
    """
    t = 0.0
    iterations = int(duration_seconds / time_step)

    for _ in range(iterations):
        # Deliver messages via all transports
        for transport in transports.values():
            transport.deliver(t)

        # Update all non-crashed nodes
        for node in nodes:
            if not node.is_crashed():
                node.update(t)

        t += time_step
        time.sleep(0.001)  # Small sleep to prevent CPU spinning

    return t


if __name__ == "__main__":
    print("=" * 60)
    print(f"Lab 1 Test - Scenario: {SCENARIO}")
    print("=" * 60)

    # Setup nodes
    r = random.Random(42)
    nodes = []
    for i in range(NUM_SERVERS):
        m = Messenger(i, NUM_SERVERS)
        n = Node(m, i, NUM_SERVERS, r)
        nodes.append(n)

    # Setup transports based on scenario
    transports = create_transports(nodes, SCENARIO, r)

    # Create entries from all servers
    print(f"\nCreating {NUM_ENTRIES} entries from each of {NUM_SERVERS} servers...")
    start_time = time.time()

    for i in range(NUM_ENTRIES):
        for server_id in range(NUM_SERVERS):
            nodes[server_id].create_entry(f"Server{server_id}_Entry{i}")

    # Run simulation long enough for all messages to be delivered
    # Adjust duration based on scenario (harder scenarios might need more time)
    duration = 10.0

    print(f"Running simulation for {duration}s...")
    run_simulation(nodes, transports, duration_seconds=duration)

    elapsed = time.time() - start_time
    print(f"Time taken: {elapsed:.2f}s")

    # Check consistency
    print("\nChecking consistency across all nodes...")
    reference_entries = nodes[0].get_entries()
    total_expected = NUM_ENTRIES * NUM_SERVERS

    all_consistent = True
    for node in nodes:
        entries = node.get_entries()
        print(f"Node {node.own_id}: {len(entries)} entries")

        if len(entries) != total_expected:
            print(f"  Expected {total_expected} entries, got {len(entries)}")
            all_consistent = False
        elif entries != reference_entries:
            print(f"  Entries differ from Node 0")
            all_consistent = False
        else:
            print(f"  Consistent")

    if all_consistent:
        print("\n" + "=" * 60)
        print("TEST PASSED - All nodes consistent!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("TEST FAILED - Inconsistency detected!")
        print("=" * 60)
        print("\nTips:")
        print("  - For 'easy' scenario: Check your coordinator pattern implementation")
        print("  - For 'medium'/'hard': You may need to implement reliability mechanisms")
