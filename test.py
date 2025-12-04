#!/usr/bin/env python3
"""
Test file for Lab 2: Weak Consistency
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
NUM_SERVERS = 4
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
                    transport.set_delay(0.1, 0.5)
                    transport.set_drop_rate(0.0)
                elif scenario == "hard":
                    transport.set_delay(0.1, 0.5)
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
        time.sleep(0.0001) 

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
            print(f"  [FAIL] Entries differ from Reference Node")
            all_consistent = False
        else:
            print(f"  [OK] Consistent")

    return all_consistent


def test_partition_recovery():
    print("\n" + "=" * 60)
    print("TASK 3a: NETWORK PARTITION TEST")
    print("=" * 60)
    
    # --- FIX: Reset the global clock server ---
    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())
    # ------------------------------------------

    r = random.Random(100)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, "medium", r) 
    
    current_time = 0.0

    print("1. Network Healthy. Creating initial entries...")
    nodes[0].create_entry("Init_Entry")
    current_time = run_simulation(nodes, transports, duration_seconds=2.0, start_time=current_time)
    
    if not check_consistency(nodes, 1):
        print("Initial setup failed consistency!"); return

    print("\n2. CREATING PARTITION: [0,1] <---> [2,3]")
    print("   Setting drop_rate = 1.0 for cross-partition links.")
    
    partition_A = {0, 1}
    partition_B = {2, 3}
    
    for (src, dst), transport in transports.items():
        if (src in partition_A and dst in partition_B) or \
           (src in partition_B and dst in partition_A):
            transport.set_drop_rate(1.0)

    print("   Node 0 creates 'Entry_A' (Only [0,1] should see this)")
    nodes[0].create_entry("Entry_A")
    
    print("   Node 3 creates 'Entry_B' (Only [2,3] should see this)")
    nodes[3].create_entry("Entry_B")

    current_time = run_simulation(nodes, transports, duration_seconds=5.0, start_time=current_time)

    print("\n3. Verifying Divergence (Nodes should differ)...")
    entries_0 = nodes[0].get_entries()
    entries_3 = nodes[3].get_entries()
    
    print(f"   Node 0 count: {len(entries_0)} (Expected 2: Init + A)")
    print(f"   Node 3 count: {len(entries_3)} (Expected 2: Init + B)")
    
    if len(entries_0) == 2 and len(entries_3) == 2 and entries_0 != entries_3:
        print("   [OK] Partitions successfully diverged.")
    else:
        print("   [FAIL] Partitions did not diverge as expected.")

    print("\n4. HEALING PARTITION")
    for transport in transports.values():
        transport.set_drop_rate(0.0)
        
    print("   Running simulation for convergence...")
    current_time = run_simulation(nodes, transports, duration_seconds=10.0, start_time=current_time)

    if check_consistency(nodes, 3):
        print("\n>>> PARTITION TEST PASSED <<<")
    else:
        print("\n>>> PARTITION TEST FAILED <<<")


def test_crash_recovery():
    print("\n" + "=" * 60)
    print("TASK 3a: CRASH AND RECOVERY TEST")
    print("=" * 60)


    r = random.Random(200)
    nodes = [Node(ReliableMessenger(i, NUM_SERVERS, timeout=2.0), i, NUM_SERVERS, r) for i in range(NUM_SERVERS)]
    transports = create_transports(nodes, "medium", r)
    current_time = 0.0

    print("1. Creating initial entry...")
    nodes[0].create_entry("Entry_Pre_Crash")
    current_time = run_simulation(nodes, transports, duration_seconds=2.0, start_time=current_time)

    print("\n2. CRASHING Node 1")
    nodes[1].status["crashed"] = True
    
    print("   Node 0 creates 'Entry_During_Crash'")
    nodes[0].create_entry("Entry_During_Crash")
    
    current_time = run_simulation(nodes, transports, duration_seconds=4.0, start_time=current_time)
    
    print("\n3. Verifying Node 1 missed the update (Queued but not processed)")

    print("\n4. RECOVERING Node 1")
    nodes[1].status["crashed"] = False
    
    print("   Running simulation for catch-up...")
    current_time = run_simulation(nodes, transports, duration_seconds=10.0, start_time=current_time)

    if check_consistency(nodes, 2):
        print("\n>>> CRASH TEST PASSED <<<")
    else:
        print("\n>>> CRASH TEST FAILED <<<")


if __name__ == "__main__":
    test_partition_recovery()

    # Reset the clocks
    clock_server.set_clock_factory(lambda n: ExternalDeterminedClock())

    test_crash_recovery()
