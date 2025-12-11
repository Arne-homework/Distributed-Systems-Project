"""
Tests for the Bloom Clock implementation.
"""

import unittest
from bloom_clock import BloomClock, BloomTimestamp


class TestBloomTimestamp(unittest.TestCase):
    """Test cases for BloomTimestamp."""

    def test_timestamp_creation(self):
        """Test creating a bloom timestamp."""
        filter_bits = [0, 1, 0, 1, 0]
        counter = 5
        ts = BloomTimestamp(filter_bits, counter)
        self.assertEqual(ts.counter, 5)
        self.assertEqual(ts.filter, [0, 1, 0, 1, 0])

    def test_timestamp_from_list(self):
        """Test creating timestamp from list."""
        ts = BloomTimestamp.from_list([1, 0, 1], 3)
        self.assertEqual(ts.counter, 3)
        self.assertEqual(ts.filter, [1, 0, 1])

    def test_timestamp_to_list(self):
        """Test converting timestamp to list."""
        ts = BloomTimestamp([1, 0, 1], 3)
        filter_bits, counter = ts.to_list()
        self.assertEqual(counter, 3)
        self.assertEqual(filter_bits, [1, 0, 1])

    def test_timestamp_equality(self):
        """Test timestamp equality."""
        ts1 = BloomTimestamp([1, 0, 1], 3)
        ts2 = BloomTimestamp([1, 0, 1], 3)
        ts3 = BloomTimestamp([1, 0, 1], 4)
        self.assertEqual(ts1, ts2)
        self.assertNotEqual(ts1, ts3)

    def test_subset_relation(self):
        """Test bloom filter subset relation for causality."""
        # Empty filter is subset of any filter
        ts_empty = BloomTimestamp([0, 0, 0], 1)
        ts_some = BloomTimestamp([1, 0, 1], 2)
        self.assertTrue(ts_empty.is_subset_of(ts_some))
        self.assertFalse(ts_some.is_subset_of(ts_empty))

        # Same filter at different counters
        ts1 = BloomTimestamp([1, 0, 1], 1)
        ts2 = BloomTimestamp([1, 0, 1], 2)
        self.assertTrue(ts1.is_subset_of(ts2))
        self.assertTrue(ts2.is_subset_of(ts1))  # Same bits, different counter

    def test_concurrent_detection(self):
        """Test detection of concurrent events."""
        ts1 = BloomTimestamp([1, 0, 0], 1)
        ts2 = BloomTimestamp([0, 1, 0], 1)
        # Neither is subset of the other -> concurrent
        self.assertTrue(ts1.is_concurrent(ts2))

        ts3 = BloomTimestamp([1, 0, 0], 1)
        ts4 = BloomTimestamp([1, 1, 0], 1)
        # ts3 is subset of ts4 -> not concurrent
        self.assertFalse(ts3.is_concurrent(ts4))


class TestBloomClock(unittest.TestCase):
    """Test cases for BloomClock."""

    def test_clock_creation(self):
        """Test creating a bloom clock."""
        clock = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        self.assertEqual(clock._node_id, 0)
        self.assertEqual(clock._filter_size, 256)
        self.assertEqual(clock._num_hash_functions, 4)
        ts = clock.current_timestamp
        self.assertEqual(ts.counter, 0)
        self.assertEqual(len(ts.filter), 256)

    def test_clock_create_new(self):
        """Test creating a clock with create_new factory."""
        clock = BloomClock.create_new(node_id=1, num_servers=5)
        ts = clock.current_timestamp
        self.assertEqual(ts.counter, 0)
        self.assertTrue(all(bit == 0 for bit in ts.filter))

    def test_increment(self):
        """Test incrementing a bloom clock."""
        clock = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        initial_counter = clock.current_timestamp.counter
        clock.increment()
        self.assertEqual(clock.current_timestamp.counter, initial_counter + 1)
        # Bloom filter should have some bits set from hashing the node_id
        self.assertTrue(any(bit == 1 for bit in clock.current_timestamp.filter))

    def test_increment_monotonic(self):
        """Test that counter is monotonically increasing."""
        clock = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        counter_values = []
        for _ in range(5):
            clock.increment()
            counter_values.append(clock.current_timestamp.counter)
        # Check monotonic increase
        for i in range(1, len(counter_values)):
            self.assertGreater(counter_values[i], counter_values[i - 1])

    def test_update(self):
        """Test updating with another timestamp."""
        clock1 = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        clock2 = BloomClock(node_id=1, bloom_filter_size=256, num_hash_functions=4)

        # Increment both clocks
        clock1.increment()
        clock2.increment()
        clock2.increment()

        ts1 = clock1.current_timestamp
        ts2 = clock2.current_timestamp

        # clock1 updates with ts2
        initial_counter = ts1.counter
        clock1.update(ts2)
        
        # Counter should be updated and incremented
        self.assertGreater(clock1.current_timestamp.counter, initial_counter)
        
        # Bloom filter should have bits from both ts1 and ts2
        # (via OR operation)
        for i in range(len(ts1.filter)):
            if ts1.filter[i] == 1 or ts2.filter[i] == 1:
                self.assertEqual(clock1.current_timestamp.filter[i], 1)

    def test_causality_tracking(self):
        """Test that bloom clock tracks causality correctly."""
        clock1 = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        clock2 = BloomClock(node_id=1, bloom_filter_size=256, num_hash_functions=4)

        # Node 0 creates an event
        clock1.increment()
        ts1 = clock1.current_timestamp

        # Node 1 receives the event and processes it
        clock2.update(ts1)
        ts2 = clock2.current_timestamp

        # The timestamp from node 1 should have all bits from ts1
        # (due to the OR operation during update)
        for i in range(len(ts1.filter)):
            if ts1.filter[i] == 1:
                self.assertEqual(ts2.filter[i], 1,
                                f"Bit {i} should be set in ts2 after update")

    def test_different_filter_sizes(self):
        """Test bloom clocks with different filter sizes."""
        for size in [64, 128, 256, 512]:
            clock = BloomClock(node_id=0, bloom_filter_size=size)
            self.assertEqual(len(clock.current_timestamp.filter), size)
            clock.increment()
            self.assertEqual(len(clock.current_timestamp.filter), size)

    def test_different_hash_functions(self):
        """Test bloom clocks with different numbers of hash functions."""
        for k in [1, 2, 4, 8]:
            clock = BloomClock(node_id=0, num_hash_functions=k)
            clock.increment()
            # Count bits set
            bits_set = sum(clock.current_timestamp.filter)
            # Should have at most k bits set (possibly fewer due to collisions)
            self.assertLessEqual(bits_set, k)


class TestBloomClockIntegration(unittest.TestCase):
    """Integration tests for bloom clocks."""

    def test_multiple_nodes_causality(self):
        """Test causality tracking across multiple nodes."""
        clocks = [BloomClock(node_id=i) for i in range(3)]

        # Node 0 creates event
        clocks[0].increment()
        ts0 = clocks[0].current_timestamp

        # Node 1 receives event from node 0
        clocks[1].update(ts0)
        ts1 = clocks[1].current_timestamp

        # Node 2 receives event from node 1
        clocks[2].update(ts1)
        ts2 = clocks[2].current_timestamp

        # The timestamps should form a causality chain
        # ts0 bits should be subset of ts1, and ts1 bits should be subset of ts2
        self.assertTrue(ts0.is_subset_of(ts1))
        self.assertTrue(ts1.is_subset_of(ts2))

    def test_concurrent_events(self):
        """Test detection of concurrent events from different nodes."""
        clock1 = BloomClock(node_id=0)
        clock2 = BloomClock(node_id=1)

        # Both nodes create events independently (without receiving from each other)
        clock1.increment()
        clock2.increment()

        ts1 = clock1.current_timestamp
        ts2 = clock2.current_timestamp

        # These should be concurrent (neither is subset of other)
        # Note: due to hash collisions, this might not always be true,
        # but for large enough filters with few hash functions, it should be
        # This test is probabilistic but should pass with high probability
        self.assertTrue(ts1.is_concurrent(ts2),
                       "Independently created events should be concurrent")


if __name__ == "__main__":
    unittest.main()
