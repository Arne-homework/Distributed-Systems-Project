"""
Tests for the Bloom Clock implementation.
"""

import unittest
from bloom_clock import BloomClock, BloomTimestamp


class TestBloomTimestamp(unittest.TestCase):
    """Test cases for BloomTimestamp."""

    def test_timestamp_creation(self):
        """Test creating a bloom timestamp."""
        filter = [1, 15, 3, 4, 8]
        ts = BloomTimestamp(filter)
        self.assertEqual(ts.filter, filter)

    def test_timestamp_from_list(self):
        """Test creating timestamp from list."""
        ts = BloomTimestamp.from_list([1, 0, 1])
        self.assertEqual(ts.filter, [1, 0, 1])

    def test_timestamp_to_list(self):
        """Test converting timestamp to list."""
        ts = BloomTimestamp([1, 0, 1])
        filter = ts.to_list()
        self.assertEqual(filter, [1, 0, 1])

    def test_timestamp_equality(self):
        """Test timestamp equality."""
        ts1 = BloomTimestamp([1, 0, 1])
        ts2 = BloomTimestamp([1, 0, 1])
        ts3 = BloomTimestamp([1, 1, 1])
        self.assertEqual(ts1, ts2)
        self.assertNotEqual(ts1, ts3)

    def test_lt_relation(self):
        """BloomTimestamp has a  <  showing causality.

        timestamp0 < timestamp1 => timestamp0 before timestamp1
        """
        timestamp_empty = BloomTimestamp([0, 0, 0])
        timestamp_some = BloomTimestamp([1, 0, 1])
        timestamp_other = BloomTimestamp([0, 1, 0])

        self.assertTrue(timestamp_empty < timestamp_some)
        self.assertTrue(timestamp_empty < timestamp_other)
        self.assertFalse(timestamp_some < timestamp_some)
        self.assertFalse(timestamp_some < timestamp_other)

    def test_concurrent_detection(self):
        """Test detection of concurrent events."""
        ts1 = BloomTimestamp([1, 0, 0])
        ts2 = BloomTimestamp([0, 1, 0])
        # Neither is subset of the other -> concurrent
        self.assertTrue(ts1.is_concurrent(ts2))

        ts3 = BloomTimestamp([1, 0, 0])
        ts4 = BloomTimestamp([1, 1, 0])
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
        timestamp = clock.current_timestamp
        self.assertEqual(len(timestamp.filter), 256)

    def test_clock_create_new(self):
        """Test creating a clock with create_new factory."""
        clock = BloomClock.create_new(node_id=1)
        timestamp = clock.current_timestamp
        self.assertTrue(all(number == 0 for number in timestamp.filter))

    def test_increment(self):
        """Test incrementing a bloom clock."""
        event_id = "deadbeef"
        clock = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        clock.increment(event_id)
        # Bloom filter should have some bits set from hashing the node_id
        self.assertTrue(any(bit == 1 for bit in clock.current_timestamp.filter))

    def test_increment_monotonic(self):
        """Test that counter is monotonically increasing."""
        clock = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        timestamp_values = []
        for i in range(5):
            event_id = "deadbeef"+str(i)
            clock.increment(event_id)
            timestamp_values.append(clock.current_timestamp)
        # Check monotonic increase
        for i in range(1, len(timestamp_values)):
            self.assertLess(timestamp_values[i - 1], timestamp_values[i])

    def test_update(self):
        """Test updating with another timestamp."""
        clock1 = BloomClock(node_id=0, bloom_filter_size=256, num_hash_functions=4)
        clock2 = BloomClock(node_id=1, bloom_filter_size=256, num_hash_functions=4)

        event_id0 = "deadbeef"
        event_id1 = "deadbeee"
        event_id2 = "deadbeed"
        # Increment both clocks
        clock1.increment(event_id0)
#        timestamp0 = clock1.current_timestamp
        clock2.increment(event_id1)
        timestamp1 = clock2.current_timestamp
        clock2.increment(event_id2)
        timestamp2 = clock2.current_timestamp

        self.assertTrue(timestamp1 < timestamp2)

        clock1.update(timestamp1)

        # Counter should be updated and incremented
        self.assertLess(timestamp1, clock1.current_timestamp)

    def test_causality_tracking(self):
        """Test that bloom clock tracks causality correctly."""
        clock0 = BloomClock(node_id=0,
                            bloom_filter_size=256,
                            num_hash_functions=4)
        clock1 = BloomClock(node_id=1,
                            bloom_filter_size=256,
                            num_hash_functions=4)

        event_id0 = "deadbeef"
        event_id1 = "deadbeee"
        # Node 0 creates an event
        clock0.increment(event_id0)
        timestamp0 = clock1.current_timestamp

        # Node 1 generates its own event
        clock1.increment(event_id1)
        # Node 1 receives the event and processes it
        clock1.update(timestamp0)
        timestamp1 = clock1.current_timestamp

        self.assertLess(timestamp0, timestamp1)


class TestBloomClockIntegration(unittest.TestCase):
    """Integration tests for bloom clocks."""

    def test_multiple_nodes_causality(self):
        """Test causality tracking across multiple nodes."""
        clocks = [BloomClock(node_id=i) for i in range(3)]
        event_id0 = "deadbeef"
        event_id1 = "deadbeee"# Node 0 creates event
        event_id2 = "deadbeed"# Node 0 creates event
        event_id3 = "deadbeec"# Node 0 creates event
        event_id4 = "deadbeeb"# Node 0 creates event
        clocks[0].increment(event_id0)
        timestamp0 = clocks[0].current_timestamp

        # Node 1 receives event from node 0
        clocks[1].update(timestamp0)
        # other than lmport and vector clocks, bloom clocks do not increase upon recieving an event.
        clocks[1].increment(event_id1)
        timestamp1 = clocks[1].current_timestamp

        # Node 2 receives event from node 1
        clocks[2].update(timestamp1)
        clocks[2].increment(event_id2)
        timestamp2 = clocks[2].current_timestamp

        # The timestamps should form a causality chain
        self.assertLess(timestamp0, timestamp1)
        self.assertLess(timestamp1, timestamp2)


if __name__ == "__main__":
    unittest.main()
