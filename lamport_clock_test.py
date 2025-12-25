"""
Tests for the Lamport Clock implementation.
"""

import unittest
from lamport_clock import LamportClock

class TestLamportClock(unittest.TestCase):
    """Test cases for LamportClock."""

    def test_clock_initialization(self):
        """Test that the clock starts at 0."""
        clock = LamportClock.create_new()
        self.assertEqual(clock.value, 0)

    def test_increment(self):
        """Test that increment increases the clock by 1."""
        clock = LamportClock(10)
        clock.increment()
        self.assertEqual(clock.value, 11)
        clock.increment()
        self.assertEqual(clock.value, 12)

    def test_update_from_lower(self):
        """Test update when internal clock is higher than received timestamp."""
        clock = LamportClock(10)
        # Received timestamp 5 is lower than local 10
        # Formula: max(10, 5) + 1 = 11
        clock.update(5)
        self.assertEqual(clock.value, 11)

    def test_update_from_higher(self):
        """Test update when internal clock is lower than received timestamp."""
        clock = LamportClock(10)
        # Received timestamp 20 is higher than local 10
        # Formula: max(10, 20) + 1 = 21
        clock.update(20)
        self.assertEqual(clock.value, 21)

    def test_monotonic_property(self):
        """Test that the clock never goes backward."""
        clock = LamportClock(100)
        clock.update(50)
        self.assertGreaterEqual(clock.value, 100)

if __name__ == "__main__":
    unittest.main()
