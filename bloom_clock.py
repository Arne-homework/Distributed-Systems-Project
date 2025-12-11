"""
Bloom Clock implementation based on 1905.13064v4.pdf

A Bloom Clock is a probabilistic alternative to Vector Clocks that uses a Bloom filter
to represent the causality information with fixed space complexity O(m) instead of O(n),
where m is the bloom filter size and n is the number of nodes.
"""

from typing import Self
import hashlib


class BloomTimestamp:
    """Represents a bloom clock timestamp with a bloom filter and logical counter."""

    def __init__(self, bloom_filter: list[int], counter: int):
        """
        Initialize a bloom timestamp.

        Args:
            bloom_filter: List of bits representing the bloom filter state
            counter: Logical clock counter value
        """
        self._filter = bloom_filter[:]  # Copy to avoid external modifications
        self._counter = counter

    @property
    def filter(self):
        """Returns a copy of the current bloom filter as a list"""
        return self._filter[:]

    @property
    def counter(self):
        """Returns the logical clock counter value"""
        return self._counter

    @classmethod
    def from_list(cls, filter_bits: list[int], counter: int) -> Self:
        """Create a timestamp from a filter and counter"""
        return cls(filter_bits, counter)

    def to_list(self) -> tuple[list[int], int]:
        """Convert the timestamp to a tuple of (filter, counter)"""
        return (self.filter, self._counter)

    def __eq__(self, other) -> bool:
        return self._filter == other._filter and self._counter == other._counter

    def __repr__(self):
        return f"BloomTimestamp(counter={self._counter}, filter={''.join(map(str, self._filter))})"

    def is_subset_of(self, other: Self) -> bool:
        """
        Check if this bloom filter is a subset of another.
        This is used for causality comparison.
        """
        if len(self._filter) != len(other._filter):
            return False
        for i in range(len(self._filter)):
            if self._filter[i] > other._filter[i]:
                return False
        return True

    def is_concurrent(self, other: Self) -> bool:
        """
        Check if two timestamps are definitely concurrent.
        Neither bloom filter is a subset of the other → definitely incomparable
        """
        return not self.is_subset_of(other) and not other.is_subset_of(self)


class BloomClock:
    """
    A Bloom Clock for tracking causality in distributed systems.
    Uses a bloom filter to efficiently represent which nodes have sent messages,
    achieving O(m) space complexity instead of O(n) for vector clocks.
    """

    def __init__(
        self,
        node_id: int,
        bloom_filter_size: int = 256,
        num_hash_functions: int = 4,
    ):
        """
        Initialize a new Bloom Clock.

        Args:
            node_id: The ID of the node owning this clock
            bloom_filter_size: Size of the bloom filter in bits (m)
            num_hash_functions: Number of independent hash functions to use (k)
        """
        self._node_id = node_id
        self._filter_size = bloom_filter_size
        self._num_hash_functions = num_hash_functions
        self._current_timestamp = BloomTimestamp(
            [0] * bloom_filter_size, 0
        )

    @classmethod
    def create_new(
        cls,
        node_id: int,
        num_servers: int = None,
        bloom_filter_size: int = 256,
        num_hash_functions: int = 4,
    ) -> Self:
        """
        Create a new BloomClock for the node.

        Args:
            node_id: The ID of the node
            num_servers: Number of servers (unused for bloom clocks, kept for API compatibility)
            bloom_filter_size: Size of the bloom filter
            num_hash_functions: Number of hash functions
        """
        return cls(
            node_id,
            bloom_filter_size=bloom_filter_size,
            num_hash_functions=num_hash_functions,
        )

    def _hash_element(self, element: int, hash_function_index: int) -> int:
        """
        Hash an element using one of the independent hash functions.

        Args:
            element: The element to hash (typically a node ID)
            hash_function_index: Which hash function to use (0 to k-1)

        Returns:
            A position in the bloom filter (0 to m-1)
        """
        # Create a unique hash input by combining element and hash function index
        hash_input = f"{element}:{hash_function_index}".encode()
        hash_obj = hashlib.md5(hash_input)
        hash_value = int(hash_obj.hexdigest(), 16)
        return hash_value % self._filter_size

    def _set_bits_for_element(self, filter_bits: list[int], element: int) -> None:
        """
        Set the k bits corresponding to an element in the bloom filter.

        Args:
            filter_bits: The bloom filter to modify (in-place)
            element: The element to add (typically node ID)
        """
        for i in range(self._num_hash_functions):
            pos = self._hash_element(element, i)
            filter_bits[pos] = 1

    def increment(self):
        """
        Increment the clock for a local event.
        Updates the bloom filter with the local node's ID and increments the counter.
        """
        filter_bits = self._current_timestamp.filter
        self._set_bits_for_element(filter_bits, self._node_id)
        counter = self._current_timestamp.counter + 1
        self._current_timestamp = BloomTimestamp(filter_bits, counter)

    def update(self, other: BloomTimestamp):
        """
        Update the clock when receiving a timestamp from another node.
        Merges the bloom filters and updates the counter.

        Args:
            other: The bloom timestamp received from another node
        """
        # Merge bloom filters using bitwise OR
        merged_filter = [
            self._current_timestamp.filter[i] | other.filter[i]
            for i in range(self._filter_size)
        ]

        # Update counter: take max and increment
        counter = max(self._current_timestamp.counter, other.counter) + 1

        # Set bits for this node's ID
        self._set_bits_for_element(merged_filter, self._node_id)

        self._current_timestamp = BloomTimestamp(merged_filter, counter)

    @property
    def current_timestamp(self) -> BloomTimestamp:
        """Get the current bloom clock timestamp"""
        return self._current_timestamp
