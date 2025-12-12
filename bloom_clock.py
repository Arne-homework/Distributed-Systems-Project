"""
Bloom Clock implementation based on "The Bloom Clock" by Lum Ramabaja, http://arxiv.org/abs/1905.13064

A Bloom Clock is a probabilistic alternative to Vector Clocks that uses a Bloom filter
to represent the causality information with fixed space complexity O(m) instead of O(n),
where m is the bloom filter size and n is the number of nodes.
"""

from typing import Self
import hashlib


class BloomTimestamp:
    """Represents a bloom clock timestamp with a bloom filter and logical counter."""

    def __init__(self, bloom_filter: list[int]):
        """
        Initialize a bloom timestamp.

        Args:
            bloom_filter: List of bits representing the bloom filter state
        """
        self._filter = bloom_filter[:]  # Copy to avoid external modifications

    @property
    def filter(self):
        """Returns a copy of the current bloom filter as a list"""
        return self._filter[:]

    @classmethod
    def from_list(cls, filter: list[int]) -> Self:
        """Create a timestamp from a filter """
        return cls(filter)

    def to_list(self) -> list[int]:
        """Convert the timestamp to a list (the bloom filter)"""
        return self.filter[:]

    def __eq__(self, other) -> bool:
        return self._filter == other._filter 

    def __repr__(self):
        return f"BloomTimestamp( filter=[{', '.join(map(str, self._filter))}])"

    def __lt__(self, other: Self) -> bool:
        """
        Check if this bloom filter is a subset of another.
        This is used for causality comparison.
        """
        if len(self._filter) != len(other._filter):
            raise ValueError("BloomTimestamps must be of the same size for comparison.")
        return_value = False
        for i in range(len(self._filter)):
            if self._filter[i] < other._filter[i]:
                return_value = True
            elif self._filter[i] > other._filter[i]:
                return False
        return return_value

    def is_concurrent(self, other: Self) -> bool:
        """
        Check if two timestamps are definitely concurrent.
        Neither bloom filter is a causal to the other -> concurrent.

        We consider a filter causal to itself.
        """
        return not self < other and not other < self


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
            [0] * bloom_filter_size
        )

    @classmethod
    def create_new(
        cls,
        node_id: int,
        bloom_filter_size: int = 256,
        num_hash_functions: int = 4,
    ) -> Self:
        """
        Create a new BloomClock for the node.

        Args:
            node_id: The ID of the node
            num_servers: Number of servers
                         (unused for bloom clocks, kept for API compatibility)
            bloom_filter_size: Size of the bloom filter
            num_hash_functions: Number of hash functions
        """
        return cls(
            node_id,
            bloom_filter_size=bloom_filter_size,
            num_hash_functions=num_hash_functions,
        )

    def _hash_id_with_function(self, id_: str, hash_function_index: int) -> int:
        """
        Hash an id using one of the independent hash functions.

        Args:
            id: The id to hash (typically a element/event ID)
            hash_function_index: Which hash function to use
                                (0 to num_hash_functions-1)

        Returns:
            A position in the bloom filter (0 to filter_size-1)
        """
        # Create a unique hash input by combining element and hash function index
        hash_input = f"{id_}:{hash_function_index}".encode()
        hash_obj = hashlib.md5(hash_input)
        hash_value = int(hash_obj.hexdigest(), 16)
        return hash_value % self._filter_size

    def _hash_id(self,  id_: str) -> list[int]:
        """
        Set the k bits corresponding to an element in the bloom filter.

        Args:
            filter_bits: The bloom filter to modify (in-place)
            element: The element to add (typically node ID)
        """
        filter = [0] * self._filter_size
        for i in range(self._num_hash_functions):
            pos = self._hash_id_with_function(id_, i)
            filter[pos] = 1
        return filter

    def increment(self, event_id: str):
        """
        Increment the clock for a local event.
        Updates the bloom filter with the events id.
        """
        update = self._hash_id(event_id)
        new_filter = [0] * self._filter_size
        for position in range(self._filter_size):
            new_filter[position] = (self._current_timestamp.filter[position]
                                    + update[position])
        self._current_timestamp = BloomTimestamp(new_filter)

    def update(self, other: BloomTimestamp):
        """
        Update the clock when receiving a timestamp from another node.
        Merges the bloom filters and updates the counter.

        Args:
            other: The bloom timestamp received from another node
        """
        merged_filter = [0]*self._filter_size
        for position in range(self._filter_size):
            merged_filter[position] = max(
                self._current_timestamp.filter[position],
                other.filter[position])

        self._current_timestamp = BloomTimestamp(merged_filter)

    @property
    def current_timestamp(self) -> BloomTimestamp:
        """Get the current bloom clock timestamp"""
        return self._current_timestamp
