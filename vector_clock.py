from typing import Self


class VectorTimestamp:
    def __init__(self, vector):
        self._vector = [int(i) for i in vector]

    @property
    def vector(self):
        """returns a copy of the current counters as a list"""
        return self._vector[:]

    @classmethod
    def from_list(cls, entries: list[int]) -> Self:
        """Initializes the timestamp from a list"""
        return cls(entries)

    def to_list(self) -> list:
        """converts the Timestamp to a list"""
        return self.vector

    def __eq__(self, other) -> bool:
        return self._vector == other._vector

    def __repr__(self):
        return f"VectorTimestamp({self._vector})"

    def is_concurrent(self, other):
        """
        Checks if two timestamps are concurrent(without causal ordering).

        equal timestamps are currently treated as concurrent.
        """
        return (not self < other) and (not other < self)

    def __lt__(self, other: Self):
        assert len(other._vector) == len(self._vector)
        res = False
        for i in range(len(self._vector)):
            if self._vector[i] < other._vector[i]:
                res = True
            elif self._vector[i] > other._vector[i]:
                return False
        return res


class VectorClock():
    def __init__(self, node_id, start_timestamp):
        self._node_id = node_id
        self._current_timestamp = start_timestamp

    @classmethod
    def create_new(cls, node_id, number_of_nodes):
        """Create a new VectorClock for the node with the node_id and a specified number of nodes"""
        return cls(node_id, VectorTimestamp([0 for i in range(number_of_nodes)]))

    def increment(self):
        """Increment the counter for the associated node."""
        vector = self._current_timestamp.vector
        vector[self._node_id] += 1
        self._current_timestamp = VectorTimestamp(vector)

    def update(self, other: VectorTimestamp):
        """
        Updates the counters with counters from another timestamp
        (Always chooses the larger counter for each node)
        AND increments the own counter.
        """
        assert len(other.vector) == len(self._current_timestamp.vector)
        vector = []
        for i in range(len(other.vector)):
            vector.append(max(self._current_timestamp.vector[i],
                              other.vector[i]))
        self._current_timestamp = VectorTimestamp(vector)
        self.increment()

    @property
    def current_timestamp(self):
        return self._current_timestamp
