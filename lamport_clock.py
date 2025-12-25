"""
Lamport Clock implementation
"""

class LamportClock:
    def __init__(self, initial_time: int = 0):
        self._value = initial_time

    @classmethod
    def create_new(cls):
        return cls(0)

    def increment(self):
        """Increment the clock before a local event or sending a message."""
        self._value += 1
        return self._value

    def update(self, received_timestamp: int):
        """
        Update the clock upon receiving a message.
        C = max(local_clock, received_timestamp) + 1
        """
        self._value = max(self._value, received_timestamp) + 1
        return self._value

    @property
    def value(self) -> int:
        return self._value
