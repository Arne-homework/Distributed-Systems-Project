"""
Module to create node specific (localized) clocks.

"""

from typing import Callable, Iterable

__all__ = ["IClock", "clock_server"]


class IClock:
    def get_time(self) -> float:
        """
        returns the time.
        Is guaranteed to deliver a monoton increasing time.
        """
        raise NotImplementedError()


class ExternalDeterminedClock(IClock):
    """
    Default clock class.
    """

    def __init__(self):
        self._time = 0.0

    def set_time(self, time: float):
        """
        Set the time which the following calls to get_time will return.
        """
        assert time >= self._time
        self._time = time

    def get_time(self) -> float:
        """
        returns the time.
        Is guaranteed to deliver a monoton increasing time.
        """
        return self._time


class ClockServer:
    def __init__(self, factory: Callable[[int], IClock]):
        self._clocks = {}
        self._clock_factory = factory

    def get_clock_for_node(self, node_id: int) -> IClock:
        """
        Get a clock for a specific node.
        """
        if node_id not in self._clocks:
            self._clocks[node_id] = self._clock_factory(node_id)
        return self._clocks[node_id]

    def all_clocks(self) -> Iterable[tuple[int,IClock]]:
        """
        Returns a generator for all already created clocks.
        """
        for node_id, clock in self._clocks.items():
            yield node_id, clock

    def set_clock_factory(self, factory: Callable[[int], IClock]):
        """
        Set a factory for clocks.

        Clears all already created clocks.
        New calls to get_clock_for_node will create new clocks,
        however previously retrieved clocks outside this class remain valid.

        intended for testing.
        """
        self._clock_factory = factory
        self._clocks = {}


clock_server = ClockServer(lambda n: ExternalDeterminedClock())
