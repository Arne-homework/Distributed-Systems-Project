import unittest as ut

from event import Event


class TestEvent(ut.TestCase):
    def test_serialization(self):
        """
        An Event can be serialized to a dict and deserialized from a dict.
        """
        event0 = Event(
            "cc6df142bed34192b18a826c122c7c04",
            "d103ba6d3491443d8646e877a43fd4d7",
            "create",
            "Entry was created")
        event1 = Event.from_dict(event0.to_dict())
        self.assertEqual(event0, event1)


