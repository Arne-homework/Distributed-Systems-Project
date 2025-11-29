import unittest as ut

from sqlalchemy import text
from event import Event, EventStore


class TestEvent(ut.TestCase):
    def test_serialization(self):
        """An Event can be serialized to a dict and deserialized from a dict."""
        event0 = Event("asdfasd011-56", "ygdras", 15, "create", "hsdsf", [])
        event1 = Event.from_dict(event0.to_dict())
        self.assertEqual(event0, event1)


class TestEventStore(ut.TestCase):
    def test_add_event(self):
        """an event can be added to the EventStore"""
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        event0 = Event("asdfasd011-56", "ygdras", 15, "create", "hsdsf", [])

        event_store1.add_event(event0)

    def test_add_two_dependend_events(self):
        """Multiple Events depending upon each other can be added to the store.
        """
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        event0 = Event("asdfasd011-56", "ygdras", 15, "create", "hsdsf", [])
        event1 = Event("grasded013-57", "ygdras", 16,
                       "update", "hadsf", ["asdfasd011-56"])

        event_store1.add_event(event0)
        event_store1.add_event(event1)

    def test_get_history(self):
        """The history of an Entry can be retrieved, containing all the events
        relating to that Entry and their inverted dependencies.
        """
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        event0 = Event("asdfasd011-56", "ygdras", 15, "create", "hsdsf", [])
        event1 = Event("grasded013-57", "ygdras", 16,
                       "update", "hadsf", ["asdfasd011-56"])

        event_store1.add_event(event0)
        event_store1.add_event(event1)
        history = event_store1.get_history("ygdras")
        self.assertEqual(len(history.events), 2)
        self.assertEqual(len(history.inverted_dependencies), 1)
        self.assertEqual(history.root_event.event_id, "asdfasd011-56")
