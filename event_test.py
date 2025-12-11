import unittest as ut

from sqlalchemy import text
from event import Event, EventStore
from vector_clock import VectorTimestamp
from bloom_clock import BloomTimestamp


class TestEvent(ut.TestCase):
    def test_serialization(self):
        """An Event can be serialized to a dict and deserialized from a dict."""
        vector_ts = VectorTimestamp([1, 0, 0])
        bloom_ts = BloomTimestamp([1, 0, 1, 0], 1)
        event0 = Event("asdfasd011-56", "ygdras", 15, vector_ts, bloom_ts, 0, "create", "hsdsf", [])
        event1 = Event.from_dict(event0.to_dict())
        self.assertEqual(event0, event1)


class TestEventStore(ut.TestCase):
    def test_add_event(self):
        """an event can be added to the EventStore"""
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        vector_ts = VectorTimestamp([1, 0, 0])
        bloom_ts = BloomTimestamp([1, 0, 1, 0], 1)
        event0 = Event("asdfasd011-56", "ygdras", 15, vector_ts, bloom_ts, 0, "create", "hsdsf", [])

        event_store1.add_event(event0)

    def test_add_two_dependend_events(self):
        """Multiple Events depending upon each other can be added to the store.
        """
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        vector_ts1 = VectorTimestamp([1, 0, 0])
        bloom_ts1 = BloomTimestamp([1, 0, 1, 0], 1)
        event0 = Event("asdfasd011-56", "ygdras", 15, vector_ts1, bloom_ts1, 0, "create", "hsdsf", [])
        
        vector_ts2 = VectorTimestamp([2, 0, 0])
        bloom_ts2 = BloomTimestamp([1, 1, 1, 0], 2)
        event1 = Event("grasded013-57", "ygdras", 16, vector_ts2, bloom_ts2, 0,
                       "update", "hadsf", ["asdfasd011-56"])

        event_store1.add_event(event0)
        event_store1.add_event(event1)

    def test_get_history(self):
        """The history of an Entry can be retrieved, containing all the events
        relating to that Entry and their inverted dependencies.
        """
        event_store1 = EventStore("sqlite+pysqlite:///:memory:", False)
        event_store1.initialize_database()

        vector_ts1 = VectorTimestamp([1, 0, 0])
        bloom_ts1 = BloomTimestamp([1, 0, 1, 0], 1)
        event0 = Event("asdfasd011-56", "ygdras", 15, vector_ts1, bloom_ts1, 0, "create", "hsdsf", [])
        
        vector_ts2 = VectorTimestamp([2, 0, 0])
        bloom_ts2 = BloomTimestamp([1, 1, 1, 0], 2)
        event1 = Event("grasded013-57", "ygdras", 16, vector_ts2, bloom_ts2, 0,
                       "update", "hadsf", ["asdfasd011-56"])

        event_store1.add_event(event0)
        event_store1.add_event(event1)
        history = event_store1.get_history("ygdras")
        self.assertEqual(len(history.events), 2)
        self.assertEqual(len(history.inverted_dependencies), 1)
        self.assertEqual(history.root_events[0].event_id, "asdfasd011-56")
