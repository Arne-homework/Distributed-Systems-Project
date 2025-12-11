# coding=utf-8
"""
Implementing Eventsourcing
"""
from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy import create_engine
from vector_clock import VectorTimestamp
import json


create_events_table_string = """
CREATE TABLE events(
   event_id VARCHAR NOT NULL PRIMARY KEY,
   entry_id VARCHAR NOT NULL,
   creation_timestamp INTEGER NOT NULL,
   vector_timestamp VARCHAR NOT NULL,
   bloom_timestamp VARCHAR NOT NULL,
   lamport_timestamp INTEGER NOT NULL,
   action VARCHAR NOT NULL,
   value VARCHAR NOT NULL
);"""

create_depended_events_table_string = """
CREATE TABLE depended_events(
  row_id INTEGER NOT NULL PRIMARY KEY,
  event_id VARCHAR NOT NULL,
  depended_event_id VARCHAR NOT NULL,
  FOREIGN KEY(event_id) REFERENCES events(event_id)
);"""


class Event:
    def __init__(self, event_id, entry_id, creation_timestamp,
                 vector_timestamp, bloom_timestamp, lamport_timestamp,
                 action, value, depended_event_ids):
        # The unique id of this event
        self._event_id = event_id
        # Id of the entry this event modifies.
        self._entry_id = entry_id
        # timestamp of the creation of this event.
        # This based upon the local time of the creating node.
        self._creation_timestamp = int(creation_timestamp)
        # Vector timestamp to track causality between events.
        self._vector_timestamp = vector_timestamp
        # Bloom timestamp to track causality between events.
        self._bloom_timestamp = bloom_timestamp
        # the Lamport timestamp
        self._lamport_timestamp = int(lamport_timestamp)
        # Action that created this event.
        self._action = action
        # Usually the new value of the entry; "" for Deletion events.
        self._value = value
        # ids of events on which this event depends.
        self._depended_event_ids = depended_event_ids

    @property
    def event_id(self):
        return self._event_id

    @property
    def entry_id(self):
        return self._entry_id

    @property
    def creation_timestamp(self):
        return self._creation_timestamp

    @property
    def vector_timestamp(self):
        return self._vector_timestamp

    @property
    def bloom_timestamp(self):
        return self._bloom_timestamp

    @property
    def lamport_timestamp(self):
        return self._lamport_timestamp

    @property
    def action(self):
        return self._action

    @property
    def value(self):
        return self._value

    @property
    def depended_event_ids(self):
        return self._depended_event_ids

    def __eq__(self, other):
        return ((self._event_id == other._event_id)
                and (self._entry_id == other._entry_id)
                and (self._creation_timestamp == other._creation_timestamp)
                and (self._vector_timestamp == other._vector_timestamp)
                and (self._bloom_timestamp == other._bloom_timestamp)
                and (self._lamport_timestamp == other._lamport_timestamp)
                and (self._action == other._action)
                and (self._value == other._value)
                and (self._depended_event_ids == other._depended_event_ids))

    def to_dict(self):
        bloom_filter, bloom_counter = self._bloom_timestamp.to_list()
        return {"event_id": self._event_id,
                "entry_id": self._entry_id,
                "creation_timestamp": self._creation_timestamp,
                "vector_timestamp": self._vector_timestamp.to_list(),
                "bloom_timestamp": {
                    "filter": bloom_filter,
                    "counter": bloom_counter
                },
                "lamport_timestamp": self._lamport_timestamp,
                "action": self._action,
                "value": self._value,
                "depended_event_ids": self._depended_event_ids}

    @staticmethod
    def from_dict(dict_):
        from bloom_clock import BloomTimestamp
        dd = dict_  # just make it shorter to write
        bloom_ts_data = dd.get("bloom_timestamp", {"filter": [], "counter": 0})
        bloom_timestamp = BloomTimestamp(
            bloom_ts_data["filter"],
            bloom_ts_data["counter"]
        )
        return Event(
            dd["event_id"],
            dd["entry_id"],
            dd["creation_timestamp"],
            VectorTimestamp.from_list(dd["vector_timestamp"]),
            bloom_timestamp,
            dd["lamport_timestamp"],
            dd["action"],
            dd["value"],
            dd["depended_event_ids"])


class History:
    """
    Represents the history of an entry.

    The history is in general a DAG of events.
    """

    def __init__(self, events: dict[str, Event],
                 inverted_dependencies: dict[str, list[str]],
                 root_events: list[Event]):
        # A mapping from the event ids to the event.
        self.events = events
        # mapping the id of an event to its successors.
        self.inverted_dependencies = inverted_dependencies
        # the events that have no predecessor in the history.
        self.root_events = root_events


class EventStore:
    def __init__(self, name: str, echo: bool = True):
        self._engine = self._create_engine(name, echo)

    def _create_engine(self, name, echo=True):
        """
        creates the database engine

        if the echo parameter is left true,
          the engine will print all queries and commands send to the database.
        """
        return create_engine(name, echo=echo)

    def initialize_database(self):
        """
        Creates the tables of the database, and performs any other setup work.

        Not needed if the database connects to
          an already existing database file.
        """
        with self._engine.begin() as conn:
            conn.execute(
                text(create_events_table_string)
            )
            conn.execute(
                text(create_depended_events_table_string)
            )

    def add_event(self, event: Event):
        """
        Add an event to the store.

        May throw if any constraints are violated.
        """
        bloom_filter, bloom_counter = event.bloom_timestamp.to_list()
        with self._engine.begin() as conn:
            conn.execute(
                text("INSERT INTO events"
                     " (event_id, entry_id, creation_timestamp, vector_timestamp,"
                     " bloom_timestamp, lamport_timestamp, action, value)"
                     "VALUES(:event_id,:entry_id,:creation_timestamp, :vector_timestamp,"
                     " :bloom_timestamp, :lamport_timestamp, :action,:value)"),
                {"event_id": event.event_id,
                 "entry_id": event.entry_id,
                 "creation_timestamp": event.creation_timestamp,
                 "vector_timestamp": json.dumps(
                     event.vector_timestamp.to_list()),
                 "bloom_timestamp": json.dumps({
                     "filter": bloom_filter,
                     "counter": bloom_counter
                 }),
                 "lamport_timestamp": event.lamport_timestamp,
                 "action": event.action,
                 "value": event.value})
            if len(event.depended_event_ids) > 0:
                conn.execute(
                    text(
                        "INSERT INTO depended_events"
                        "(event_id, depended_event_id)"
                        "VALUES (:event_id, :depended_event_id)"),
                    [{"event_id": event.event_id,
                      "depended_event_id": depended_event_id}
                     for depended_event_id in event.depended_event_ids]
                )

    def get_history(self, entry_id: String):
        """
        return the full history of an entry identified by the entries id.
        """
        from bloom_clock import BloomTimestamp
        with self._engine.begin() as conn:
            events_list = list(conn.execute(
                text(
                    "SELECT event_id, entry_id, creation_timestamp,"
                    " vector_timestamp, bloom_timestamp, lamport_timestamp, action, value"
                    " FROM events WHERE entry_id LIKE :entry_id"
                ),
                {"entry_id": entry_id}
            )
                           )
            events = {}
            dependend_events = []
            for event_tuple in events_list:
                dependend_events.extend(
                    list(
                        conn.execute(
                            text(
                                "SELECT event_id, depended_event_id"
                                " FROM depended_events"
                                " WHERE event_id=:event_id"
                            ),
                            {"event_id": event_tuple[0]}
                        )
                    )
                )
            dependencies = {}
            inverted_dependencies = {}
            for entry in dependend_events:
                if entry[0] not in dependencies:
                    dependencies[entry[0]] = []
                if entry[1] not in inverted_dependencies:
                    inverted_dependencies[entry[1]] = []
                dependencies[entry[0]].append(entry[1])
                inverted_dependencies[entry[1]].append(entry[0])
            for event_tuple in events_list:
                event_id, entry_id, creation_timestamp, vector_timestamp, \
                     bloom_timestamp, lamport_timestamp, action, value = event_tuple
                vector_timestamp = VectorTimestamp(json.loads(vector_timestamp))
                bloom_ts_data = json.loads(bloom_timestamp) if bloom_timestamp else {"filter": [], "counter": 0}
                bloom_timestamp = BloomTimestamp(
                    bloom_ts_data.get("filter", []),
                    bloom_ts_data.get("counter", 0)
                )
                dependend_events = (dependencies[event_id]
                                    if (event_id in dependencies)
                                    else [])
                events[event_id] = Event(event_id, entry_id,
                                         creation_timestamp, vector_timestamp,
                                         bloom_timestamp, lamport_timestamp, action,
                                         value, dependend_events)
        diff = set(events.keys())-set(dependencies.keys())
        root_events = [events[id] for id in diff]
        return History(events, inverted_dependencies, root_events)
