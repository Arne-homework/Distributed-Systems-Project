from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy import create_engine


create_events_table_string = """
CREATE TABLE events(
   event_id VARCHAR NOT NULL PRIMARY KEY,
   entry_id VARCHAR NOT NULL,
   timestamp INTEGER NOT NULL,
   action VARCHAR NOT NULL,
   value VARCHAR NOT NULL
)"""

create_depended_events_table_string = """
CREATE TABLE depended_events(
  row_id INTEGER NOT NULL PRIMARY KEY,
  event_id VARCHAR NOT NULL,
  depended_event_id VARCHAR NOT NULL,
  FOREIGN KEY(event_id) REFERENCES events(event_id),
  FOREIGN KEy(depended_event_id) REFERENCES events(event_id)
)"""


class Event:
    def __init__(self, event_id, entry_id, timestamp,
                 action, value, depended_event_ids):
        self._event_id = event_id
        self._entry_id = entry_id
        self._timestamp = int(timestamp)
        self._action = action
        self._value = value
        self._depended_event_ids = depended_event_ids

    @property
    def event_id(self):
        return self._event_id

    @property
    def entry_id(self):
        return self._entry_id

    @property
    def timestamp(self):
        return self._timestamp

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
                and (self._timestamp == other._timestamp)
                and (self._action == other._action)
                and (self._value == other._value)
                and (self._depended_event_ids == other._depended_event_ids) )
    
    def to_dict(self):
        return {"event_id": self._event_id,
                "entry_id": self._entry_id,
                "timestamp": self._timestamp,
                "action": self._action,
                "value": self._value,
                "depended_event_ids": self._depended_event_ids}

    @staticmethod
    def from_dict( dict_):
        dd = dict_  # just make it shorter to write
        return Event(
            dd["event_id"],
            dd["entry_id"],
            dd["timestamp"],
            dd["action"],
            dd["value"],
            dd["depended_event_ids"])


class History:
    def __init__(self, events: dict[str, Event],
                 inverted_dependencies: dict[str, list[str]],
                 root_event: Event):
        self.events = events
        self.inverted_dependencies = inverted_dependencies
        self.root_event = root_event


class EventStore:
    def __init__(self, name: String, echo: bool = True):
        self._engine = self._create_engine(name, echo)

    def _create_engine(self, name, echo=True):
        return create_engine(name, echo=echo)

    def initialize_database(self):
        """
        Creates the tables of the database, and performs other setup work.

        """
        with self._engine.begin() as conn:
            conn.execute(
                text(create_events_table_string)
            )
            conn.execute(
                text(create_depended_events_table_string)
            )

    def add_event(self, event: Event):
        with self._engine.begin() as conn:
            conn.execute(
                text("INSERT INTO events"
                     "(event_id, entry_id, timestamp, action, value)"
                     "VALUES(:event_id,:entry_id,:timestamp,:action,:value)"),
                {"event_id": event.event_id,
                 "entry_id": event.entry_id,
                 "timestamp": event.timestamp,
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
        with self._engine.begin() as conn:
            events_list = list(conn.execute(
                text(
                    "SELECT event_id, entry_id, timestamp, action, value"
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
                event_id, entry_id, timestamp, action, value = event_tuple
                dependend_events = (dependencies[event_id]
                                    if (event_id in dependencies)
                                    else [])
                events[event_id] = Event(event_id, entry_id, timestamp, action,
                                         value, dependend_events)
        diff = set(events.keys())-set(dependencies.keys())
        assert len(diff) == 1
        root_event = events[diff.pop()]
        return History(events, inverted_dependencies, root_event)
