from sqlalchemy import Column, Integer, String, ForeignKey, Table
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

create_previous_events_table_string = """
CREATE TABLE previous_events(
  row_id INTEGER NOT NULL PRIMARY KEY,
  event_id VARCHAR NOT NULL,
  previous_event_id VARCHAR NOT NULL,
  FOREIGN KEY(event_id) REFERENCES events(event_id),
  FOREIGN KEy(previous_event_id) REFERENCES events(event_id)
)"""


class Event:
    pass


class EventStore:
    def __init__(self, name: String):
        self._engine = self._create_engine(name)

    def _create_engine(self, name):
        return create_engine(name, echo=True)

    def initialize_database(self):
        """
        Creates the tables of the database, and performs other setup work.

        """
        with self._engine.connect() as conn:
            conn.execute(
                text(create_events_table_string)
            )
            conn.execute(
                text(create_previous_events_table_string)
            )
            conn.commit()

    def add_event(self, event: Event):
        pass
