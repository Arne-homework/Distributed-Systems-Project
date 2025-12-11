# coding=utf-8
import random
import messenger
import logging
from abc import ABC, abstractmethod
from clock import clock_server
from event import Event, EventStore
from id_generator import RandomGenerator
from vector_clock import VectorClock
from bloom_clock import BloomClock
from typing import Union

logger = logging.getLogger(__name__)


class Entry:
    def __init__(self, id, value, vector_timestamp, bloom_timestamp, last_event_id):
        self.id = id
        self.value = value
        self.vector_timestamp = vector_timestamp
        self.bloom_timestamp = bloom_timestamp
        # The id of the last event defining this entries value.
        self.last_event_id = last_event_id

    def to_dict(self) -> dict:
        return {"id": self.id, "value": self.value}

    def __str__(self):
        return str(self.to_dict())


class ISorter(ABC):
    # Interface for sorters.
    # We are supposed to compare different Logical Clocks
    # To quickly switch between the used Clock, we implement different sorters
    #  for differen clocks
    #  as the clocks are basically only used for sorting.
    @abstractmethod
    def sort_entries(self, entries: list[Entry]):
        """
        method to sort entries on the board.
        Returns a sorted copy of the input list.
        """
        return entries[:]

    @abstractmethod
    def sort_events(self, events: list[Event]):
        """
        method to sort events.
        Returns a sorted copy of the input list.
        """
        return events[:]


class Board:
    def __init__(self, sorter: ISorter):
        self.indexed_entries = {}
        self._sorter = sorter

    def add_entry(self, entry):
        self.indexed_entries[entry.id] = entry

    def get_ordered_entries(self):
        return self._sorter.sort_entries(list(self.indexed_entries.values()))

    def get_number_of_entries(self):
        return len(self.indexed_entries)


class VectorClockSorter(ISorter):
    """
    Sorts Entries and Events according to their vector clock.
    Uses the ids as tiebreaker.
    """

    def sort_entries(self, entries: list[Entry]):
        # pythons sort is by default stable.
        # we first sort according to the tiebreaker.
        #  these entries stay in the tiebreaker order,
        #  if they are tied in the second sort
        entries = sorted(
            entries,
            key=lambda e: e.id)
        return sorted(entries, key=lambda e: e.vector_timestamp)

    def sort_events(self, events: list[Event]):
        # see sort_entries
        events = sorted(
            events,
            key=lambda e: e.event_id)
        events = sorted(
            events,
            key=lambda e: e.vector_timestamp)
        return events


class BloomClockSorter(ISorter):
    """
    Sorts Entries and Events according to their bloom clock.
    Uses counter as primary sort key, then IDs as tiebreaker.
    """

    def sort_entries(self, entries: list[Entry]):
        # Sort by ID first (tiebreaker)
        entries = sorted(entries, key=lambda e: e.id)
        # Then sort by bloom clock counter
        return sorted(entries, key=lambda e: e.bloom_timestamp.counter)

    def sort_events(self, events: list[Event]):
        # Sort by event ID first (tiebreaker)
        events = sorted(events, key=lambda e: e.event_id)
        # Then sort by bloom clock counter
        return sorted(events, key=lambda e: e.bloom_timestamp.counter)


class Node:
    def __init__(
            self, m: messenger.ReliableMessenger,
            own_id: int, num_servers: int, r: random.Random,
            sorter: Union[ISorter, None] = None
    ):
        sorter = sorter if sorter is not None else VectorClockSorter()
        self._sorter = sorter
        self._clock = clock_server.get_clock_for_node(own_id)
        self._vector_clock = VectorClock.create_new(own_id, num_servers)
        self._bloom_clock = BloomClock.create_new(own_id, num_servers)
        self.messenger = m
        self.own_id = own_id
        self.num_servers = num_servers
        self.all_servers = range(num_servers)
        self.other_servers = [i for i in self.all_servers if i != own_id]
        self.board = Board(sorter)
        self.status = {
            "crashed": False,
            "notes": "",
        }
        # currently we use an in memory database
        self._event_store = EventStore("sqlite:///:memory:", False)
        self._event_store.initialize_database()
        self._event_id_generator = RandomGenerator()
        self._entry_id_generator = RandomGenerator()
        self.r = r

    def is_crashed(self):
        return self.status["crashed"]

    def get_current_vector_timestamp(self):
        return self._vector_clock.current_timestamp

    def get_current_bloom_timestamp(self):
        return self._bloom_clock.current_timestamp

    def get_entries(self):
        ordered_entries = self.board.get_ordered_entries()
        return list(map(lambda entry: entry.to_dict(), ordered_entries))

    def create_entry(self, value):
        """
        Create a new entry
          with a globally unique ID and propagate it to all other nodes.

        In this lab, there is no coordinator.
          Each node can create entries independently,
          and must propagate them to all other nodes
          using a gossip-style protocol.
        """
        timestamp = self._get_timestamp()
        self._vector_clock.increment()
        self._bloom_clock.increment()
        event = Event(
            self._event_id_generator.generate(),
            self._entry_id_generator.generate(),
            timestamp,
            self._vector_clock.current_timestamp,
            self._bloom_clock.current_timestamp,
            0,
            "create",
            value,
            [])
        try:
            self._apply_event(event)
        except:
            logger.exception("Could not create event")
        else:
            logger.info(
                f"Node {self.own_id}: Created entry {event.entry_id}"
                f" with value '{value}'"
            )
            self._send_event(event)

    def update_entry(self, entry_id, value):
        creation_timestamp = self._get_timestamp()
        depended_event_id = self.board.indexed_entries[entry_id].last_event_id
        self._vector_clock.increment()
        self._bloom_clock.increment()
        event = Event(
            self._event_id_generator.generate(),
            entry_id,
            creation_timestamp,
            self._vector_clock.current_timestamp,
            self._bloom_clock.current_timestamp,
            0,
            "update",
            value,
            [depended_event_id])
        try:
            self._apply_event(event)
        except:
            logger.exception("Could not update entry.")
        else:
            logger.info(
                f"Node {self.own_id}: Updated entry {event.entry_id}"
                f" with value '{value}'"
            )
            self._send_event(event)

    def delete_entry(self, entry_id):
        creation_timestamp = self._get_timestamp()
        depended_event_id = self.board.indexed_entries[entry_id].last_event_id
        self._vector_clock.increment()
        self._bloom_clock.increment()
        event = Event(
            self._event_id_generator.generate(),
            entry_id,
            creation_timestamp,
            self._vector_clock.current_timestamp,
            self._bloom_clock.current_timestamp,
            0,
            "delete",
            "",
            [depended_event_id])
        try:
            self._apply_event(event)
        except:
            logger.exception("Could not delete entry.")
        else:
            logger.info(
                f"Node {self.own_id}: Deleted {event.entry_id}"
            )
            self._send_event(event)

    # Return timestamp
    def _get_timestamp(self):
        # The timestamp has a granularity of 1 millisecond.
        return int(self._clock.get_time()*1000)

    def _apply_event(self, event: Event):
        """
        Apply a new event to the board.
        """
        self._event_store.add_event(event)
        self._regenerate_entry(event.entry_id)

    def _regenerate_entry(self, entry_id):
        """
        Generate an entry from the stored events.
        """
        if entry_id in self.board.indexed_entries:
            del self.board.indexed_entries[entry_id]
        history = self._event_store.get_history(entry_id)
        if len(history.root_events) == 0:
            # the creation event hasn't been propagated to this node yet.
            return
        elif len(history.root_events) == 1:
            event = history.root_events[0]
        else:
            raise Exception(
                "cannot (yet) handle a history with multiple root elements")
        assert event.action == "create"
        entry = Entry(entry_id,
                      event.value,
                      event.vector_timestamp,
                      event.bloom_timestamp,
                      event.event_id)
        while (event.event_id in history.inverted_dependencies):
            successors = [history.events[successor_id]
                          for successor_id
                          in history.inverted_dependencies[event.event_id]]
            assert len(successors) > 0
            successors = self._sorter.sort_events(successors)
            event = successors[0]
            if event.action == "update":
                entry.value = event.value
                entry.bloom_timestamp = event.bloom_timestamp
                entry.last_event_id = event.event_id
            elif event.action == "delete":
                return
            else:
                raise Exception("unknown action f{event.action}")
        self.board.indexed_entries[entry_id] = entry

    def _send_event(self, event):
        """send an event to all other nodes"""
        for i in self.other_servers:
            self.messenger.send(i, event.to_dict(), self._clock.get_time())

    def handle_message(self, message):
        """
        Handle incoming messages from other nodes.
        """
        logger.info(f"recieved message:{message}")
        event = Event.from_dict(message[1])
        try:
            self._vector_clock.update(event.vector_timestamp)
            self._bloom_clock.update(event.bloom_timestamp)
            self._apply_event(event)
        except:
            logger.exception("Could not handle the message")

    def update(self):
        """
        Called periodically by the server to process incoming messages.
        """
        time = self._clock.get_time()
        msgs = self.messenger.receive(time)
        for msg in msgs:
            self.handle_message(msg)
