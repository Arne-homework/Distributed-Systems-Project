# coding=utf-8
import random
import messenger
import logging
from clock import clock_server
from event import Event, EventStore
from id_generator import RandomGenerator
import logging_config
logger = logging.getLogger(__name__)


class Entry:
    def __init__(self, id, value, last_event_id):
        self.id = id
        self.value = value
        # id of the last event that set this entries value.
        #  allows us to often avoid having to create the full history.
        self.last_event_id = last_event_id

    def to_dict(self) -> dict:
        return {"id": self.id, "value": self.value}

    def __str__(self):
        return str(self.to_dict())


class Board:
    def __init__(self):
        self.indexed_entries = {}

    def add_entry(self, entry):
        self.indexed_entries[entry.id] = entry

    def get_ordered_entries(self):
        ordered_indices = sorted(list(self.indexed_entries.keys()))
        return [self.indexed_entries[k] for k in ordered_indices]

    def get_number_of_entries(self):
        return len(self.indexed_entries)


class Node:
    def __init__(
            self, m: messenger.ReliableMessenger,
            own_id: int, num_servers: int, r: random.Random
    ):
        self._clock = clock_server.get_clock_for_node(own_id)
        self.messenger = m
        self.own_id = own_id
        self.num_servers = num_servers
        self.all_servers = range(num_servers)
        self.other_servers = [i for i in self.all_servers if i != own_id]
        self.board = Board()
        self.status = {
            "crashed": False,
            "notes": "",
        }
        # currently we use an in memory database
        self._event_store = EventStore("sqlite+pysqlite:///:memory:", False)
        self._event_store.initialize_database()
        self._event_id_generator = RandomGenerator()
        self._entry_id_generator = RandomGenerator()
        self.r = r

    def is_crashed(self):
        return self.status["crashed"]

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
        event = Event(
            self._event_id_generator.generate(),
            self._entry_id_generator.generate(),
            timestamp,
            "create",
            value,
            [])
        try:
            self._apply_event(event)
        except err:
            logger.exeption()
        else:
            logger.info(
                f"Node {self.own_id}: Created entry {event.entry_id}"
                f" with value '{value}'"
            )
            self._send_event(event)

    def update_entry(self, entry_id, value):
        timestamp = self._get_timestamp()
        depended_event_id = self.board.indexed_entries[entry_id].last_event_id
        event = Event(
            self._event_id_generator.generate(),
            entry_id,
            timestamp,
            "update",
            value,
            [depended_event_id])
        try:
            self._apply_event(event)
        except err:
            logger.exeption()
        else:
            logger.info(
                f"Node {self.own_id}: Updated entry {event.entry_id}"
                f" with value '{value}'"
            )
            self._send_event(event)

    def delete_entry(self, entry_id):
        timestamp = self._get_timestamp()
        depended_event_id = self.board.indexed_entries[entry_id].last_event_id
        event = Event(
            self._event_id_generator.generate(),
            entry_id,
            timestamp,
            "delete",
            "",
            [depended_event_id])
        try:
            self._apply_event(event)
        except err:
            logger.exeption()
        else:
            logger.info(
                f"Node {self.own_id}: Deleted {event.entry_id}"
            )
            self._send_event(event)

    def _get_timestamp(self):
        return 0  # TODO: replace by useful timestamp

    def _apply_event(self, event: Event):
        """
        Apply a new event to the board.
        """
        self._event_store.add_event(event)
        indexed_entries = self.board.indexed_entries  # shorten this.
        if event.action == "create":
            assert event.entry_id not in indexed_entries
            indexed_entries[event.entry_id] = Entry(
                event.entry_id,
                event.value,
                event.event_id)
            return
        elif event.action == "update":
            # update (and delete) entries can occur if
            #  node 1 creates an entry and node 2 updates it
            #  and node 2 sends its event to us before node 1 sends its own.
            if (event.entry_id in indexed_entries
                # if the event builds on the last event
                #  that set this entries value,
                #  we can avoid using the whole history
                and indexed_entries[event.entry_id].last_event_id
                    in event.depended_event_ids):
                indexed_entries[event.entry_id] = Entry(
                    event.entry_id,
                    event.value,
                    event.event_id)
                return
        elif event.action == "delete":
            if (event.entry_id in indexed_entries
                and indexed_entries[event.entry_id].last_event_id
                    in event.depended_event_ids):
                del self.board.indexed_entries[event.entry_id]
                return
        else:
            logger.error(f"Node {self.own_id} recieved event of unkonwn type:"
                         f"{event.to_dict()}")
        self._regenerate_entry(self.event.entry_id)

    def _regenerate_entry(self, entry_id):
        """
        Generate an entry from the stored events.
        """
        if entry_id in self.board.indexed_entries:
            del self.board.indexed_entries[entry_id]
        history = self._event_store.get_history(entry_id)
        if len(history.root_events) == 0:
            raise Exception("History without root event")
        elif len(history.root_events) == 1:
            event = history.root_events[0]
        else:
            # History has multiple events without predecessors.
            # if one of them is a creation event use this as root.
            #  the others are assumed to be
            #   update/delete events with a missing intermediate
            # if the creation event is missing, ignore the entry for now.
            event = None
            for candidate in history.root_events:
                if candidate.action == "create":
                    event = candidate
                    break
            if event is None:
                return
        assert event.action == "create"
        entry = Entry(entry_id, event.value, event.event_id)
        while (event.event_id in history.inverted_dependencies):
            successors = [history.events[successor_id]
                          for successor_id
                          in history.inverted_dependencies[event.event_id]]
            assert len(successors) > 0
            # we sort by timestamp & event_id
            # if the time difference between events is larger than
            # the clock deviation between the nodes clocks was,
            # this allows us to prefer earlier events,
            # otherways it is by chance but deterministic.
            # if timestamps are equal, event_id works as a tiebreaker
            successors.sort(key=lambda e: (e.timestamp, e.event_id))
            event = successors[0]
            if event.action == "update":
                entry = Entry(entry_id, event.value, event.event_id)
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
            self._apply_event(event)
        except err:
            logger.exeption()

    def update(self, t: float):
        """
        Called periodically by the server to process incoming messages.
        """
        self._clock.set_time(t)
        msgs = self.messenger.receive(t)
        for msg in msgs:
            self.handle_message(msg)
