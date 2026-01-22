# coding=utf-8
import random
import messenger
import logging
from clock import clock_server
from event import Event
from id_generator import RandomGenerator
from lamport_clock import LamportClock
from board import Board

logger = logging.getLogger(__name__)


class AppMessage:
    REQUEST_TYPE = "Request"
    REPLY_TYPE = "Reply"
    PROPAGATE_TYPE = "Propagate"
    CONFIRM_TYPE = "Confirm"

    def __init__(self,
                 message_type,
                 origin,
                 lamport_timestamp,
                 event_id,
                 event_dict):
        # one of REQUEST_TYPE, REPLY_TYPE, PROPAGATE_TYPE, CONFIRM_TYPE
        self._message_type = message_type
        # node_id where the message originated
        self._origin = origin
        # the lamport timestamp
        self._lamport_timestamp = lamport_timestamp
        # id of the event that originated the message chain.
        self._event_id = event_id
        # either the dict of the event or an empty string.
        #  only propagate message has an event.
        self._event_dict = event_dict

    def to_dict(self) -> dict:
        return {"message_type": self._message_type,
                "origin": str(self._origin),
                "lamport_timestamp": str(self._lamport_timestamp),
                "event_id": self._event_id,
                "event_dict": self._event_dict}

    @staticmethod
    def from_dict(dict_):
        return AppMessage(
            dict_["message_type"],
            int(dict_["origin"]),
            int(dict_["lamport_timestamp"]),
            dict_["event_id"],
            dict_["event_dict"])

    @property
    def message_type(self):
        return self._message_type

    @property
    def origin(self):
        return self._origin

    @property
    def lamport_timestamp(self):
        return self._lamport_timestamp

    @property
    def event_id(self):
        return self._event_id

    @property
    def event_dict(self):
        return self._event_dict


class Node:
    IDLE_STATUS = "Idle"
    REQUESTING_STATUS = "Requesting"
    INCRITICALSECTION_STATUS = "InCriticalSection"

    def __init__(
            self, m: messenger.ReliableMessenger,
            own_id: int, num_servers: int, r: random.Random
    ):
        self._status = Node.IDLE_STATUS
        self._clock = clock_server.get_clock_for_node(own_id)
        self._lamport_clock = LamportClock.create_new()
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
        self._event_id_generator = RandomGenerator()
        self._entry_id_generator = RandomGenerator()
        self._event_queue = []
        # either None or a list of the
        # [lamport_timestamp, event, recieved_replies]
        self._request_record = None
        self._request_queue = []
        self.r = r

    def get_logical_time(self):
        return self._lamport_clock.value

    def test_get_event_queue(self):
        """
        get the event queue.
        Intended for testing/debugging only.
        """
        return self._event_queue

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

        @param value the value of the newly created entry
        """
        event_id = self._event_id_generator.generate()
        event = Event(
            event_id,
            self._entry_id_generator.generate(),
            "create",
            value)
        self._lamport_clock.increment()
        self._event_queue.append(event)

    def update_entry(self, entry_id, value):
        """
        Update an entry
           if the entry doesn't exist (re)-create it.

        @param entry_id the id of the entry to be updated
        @param value new value of the entry
        """
        event_id = self._event_id_generator.generate()
        event = Event(
            event_id,
            entry_id,
            "update",
            value)
        self._lamport_clock.increment()
        self._event_queue.append(event)

    def delete_entry(self, entry_id):
        """
        Delete an entry
        is ignored if the entry doesn't exist.

        @param entry_id the id of the entry to be deleted
        """
        event_id = self._event_id_generator.generate()
        event = Event(
            event_id,
            entry_id,
            "delete",
            "")
        self._lamport_clock.increment()
        self._event_queue.append(event)

    def _apply_event(self, event):
        try:
            if event.action == "create":
                try:
                    self.board.add_entry(event.entry_id, event.value)
                except KeyError:
                    self.board.update_entry(event.entry_id, event.value)
            elif event.action == "update":
                try:
                    self.board.update_entry(event.entry_id, event.value)
                except KeyError:
                    self.board.add_entry(event.entry_id, event.value)
            elif event.action == "delete":
                try:
                    self.board.delete_entry(event.entry_id)
                except KeyError:
                    pass
            else:
                raise Exception("unknown event")
        except Exception:
            logger.exception(f"could not apply event {event.to_dict()}")

    def _handle_message(self, message):
        """
        Handle incoming messages from other nodes.
        """
        self._lamport_clock.update(message.lamport_timestamp)
        if message.message_type == AppMessage.REQUEST_TYPE:
            self._handle_request_message(message)
        elif message.message_type == AppMessage.REPLY_TYPE:
            self._handle_reply_message(message)
        elif message.message_type == AppMessage.PROPAGATE_TYPE:
            self._handle_propagate_message(message)
        elif message.message_type == AppMessage.CONFIRM_TYPE:
            self._handle_confirm_message(message)
        else:
            logger.warn(f"Could not handle the message {message.to_dict()}")
            return

    def _handle_confirm_message(self, message):
        assert self._request_record is not None
        self._request_record[3] += 1

    def _handle_propagate_message(self, message):
        self._apply_event(Event.from_dict(message.event_dict))
        app_message = AppMessage(
            AppMessage.CONFIRM_TYPE,
            self.own_id,
            self._lamport_clock.value,
            message.event_id,
            ""
            )
        self._send_to_one(message.origin, app_message)

    def _handle_reply_message(self, message):
        assert self._request_record is not None
        self._request_record[2] += 1

    def _handle_request_message(self, message):
        if self._status == Node.IDLE_STATUS:
            app_message = AppMessage(
                AppMessage.REPLY_TYPE,
                self.own_id,
                self._lamport_clock.value,
                message.event_id,
                "")
            self._send_to_one(message.origin, app_message)
        elif self._status == Node.REQUESTING_STATUS:
            timestamp, event, _, _ = self._request_record
            if ((timestamp > message.lamport_timestamp)
                or (timestamp == message.lamport_timestamp
                    and event.event_id > message.event_id)):
                app_message = AppMessage(
                    AppMessage.REPLY_TYPE,
                    self.own_id,
                    self._lamport_clock.value,
                    message.event_id,
                    "")
                self._send_to_one(message.origin, app_message)
            else:
                self._request_queue.append(message)
        else:
            self._request_queue.append(message)

    def _request_critical_section(self, event):
        self._status = Node.REQUESTING_STATUS
        self._request_record = [self._lamport_clock.value, event, 0, 0]
        app_message = AppMessage(
            AppMessage.REQUEST_TYPE,
            self.own_id,
            self._lamport_clock.value,
            event.event_id,
            "")
        self._send_to_all_others(app_message)

    def _send_to_one(self, destination, app_message):
        self.messenger.send(
            destination,
            app_message.to_dict(),
            self._clock.get_time())

    def _send_to_all_others(self, app_message):
        for node_id in self.all_servers:
            if node_id == self.own_id:
                continue
            self.messenger.send(
                node_id,
                app_message.to_dict(),
                self._clock.get_time())

    def _enter_critical_section(self):
        self._status = Node.INCRITICALSECTION_STATUS
        event = self._request_record[1]
        self._apply_event(event)
        app_message = AppMessage(
            AppMessage.PROPAGATE_TYPE,
            self.own_id,
            self._lamport_clock.value,
            event.event_id,
            event.to_dict())
        self._send_to_all_others(app_message)

    def _exit_critical_section(self):
        self._request_record = None
        self._status = Node.IDLE_STATUS
        while len(self._request_queue) > 0:
            self._handle_request_message(self._request_queue.pop(0))

    def update(self):
        """
        Called periodically by the server to process incoming messages.
        """
        if self._status == Node.IDLE_STATUS:
            if len(self._event_queue) > 0:
                self._request_critical_section(self._event_queue.pop(0))
        time = self._clock.get_time()
        msgs = self.messenger.receive(time)
        for msg in msgs:
            logger.info(f"Node {self.own_id} recieved message:{msg} "
                        f"at logical time {self._lamport_clock.value}")
            self._handle_message(AppMessage.from_dict(msg[1]))
        if self._status == Node.IDLE_STATUS:
            assert self._request_record is None
        elif self._status == Node.REQUESTING_STATUS:
            assert self._request_record is not None
            if self._request_record[2] == self.num_servers - 1:
                self._enter_critical_section()
        elif self._status == Node.INCRITICALSECTION_STATUS:
            assert self._request_record is not None
            if self._request_record[3] == self.num_servers - 1:
                self._exit_critical_section()
                assert self._request_record is None
        else:
            logger.error(f"node {self.own_id} in unkown state: {self._status}")
