# coding=utf-8
import random
import messenger
import logging
from id_generator import RandomGenerator
from board import Board
from event import Event
from blockchain import Blockchain, Block, BlockReference
from clock import clock_server

logger = logging.getLogger(__name__)


class Node:
    def __init__(
            self,
            m: messenger.ReliableMessenger,
            own_id: int,
            num_servers: int,
            log_difficulty: int,
            r: random.Random):
        self._messenger = m
        self.messenger = m
        self._status = {
            "crashed": False,
            }
        self._own_id = own_id
        self.own_id = own_id
        self._log_difficulty = log_difficulty
        self._num_servers = num_servers
        self._event_id_generator = RandomGenerator()
        self._entry_id_generator = RandomGenerator()
        self._events = []
        self._randomizer = r
        self._board = Board()
        self._blockchain = Blockchain.create_new()
        self._clock = clock_server.get_clock_for_node(own_id)
        
    def is_crashed(self) -> bool:
        return self._status["crashed"]

    def get_entries(self):
        ordered_entries = self._board.get_ordered_entries()
        return list(map(lambda entry: entry.to_dict(), ordered_entries))

    def create_entry(self, value: str) -> None:
        """
        Create a new entry
          with a globally unique ID and propagate it to all other nodes.

        @param value the value of the newly created entry
        """
        event_id = self._event_id_generator.generate()
        event = Event(
            event_id,
            self._entry_id_generator.generate(),
            "create",
            value)
        self._events.append(event)

    def update_entry(self, entry_id: int, value: str) -> None:
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
        self._events.append(event)

    def delete_entry(self, entry_id: int) -> None:
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
        self._events.append(event)

    def _try_create_block(self):
        if len(self._events) > 0:
            logger.info(f"Node {self._own_id} trying to create block")
            head_block = self._blockchain.get_head_block()
            head_reference = BlockReference(
                head_block.hashvalue,
                head_block.origin)
            new_block = Block.create_new(
                head_reference,
                self._own_id,
                head_block.index + 1,
                self._randomizer.randint(0,1024*2**self._log_difficulty),
                self._events
                )
            if int(new_block.hashvalue, 16) % (2**self._log_difficulty) == 0:
                logger.info(f"Node {self._own_id} created block")
                self._blockchain.add_block(new_block)
                self._send_to_all_others(new_block)
                self._events = []

    def _handle_message(self, message: dict):
        new_block = Block.from_dict(message)
        self._blockchain.add_block(new_block)

    def _apply_event(self, board: Board, event: Event):
        try:
            if event.action == "create":
                try:
                    board.add_entry(event.entry_id, event.value)
                except KeyError:
                    board.update_entry(event.entry_id, event.value)
            elif event.action == "update":
                try:
                    board.update_entry(event.entry_id, event.value)
                except KeyError:
                    board.add_entry(event.entry_id, event.value)
            elif event.action == "delete":
                try:
                    board.delete_entry(event.entry_id)
                except KeyError:
                    pass
            else:
                raise Exception("unknown event")
        except Exception:
            logger.exception(f"could not apply event {event.to_dict()}")

    def _recreate_board(self):
        board = Board()
        for block in self._blockchain.get_longest_chain():
            for event in block.events:
                self._apply_event(board, event)
        self._board = board

    def _send_to_all_others(self, app_message):
        for node_id in range(self._num_servers):
            if node_id == self._own_id:
                continue
            self._messenger.send(
                node_id,
                app_message.to_dict(),
                self._clock.get_time())

    def update(self) -> None:
        """
        Called periodically by the server to process incoming messages.
        """
        head_hashvalue = self._blockchain.get_head_block().hashvalue
        if len(self._events) > 0:
            self._try_create_block()
        time = self._clock.get_time()
        msgs = self._messenger.receive(time)
        for msg in msgs:
            logger.info(f"Node {self._own_id} recieved message:{msg} ")
            self._handle_message(msg[1])
        if head_hashvalue != self._blockchain.get_head_block().hashvalue:
            self._recreate_board()
