# coding=utf-8
import random
import messenger
import uuid


class Entry:
    def __init__(self, id, value):
        self.id = id
        self.value = value

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "value": self.value
        }

    def from_dict(data: dict):
        return Entry(data['id'], data['value'])

    def __str__(self):
        return str(self.to_dict())

class Board():
    def __init__(self):
        self.indexed_entries = {}

    def add_entry(self, entry):
        self.indexed_entries[entry.id] = entry

    def get_ordered_entries(self):
        ordered_indices = sorted(list(self.indexed_entries.keys()))
        return [self.indexed_entries[k] for k in ordered_indices]


class Node:
    def __init__(self, m: messenger.Messenger, own_id: int, num_servers: int, r : random.Random):
        self.messenger = m
        self.own_id = own_id
        self.num_servers = num_servers
        self.all_servers = range(num_servers)
        self.other_servers = [i for i in self.all_servers if i != own_id]
        self.board = Board()
        self.status = {
            "crashed": False,
            "notes": "",
            "num_entries": 0,  # we use this to count entries created by this node
        }
        self.r = r

    def is_crashed(self):
        return self.status["crashed"]

    def get_entries(self):
        ordered_entries = self.board.get_ordered_entries()
        return list(map(lambda entry: entry.to_dict(), ordered_entries))

    def create_entry(self, value):
        """
        Create a new entry with a globally unique ID and propagate it to all other nodes.

        In this lab, there is no coordinator. Each node can create entries independently,
        and must propagate them to all other nodes using a gossip-style protocol.
        """
        # TODO: Generate a globally unique ID
        # For now, we use a simple counter (this won't work in a distributed setting!)
        self.status['num_entries'] += 1
        entry_id = self.status['num_entries']

        entry = Entry(entry_id, value)
        self.board.add_entry(entry)

        print(f"Node {self.own_id}: Created entry {entry_id} with value '{value}'")

        # TODO: Propagate the entry to all other servers? Use your solution for lab 1 to send messages between servers reliably
        # - What if the request gets lost?
        # - What if the request is delayed?
        # - What if the response gets lost?
        
    def update_entry(self, entry_id, value):
        print(f"Node {self.own_id}: tried to update {entry_id} to {value}, but update not implemented.")
        # TODO (Optional): Implement modify operation with conflict resolution

    def delete_entry(self, entry_id):
        print(f"Node {self.own_id}: tried to delete {entry_id}, but delete not implemented.")
        # TODO (Optional): Implement delete operation with conflict resolution

    def handle_message(self, message):
        """
        Handle incoming messages from other nodes.
        # TODO: Implement message handling logic, use your solution for lab 1 to send messages between servers reliably
        """
        msg_content = message.get_content()

        if 'type' not in msg_content:
            print(f"Node {self.own_id}: Received message without type: {msg_content}")
            return

        msg_type = msg_content['type']

        if msg_type == 'propagate':
            print(f"Node {self.own_id}: Received propagate message: {msg_content}")
            pass
        


    def update(self, t: float):
        """
        Called periodically by the server to process incoming messages.
        """
        msgs = self.messenger.receive()
        for msg in msgs:
            self.handle_message(msg)
