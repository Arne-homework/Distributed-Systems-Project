# coding=utf-8
import random
import messenger



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
            "num_entries": 0,  # we use this to generate ids for the entries
        }
        self.r = r

    def is_crashed(self):
        return self.status["crashed"]

    def get_entries(self):
        ordered_entries = self.board.get_ordered_entries()
        return list(map(lambda entry: entry.to_dict(), ordered_entries))

    def create_entry(self, value):
        """
        Create a new entry by sending an 'add_entry' request to the coordinator (node 0).
        The coordinator will handle the rest.
        """
        print(f"Node {self.own_id}: Sending 'add_entry' request to coordinator for value: {value}")
        self.messenger.send(0, messenger.Message({
            'type': 'add_entry',
            'entry_value': value
        }))

    def update_entry(self, entry_id, value):
        pass  # TODO (Optional Task 4): Implement update logic similar to create_entry

    def delete_entry(self, entry_id):
        pass  # TODO (Optional Task 4): Implement delete logic similar to create_entry

    def handle_message(self, message):
        """
        Handle incoming messages for the coordinator pattern.

        We provide a basic implementation:
        - If a node wants to add a new entry, it sends an 'add_entry' message to the coordinator
        - The coordinator propagates it to all servers (including itself)
        - If a node receives a 'propagate' message, it adds the entry to the board

        Note: This implementation has some issues!
        """
        msg_content = message.get_content()

        if 'type' not in msg_content:
            print(f"Node {self.own_id}: Received message without type: {msg_content}")
            return

        msg_type = msg_content['type']

        if msg_type == 'add_entry':
            # Only coordinator should receive this
            assert self.own_id == 0, "Only coordinator (node 0) should receive 'add_entry' messages"

            entry_value = msg_content['entry_value']
            print(f"Coordinator: Received add_entry for '{entry_value}', broadcasting to all nodes")

            for node_id in self.all_servers:
                self.messenger.send(node_id, messenger.Message({
                    'type': 'propagate',
                    'entry_value': entry_value
                }))

        elif msg_type == 'propagate':
            entry_value = msg_content['entry_value']
            # Let's hope this is from the coordinator
            # Each node assigns its own ID... what could go wrong?
            self.status['num_entries'] += 1
            entry = Entry(self.status['num_entries'], entry_value)
            self.board.add_entry(entry)
            print(f"Node {self.own_id}: Added entry ID {self.status['num_entries']} with value '{entry_value}'")

    def update(self, t: float):
        """
        Called periodically by the server to process incoming messages.
        """
        msgs = self.messenger.receive()
        for msg in msgs:
            print(f"Node {self.own_id} received message at time {t}: {msg}")
            self.handle_message(msg)