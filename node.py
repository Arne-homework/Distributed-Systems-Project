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
        e = Entry("test", value)
        for i in self.other_servers:
            print("Sending message from {} to {}".format(self.own_id, i))
            self.messenger.send(i, messenger.Message("Hello from {}".format(self.own_id)))
        self.board.add_entry(e)

    def update_entry(self, entry_id, value):
        pass # todo

    def update(self, t: float):
        msgs = self.messenger.receive()
        for msg in msgs:
            print("Node {} received message: {}".format(self.own_id, msg))