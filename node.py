# coding=utf-8
import random
import messenger
import logging_config
import logging

logger = logging.getLogger(__name__)

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
        return Entry(int(data['id']), data['value'])

    def __str__(self):
        return str(self.to_dict())

class Board():
    def __init__(self):
        self.indexed_entries = {}

    def add_entry(self, entry):
        self.indexed_entries[entry.id] = entry

    def get_entry(self, entry_id:float):
        return self.indexed_entries[entry_id]

    def delete_entry(self, entry_id:float):
        logger.debug(f"delete_entry:{entry_id}")
        del self.indexed_entries[entry_id]
        
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

    def create_entry(self, value, time):
        """
        Create a new entry by sending an 'add_entry' request to the coordinator (node 0).
        The coordinator will handle the rest.
        """
        logger.info(f"Node {self.own_id}: Sending 'add_entry' request to coordinator for value: {value}")
        self.messenger.send(0, {
            'type':'entry_change_request',
            'request':{
                'subtype': 'add_entry',
                'entry_value': value
                }
        }, time)

    def update_entry(self, entry_id, value, time):
        logger.info(f"Node {self.own_id}: Sending 'update_entry' request to coordinator for entry: {entry_id} and value: {value}")
        self.messenger.send(0, {
            'type':'entry_change_request',
            'request':{
                'subtype': 'update_entry',
                'entry_id':entry_id,
                'entry_value': value
                }
        }, time)
    
    def delete_entry(self, entry_id, time):
        logger.info(f"Node {self.own_id}: Sending 'delete_entry' request to coordinator for entry: {entry_id}")
        self.messenger.send(0, {
            'type':'entry_change_request',
            'request':{
                'subtype': 'delete_entry',
                'entry_id': entry_id
                }
        }, time)

    def handle_message(self, message, time):
        """
        Handle incoming messages for the coordinator pattern.

        We provide a basic implementation:
        - If a node wants to add a new entry, it sends an 'add_entry' message to the coordinator
        - The coordinator propagates it to all servers (including itself)
        - If a node receives a 'propagate' message, it adds the entry to the board

        Note: This implementation has some issues!
        """
        msg_content = message

        if 'type' not in msg_content:
            logger.info(f"Node {self.own_id}: Received message without type: {msg_content}")
            return

        msg_type = msg_content['type']
        msg_request = msg_content["request"]
        if msg_type == 'entry_change_request':
            # Only coordinator should receive this
            assert self.own_id == 0, "Only coordinator (node 0) should receive 'entry_change_request' messages"
            try:
                self._apply_request(msg_request)
            except:
                logger.exception(f"At node {self.own_id} Could not apply change request {msg_content['request']}")
            else:
                for node_id in self.all_servers:
                    if node_id != self.own_id:
                        self.messenger.send(node_id, {
                            'type': 'propagate',
                            'request': msg_request
                        }, time)
                    else:
                        pass
        elif msg_type == 'propagate':
            msg_request = msg_content['request']
            # Let's hope this is from the coordinator
            # Each node assigns its own ID... what could go wrong?
            entry = self._apply_request(msg_request)
#            self.status['num_entries'] += 1
#            entry = Entry(self.status['num_entries'], entry_value)
#            self.board.add_entry(entry)
#            print(f"Node {self.own_id}: Added entry ID {self.status['num_entries']} with value '{entry_value}'")

    def _apply_request(self, request):
        logger.debug(request)
        subtype = request['subtype']
        if subtype == 'add_entry':
            self.status['num_entries'] += 1
            entry = Entry(self.status['num_entries'], request['entry_value'])
            self.board.add_entry(entry)
            logger.info(f"Node {self.own_id}: Added entry ID {self.status['num_entries']} with value '{request['entry_value']}'")
        elif subtype == 'update_entry':
            entry_id = int(request['entry_id'])
            entry_value = request['entry_value']
            entry = self.board.get_entry(entry_id)
            entry.value = entry_value
        elif subtype == 'delete_entry':
            entry_id = int(request['entry_id'])
            self.board.delete_entry(entry_id)
                
    def update(self, t: float):
        """
        Called periodically by the server to process incoming messages.
        """
        msgs = self.messenger.receive(t)
        for source,msg in msgs:
            print(f"Node {self.own_id} received message at time {t}: {msg}")
            self.handle_message(msg, t)
