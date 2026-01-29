



logger = logging.getLogger(__name__)


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
