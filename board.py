

class Entry:
    def __init__(self,
                 number,
                 id,
                 value):
        self.number = number
        self.id = id
        self.value = value

    def to_dict(self) -> dict:
        return {"id": self.id, "value": self.value}

    def __str__(self):
        return f"{{{self.value}, {self.id}, {self.value}}}"


class Board:
    def __init__(self):
        self._entry_number = 0
        self._indexed_entries = {}

    def add_entry(self, entry_id: str, value: str):
        """ add a new entry to the board.

        raises an KeyError if the entry already exists.
        """
        if entry_id in self._indexed_entries:
            raise KeyError(f"entry {entry_id} already exists")
        else:
            self._indexed_entries[entry_id] = Entry(
                self._entry_number,
                entry_id,
                value)
            self._entry_number += 1

    def update_entry(self, entry_id: str, value: str):
        """
        updates an entry.

        raises an KeyError if the entry doesn't yet exist.
        """
        try:
            self._indexed_entries[entry_id].value = value
        except KeyError:
            raise KeyError(
                f"Entry {entry_id} doesn't exist yet and can't be updated.")

    def delete_entry(self, id):
        """
        deletes an entry.

        raises an KeyError if the entry doesn't exist.
        """
        try:
            del self._indexed_entries[id]
        except KeyError:
            raise KeyError(f"No entry with id {id}")

    def get_ordered_entries(self):
        return sorted(
            list(self._indexed_entries.values()),
            key=lambda e: e.number)

    def get_number_of_entries(self):
        return len(self._indexed_entries)
