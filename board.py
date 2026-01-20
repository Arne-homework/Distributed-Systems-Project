

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

    def add_entry(self, id, value):
        """ add a new entry to the board.

        raises an IndexError if the entry already exists.
        """
        if id in self._indexed_entries:
            raise IndexError(f"entry {id} already exists")
        else:
            self._indexed_entries[id] = Entry(self._entry_number, id, value)
            self._entry_number += 1

    def update_or_create_entry(self, id, value):
        """
        either updates an existing entry or creates the entry anew.
        """
        if id in self._indexed_entries:
            self._indexed_entries[id].value = value
        else:
            self.add_entry(id, value)

    def delete_enty(self, id):
        """
        deletes an entry.

        raises an IndexError if the entry doesn't exist.
        """
        try:
            del self._indexed_entry[id]
        except IndexError:
            raise IndexError(f"No entry with id {id}")

    def get_ordered_entries(self):
        return sorted(list(self._indexed_entries.values()), key=lambda e: e.number)

    def get_number_of_entries(self):
        return len(self._indexed_entries)
