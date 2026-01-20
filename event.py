# coding=utf-8
"""
Implements events encoding the actions that can be applied in a transaction.
"""

# In commit f07ae884a999aa2c9415c1a4d793b1a15a8b6b29
# exists an sqlite implementation for event sourcing.


class Event:
    def __init__(self, event_id, entry_id,
                 action, value):
        # The unique id of this event
        self._event_id = event_id
        # Id of the entry this event modifies.
        self._entry_id = entry_id
        # Action that created this event.
        self._action = action
        # Usually the new value of the entry; "" for Deletion events.
        self._value = str(value)
        # ids of events on which this event depends.

    @property
    def event_id(self):
        return self._event_id

    @property
    def entry_id(self):
        return self._entry_id

    @property
    def action(self):
        return self._action

    @property
    def value(self):
        return self._value

    def __eq__(self, other):
        return all([(self._event_id == other._event_id),
                    (self._entry_id == other._entry_id),
                    (self._action == other._action),
                    (self._value == other._value)])

    @staticmethod
    def from_dict(dict_):
        return Event(
            dict_["event_id"],
            dict_["entry_id"],
            dict_["action"],
            dict_["value"])

    def to_dict(self):
        return {"event_id": self._event_id,
                "entry_id": self._entry_id,
                "action": self._action,
                "value": self._value
            }
