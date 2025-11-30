import uuid


class RandomGenerator:
    """Generator to generate random ids."""

    def generate(self):
        return uuid.uuid4().hex


class NodeAwareGenerator:
    """Generates a unique id given a unique node_id."""

    def __init__(self, node_id, max_node_id):
        self._i = 0
        self._max_node_id = max_node_id
        self._node_id = node_id

    def generate(self):
        new_id = self._i * (self._max_node_id+1)+self._node_id
        self._i += 1
        return f"{new_id:032X}"
