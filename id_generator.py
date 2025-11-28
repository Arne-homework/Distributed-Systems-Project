import uuid
import secrets


def generate_id_v1(node_id: int, time: float):
    return uuid.uuid4()


def generate_id_v2(node_id: int, time: float, nbytes=4):
    return secrets.token_hex(nbytes) + f"{int(time*100):0>13}" + f"{node_id:0>3}"
