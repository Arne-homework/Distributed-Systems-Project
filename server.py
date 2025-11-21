# coding=utf-8
import queue
import random
import traceback

from bottle import Bottle, request, HTTPError, run, response, static_file

from paste import httpserver

import threading
import os
import time
import json
import hashlib

from messenger import Messenger, Transport, UnreliableTransport
from node import Node
import time

NUM_NODES = int(os.getenv('NUM_NODES')) if os.getenv('NUM_NODES') else 2
GROUP_NAME = os.getenv('GROUP_NAME')
# Port for the server to listen on
SERVER_PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 80
# External port for frontend to connect to (used when running in Docker with port mapping)
# In Docker: server listens on 80, but externally accessible on 8000
# Locally: server listens on 8000, externally accessible on 8000
EXTERNAL_PORT = int(os.getenv('EXTERNAL_PORT')) if os.getenv('EXTERNAL_PORT') else SERVER_PORT

def index(server=0):
    with open('frontend/index.html') as f:
        s = f.read()
        s = s.replace("%SERVER_LIST%", ",".join([f"127.0.0.1:{EXTERNAL_PORT}/nodes/{i}" for i in range(NUM_NODES)]))  # replace list of servers
        s = s.replace("%SERVER_ID%", str(server))  # replace selected server
        s = s.replace("%GROUP_NAME%", GROUP_NAME)  # replace group name
        return s

def serve_static_file(filename):
    response = static_file(filename, root="./frontend/")
    response.set_header("Cache-Control", "max-age=0")
    return response


# ------------------------------------------------------------------------------------------------------
class Server(Bottle):

    def __init__(self):
        super(Server, self).__init__()

        self.lock = threading.RLock()  # use reentry lock for the server

        # Handle CORS
        self.route('/<:re:.*>', method='OPTIONS', callback=self.add_cors_headers)
        self.add_hook('after_request', self.add_cors_headers)

        # Those two http calls simulate crashes, i.e., unavailability of the server
        self.post('/nodes/<node_id:int>/crash', callback=self.crash_request)
        self.post('/nodes/<node_id:int>/recover', callback=self.recover_request)
        self.get('/nodes/<node_id:int>/status', callback=self.status_request)

        # Define REST URIs for the frontend (note that we define multiple update and delete routes right now)
        self.post('/nodes/<node_id:int>/entries', callback=self.create_entry_request)
        self.get('/nodes/<node_id:int>/entries', callback=self.list_entries_request)
        self.post('/nodes/<node_id:int>/entries/<entry_id>', callback=self.update_entry_request)
        self.post('/nodes/<node_id:int>/entries/<entry_id>/delete', callback=self.delete_entry_request)

        self.get('/', callback=index)
        self.get('/server/<server>', callback=index)
        self.get('/<filename:path>', callback=serve_static_file)

        self.r = random.Random(42)  # use a fixed seed for replayability

        self.nodes = []

        # define nodes
        for node_id in range(NUM_NODES):
            m = Messenger(node_id, NUM_NODES)
            n = Node(m, node_id, NUM_NODES, self.r)
            self.nodes.append(n)

        # define transport from one server to all others
        # Start with reliable transport for basic implementation
        # Students will enable unreliable transport for Task 4 (Medium/Hard scenarios)
        self.transports = {}
        for from_id in range(NUM_NODES):
            for to_id in range(NUM_NODES):
                # Use Transport for perfect/reliable delivery (no delays, no drops)
                # transport = Transport(self.nodes[from_id].messenger.out_queues[to_id], self.nodes[to_id].messenger.in_queue, self.r)

                # Replace with UnreliableTransport and configure delays/drops
                transport = UnreliableTransport(self.nodes[from_id].messenger.out_queues[to_id], self.nodes[to_id].messenger.in_queue, self.r)
                transport.set_delay(0.5, 2.0)  # 0.5-2.0 second delay
                transport.set_drop_rate(0.1)   # 10% packet loss

                self.transports[(from_id, to_id)] = transport

        # start a thread which updates all nodes in a loop
        self.node_update_time_delta = 0.01 # seconds
        self.node_worker_thread = threading.Thread(target=self.update_nodes)
        self.node_worker_thread.daemon = True
        self.node_worker_thread.start()

    def update_nodes(self):

        t = 0.0
        rand_nodes = self.nodes.copy() # we copy it here because we reorder it
        while True:

            # iterate through all transports and deliver messages
            # no lock needed here as the transport only works on the queues
            for transport in self.transports.values():
                transport.deliver(t)

            # shuffle nodes based on the random generator
            self.r.shuffle(rand_nodes)

            # update all alive nodes, note that we lock!
            with self.lock:
                for node in rand_nodes:
                    if not node.is_crashed():
                        try:
                            node.update(t)
                        except Exception as e:
                            print("[ERROR] " + str(e))
                            print(traceback.format_exc())
                            pass

            time.sleep(self.node_update_time_delta)  # sleep a bit, note that this is not very precise i.e., the actual time delta might be larger
            t += self.node_update_time_delta


    # Please try to avoid modifying the following methods
    # ------------------------------------------------------------------------------------------------------
    def add_cors_headers(self):
        """
        You need to add some headers to each request.
        Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
        """
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

    def list_entries_request(self, node_id: int):
        # DONT use me for server to server stuff as this is also available when crashed. Please implement another method for that (which also handles the crashed state!)
        try:

            with self.lock:
                dict_entries = self.nodes[node_id].get_entries()

                return {
                    "entries": dict_entries,
                    "server_status": {
                        "len": len(dict_entries),
                        "hash": hashlib.sha256((json.dumps(tuple(dict_entries)).encode('utf-8'))).hexdigest(),
                        "crashed": self.nodes[node_id].status["crashed"],
                        "notes": self.nodes[node_id].status["notes"]
                    }  # we piggyback here allowing for a simple frontend implementation
                }
        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def status_request(self, node_id: int):
        # DONT use me for server to server stuff as this is also available when crashed. Please implement another method for that (which also handles the crashed state!)
        try:

            with self.lock:
                dict_entries = self.nodes[node_id].get_entries()
                return {
                    "len": len(dict_entries),
                    "hash": hashlib.sha256((json.dumps(tuple(dict_entries)).encode('utf-8'))).hexdigest(),
                    "crashed": self.nodes[node_id].status["crashed"],
                    "notes": self.nodes[node_id].status["notes"]
                }
        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def crash_request(self, node_id: int):
        try:
            if not self.nodes[node_id].status["crashed"]:
                self.nodes[node_id].status["crashed"] = True
        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def recover_request(self, node_id: int):
        try:
            if self.nodes[node_id].status["crashed"]:
                self.nodes[node_id].status["crashed"] = False
        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def create_entry_request(self, node_id):
        try:
            if self.nodes[node_id].status["crashed"]:
                response.status = 408
                return

            entry_value = request.forms.get('value')

            with self.lock:
                return self.nodes[node_id].create_entry(entry_value)

        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def update_entry_request(self, node_id: int, entry_id):
        try:
            if self.nodes[node_id].status["crashed"]:
                response.status = 408
                return

            entry_value = request.forms.get('value')

            with self.lock:
                return self.nodes[node_id].update_entry(entry_id, entry_value)
        except Exception as e:
            print("[ERROR] " + str(e))
            raise e

    def delete_entry_request(self, node_id: int, entry_id):
        try:
            if self.nodes[node_id].status["crashed"]:
                response.status = 408
                return

            entry_value = request.forms.get('value')

            with self.lock:
                return self.nodes[node_id].delete_entry(entry_id)

        except Exception as e:
            print("[ERROR] " + str(e))
            raise e


# Sleep a bit to allow logging to be attached
time.sleep(2)

server = Server()

NUM_THREADS = 2
print("#### Starting labs server with {} nodes on port {}".format(NUM_NODES, SERVER_PORT))
httpserver.serve(server, host='0.0.0.0', port=SERVER_PORT, threadpool_workers=NUM_THREADS,                  threadpool_options={"spawn_if_under": NUM_THREADS})