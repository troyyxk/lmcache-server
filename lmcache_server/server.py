import socket
import time
import threading
import torch
from io import BytesIO
from lmcache.protocol import ClientMetaMessage, ServerMetaMessage, Constants
import numpy as np
from utils import softmax
import random

class LMCacheServer:
    def __init__(self, host, port, num_senders=1):
        self.host = host
        self.port = port
        self.data_store = {}
        # the format is {address: [count, [(socket, request), ...]]}
        self.send_queue = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, port))
        self.server_socket.listen()

        # enable the sending
        self.lock = threading.Lock()
        for _ in range(num_senders):
            threading.Thread(target=self.send_messages, args=(self.lock,)).start()

    def send_messages(self, lock):
        while True:
            if len(self.send_queue) == 0:
                print("No more messages to send, waiting...")
                # if there are no requests to be sent, sleep 2 second
                time.sleep(2)
            else:
                # get the data with lock
                lock.acquire()
                ips = self.send_queue.keys()

                # pick the message to send, priority to the one with the least amount of request
                ip_message_count = []
                # total_holding_messages_count = 0
                for ip in ips:
                    ip_message_count.append(self.send_queue[ip][0])
                    # total_holding_messages_count += self.send_queue[ip][0]
                # ip_probability = np.array(ip_message_count) / total_holding_messages_count
                # ip_probability = [1 - x for x in ip_probability]
                ip_probability = 1 / np.array(ip_message_count)
                ip_probability = softmax(ip_probability)
                target_ip = random.choices(list(ips), ip_probability.tolist(), k=1)

                # pop and get the data to send
                self.send_queue[target_ip][0] -= 1
                post = self.send_queue[target_ip][1].pop(0)
                assert self.send_queue[target_ip][0] == len(self.send_queue[target_ip][1])
                if self.send_queue[target_ip][0] == 0:
                    del self.send_queue[target_ip]
                lock.release()

                # send the data
                post[0].sendall(post[1])

    def add_data_to_send(self, lock, addr, data):
        lock.acquire()
        if addr not in self.send_queue:
            self.send_queue[addr] = [0, []]
        self.send_queue[addr][0] += 1
        self.send_queue[addr][1].append(data)
        lock.release()

    def receive_all(self, client_socket, n):
        data = bytearray()
        while len(data) < n:
            packet = client_socket.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def handle_client(self, lock, client_socket, addr):
        try:
            while True:
                header = self.receive_all(client_socket, ClientMetaMessage.packlength())
                if not header:
                    break
                meta = ClientMetaMessage.deserialize(header)

                match meta.command:
                    case Constants.CLIENT_PUT:
                        s = self.receive_all(client_socket, meta.length)
                        self.data_store[meta.key] = s

                    case Constants.CLIENT_GET:
                        data_string = self.data_store.get(meta.key, None)
                        if data_string is not None:
                            self.add_data_to_send(lock, addr, ServerMetaMessage(Constants.SERVER_SUCCESS, len(data_string)).serialize())
                            self.add_data_to_send(lock, addr, data_string)
                        else:
                            self.add_data_to_send(lock, addr, ServerMetaMessage(Constants.SERVER_FAIL, 0).serialize())

                    case Constants.CLIENT_EXIST:
                        code = Constants.SERVER_SUCCESS if meta.key in self.data_store else Constants.SERVER_FAIL
                        self.add_data_to_send(lock, addr, ServerMetaMessage(code, 0).serialize())

                    case Constants.CLIENT_LIST:
                        keys = list(self.data_store.keys())
                        data = "\n".join(keys).encode()
                        self.add_data_to_send(lock, addr, ServerMetaMessage(Constants.SERVER_SUCCESS, len(data)).serialize())
                        self.add_data_to_send(lock, addr, data)

        finally:
            client_socket.close()

    def run(self):
        print(f"Server started at {self.host}:{self.port}")
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                print(f"Connected by {addr}")
                threading.Thread(target=self.handle_client, args=(self.lock, client_socket, addr, )).start()
        finally:
            self.server_socket.close()

if __name__ == "__main__":
    import os, sys
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <host> <port> <number of senders>")
        exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    num_sender = int(sys.argv[3])

    server = LMCacheServer(host, port, num_sender)
    server.run()

