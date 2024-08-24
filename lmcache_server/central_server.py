import socket
import time
import threading
import torch
from io import BytesIO
from lmcache.protocol import ClientMetaMessage, ServerMetaMessage, Constants
from lmcache_server.server import LMCacheServer
from lmcache.utils import CacheEngineKey


class CentralServer(LMCacheServer):
    def __init__(self, host, port):
        super().__init__(host, port)

    def handle_client_put(self, key_timestamp, value):
        key, timestamp = CacheEngineKey.seperate_timestamp(key_timestamp)
        if key not in self.data_store or self.data_store[key][0] <= timestamp:
            self.data_store[key] = (timestamp, value)

    def client_send(self, client_socket, key_without_timestamp, timestamp, value):
        key = CacheEngineKey.concate_timestamp(key_without_timestamp, timestamp)
        client_socket.sendall(ClientMetaMessage(Constants.SERVER_PUT, key, len(value)).serialize())
        client_socket.sendall(value)

    def send_all(self, client_socket):
        keys = self.data_store.keys()
        count = len(self.data_store.keys())
        for i in range(count):
            self.client_send(client_socket, keys[i], self.data_store[keys[i]][0], self.data_store[keys[i]][1])

    def handle_client(self, client_socket):
        try:
            while True:
                header = self.receive_all(client_socket, ClientMetaMessage.packlength())
                if not header:
                    break
                meta = ClientMetaMessage.deserialize(header)

                match meta.command:
                    case Constants.SERVER_SYNC:
                        # TODO, asynchronize problem here
                        self.send_all(client_socket)

                    case Constants.CLIENT_PUT:
                        s = self.receive_all(client_socket, meta.length)
                        self.handle_client_put(meta.key, s)

                    case Constants.CLIENT_GET:
                        key, _ = CacheEngineKey.seperate_timestamp(meta.key)
                        stored_tuple = self.data_store.get(key, None)
                        if stored_tuple is None:
                            client_socket.sendall(ServerMetaMessage(Constants.SERVER_FAIL, "", 0, 0).serialize())
                        else:
                            timestamp = stored_tuple[0]
                            data = stored_tuple[1]
                            new_key = CacheEngineKey.concate_timestamp(key, timestamp)
                            client_socket.sendall(ServerMetaMessage(Constants.SERVER_SUCCESS, new_key, len(data)).serialize())
                            client_socket.sendall(data)

                    case _:
                        # should not be here
                        raise Exception("Invalid request for central server: " + str(meta.command))
        finally:
            client_socket.close()


if __name__ == "__main__":
    import os, sys

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <host> <port>")
        exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    server = CentralServer(host, port)
    server.run()