import socket
import time
import threading
import torch
from io import BytesIO
from lmcache.protocol import ClientMetaMessage, ServerMetaMessage, Constants
from lmcache.utils import CacheEngineKey
from lmcache_server.central_server import CentralServer

class RegionalServer(CentralServer):
    def __init__(self, host, port, central_host, central_port, sync_sleep = 20):
        super().__init__(host, port)

        self.central_sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.central_sync_socket.connect((central_host, central_port))
        self.sync_sleep = sync_sleep

        # for get only, prevent mixing up calls
        self.central_get_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.central_get_socket.connect((central_host, central_port))

        # For async put
        self.put_thread = threading.Thread(
                target=self.sync_worker, args=()
            )
        self.put_thread.start()

    def sync_worker(
            self,
    ):
        while True:
            time.sleep(self.sync_sleep)
            self.sync()

    def sync(self):
        # TODO, optimize it so it doe not require to send and receive all data
        # put what the central server do not have
        self.send_all(self.central_sync_socket)
        # get what is update or what is local

    def handle_client(self, client_socket):
        try:
            while True:
                header = self.receive_all(client_socket, ClientMetaMessage.packlength())
                if not header:
                    break
                meta = ClientMetaMessage.deserialize(header)

                match meta.command:
                    case Constants.CLIENT_PUT:
                        s = self.receive_all(client_socket, meta.length)
                        self.handle_client_put(meta.key, s)

                    case Constants.CLIENT_GET:
                        key, _ = CacheEngineKey.seperate_timestamp(meta.key)
                        stored_tuple = self.data_store.get(key, None)
                        if meta.force_latest or stored_tuple is None:
                            self.central_get_socket.sendall(ClientMetaMessage(Constants.CLIENT_GET, meta.key, 0).serialize())
                            data = self.central_get_socket.recv(ServerMetaMessage.packlength())
                            cur_meta = ServerMetaMessage.deserialize(data)
                            if cur_meta.code == Constants.SERVER_SUCCESS:
                                length = cur_meta.length
                                data = self.receive_all(self.central_get_socket, length)
                                self.handle_client_put(cur_meta.key, data)
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

    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <host> <port> <central_host> <central_port>")
        exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    central_host = sys.argv[3]
    central_port = int(sys.argv[4])

    server = RegionalServer(host, port, central_host, central_port)
    server.run()

