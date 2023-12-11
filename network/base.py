import socket, threading


class ClientBase:
    def __init__(self, addr):
        self.addr = addr
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.settimeout(1)
        self.udp_socket.bind(addr)
        self._closed = False

        def recv_thread():
            while not self._closed:
                try:
                    data, addr = self.udp_socket.recvfrom(4096)
                    self.recv(addr, data)
                except socket.timeout:
                    pass
                except Exception as e:
                    # print("test",e)
                    self._closed = True
        threading.Thread(target=recv_thread).start()

    def recv(self, addr, data):
        pass

    def send(self, addr, data):
        self.udp_socket.sendto(data, addr)

    def close(self):
        self.udp_socket.close()

    def closed(self):
        return self._closed


class StrategyBase:
    def __init__(self, client):
        self.client = client

    def iter(self):
        pass

    def recv(self, addr, data):
        pass
