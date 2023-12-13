
from alg.files import fs_init
from network.client import Client


def main(addr_list, addr, configs):
    if addr == addr_list[0]:
        fs_init()

    client = Client(addr_list, addr, configs)
    while not client.closed():
        try:
            client.strategy.iter()
        except Exception as e:
            print(e.with_traceback())
            client._closed = True