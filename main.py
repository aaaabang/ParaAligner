from network.client import Client
from alg.files import fs_init

def main(addr_list, addr, configs):
    if addr == addr_list[0]:
        fs_init()

    client = Client(addr_list, addr, configs)
    while not client.closed():
        client.strategy.iter()


if __name__ == '__main__':
    # TODO
    #addr_list [(ip, port)]
    pass
