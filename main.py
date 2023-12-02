from network.client import Client

def main(addr_list, addr, configs):
    client = Client(addr_list, addr, configs)
    while not client.closed():
        client.strategy.iter()


if __name__ == '__main__':
    # TODO
    pass
