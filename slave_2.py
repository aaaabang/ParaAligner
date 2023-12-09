from network.client import Client

def main(addr_list, addr, configs):
    client = Client(addr_list, addr, configs)
    while not client.closed():
        client.strategy.iter()


def worker(addr):
    addr_list = [("127.0.0.1", 8000), ("127.0.0.1", 8001), ("127.0.0.1", 8002)]
    configs = { "key": "value", 
                'k': 1,   
                "database": "data/databases/test.txt",
                "patterns": [
                    # "data/patterns/small.fna",
                    "data/patterns/test.txt"
                ]}
    main(addr_list, addr, configs)



worker(("127.0.0.1", 8002))