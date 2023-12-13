from network.client import Client
from test_main import main


def worker(addr):
    addr_list = [("127.0.0.1", 8000), ("127.0.0.1", 8001), ("127.0.0.1", 8002)]
    configs = { "key": "value", 
                'k': 2,   
                "database": "data/databases/covid1.txt",
                "patterns": [
                    "data/patterns/test.txt",
                    #"data/patterns/medium.txt"
                ],
                "backup_folder": "backup"

                }
    main(addr_list, addr, configs)



worker(("127.0.0.1", 8002))