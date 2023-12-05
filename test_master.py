
from network.client import Client

def main(addr_list, addr, configs):
    client = Client(addr_list, addr, configs)
    while not client.closed():
        client.strategy.iter()


def worker(addr):
    addr_list = [("localhost", 8000), ("localhost", 8001), ("localhost", 8002)]
    configs = {"key": "value", 'k': 1}
    main(addr_list, addr, configs)



# import sys
# import os

# # 获取脚本所在目录的上级目录
# project_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.dirname(project_dir)

# # 将项目的上级目录添加到 sys.path
# sys.path.append(parent_dir)

worker(("localhost", 8000))
