from multiprocessing import Process
from main import main
import json


def gen_addr(size, init=8010):
    return [('127.0.0.1', init + i) for i in range(size)]


if __name__ == '__main__':
    with open('./config.json', 'r') as f:
        config = json.load(f)
    print(config)
    addr_list =gen_addr(5) # you can add more processes to get faster
    ps = [Process(target=main, args=(addr_list, addr, config)) for addr in addr_list]
    for p in ps:
        p.start()
