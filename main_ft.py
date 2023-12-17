import os
import socket
import json
import random
import time
from network.client import Client
from alg.files import fs_init
from time import sleep

node_id = int(os.getenv('SLURM_NODEID'))
task_id = int(os.getenv('SLURM_LOCALID'))
base_port = int(os.getenv('BASE_PORT', 7800))
ntasks = int(os.getenv('SLURM_NTASKS'))
port = base_port + node_id * 10 + task_id

with open('./config.json', 'r') as f:
    config = json.load(f)

server_hostnames = os.getenv('SLURM_NODELIST')
ind_bracket = server_hostnames.find('[')
server_hostname = 'node' + server_hostnames[ind_bracket + 1: ind_bracket + 4]
server_address = (socket.gethostbyname(server_hostname), base_port)

def server(n):
    s_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s_socket.bind(server_address)
    print(f"UDP Server listening on {base_port}")
    addrs = []
    addrs.append((socket.gethostbyname(socket.gethostname()), base_port))
    while n>0:
        n -= 1
        data, addr = s_socket.recvfrom(1024)
        addrs.append(addr)
    # print(addrs)
    for addr in addrs:
        s_socket.sendto(json.dumps(addrs).encode(), addr)
    s_socket.close()
    return addrs

def client():
    c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c_address_local = (socket.gethostbyname(socket.gethostname()), port)
    c_socket.bind(c_address_local)
    sleep(10)
    message = f"Echo from Node {node_id}, Task {task_id}, no meaning here."
    c_socket.sendto(message.encode(), server_address)
    data, _ = c_socket.recvfrom(4096)
    addrs = json.loads(data.decode())
    addrs = [tuple(addr) for addr in addrs]
    c_socket.close()
    return addrs, c_address_local

def main(addr_list, addr, configs, crash_node_id, crash_task_id):
    if addr == addr_list[0]:
        fs_init()
    start_time = time.time()
    stop_duration = random.randint(10, 20)
    client = Client(addr_list, addr, configs)
    while not client.closed():
        if node_id == crash_node_id and task_id == crash_task_id and (time.time() - start_time > stop_duration):
            print("node{}-{} crashed".format(node_id, task_id))
            break

        try:
            client.strategy.iter()
        except Exception as e:
            # print(e.with_traceback())
            client._closed = True

crash_node_id = int(os.getenv('nid'))
crash_task_id = int(os.getenv('tid'))

print(crash_node_id, crash_task_id)
# crash_node_id = 0
# crash_task_id = 0

if node_id == 0 and task_id == 0:
    addrs = server(ntasks - 1) # ignore self
    main(addrs, server_address, config, crash_node_id, crash_task_id)
else:
    addrs, c_address = client()
    main(addrs, c_address, config, crash_node_id, crash_task_id)
