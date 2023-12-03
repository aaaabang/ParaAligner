import queue
import time
from .base import StrategyBase
from .client import Client
from ..alg import files

class Master(StrategyBase):

    def __init__(self, client: Client):
        super().__init__(client)
        self.client = client
        self.slaves_states = [{'update_time': time.time(), 'alive': True, 'idle': True, 'start_ind': -1, 'end_ind': -1, 'subvec': []} for _ in range(len(Client.addr_list))]
        
        # save jobs for slaves
        self.receive_queue = queue.Queue()
        # top-k value and theri pos
        self.topKs = {}

        self.msg_size = 10 # send and receive 10 blocks at a time
        
    def send_heartbeat(self, interval=3):
        for slave in Client.addr_list:
            self.client.send(slave, b"Heartbeat")
            print(f"Matser Heartbeat sent to {slave}")
            time.sleep(interval)

    def check_if_slave_alive(self, timeout=5):
        current_time = time.time()
        for slave in self.slaves_states:
            # slave timeout
            if((current_time - slave['update_time']) > timeout):
                slave['alive'] = False
                # TODO 
                '''
                what to do if a slave time out
                '''

    '''
    1. Send Heatbeats
    2. Process subvectors received from one slave
    3. Send subvector of matrix to another slave
    '''
    def iter(self):
        self.send_heartbeat(interval=3)
        self.check_if_slave_alive(timeout=5)
        pass


    def update_topKs(self, new_topKs):
        for new_pos, new_val in new_topKs.items():
            if len(self.topKs) < Client.K:
                self.topKs[new_pos] = new_val
                continue

            min_pos, min_value = next(iter(self.topks.items()))
            if new_val > min_value:
                self.topKs[new_pos] = self.topKs.pop(min_pos, None)
                self.topKs[new_pos] = new_val
            
            self.topKs = dict(sorted(self.topKs.items(), key=lambda item: item[1]))


    '''
    Master receives all data and put them into Queue
    If a package is a heartbeat from slaves, update slave_table
    '''
    def recv(self, addr, data):
        # TODO
        data = data.decode()
        rank = Client.addr_list.index(addr) # get slave's rank
        if data == "Heartbeat Response":
            # update slave's state
            self.slaves_states[rank]['update_time'] = time.time()
        elif 'alignment' not in data:
            # fillmatrix phase
            i_subv = data['i_subvec'] # index of the received parts of rightmost column
            subvec = data['subvec']
            start_ind = data['start_ind']
            end_ind = data['end_ind']
            done = data['done']
            topKs = data['topKs']
            for i in range(len(subvec)):
                self.slaves_states[rank]['subvec'][i_subv*self.msg_size + i] = subvec[i]
            
            self.update_topKs(new_topKs=topKs)
            
            if done:
                # whole subvec, i.e rightmost column of a chunck, has been received
                # ready to send to another slave for work
                files.save_block(self.slaves_states[rank]['subvec'], start_ind, end_ind)
                files.save_topK(self.topKs)

        else:
            # traceback phase
            pass

        pass