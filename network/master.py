from operator import itemgetter
import queue
import time
from .base import StrategyBase
from alg import files
from .constant import key_value as kv

class Master(StrategyBase):

    def __init__(self, client):
        super().__init__(client)
        self.client = client
        self.slaves_states = [{'addr': addr, 'update_time': time.time(), 'alive': True, 'idle': True, 'subvec': []} for addr in range(len(self.client.addr_list))]
        
        # map job to slave {(start_ind, end_ind): slave_addr}
        self.job_slave = {}
        # save jobs for slaves
        self.receive_queue = queue.Queue()
        # top-k value and theri pos
        self.topKs = {}
        # send and receive 10 blocks at a time
        self.msg_size = 10
        
        self.last_heartbeat = 0
        
    def send_heartbeat(self, interval=3):

        while((time.time() - self.last_heartbeat) < interval):
            pass

        for slave in self.client.addr_list:
            self.client.send(slave, b"Heartbeat")
            print(f"Matser Heartbeat sent to {slave}")

        self.last_heartbeat = time.time()

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

    def send_job_to_slave(self):
        while(not self.receive_queue.empty()):
            job = self.receive_queue.get()
            start_ind = job[kv.START]
            end_ind = job[kv.END]

            if job['subvec'] == 0:
                # find a new slave for a new job
                sent_flag = 0 # if find a idle slave, set 1 otherwise set 0
                for slave in self.slaves_states:
                    if slave['idle'] == True and slave['alive'] == True:
                        data = job.encode()
                        self.job_slave[(start_ind, end_ind)] = slave['addr']
                        slave['idle'] = False
                        self.client.send(slave['addr'], data)
                        sent_flag = 1
                
                if not sent_flag:
                    # have not found a idle slave, put back job into queue
                    self.receive_queue.put(job)

            else:
                slave = self.job_slave[(start_ind, end_ind)]# get the address of slave which possesses current chunk[start_ind, end_ind]
                data[kv.TYPE] = kv.F_TYPE
                data = job.encode()
                self.client.send(slave, data)
              
    '''
    1. Send Heatbeats
    2. Process subvectors received from one slave
    3. Send subvector of matrix to another slave
    '''
    def iter(self):
        self.send_heartbeat(interval=3)
        self.check_if_slave_alive(timeout=5)
        self.send_job_to_slave()
        pass


    def update_topKs(self, new_topKs):
        for new_pos, new_val in new_topKs.items():
            if len(self.topKs) < self.client.K:
                self.topKs[new_pos] = new_val
                continue

            min_pos, min_value = next(iter(self.topks.items()))
            if new_val > min_value:
                self.topKs[new_pos] = self.topKs.pop(min_pos, None)
                self.topKs[new_pos] = new_val
            
            self.topKs = dict(sorted(self.topKs.items(), key=lambda item: item[1]))

    def set_slave_idle(self, slave_addr):
        for slave in self.slaves_states:
            if(slave["addr"] == slave_addr):
                slave['idle'] = True
                return
    '''
    Master receives all data and put them into Queue
    If a package is a heartbeat from slaves, update slave_table
    '''
    def recv(self, addr, data):
        data = data.decode()
        # rank = Client.addr_list.index(addr) # get slave's rank
        if data == "Heartbeat Response":
            # update slave's state
            self.slaves_states[addr]['update_time'] = time.time()
            print(f"Receive Slave {addr} responsing Heartbeat")
        elif 'alignment' not in data:
            # fillmatrix phase
            # i_subv = data['i_subvec'] # index of the received parts of rightmost column
            # subvec = data['subvec']
            # start_ind = data['start_ind']
            # end_ind = data['end_ind']
            # done = data['done']
            # topKs = data['topKs']

            keys = [kv.I_SUBVEC, kv.SUBVEC, kv.START, kv.END, kv.Done, kv.TOPKS]
            i_subv, subvec, start_ind, end_ind, done, topKs = map(itemgetter(*keys), [data] * len(keys))
            for i in range(len(subvec)):
                self.slaves_states[addr]['subvec'][i_subv*self.msg_size + i] = subvec[i]
            
            self.update_topKs(new_topKs=topKs)

            job_item = {'subvec': subvec, 'start_ind': end_ind + 1, "end_ind": end_ind + self.msg_size, 'i_subv': i_subv}
            self.receive_queue.put(job_item)
            
            if done:
                # whole subvec, i.e rightmost column of a chunck, has been received
                # ready to send to another slave for work
                files.save_block(self.slaves_states[addr]['subvec'], start_ind, end_ind)
                files.save_topK(self.topKs)
                self.set_slave_idle(addr)

        else:
            # traceback phase
            # TODO
            pass

        pass