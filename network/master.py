import math
from operator import itemgetter
import queue
import time
from .base import StrategyBase
from alg import files
from .constant import key_value as kv
import numpy as np
from alg.seq import get_fna_length
class Master(StrategyBase):

    def __init__(self, client):
        super().__init__(client)
        self.client = client
        self.slaves_states = [{'addr': addr, 'update_time': time.time(), 'alive': True, 'idle': True, 'subvec': []} for addr in self.client.addr_list if addr != client.addr]
        # map job to slave {(i_th_pattern, start_ind, end_ind): slave_addr}
        self.job_slave = {}
        # save jobs for slaves
        self.receive_queue = queue.Queue()
        # top-k value and theri pos {i_th_pattern: [{'pos', 'val', 'start_ind', 'end_ind'}] }
        self.topKs = {}
        
        # send and receive a msg of 50 grids at a time within a subvector
        self.msg_size = 50
        # divide database into blocks
        self.database_size = get_fna_length(self.client.configs[kv.DATABASE])
        self.block_size = int(math.sqrt(self.database_size))
        #support multiple patterns
        self.patterns_sizes = []
        # the last time master sends heartbeat
        self.last_heartbeat = 0

        self.init_jobs()
        
    def init_jobs(self):   

        patterns = self.client.configs[kv.PATTERN]
        for i, pt in enumerate(patterns):
            self.patterns_sizes.append(get_fna_length(pt))
            print(f"{i} th pattern's size is {self.patterns_sizes[i]}")
            j = -1 # i_subvec
            while True:
                current_subvec_size = (j+1)* self.msg_size
                remain_subvec_size = self.patterns_sizes[i] - current_subvec_size
                if(remain_subvec_size <= 0):
                    break
                else:
                    size = min(remain_subvec_size, self.msg_size)
                    subvec = np.zeros(size)
                    j += 1
                job_item = {kv.SUBVEC: subvec, kv.START: 0, kv.END: self.block_size, kv.I_SUBVEC: j, kv.Ith_PATTERN: i}
                self.receive_queue.put(job_item)

    
    def send_heartbeat(self, interval=3):

        while((time.time() - self.last_heartbeat) < interval):
            pass

        for slave in self.slaves_states:
            self.client.send(slave['addr'], b"Heartbeat")
            print(f"Matser Heartbeat sent to {slave['addr']}")

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
            i_th_pattern = job[kv.Ith_PATTERN]
            if job['subvec'] == 0:
                # find a new slave for a new job
                sent_flag = 0 # if find a idle slave, set 1 otherwise set 0
                for slave in self.slaves_states:
                    if slave['idle'] == True and slave['alive'] == True:
                        # update job_slave and slaves_states
                        self.job_slave[(i_th_pattern, start_ind, end_ind)] = slave['addr']
                        slave['idle'] = False
                        # send to slave
                        data = job.encode()
                        self.client.send(slave['addr'], data)
                        # see if job is sent successfully, if not, put it back into queue
                        sent_flag = 1
                
                if sent_flag == 0:
                    # have not found a idle slave, put back job into queue
                    self.receive_queue.put(job)

            else:
                slave = self.job_slave[(i_th_pattern, start_ind, end_ind)]# get the address of slave which possesses current chunk[start_ind, end_ind]
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


    def update_topKs(self, i_th_pattern, new_topKs, start_ind, end_ind):
        i_topKs = self.topKs.setdefault(i_th_pattern, [])
        for new_topk in new_topKs:
            new_pos = new_topk[1]
            new_val = new_topk[0]
            if len(i_topKs) < self.client.K:
                i_topKs.append({"pos": new_pos, "val": new_val, kv.START: start_ind, kv.END: end_ind})
                continue

            if new_val > min_value:
                i_topKs[0] = {"pos": new_pos, "val": new_val, kv.START: start_ind, kv.END: end_ind}
            
            # 按照 "val" 键进行排序
            sorted_i_topKs = sorted(i_topKs, key=lambda x: x["val"])
            self.topKs[i_th_pattern] = sorted_i_topKs

    def set_slave_idle(self, slave_addr):
        for slave in self.slaves_states:
            if(slave["addr"] == slave_addr):
                slave['idle'] = True
                return
    
    def init_traceback(self, i_th_pattern):
        i_topKs = self.topKs[i_th_pattern] # this is a [{}]
        for topk in i_topKs:
            job_item = {}
            job_item[kv.TYPE] = kv.T_TYPE
            job_item[kv.Ith_PATTERN] = i_th_pattern
            job_item[kv.TOPK_POS] = topk["pos"]
            job_item[kv.TOPK_VALUE] = topk["val"]
            job_item[kv.START] = topk[kv.START]
            job_item[kv.END] = topk[kv.END]

            self.receive_queue.put(job_item)
    '''
    Master receives all data and put them into Queue
    If a package is a heartbeat from slaves, update slave_table
    '''
    def recv(self, addr, data):
        data = data.decode()
        print(f"receive: {data} from {addr}")
        if data == "Heartbeat Response":
            # update slave's state
            rank = self.client.addr_list.index(addr)
            self.slaves_states[rank-1]['update_time'] = time.time()
        elif data[kv.TYPE] == kv.F_TYPE:
            # fillmatrix phase
            # i_subv = data['i_subvec'] # index of the received parts of rightmost column
            # subvec = data['subvec']
            # start_ind = data['start_ind']
            # end_ind = data['end_ind']
            # done = data['done']
            # topKs = data['topKs']
            keys = [kv.Ith_PATTERN, kv.I_SUBVEC, kv.SUBVEC, kv.START, kv.END, kv.Done, kv.TOPKS]
            i_th_pattern, i_subv, subvec, start_ind, end_ind, done, topKs = map(itemgetter(*keys), [data] * len(keys))
            for i in range(len(subvec)):
                self.slaves_states[addr]['subvec'].insert(i_subv*self.msg_size + i, subvec[i])
            
            self.update_topKs(i_th_pattern, topKs, start_ind, end_ind)

            if done:
                # whole subvec, i.e rightmost column of a chunck, has been received
                # ready to send to another slave for work
                files.save_block(self.slaves_states[addr]['subvec'], i_th_pattern, start_ind, end_ind)
                files.save_topK(self.topKs, i_th_pattern)
                self.set_slave_idle(addr)
                del self.job_slave[(i_th_pattern, start_ind, end_ind)]

                if end_ind >= self.database_size - 1:
                    # fill_matrix done!!!
                    self.init_traceback(i_th_pattern)
                    return

            data[kv.START] = end_ind + 1
            data[kv.END] = min(end_ind + self.block_size, self.database_size - 1)
            # job_item = {kv.Ith_PATTERN: i_th_pattern, kv.SUBVEC: subvec, 'start_ind': end_ind + 1, "end_ind": end_ind + self.block_size, 'i_subv': i_subv}
            self.receive_queue.put(data)

        else:
            # traceback phase
            alignment = data[kv.ALI]
            i_th_pattern = data[kv.Ith_PATTERN]
            topK_pos = data[kv.TOPK_POS]
            topK_val = data[kv.TOPK_VALUE]
            
            files.save_output(i_th_pattern, alignment)
            print(f"{i_th_pattern} pattern get one alignment of topk {topK_val} : {alignment}")
        pass
