import copy
import json
import math
from operator import itemgetter
import pickle
import queue
import time

import numpy

from .base import StrategyBase
from alg import files
from .constant import key_value as kv
from .constant import params

import numpy as np
from alg.seq import get_str_length


INT_MIN = np.iinfo(np.int32).min // 2

class Master(StrategyBase):

    def __init__(self, client, term):
        super().__init__(client)
        
        self.client = client
        self.slaves_states = [{'addr': addr, 'update_time': time.time(), 'alive': True, 'idle': True, 'subvec': None} for addr in self.client.addr_list if addr != client.addr]
        # map job to slave {(i_th_pattern, start_ind, end_ind): slave_addr}
        self.job_slave = {}
        # save jobs for slaves
        self.receive_queue = queue.Queue()
        # top-k value and theri pos {i_th_pattern: [{'pos', 'val', 'start_ind', 'end_ind'}] }
        self.topKs = {}
        
        # send and receive a msg of 50 grids at a time within a subvector
        self.msg_size = params.SUBVEC_SIZE
        # divide database into blocks
        self.database_size = get_str_length(self.client.configs[kv.DATABASE])
        self.block_size = int(math.sqrt(self.database_size))
        #support multiple patterns
        self.patterns_sizes = []
        # the last time master sends heartbeat
        self.last_heartbeat = 0

        self.exist_aligns = 0
        self.total_aligns = 0

        self.term = term
        self.__init_jobs()
        print(f"new master {self.client.addr} with term {self.term}")
        
    def __init_jobs(self):   
        patterns = self.client.configs[kv.PATTERN]
        self.total_aligns = len(patterns)*self.client.configs['k']
        backup = files.fs_recover_info(self.client.configs)
        for i, pt in enumerate(backup):
            self.patterns_sizes.append(get_str_length(patterns[i]))
            if(pt['latest_col'] != None):
                total_subvec = files.load_block(i, pt['latest_col'])
                if pt['latest_col'] == self.database_size-1:#last column
                    self.__init_traceback(i)
                    return
                else:
                    start_ind = pt['latest_col'] + 1
                    end_ind = min(start_ind+self.block_size-1, self.database_size-1)
                    print(f"backup load {start_ind} to {end_ind}")
            else:
                total_subvec = np.full(self.patterns_sizes[i] + 1, 0)
                start_ind = 0
                end_ind = self.block_size-1
                # print(f"no backup{total_subvec} {self.patterns_sizes}")

            total_subvec_number = 0
            should_subvec_number = int(get_str_length(patterns[i])/(params.SUBVEC_SIZE - 1)) + 1
            j = -1 # i_subvec
            st_subvec = 0
            end_subvec = 0
            while True:
                current_subvec_size = (j+1)* (self.msg_size-1) + 1
                remain_subvec_size = (self.patterns_sizes[i] + 1) - current_subvec_size + 1
                # print("remain:", remain_subvec_size)
                # print("curren_subvec: ", current_subvec_size)
                # print("self pat: ", self.patterns_sizes[i])
                if(total_subvec_number >= should_subvec_number):
                    # print("curren_subvec: ", current_subvec_size)
                    # print("self pat: ", self.patterns_sizes[i])

                    break
                else:

                    size = min(remain_subvec_size, self.msg_size)
                    if size == 0:
                        break
                    end_subvec = st_subvec + size
                    # print(f"size:{size} st {st_subvec} end_sub {end_subvec}")
                    # print(f"total_su{total_subvec}")

                    subvec = total_subvec[st_subvec:end_subvec]
                    st_subvec += size - 1
                    # total_size += size
                    # print(f"size:{size} total {total_size}")
                    # print("total", total_subvec_number)
                    total_subvec_number += 1
                    j += 1

                job_item = {
                    kv.SUBVEC: subvec, 
                    kv.START: start_ind, 
                    kv.END: end_ind, 
                    kv.I_SUBVEC: j, 
                    kv.Ith_PATTERN: i,
                    # kv.TYPE: kv.T_TYPE,
                    kv.TYPE: kv.F_TYPE,
                    kv.TERM: self.term
                }
                self.receive_queue.put(job_item)

    def __send_heartbeat(self, interval=3):

        while((time.time() - self.last_heartbeat) < interval):
            return

        for slave in self.slaves_states:
            data = "Heartbeat"
            data = pickle.dumps(data)
            self.client.send(slave['addr'], data)
            # print(f"Matser Heartbeat sent to {slave['addr']}")

        self.last_heartbeat = time.time()

    def __check_if_slave_alive(self, timeout=5):
        current_time = time.time()
        for slave in self.slaves_states:
            # slave timeout
            if((current_time - slave['update_time']) > timeout):
                slave['alive'] = False
                print(f"slave {slave['addr']} dies ")
                #remove crashed 

                #update addr_list
                new_addr = [addr for addr in self.client.addr_list if  addr != slave['addr']]
                self.client.addr_list = new_addr
                #update term
                self.term += 1

                #restart Master
                self.client.set_state('M', self.term)

                #inform other slaves
                data = {
                    kv.TYPE: kv.RESTART,
                    kv.TERM: self.term,
                    kv.ADDR_LIST: new_addr
                }
                data = pickle.dumps(data)

                for addr in new_addr:
                    if(addr != self.client.addr):
                        self.client.send(addr, data)
                break # only one slave would crash
        
    def __send_job_to_slave(self):
        while(not self.receive_queue.empty()):
            job = self.receive_queue.get()
            start_ind = job[kv.START]
            end_ind = job[kv.END]
            i_th_pattern = job[kv.Ith_PATTERN]

            if job[kv.TYPE] == kv.T_TYPE:
                sent_flag = 0 # if find a idle slave, set 1 otherwise set 0
                for slave in self.slaves_states:
                    if slave['idle'] == True and slave['alive'] == True:
                        slave['idle'] = False
                        data = pickle.dumps(job)
                        self.client.send(slave['addr'], data)
                        print(f"[send traceback] new job {job} to Slave{slave['addr']}")

                        # see if job is sent successfully, if not, put it back into queue
                        sent_flag = 1
                        break
                if sent_flag == 0:
                    # have not found a idle slave, put back job into queue
                    self.receive_queue.put(job)
                continue
            # Fillmatrix job
            if job[kv.I_SUBVEC] == 0:
                # find a new slave for a new job
                sent_flag = 0 # if find a idle slave, set 1 otherwise set 0
                for slave in self.slaves_states:
                    if slave['idle'] == True and slave['alive'] == True:
                        slave['idle'] = False
                        # update job_slave and slaves_states
                        self.job_slave[(i_th_pattern, start_ind, end_ind)] = slave['addr']
                        slave[kv.SUBVEC] = np.full(self.patterns_sizes[i_th_pattern] + 1, INT_MIN)
                        # send to slave
                        data = pickle.dumps(job)
                        self.client.send(slave['addr'], data)
                        # see if job is sent successfully, if not, put it back into queue
                        sent_flag = 1
                        
                        print(f"[send fillmartix] new job {job} to Slave{slave['addr']}")
                        break

                if sent_flag == 0:
                    # have not found a idle slave, put back job into queue
                    self.receive_queue.put(job)
                    return

            else:
                try:
                    key = (i_th_pattern, start_ind, end_ind)
                    if key in self.job_slave:
                        slave = self.job_slave[(i_th_pattern, start_ind, end_ind)]# get the address of slave which possesses current chunk[start_ind, end_ind]
                        data = pickle.dumps(job)
                        self.client.send(slave, data)
                        # print(f"send new job {job} to Slave{slave}")
                    else:
                        self.receive_queue.put(job)
                except Exception as e:
                    print(f"Exception {e}")
                    print(f"job: {job}")
                    queue_copy = self.receive_queue.queue.copy()

                    # 访问队列副本但不处理
                    while queue_copy:
                        item = queue_copy.pop()  # 或者使用 queue_copy.pop()，取决于你想如何处理队列元素的顺序
                        print("item", item)

          
    '''
    1. Send Heatbeats
    2. Process subvectors received from one slave
    3. Send subvector of matrix to another slave
    '''
    def iter(self):
        self.__send_heartbeat(interval=3)
        self.__check_if_slave_alive(20)
        self.__send_job_to_slave()
        pass


    def __update_topKs(self, i_th_pattern, new_topKs, start_ind, end_ind):
        i_topKs = self.topKs.setdefault(i_th_pattern, [])
        for new_topk in new_topKs:
            new_pos = new_topk[1]
            new_val = new_topk[0]
            if len(i_topKs) < self.client.K:
                i_topKs.append({"pos": new_pos, "val": new_val, kv.START: start_ind, kv.END: end_ind})
                continue

            if new_val > i_topKs[0]['val']:
                i_topKs[0] = {"pos": new_pos, "val": new_val, kv.START: start_ind, kv.END: end_ind}
            
            # 按照 "val" 键进行排序
            sorted_i_topKs = sorted(i_topKs, key=lambda x: x["val"])
            i_topKs = sorted_i_topKs

        self.topKs[i_th_pattern] = i_topKs

        # print(self.topKs)

        


    def __set_slave_idle(self, slave_addr):
        for slave in self.slaves_states:
            if(slave["addr"] == slave_addr):
                slave['idle'] = True
                return
    
    def __init_traceback(self, i_th_pattern):
        if len(self.topKs) == 0:
            #traceback中途重启的
            i_topKs = files.load_topK(i_th_pattern)
            self.topKs[i_th_pattern] = i_topKs
        else:
            i_topKs = self.topKs[i_th_pattern] # this is a [{}]
        for topk in i_topKs:
            job_item = {
                kv.TYPE: kv.T_TYPE,
                kv.Ith_PATTERN: i_th_pattern,
                kv.TOPK_POS: topk["pos"],
                kv.TOPK_VALUE: topk["val"],
                kv.START: topk[kv.START],
                kv.END: topk[kv.END],
                kv.DB_SIZE: self.database_size,
                kv.TERM: self.term
            }
            self.receive_queue.put(job_item)
    
    '''
    Master receives all data and put them into Queue
    If a package is a heartbeat from slaves, update slave_table
    '''
    def recv(self, addr, data):

        data = pickle.loads(data)
        

        rank = self.client.addr_list.index(addr)

        if data == "Heartbeat Response":
            # update slave's state
            self.slaves_states[rank-1]['update_time'] = time.time()
        elif (data[kv.TERM] < self.term):
            print(f"received outdated data from {addr} in term {data[kv.TERM]}")
            return
        elif data[kv.TYPE] == kv.F_TYPE:

            # fillmatrix phase
            # i_subv = data['i_subvec'] # index of the received parts of rightmost column
            # subvec = data['subvec']
            # start_ind = data['start_ind']
            # end_ind = data['end_ind']
            # done = data['done']
            # topKs = data['topKs']
            # keys = [kv.Ith_PATTERN, kv.I_SUBVEC, kv.SUBVEC, kv.START, kv.END, kv.Done, kv.TOPKS]
            # i_th_pattern, i_subv, subvec, start_ind, end_ind, done, topKs = map(itemgetter(*keys), [data] * len(keys))
            i_th_pattern = data[kv.Ith_PATTERN]
            i_subv = data[kv.I_SUBVEC]
            subvec = data[kv.SUBVEC]
            topKs = data[kv.TOPKS]
            start_ind = data[kv.START]
            end_ind = data[kv.END]
            done = data[kv.Done]

            print(f"[receive fillmatrix] {data} from {addr}")

            for i in range(len(subvec)):
                # Assuming self.slaves_states[rank-1]['subvec'] is a list
                subvec_list = self.slaves_states[rank-1]['subvec']
                if (i_subv == 0):
                    subvec_list[i] = subvec[i]
                else:
                    if i+1 >= len(subvec):
                        break
                    # j = -99:
                    subvec_list[self.msg_size + (i_subv-1)*(self.msg_size-2) + i] = subvec[i+1]
                    # j = self.msg_size + (i_subv-1)*(self.msg_size-1) + i
                    # print("i_subv*self.msg_size + i",j)
                    # print("i+1",i+1)
                    # print("subvec_list, ", subvec_list)
            self.__update_topKs(i_th_pattern, topKs, start_ind, end_ind)

            # print(f"i_pat {i_th_pattern} i_subv {i_subv} len_subvec_list = {(subvec_list)}")


            if not (self.slaves_states[rank-1]['subvec'] == INT_MIN).any():
                #print(f"i_pat {i_th_pattern} i_subv {i_subv} len_subvec_list = {(subvec_list)}")

                # whole subvec, i.e rightmost column of a chunck, has been received
                # ready to send to another slave for work
                # print("self.slaves_states:", self.slaves_states)
                files.save_block(self.slaves_states[rank-1]['subvec'], i_th_pattern, start_ind, end_ind)
                files.save_topK(self.topKs[i_th_pattern], i_th_pattern)
                self.__set_slave_idle(addr)
                # print("self.job_slave", self.job_slave)
                del self.job_slave[(i_th_pattern, start_ind, end_ind)]
                if end_ind >= self.database_size - 1:
                    # fill_matrix done!!!
                    self.__init_traceback(i_th_pattern)
                    return

            if end_ind >= self.database_size - 1:
                # fill_matrix最右一个矩阵块
                return
            
            data[kv.START] = end_ind + 1
            data[kv.END] = min(end_ind + self.block_size, self.database_size - 1)
            # job_item = {kv.Ith_PATTERN: i_th_pattern, kv.SUBVEC: subvec, 'start_ind': end_ind + 1, "end_ind": end_ind + self.block_size, 'i_subv': i_subv}
            self.receive_queue.put(data)
            # print("size queue", self.receive_queue.qsize())
            # queue_copy = self.receive_queue.queue.copy()

            # # 访问队列副本但不处理
            # while queue_copy:
            #     item = queue_copy.pop()  # 或者使用 queue_copy.pop()，取决于你想如何处理队列元素的顺序
            #     print("item", item)
        elif data[kv.TYPE] == kv.T_TYPE:
            print(f"[receive traceback]: {data} from {addr}")
            # traceback phase
            alignment = data[kv.ALI]
            i_th_pattern = data[kv.Ith_PATTERN]
            topK_pos = data[kv.TOPK_POS]
            topK_val = data[kv.TOPK_VALUE]
        
            self.__set_slave_idle(addr)

            # del self.job_slave[(i_th_pattern, start_ind, end_ind)]
            self.job_slave = dict(filter(lambda item: item[1] != addr, self.job_slave.items()))
            files.save_output(i_th_pattern, alignment, topK_val)
            print(f"{i_th_pattern} pattern get one alignment of topk {topK_val} : {alignment}")

            self.exist_aligns += 1
            print(f"self.exist_aligns {self.exist_aligns}")
            print(f"self.total_aligns {self.total_aligns}")

            if self.exist_aligns >= self.total_aligns:
                print("master close")
                for addr in self.client.addr_list:
                    if addr != self.client.addr:
                        data = kv.CLOSE
                        data = pickle.dumps(data)
                        self.client.send(addr, data)
                self.client.close()
