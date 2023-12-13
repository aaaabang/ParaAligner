import pickle
import queue
import time
import json
import pickle
import numpy as np
from .constant import params
from .base import StrategyBase
from alg.alg import fill_matrix, trace_back
from network.constant.params import SUBVEC_SIZE
# from alg.test_alg import fill_matrix, trace_back
from alg.seq import read_str, get_str_length
from .constant import key_value as kv
# from .master import Master


class Slave(StrategyBase):
    def __init__(self, client, term):
        super().__init__(client)
        self.job_queue = queue.Queue()
        self.client = client
        self.rank = client.rank # rank of this slave 
        self.last_heartbeat_time = time.time()
        self.master_addr = client.master_addr
        # master_addr = self.client.addr_list[0]
        self.master_addr = self.client.addr_list[0]
        self.term = term      
        self.previous_bottom_vec = np.zeros(0) 
        

    def send_heartbeat_response(self):
        # response heartbeat
        data = "Heartbeat Response"
        data = pickle.dumps(data)
        self.client.send(self.master_addr, data)
        print(f"Slave {self.rank} responses Heartbeat to {self.master_addr}")
        

    def check_if_master_alive(self, timeout=20):
        current_time = time.time()
        if (current_time - self.last_heartbeat_time) > timeout:
            print('Master is considered as timed out.')
            time.sleep(3)
            # rank 
            if self.rank == 1:
                #update addr_list
                new_addr = [addr for addr in self.client.addr_list if  addr != self.master_addr]
                self.client.addr_list = new_addr
                # update term
                self.term += 1
                #restart Master
                self.client.set_state('M', self.term)     
                
                data = {
                    kv.TERM: self.term,
                    kv.ADDR_LIST: new_addr,
                    kv.TYPE: kv.RESTART
                        }
                

                data = pickle.dumps(data)

                for addr in new_addr[1:]:
                    self.client.send(addr, data)
                    print(f"restart message {data} have been sent to {addr}")
            # else:
            #     self.client.set_state('S', self.term)
            #     self.term = self.term + 1
            #     new_addr = [addr for addr in self.client.addr_list if  addr != self.master_addr]
            #     self.master_addr = new_addr[0]
            #     self.client.addr_list = new_addr

    def handle_restart_command(self, data):
        # 处理从master收到的 remake 命令
        print(f"Received 'restart' message.")
        # data = {
        #             kv.TYPE: kv.RESTART,
        #             kv.TERM: self.term,
        #             kv.ADDR_LIST: new_addr
        #         }
        self.client.set_state('S', self.term)
        self.term = data[kv.TERM] 
        self.client.addr_list = data[kv.ADDR_LIST]
        self.master_addr = self.client.addr_list[0]
        print(f"New master_addr: {self.master_addr}")
        print(f"New term: {self.term}")
        print(f"New addr_list: {self.client.addr_list}")

#computing functions
    def handle_fillmatrix(self, data):
        # 从Master接收到的数据
        '''
            data = {
                    "start_ind": 0,
                    "end_ind": 0,
                    "i_subvec": 0,
                    "subvec": [],
                    "i_th_pattern": 0,
                }
        '''
        # test
        # with open(self.configs['patterns'], 'r') as f:
        #     content = f.read()
        # print(f"Content: {content}")

       
        # 从文件系统读对应的sequence, pattern
        i_th_pattern = data['i_th_pattern'] # 0, 1, 2, 3
        sequence = read_str(self.client.configs['database'], data['start_ind'], data['end_ind'])
        # print(f"sequence: {sequence}") # test

        pat_len = get_str_length(self.client.configs['patterns'][i_th_pattern])
        pattern = read_str(self.client.configs['patterns'][i_th_pattern], 0, pat_len)
        
        # pattern = read_str("data/patterns/test.txt", 0, 16)
        # pattern = read_str("data/patterns/medium.txt", 0, pat_len)
        # print(f"pattern: {pattern}") # test

        N = len(sequence)
        M = len(pattern)    
        
        # print(N, M) # test
        
        subvec_length = len(data['subvec'])

        # Conputing the first subvec, then get the number of subvecs
        if data['i_subvec'] == 0:
            # self.num_subvecs = int(M/(subvec_length - 1)) + 1
            self.num_subvecs = int(M/(params.SUBVEC_SIZE - 1)) + 1
        
        print(f"subvec_nums:{self.num_subvecs}" )

        #如果是第一块, upvec传空, 否则传上一块的最后一行
        # up_vec = [0,0]
        up_vec = [0 for _ in range(N)] if data['i_subvec'] == 0 else self.previous_bottom_vec
        # print(len(up_vec)) 
        

        #for data['i_subvec'] in range(num_subvecs):
        if data['i_subvec'] < self.num_subvecs - 1:
            # print(f"data 1:{data['i_subvec']}")
            pattern_subvec = pattern[data['i_subvec'] * (subvec_length-1) : (data['i_subvec'] + 1) * (subvec_length-1) ]


            # print(f"pattern_subvec: {pattern_subvec}")
            # print(f"sequence: {sequence}")
            # print(f"up_vec: {up_vec}")
            # print(f"data['i_subvec']: {data['i_subvec']}")
            # print(f"data['start_ind']: {data['start_ind']}")
            # print(f"data['end_ind']: {data['end_ind']}")
            # print(f"self.client.K: {self.configs['k']}")
            right_vec, bottom_vec, topK_dict = fill_matrix( data['subvec'], up_vec, data['i_subvec'], sequence, pattern_subvec, self.client.configs['k'],data['start_ind']) #加了个参数
            self.previous_bottom_vec = bottom_vec
            
            # test
            right_vec = right_vec.tolist()

            response_data = {
                'type': 'fillmatrix',
                'i_th_pattern': data['i_th_pattern'],
                'start_ind': data['start_ind'],
                'end_ind': data['end_ind'],
                'i_subvec': data['i_subvec'],
                'subvec': right_vec,
                'topks': topK_dict,
                'done': False,
                kv.TERM : self.term
            }
            
            # TEST
            print("result test:" , response_data)
            self.send_fillmatirx(response_data)

        elif data['i_subvec'] == self.num_subvecs - 1:
            print(f"data 1:{data['i_subvec']}")
            pattern_subvec = pattern[data['i_subvec'] * (SUBVEC_SIZE-1) :]   #改了 
            right_vec, bottom_vec, topK_dict = fill_matrix( data['subvec'], up_vec, data['i_subvec'], sequence, pattern_subvec, self.client.configs['k'],data['start_ind'])
            right_vec = right_vec.tolist()
            response_data = {
                'type': 'fillmatrix',
                'i_th_pattern': data['i_th_pattern'],
                'start_ind': data['start_ind'],
                'end_ind': data['end_ind'],
                'i_subvec': data['i_subvec'],
                'subvec': right_vec,
                'topks': topK_dict,
                'done': True,
                kv.TERM : self.term
            }
            print("result test:" , response_data)
            self.send_fillmatirx(response_data)

     
    def send_fillmatirx(self, data):
        # 将结果发送回 Master
        data = pickle.dumps(data)
        self.client.send(self.master_addr,data)

        print(f"Slave {self.rank} sends fillmatrix result to {self.master_addr}")
        
    # trace_back(topK, start_s, end_s)
    def handle_traceback(self, data):
        # 从Master接收到的数据
        '''
         data = {'type' : kv.T_TYPE,
            'i_th_pattern' : i_th_pattern,
            'topk_pos' : topk["pos"],
            'topk_value' : topk["val"],
            'start_ind' : topk["start_ind"],
            'end_ind' : topk["end_ind"]}                       
        '''
        # 从文件系统读对应的sequence, pattern
        i_th_pattern = data['i_th_pattern'] # 0, 1, 2, 3
        sequence_path = self.client.configs['database']
        # print(f"sequence: {sequence_path}") # test
        pattern_path = self.client.configs['patterns'][i_th_pattern]
        # print(f"pattern: {pattern_path}") # test
        
        topK = {
            "value": data['topk_value'],
            # "i_subvec": 0, # TODO
            "xy": data['topk_pos']
        }
        # TODO
        block_size = data['start_ind'] - data['end_ind']
        aligned_p_s, aligned_s_s = trace_back(topK, data['start_ind'], data['end_ind'], sequence_path, pattern_path, i_th_pattern)

        # 将结果发送回 Master
        response_data = {
            'alignment':  (aligned_p_s,aligned_s_s),
            'i_th_pattern': i_th_pattern,
            'topk_pos': data['topk_pos'],
            'topk_value': data['topk_value'],
            # 'start_ind': data['start_ind'],
            # 'end_ind': data['end_ind'],
            'type': kv.T_TYPE,
            kv.TERM : self.term
        }
        print("tb_response_data:" , response_data)
        self.send_traceback(response_data)

    def send_traceback(self, data):
        # 将结果发送回 Master
        data = pickle.dumps(data)
        self.client.send(self.master_addr,data)

        print(f"Slave {self.rank} sends traceback result to {self.master_addr}")


    def iter(self):

        self.check_if_master_alive()
        # handle jobs in the queue
        while not self.job_queue.empty():
            task = self.job_queue.get()
            if task[kv.TERM] < self.term:
                return         
            else:    
                if task['type'] == 'fillmatrix':
                    print("job type is fillmatrix")
                    self.handle_fillmatrix(task)
                elif task['type'] == 'traceback':
                    self.handle_traceback(task)
           
                


    #从master接收数据
    def recv(self, addr, data):
        if data:
            data = pickle.loads(data)
            if data == 'Heartbeat':
                # 处理心跳包
                self.last_heartbeat_time = time.time()  # 更新最后一次心跳时间
                self.send_heartbeat_response()
                # print("Slave received heartbeat from Master")
            # elif data == 'remake':
            #     self.handle_remake_command()
            elif data == kv.CLOSE:  
                print("Slave is closing.")
                self.client.close()
            elif data['term'] < self.term:
                print("Slave received data from old Master")
                return
            elif 'type' in data and data['type'] == kv.RESTART:
                self.handle_restart_command(data)
            elif 'type' in data:
                self.job_queue.put(data)
                print("Slave received data from Master")
                print("Current job_queue contents:", list(self.job_queue.queue))



       
    
    
               


