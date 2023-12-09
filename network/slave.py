import queue
from .base import StrategyBase
from alg.alg import fill_matrix, trace_back
import time
from .constant import key_value as kv
from alg import files
from alg.seq import read_fna
# from .master import Master

class Slave(StrategyBase):
    def __init__(self, client):
        super().__init__(client)
        self.job_queue = queue.Queue()
        self.client = client
        self.rank = client.rank # rank of this slave 
        self.last_heartbeat_time = time.time()
        self.master_addr = client.master_addr
        master_addr = self.client.addr_list[0]

        # 停止任务的标志
        self.master_timed_out = False
        self.stop_current_task = False

#already done
    def send_heartbeat_response(self):
        # response heartbeat
        self.client.send(self.master_addr, b"Heartbeat Response")
        print(f"Slave {self.rank} responses Heartbeat to {self.master_addr}")
        

    def check_if_master_alive(self, timeout=5):
        current_time = time.time()
        if (current_time - self.last_heartbeat_time) > timeout and not self.master_timed_out:
            # if the master timeout
            print('Master is considered as timed out.')
            self.handle_master_timeout()



#computing functions
    def handle_fillmatrix(self, data):
        # 从Master接收到的数据
        '''
            data = {
                    "start_ind": 0,
                    "end_ind": 0,
                    "i_subvec": 0,
                    "subvec": [],
                    "i_th_database": 0,
                }
        '''

        # 从文件系统读对应的sequence, pattern
        i_th_database = data['i_th_database'] # 0, 1, 2, 3
        sequence = read_fna(self.client.configs['database'][i_th_database], data['start_ind'], data['end_ind'])
        pattern = read_fna(self.client.configs['patterns'], data['start_ind'], data['end_ind'])
        N = len(sequence)
        M = len(pattern)
        
        # 计算第一个subvec的长度, 用于判断传给fillmatrix的pattern的长度
        if data['i_subvec'] == 0:
            subvec_length = len(data['subvec'])

        # 计算一个 pattern 要划分成几个 subvec
        num_subvecs, remainder = divmod(M, subvec_length)
        num_subvecs += remainder > 0

        #如果是第一块, upvec传空, 否则传上一块的最后一行
        up_vec = [0 for _ in range(M)] if data['i_subvec'] == 0 else self.previous_bottom_vec

        #for data['i_subvec'] in range(num_subvecs):
        if data['i_subvec'] < num_subvecs - 1:
            pattern_subvec = pattern[data['i_subvec'] * subvec_length : (data['i_subvec'] + 1) * (subvec_length-1)]
            right_vec, bottom_vec, topK_dict = fill_matrix(pattern_subvec, sequence, up_vec, data['i_subvec'], data['start_ind'], data['end_ind'],self.client.K)
            self.previous_bottom_vec = bottom_vec
            response_data = {
                'start_ind': data['start_ind'],
                'end_ind': data['end_ind'],
                'i_subvec': data['i_subvec'],
                'subvec': right_vec,
                'topK': topK_dict,
                'done': False
            }
            self.send_fillmatirx(response_data)


        elif data['i_subvec'] == num_subvecs - 1:
            pattern_subvec = pattern[data['i_subvec'] * subvec_length :]
            right_vec, bottom_vec, topK_dict = fill_matrix(pattern_subvec, sequence, up_vec, data['i_subvec'], data['start_ind'], data['end_ind'],self.client.K)
            response_data = {
                'start_ind': data['start_ind'],
                'end_ind': data['end_ind'],
                'i_subvec': data['i_subvec'],
                'subvec': right_vec,
                'topK': topK_dict,
                'done': True
            }
            self.send_fillmatirx(response_data)

     


    def send_fillmatirx(self, data):
        # 将结果发送回 Master
        self.client.send(self.master_addr, data)
        print(f"Slave {self.rank} sends fillmatrix result to {self.master_addr}")
        
    def handle_traceback(self, data):
        # 执行 traceback 任务
        result = trace_back(data['top_k_i'], data['x'], data['y'], data['start_ind'], data['end_ind'])
        # 将结果发送回 Master
        #TODO

        # top_k_i: value
        # x:
        # y:
        # start_ind
        # end_ind
        pass



    def iter(self):
        # TODO
        # 检查是否收到master的心跳包，如果超时，则处理master超时
        # call alg to fill matrix, traceback
        # send to M
        while True:
            # 检查是否收到master的心跳包，如果超时，则处理master超时
            self.check_if_master_alive(timeout=5)
           # 处理任务队列中的任务
            if not self.job_queue.empty():
                task = self.job_queue.get()

                if self.stop_current_task:
                    print("Slave is stopping current task.")
                    continue  # 跳过当前任务

                if task['type'] == 'fillmatrix':
                    self.handle_fillmatrix(task)
                elif task['type'] == 'traceback':
                    self.handle_traceback(task)

            time.sleep(0.1)  # 休眠0.1秒，避免CPU占用过高



    #从master接收数据
    def recv(self, addr, data):
        if data:
            data = data.decode()
            print("receive: ", data)
            if data == 'Heartbeat':
                # 处理心跳包
                self.last_heartbeat_time = time.time()  # 更新最后一次心跳时间
                self.send_heartbeat_response()
                print("Slave received heartbeat from Master")
            # elif data == 'remake':
            #     self.handle_remake_command()
            else:
                self.job_queue.put(data)
            # elif data ['type']== 'remake':
            #     # 处理从master收到的 remake 命令
            #     self.job_queue.put(data)
            # elif data['type'] == 'fillmatrix':
            #     # 添加到工作队列
            #     self.job_queue.put(data)
            # elif data['type'] == 'traceback':
            #     # 添加到 工作队列
            #     self.job_queue.put(data)
    




    #Crush Handling
#     def handle_master_timeout(self):
#         self.master_timed_out = True
#         print("Handling master timeout...")
#         # 如果slave rank等于master rank+1, 则成为新的master
#         master_rank = 0
#         if self.rank == master_rank + 1: 
#             # if Slave's rank is equal to Master's rank+1, then become the new Master
#             self.become_new_master()    
#         else:
#             self.handle_remake_command()

#         time.sleep(10)
        
#     def become_new_master(self):
#     # 从磁盘重新启动并作为master
#         print(f"Slave {self.rank} is becoming the new Master.")
#        # 重置工作状态
#         self.job_queue.queue.clear()
#         self.stop_current_task = False
#         # 设置为master
#         self.client.close()
#         self.client.reopen_socket()  # 重新打开socket
#         self.client = Master(self.client)  # 创建一个新的Master对象
#         print(f"Slave {self.rank} is becoming the new Master.")
#         self.client.iter()  # 开始执行Master的任务
       
    
#     def handle_remake_command(self):
#         # 处理从master收到的 remake 命令
#         print(f"Slave {self.rank} received 'remake' message. Preparing to reset and connect to the new Master.")
        
#         self.stop_current_task = True  # 设置停止当前任务的标志
#         self.job_queue.queue.clear()  # 清空工作队列
#         print("clear job queue")
        
#         self.connect_to_new_master()
#         print("waiting for connecting to new master")
        
# #TODO
#     def connect_to_new_master(self):
#         # 重新连接到master, 设置为false
#         self.master_timed_out = False
#         master_addr = self.client.addr_list[1]
#         print("Reset complete. Ready to connect to the new Master.")
