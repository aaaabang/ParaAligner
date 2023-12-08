import queue

from alg import files
from .base import StrategyBase
import socket
from alg.alg import fill_matrix, trace_back
import time


class Slave(StrategyBase):
    def __init__(self, client):
        super().__init__(client)
        self.job_queue = queue.Queue()
        self.client = client
        self.rank = client.rank # rank of this slave
        self.master_addr = client.master_addr
        master_addr = self.client.addr_list[0]


    def send_heartbeat_response(self):
        # response heartbeat
        self.client.send(self.master_addr, b"Heartbeat Response")
        print(f"Slave {self.rank} responses Heartbeat to {self.master_addr}")
        

    # def check_if_master_alive(self, timeout=5):
    #     current_time = time.time()
    #     if (current_time - self.last_heartbeat_time) > timeout:
    #         # if the master timeout
    #         print('Master is considered as timed out.')
    #         self.handle_master_timeout()

    #Crush Handling
    def handle_master_timeout(self):
        # handle master timeout
        if self.rank == self.client.rank + 1:
            # if Slave's rank is equal to Master's rank+1
            self.send_remake_message()
            # become the new Master
        else:
            # for other Slaves
            self.connect_to_new_master()

    def send_remake_message(self):
        # 向其他 Slave 发送 remake 消息
        # TODO

        pass

    def connect_to_new_master(self):
        # 连接新的 Master
        # TODO
        pass

    def handle_remake_command(self):
        # 处理收到的 remake 命令
        # TODO
        pass

    def handle_fillmatrix(self, data):
        # 执行 fillmatrix 任务
        result, topK_dict = fill_matrix(data['subvec'], data['i_subvec'], data['start_ind'], data['end_ind'],self.client.K)

        # 判断是否全部计算完成, 假设分为N块，每块计算完后，将结果存入files.py中的save_block函数
        # done = data['i_subvec'] == N-1

        # 将结果和 top-K 得分发送回 Master
        response_data={'start_ind': result['start_ind'], 
        'end_ind': result['end_ind'], 
        'i_subvec': data['i_subvec'], 
        'subvec': result['subvec'], 
        'result': result['result'], 
        'topK': topK_dict,
        'done': done}
        # 只发送算好矩阵的最右侧一列
        # 全部算完后即i_subvec=N时，发送done=true, 其余时候发送done=false
        # 将算好的矩阵存入files.py中的save_block函数
        if done:
            files.save_block(result)
        # TODO


    def handle_traceback(self, data):
        # 执行 traceback 任务
        result = trace_back(data['top_k_i'], data['x'], data['y'], data['start_ind'], data['end_ind'])
        # 将结果发送回 Master
        #TODO


    def iter(self):
        # TODO
        #call alg to fill matrix, traceback
        #send to M
        # while True:
        #     # self.recv()  # 接收并处理来自 Master 的数据
        #     self.check_if_master_alive()  # 检查 Master 是否存活

        #     try:
        #         # 从工作队列中获取任务并处理
        #         task = self.job_queue.get(timeout=1)  # 设置超时以避免阻塞
        #         if task['type'] == 'fillmatrix':
        #             self.perform_fillmatrix(task)
        #         elif task['type'] == 'traceback':
        #             self.perform_traceback(task)
        #     except queue.Empty:
        #         # 如果队列为空，则继续循环
        #         continue
        pass


    #从master接收数据
    def recv(self, addr, data):
        # 从Master接收数据
        '''
        从master收到的数据类型有 3 种:

        1. 让slave执行fillmatrix任务
            start_ind
            end_ind
            subvec
        2. 让slave执行traceback任务
            top_k_i: value
            x,
            y,
            start_ind
            end_ind
        3. heartbeat
        '''

        # data = self.slave_socket.recv(1024)
        if data:
            data = data.decode()
          
            if data == 'Heartbeat':
                # 处理心跳包
                self.last_heartbeat_time = time.time()  # 更新最后一次心跳时间
                self.send_heartbeat_response()
            elif data['type'] == 'fillmatrix':
                # 添加到工作队列
                self.job_queue.put(data)
            elif data['type'] == 'traceback':
                # 添加到 工作队列
                self.job_queue.put(data)
