import queue
from .base import StrategyBase
import socket
# from alg.alg import fill_matrix, traceback
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
        # ...
        pass

    def connect_to_new_master(self):
        # 连接新的 Master
        # ...
        pass

    def handle_remake_command(self):
        # 处理收到的 remake 命令
        # ...
        pass

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
                self.send_heartbeat_response()
            elif data['type'] == 'fillmatrix':
                # 处理 fillmatrix 任务
                self.job_queue.put()
            elif data['type'] == 'traceback':
                # 处理 traceback 任务
                self.job_queue.put()
