from .base import ClientBase, StrategyBase
from .slave import Slave
from .master import Master


class Client(ClientBase):

    def __init__(self, addr_list, addr, configs):
        super().__init__(addr)
        self.rank = addr_list.index(addr) # addr_list at index 0 is master
        self.configs = configs
        self.strategy = StrategyBase(self)
        self.state = None
        self.addr_list = addr_list
        self.master_addr = addr_list[0]
        self.K = configs['k']#topK
        
        if self.rank == 0:
            self.set_state('M')
        else:
            self.set_state('S')

    def set_state(self, state):
        if self.state == state:
            return
        if state == 'M':
            self.strategy = Master(self)
        if state == 'S':
            self.strategy = Slave(self)

    def recv(self, addr, data):
        self.strategy.recv(addr, data)