from asyncio import Queue
from .base import StrategyBase
from .client import Client


class Slave(StrategyBase):
    def __init__(self, client:Client):
        super().__init__(client)
        self.job_queue = Queue.queue()
        self.client = client
        master = Client.addr_list[0]

    def iter(self):
        # TODO
        #call alg
        #send to M
        pass

    def recv(self, addr, data):
        # TODO
        # if hearbeat

        #elif type == fillmatrix
        # job_queue.put()
        #elif type == traceback
        # job_queue.put()
        pass