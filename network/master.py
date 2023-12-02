from .base import StrategyBase


class Master(StrategyBase):
    def __init__(self, client):
        super().__init__(client)

    def iter(self):
        # TODO
        pass

    def recv(self, addr, data):
        # TODO
        pass