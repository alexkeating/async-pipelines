import abc


class BaseHandler(abc.ABC):
    @classmethod
    @abc.abstractmethod
    async def handle(cls, batch, raw_batch):
        pass
