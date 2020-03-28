import abc


class BaseHandler(abc.ABC):
    @classmethod
    @abc.abstractmethod
    async def handle(cls):
        pass


class SQSReceiverHandler(BaseHandler):
    @classmethod
    async def handle(cls, validated_batch, raw_batch):
        pass


class SQSPublisherHandler(BaseHandler):
    @classmethod
    async def handle(cls, data):
        pass
