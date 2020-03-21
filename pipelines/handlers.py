import abc


class BaseHandler(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def handle(cls):
        pass
