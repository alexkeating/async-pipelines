import abc
from typing import List, Optional


try:
    import marshmellow
except ImportError:  # pragma: nocover
    marshmellow = None  # type: ignore


class BaseValidator(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def validate(cls):
        pass


class MarshmellowValidator(BaseValidator):
    schema: Optional["marshmellow.Schema"] = None

    @classmethod
    def validate(data: str) -> "marshmellow.Schema":
        assert (
            marshmellow is not None
        ), "marchmellow must be installed to use MarshmellowValidator"
        return schema.load(msg)
