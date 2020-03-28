import abc
import json

from typing import List, Optional


try:
    import marshmallow
except ImportError:  # pragma: nocover
    marshmallow = None  # type: ignore


class BaseValidator(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def validate(cls):
        pass


class MarshmallowValidator(BaseValidator):
    schema: Optional["marshmellow.Schema"] = None

    @classmethod
    def validate(cls, data: str) -> "marshmallow.Schema":
        schema = cls.schema()
        assert (
            marshmallow is not None
        ), "marshmallow must be installed to use MarshmallowValidator"
        return schema.loads(data)
