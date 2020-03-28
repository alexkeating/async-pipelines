import aiobotocore
import json
import pytest
import random
import uuid

from dataclasses import dataclass
from faker import Faker
from marshmallow import fields, post_load, Schema, ValidationError

from pipelines.handlers import BaseHandler
from pipelines.validators import MarshmallowValidator

fake = Faker()


class MockHandler(BaseHandler):
    pass


@dataclass
class M:
    name: str
    uuid: str


class MockSchema(Schema):
    name = fields.Str()
    uuid = fields.Str()

    @post_load
    def make_user(self, data, **kwargs):
        try:
            return M(**data)
        except TypeError as e:
            raise ValidationError("Could not create M") from e


class MockValidator(MarshmallowValidator):
    schema = MockSchema


faker_funcs = {
    fields.Str: fake.pystr(),
    fields.UUID: str(uuid.UUID(int=random.getrandbits(128), version=4)),
}


def add_faker_data(schema: Schema):
    kwargs = {}
    for k, v in schema._declared_fields.items():
        if isinstance(v, fields.Field):
            try:
                kwargs[k] = faker_funcs[type(v)]
            except KeyError as e:
                raise KeyError(f"Please add key {k} to add_faker_data") from e
    return kwargs


@pytest.fixture
def queue_name():
    return "test_queue"


@pytest.fixture
def create_sqs_messages(sqs_client, queue_name):
    async def _create_sqs_messages(
        queue_url, num: int = 10, schema: Schema = MockSchema
    ):
        messages = []
        for _ in range(0, num):
            body = add_faker_data(schema)
            await sqs_client.send_message(
                QueueUrl=queue_url, MessageBody=json.dumps(body)
            )
            messages.append(body)
        return messages

    return _create_sqs_messages
