import aiobotocore
import json
import pytest
import random
import uuid

from faker import Faker
from marshmallow import fields, Schema

from pipelines.validators import MarshmallowValidator

fake = Faker()


class MockHandler:
    pass


class MockSchema:
    name = fields.Str()
    uuid = fields.UUID()


class MockValidator(MarshmallowValidator):
    schema = MockSchema


faker_funcs = {
    fields.Str: fake.pystr(),
    fields.UUID: str(uuid.UUID(int=random.getrandbits(128), version=4)),
}


def add_faker_data(schema: Schema):
    kwargs = {}
    for k, v in vars(schema).items():
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
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
            messages.append(body)
        return messages

    return _create_sqs_messages
