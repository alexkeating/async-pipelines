import asyncio
import pytest

from marshmallow import fields, Schema
from plugins.sqs import M, MockHandler, MockSchema, MockValidator
from pipelines._base import MultiQueue
from pipelines.exceptions import SQSJobException
from pipelines.sqs import SQSSubscriber


class MockSQSSubscriber(SQSSubscriber):
    batch_size = 10
    handler = MockHandler
    validator = MockValidator


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_msg,num_batches", [(20, 2), (22, 3)],
)
async def test_build_batches(
    sqs_client, queue_name, create_sqs_messages, num_msg, num_batches,
):
    response = await sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]
    messages = await create_sqs_messages(queue_url=queue_url, num=num_msg)
    MockSQSSubscriber.queue_url = queue_url
    batches = await MockSQSSubscriber("us-east-1", sqs_client).build_batches()
    assert len(batches) == num_batches
    assert sum([len(batch) for batch in batches]) == num_msg


@pytest.mark.asyncio
async def test_message_validation_succeeed(
    sqs_client, queue_name, create_sqs_messages,
):
    response = await sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]
    messages = await create_sqs_messages(queue_url=queue_url, num=10)
    MockSQSSubscriber.queue_url = queue_url
    batches = await MockSQSSubscriber("us-east-1", sqs_client).build_batches()
    final_batch, raw_batch = await MockSQSSubscriber(
        "us-east-1", sqs_client
    )._message_validation(batches[0])
    assert len(final_batch) == 10
    assert all([isinstance(msg, M) for msg in final_batch])
    assert all([isinstance(msg, dict) for msg in raw_batch])


@pytest.mark.asyncio
async def test_message_validation_failure(
    sqs_client, queue_name, create_sqs_messages,
):
    class FakeSchema(Schema):
        lulu = fields.Str

    response = await sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]
    messages = await create_sqs_messages(queue_url=queue_url, num=10, schema=FakeSchema)
    MockSQSSubscriber.queue_url = queue_url
    batches = await MockSQSSubscriber("us-east-1", sqs_client).build_batches()
    final_batch, raw_batch = await MockSQSSubscriber(
        "us-east-1", sqs_client
    )._message_validation(batches[0])
    assert len(final_batch) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_msg,size", [(10, 1), (22, 3)],
)
async def test_sqs_start(sqs_client, queue_name, create_sqs_messages, num_msg, size):
    response = await sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]
    messages = await create_sqs_messages(queue_url=queue_url, num=num_msg)
    MockSQSSubscriber.queue_url = queue_url

    multi_queue = MultiQueue(queues=[asyncio.Queue()])
    batches = await MockSQSSubscriber("us-east-1", sqs_client).start(
        in_q=None, out_q=multi_queue
    )

    attributes = await sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    assert attributes["Attributes"]["ApproximateNumberOfMessages"] == "0"
    assert multi_queue.queues[0].qsize() == size


@pytest.mark.asyncio
async def test_sqs_start_not_root(sqs_client, queue_name, create_sqs_messages):
    multi_queue = MultiQueue(queues=[asyncio.Queue()])
    with pytest.raises(SQSJobException):
        batches = await MockSQSSubscriber("us-east-1", sqs_client).start(
            in_q=multi_queue, out_q=multi_queue
        )
