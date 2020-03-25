import pytest

from pipelines.sqs import SQSSubscriber


@pytest.mark.asyncio
async def test_build_batches(sqs_client, queue_name, create_sqs_messages):
    response = await sqs_client.create_queue(QueueName=queue_name)
    print(response)
    messages = await create_sqs_messages(queue_url=response["QueueUrl"], num=1)
    assert False
