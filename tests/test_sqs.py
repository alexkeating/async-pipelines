import pytest

from pipelines.sqs import SQSReceive


@pytest.mark.asyncio
async def test_build_batches(sqs_client):
    response = await sqs_client.create_queue(QueueName="test_queue")
    print(response)
    assert False
