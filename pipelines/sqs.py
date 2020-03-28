import aiobotocore
import logging

from ._base import Job, Queue, MultiQueue, _retry
from .exceptions import SQSJobException
from .handlers import BaseHandler
from .validators import BaseValidator

from marshmallow import Schema, ValidationError
from typing import Dict, Optional, List, Tuple, Type

logger = logging.getLogger(__name__)


class SQSSubscriber(Job):
    """
    A job meant to be used when the root node of a pipeline gets
    data from an SQS queue. This job will batch sqs messages together
    and pass them through the entire pipeline.

    **Parameters**

    * **queue_url** - url of target sqs queue
    * **batch_size** - size of batch to be sent to next job
    * **num_batches** - number of batches to pull from SQS before processing a batch
    * **handler** - a handler class that modifies the message to be passed to the next process
    * **validator** - A validation class that verfies the sqs message is as expected
    * **num_retries** - Number retries for sqs calls
    """

    queue_url = ""
    batch_size: int = 10
    num_batches: int = 10
    handler: Optional[Type[BaseHandler]] = None
    validator: Optional[Type[BaseValidator]] = None
    num_retries: int = 3

    def __init__(self, region: str, client=None) -> None:
        if not self.handler:
            raise ValueError("All SQSJob classes must have a handler class!")
        self.region = region
        self.client = client or self._get_client()

    async def _get_client(self):
        session = aiobotocore.get_session()
        client = session.create_client("sqs", region_name=self.region)
        return client

    async def _receive_message(self):
        return await _retry(
            self.client.receive_message,
            {"QueueUrl": self.queue_url},
            attempts=self.num_retries,
        )

    async def _delete_message(self, receipt_handle: str):
        return await _retry(
            self.client.delete_message,
            {"QueueUrl": self.queue_url, "ReceiptHandle": receipt_handle},
            attempts=self.num_retries,
        )

    async def build_batches(self):
        """
        Creates batches of SQS messages

        The number of batches will match the num_batches set at the class 
        level if there is a equivelent or greater number of messages in
        the SQS.
        """
        batches = []
        response = await self._receive_message()
        messages = response.get("Messages", [])
        rem_messages = []
        while len(messages) > 0:
            if len(messages) >= self.batch_size:
                batches += [messages[: self.batch_size]]
                del messages[: self.batch_size]
            if len(batches) >= self.num_batches:
                break
            response = await self._receive_message()
            rem_messages = response.get("Messages", [])
            if len(rem_messages) == 0:
                break
            messages += rem_messages
        if len(messages) > 0:
            batches += [messages]
            messages = []
        return batches

    async def _message_validation(
        self, batch: List[str]
    ) -> Tuple[List[Schema], List[Dict[str, str]]]:
        final_batch = []
        raw_batch = []
        for msg in batch:
            try:
                result = self.validator.validate(msg["Body"])
                final_batch.append(result)
                raw_batch.append(msg)
            except ValidationError as e:
                logger.warn(
                    f"Message from SQS is invalid deleting message. err: {e.messages}. valid_data: {e.valid_data}"
                )
                await self._delete_message(receipt_handle=msg["ReceiptHandle"])
        return final_batch, raw_batch

    async def start(self, in_q: Optional[Queue], out_q: Optional[MultiQueue]):
        if in_q is not None:
            raise SQSJobException(
                "SQSReceiver cannot be a middle node or an end node. Please use the SQSPublisher class or make the job the root node"
            )
        batches = await self.build_batches()
        while batches:
            for batch in batches:
                if self.validator:
                    validated_batch, raw_batch = await self._message_validation(batch)
                processed_batch = await self.handler.handle(validated_batch, raw_batch)
                for msg in raw_batch:
                    await self._delete_message(receipt_handle=msg["ReceiptHandle"])
                await out_q.put(processed_batch)
            batches = await self.build_batches()


class SQSPublisher:
    pass
