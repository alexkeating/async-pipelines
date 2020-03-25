import aiobotocore

from ._base import Job, Queue, MultiQueue
from .exceptions import SQSJobException
from .handlers import BaseHandler
from .validators import BaseValidator

from marshmallow import Schema
from typing import Optional, List, Type

# Create Regular SQS JOB
# This job should handle batching
# it should handle message validation

# All this does it gets messages from SQS
# and passes it on to the next queue
# maybe provide a transformation function?
# There will need to be a send and receive class

# At every point in time we can determine whether the node
# is in the root or ending node. The node cannot be in the or end. Unless we have a recevier and producer class. By knowing this
# we can determine whether to apply to transformation or not?

# Add retry logic

# 1. Basic funcitonality, get batch, apply trnasformation, send to next queue or not. Once 0 messages is it end job.
# 2. Messages should be validatd with validation logic conditionally applied.
# 3. Retry logic should be implmented with the user having the ability to override or add onto the existing exceptions that are caught.
# Build message handler class
class SQSSubscriber(Job):
    queue_url = ""
    batch_size: int = 10
    num_batches: int = 10
    # This would take a handler class and pass int the
    # requiste arguments.
    handler: Optional[Type[BaseHandler]] = None
    validator: Optional[Type[BaseValidator]] = None

    def __init__(self, region: str, client=None) -> None:
        if not self.handler:
            raise ValueError("All SQSJob classes must have a handler class!")
        self.region = region
        self.client = sqs_client or self._get_client()

    async def _get_client():
        session = aiobotocore.get_session()
        client = session.create_client("sqs", region_name=self.region)
        return client

    async def build_batches():
        """This functions should build batches based
        on the provided batch size from the SQS queue.
        If the queue is empty the function should stop pushing
        and stop. Potentially we should allow this job to sleep
        and never end. I don't know how this would work

        Need to test against some semi real data

        Figure out what to do with the rest of the receive args.
        How can a user override the defaults.
        """
        batches = []
        messages = self.client.receive_message(QueueUrl=self.queue_url)
        while len(messages) > 0:
            if len(messages) >= self.batch_size:
                batches += messages[: self.batch_size]
                del messages[: self.batch]
            if batches >= self.num_batches:
                break
            rem_messages = await self.client.receivce_message(QueueUrl=self.queue_url)
            if len(rem_message) == 0:
                break
            messages += rem_messages
        if messages >= 0:
            batches += messages
            messages = []
        return batches

    # Where does the receipt handle come from
    async def _message_validation(batch: List[str]) -> Schema:
        for msg in batch:
            try:
                result = self.validator.validate(msg)
            except ValidationError as e:
                logger.warn(
                    f"Message from SQS is invalid deleting message. err: {err.message}. valid_data: {err.valid_data}"
                )
            # Delete message here

    async def start(self, in_q: Optional[Queue], out_q: Optional[MultiQueue]):
        """A default implementation should be offered here"""
        if in_q is not None:
            raise SQSJobException(
                "SQSReceiver cannot be a middle node or an end node. Please use the SQSPublisher class or make the job the root node"
            )
        batch = await self.build_batches()
        while batch:
            # validate messages
            if self.validator:
                validated_batch = self._message_validation()
            processed_batch = await self.handler.handle(validated_batch)
            await out_q.put(processed_batch)


class SQSPublisher:
    pass


# Create thread safe SQS Job
