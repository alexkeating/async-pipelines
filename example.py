import asyncio
import collections


# user can define a consumer and
# a producer. But the producer can
# be overriden with another job class where it pass
# the queue to the consumer
#
# A job can be either a producer or a consumer
# and can also be both with at most two queues
#
# I think I can get away with one method worker?
# Worker would take args and the job class would take a queue
# or upstream class?
#
#
# I want the ability to easliy write the glue for jobs
# and focus on the query or request etc...
import random
from dataclasses import dataclass
from typing import Callable, Deque, List, Optional, Set, Tuple, TypeVar, Union

from places import Job, Queue, MultiQueue


async def randsleep(a: int = 1, b: int = 5, caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


class ExampleProducer(Job):
    workers = 5

    async def start(self, in_q: Optional[Queue], out_q: Optional[MultiQueue]) -> None:
        assert out_q is not None
        for x in range(1, 10):
            await randsleep(caller=f"Producer")
            print(f"Producer {x}")
            await out_q.put(x)


class ExampleProducerConsumer(Job):
    workers = 5

    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        assert in_q is not None and out_q is not None
        while True:
            num = await in_q.get()
            print(f"Consumer/Producer {num}")
            num += 3
            await out_q.put(num)
            in_q.task_done()


class FullConsumer(Job):
    workers = 5

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        assert in_q is not None
        while True:
            num = await in_q.get()
            print(f"{self.name} {num}")
            in_q.task_done()


async def main() -> None:
    # Needs to represent a tree
    root = ExampleProducer()
    producer_consumer = ExampleProducerConsumer()
    consumer = FullConsumer(name="Full Consumer")
    consumer2 = FullConsumer(name="Partial Person Ser")
    consumer3 = FullConsumer(name="Second One to consume")

    root.set_downstream(producer_consumer)
    root.set_downstream(consumer2)
    producer_consumer.set_downstream(consumer)
    producer_consumer.set_downstream(consumer3)

    await root.execute_jobs()


# async def main():
#     # Queues will also need to be a tree
#     q = asyncio.Queue()
#     q2 = asyncio.Queue()
#     # A list order shows direction
#     # Needs to represent a tree
#     producer = ExampleProducer()
#     producer_consumer = ExampleProducerConsumer()
#     consumer = FullConsumer()

#     producers = [asyncio.create_task(producer.produce(q)) for n in range(10)]
#     consumers1 = [
#         asyncio.create_task(producer_consumer.consume(q, q2)) for n in range(10)
#     ]
#     consumers2 = [asyncio.create_task(consumer.consume(q2)) for n in range(10)]
#     await asyncio.gather(*producers)
#     await q.join()  # Implicitly awaits consumers, too
#     await q2.join()  # Implicitly awaits consumers, too
#     for c in consumers1:
#         c.cancel()
#     for c in consumers2:
#         c.cancel()


asyncio.run(main())

# class Job:
#     # Would need a second queue always possible to push
#     def __init__(upstream: Job):
#         self.queue = asyncio.Queue()
#         if upstream:
#             self.queue = upstream.queue

#     def produce():
#         pass

#     def consume():
#         pass
