# Use async await producer consumer pattern for queues
# Probably throw ina message processor
# 1. Batching
# 2. Lets go with class based approach

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


async def randsleep(a: int = 1, b: int = 5, caller: Optional[str] = None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


"""
The issue is there needs to be a seperate queue
per child job. Meaning I have to abstract away
multiple queues to act like a single queue
SNS over SQS.

It will only get from queue, but will put to all child queues
- inherit queue and override put.
- As well as store child queue
"""
_T = TypeVar("_T")


class MultiQueue:
    def __init__(self, queues: List[asyncio.Queue] = None):
        self.queues = queues or []

    async def put(self, item: _T) -> None:
        for q in self.queues:
            await q.put(item)

    def add_queue(self, q: asyncio.Queue) -> None:
        self.queues.append(q)


Queue = Union[asyncio.Queue, MultiQueue]
T = TypeVar("T", bound="Job")


@dataclass
class Pipe:
    parent: T
    child: Optional[T]
    queue: Optional[asyncio.Queue]
    subscribed_queues: MultiQueue


class Job:
    queue = asyncio.Queue
    workers = 5

    def __init__(self) -> None:
        self.children: List[Job] = []

    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        pass

    def set_downstream(self, job: T) -> None:
        self.children.append(job)

    async def execute_jobs(self) -> None:
        # Iterate through and create a list of
        # pipe objects
        #    - pipe object has parent, child, and reference to
        #    next pipe.
        # Queue enum? So, people can override default queue?
        pipe_list = self._build_ordered_pipe_list(self, None, [])
        tasks, queues = self._build_tasks(pipe_list)
        root_producer = tasks.popleft()
        await asyncio.gather(*root_producer)
        # Block queue until all items have been processed
        for q in queues:
            await q.join()
        # Cancels all tasks because we are done processing
        for task_list in tasks:
            self._cancel_tasks(task_list)

    def _cancel_tasks(self, task_list: List[asyncio.Task]) -> None:
        for t in task_list:
            t.cancel()

    def _build_tasks(
        self, pipe_list: List[Pipe]
    ) -> Tuple[Deque[List[asyncio.Task]], Set[asyncio.Queue]]:
        queues = set()
        tasks: Deque[List[asyncio.Task]] = collections.deque()
        for i, pipe in enumerate(pipe_list):
            # Can be implemented easily
            parent = pipe.parent
            parent_workers = parent.workers
            q = pipe.queue
            if q is None and i != 0:
                continue

            # child = pipe.child
            # child_workers = child.workers
            out_q = pipe.subscribed_queues
            other_q = out_q.queues
            if i == 0:
                prod_tasks = [
                    asyncio.create_task(parent.start(None, out_q))
                    for _ in range(parent_workers)
                ]
                tasks.append(prod_tasks)
                continue
            prod_tasks = [
                asyncio.create_task(parent.start(q, out_q))
                for _ in range(parent_workers)
            ]
            tasks.append(prod_tasks)

            # con_tasks = [
            #     asyncio.create_task(child.start(q, out_q)) for _ in range(child_workers)
            # ]
            # tasks.append(con_tasks)
            if q is not None:
                queues.add(q)
                for x in other_q:
                    queues.add(x)
        return tasks, queues

    def _build_ordered_pipe_list(
        self, parent: T, parent_queue: Optional[asyncio.Queue], pipe_list: List[Pipe]
    ) -> List[Pipe]:
        children = parent.children
        if not children:
            if parent_queue is None:
                raise ValueError(f"parent_queue cannot be None in final pipe: {parent}")
            pipe_list.append(
                Pipe(
                    parent=parent,
                    child=None,
                    queue=parent_queue,
                    subscribed_queues=MultiQueue(),
                )
            )
            return pipe_list
        multi_queue = MultiQueue()
        for child in children:
            child_queue: asyncio.Queue = child.queue()
            multi_queue.add_queue(child_queue)
            pipe = Pipe(
                parent=parent,
                child=child,
                queue=parent_queue,
                subscribed_queues=multi_queue,
            )
            pipe_list.append(pipe)
            pipe_list = self._build_ordered_pipe_list(child, child_queue, pipe_list)
        return pipe_list


class ExampleProducer(Job):
    workers = 5

    async def start(self, in_q: Optional[Queue], out_q: Optional[MultiQueue]) -> None:
        assert out_q is not None
        for x in range(1, 3):
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
