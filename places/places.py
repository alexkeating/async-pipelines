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

    @staticmethod
    def _build_tasks(
        pipe_list: List[Pipe],
    ) -> Tuple[Deque[List[asyncio.Task]], Set[asyncio.Queue]]:
        queues = set()
        tasks: Deque[List[asyncio.Task]] = collections.deque()
        for i, pipe in enumerate(pipe_list):
            parent = pipe.parent
            parent_workers = parent.workers
            q = pipe.queue
            if q is None and i != 0:
                continue

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

            if q is not None:
                queues.add(q)
                for x in other_q:
                    queues.add(x)
        return tasks, queues

    @classmethod
    def _build_ordered_pipe_list(
        cls, parent: T, parent_queue: Optional[asyncio.Queue], pipe_list: List[Pipe]
    ) -> List[Pipe]:
        children = parent.children
        if not children:
            pipe_list.append(
                Pipe(parent=parent, queue=parent_queue, subscribed_queues=MultiQueue(),)
            )
            return pipe_list
        multi_queue = MultiQueue()
        for child in children:
            child_queue: asyncio.Queue = child.queue()
            multi_queue.add_queue(child_queue)
            pipe = Pipe(
                parent=parent, queue=parent_queue, subscribed_queues=multi_queue,
            )
            pipe_list.append(pipe)
            pipe_list = cls._build_ordered_pipe_list(child, child_queue, pipe_list)
        return pipe_list
