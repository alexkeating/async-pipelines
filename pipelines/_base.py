from __future__ import annotations

import asyncio
import collections
import abc

from dataclasses import dataclass
from typing import Deque, List, Optional, Set, Tuple, TypeVar, Union

_T = TypeVar("_T")


class MultiQueue:
    """Multiple Queue class

    A class that allows a user to encapsulate multiple 
    queues and push messages to those suscribed queues.

    Attributes:
      queues: subscribed queues
    """

    def __init__(self, queues: List[asyncio.Queue] = None):
        self.queues = queues or []

    async def put(self, item: _T) -> None:
        """Pushes message to queues
          
        Args:
          item: A piece of data to send to all subscribed queues
        """
        for q in self.queues:
            await q.put(item)

    def add_queue(self, q: asyncio.Queue) -> None:
        """Add queue to subscribed queues
        
        Args:
          q: A asyncio queue
        """
        self.queues.append(q)


Queue = Union[asyncio.Queue, MultiQueue]


@dataclass
class Pipe:
    """Metadata to connect Jobs

    A class that holds the data for what queues
    a job should read from and what queues a job
    should write to.

    Attributes:
      parent: Job to be hooked into pipeline
      queue: Queue Job is going to get messages from
      subscribed_queues: Queue messages will be sent too
    """

    parent: Job
    queue: Optional[asyncio.Queue]
    subscribed_queues: MultiQueue


class Job(abc.ABC):
    """
    """

    queue = asyncio.Queue
    workers = 5

    def __init__(self) -> None:
        self.children: List[Job] = []

    @abc.abstractmethod
    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        pass

    def set_downstream(self, job: Job) -> None:
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
        cls, parent: Job, parent_queue: Optional[asyncio.Queue], pipe_list: List[Pipe]
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
