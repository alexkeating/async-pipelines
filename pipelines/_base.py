from __future__ import annotations

import abc
import collections
from dataclasses import dataclass
from typing import Deque, List, Optional, Set, Tuple, TypeVar, Union

import asyncio

_T = TypeVar("_T")


class MultiQueue:
    """
    A class that allows a user to encapsulate multiple
    queues and push messages to those suscribed queues.

    **Parameters**:

    * **queues**- subscribed queues
    """

    def __init__(self, queues: List[asyncio.Queue] = None):
        self.queues = queues or []

    async def put(self, item: _T) -> None:
        """
        Pushes message to queues

        **Parameters**:

        * **item** - A piece of data to send to all subscribed queues
        """
        for q in self.queues:
            await q.put(item)

    def add_queue(self, q: asyncio.Queue) -> None:
        """
        Add queue to subscribed queues

        **Parameters**:

        * **q**: A asyncio queue
        """
        self.queues.append(q)


Queue = Union[asyncio.Queue, MultiQueue]


@dataclass
class Pipe:
    """
    A class that holds the data for what queues
    a job should read from and what queues a job
    should write to.

    **Parameters**:

    * **parent** - Job to be hooked into pipeline
    * **queue** - Queue Job is going to get messages from
    * **subscribed_queues** - Queue messages will be sent too
    """

    parent: Job
    queue: Optional[asyncio.Queue]
    subscribed_queues: MultiQueue


class Job(abc.ABC):
    """
    A base class for all jobs in a pipeline. It requires
    the start method to be implmeneted and has helper utilties
    to add children jobs and execute a pipeline.

    **Parameters**

    * **queue**: Queue used to send messages to another job
    * **workers**: Number of Tasks created for this job
    * **children**: Jobs which receive messages from this Job
    """

    queue = asyncio.Queue
    workers = 5

    def __init__(self) -> None:
        self.children: List[Job] = []

    @abc.abstractmethod
    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        """
        Method executed when job is run. Typically this will not
        need to be called directly, and will instead be called within
        execute_jobs. This method should mark every task from in_q
        as done.

        **Parameters**

        * **in_q** - The queue to receive messages from to process
        * **out_q** - The queue to send messages to be processed
        """

    def set_downstream(self, job: Job) -> None:
        """
        Add child job

        **Parameters**

        * **job** - Job to added as a child
        """
        self.children.append(job)

    async def execute_jobs(self) -> None:
        """
        Method used to execute a pipeline. This should be
        called on the root node of the pipeline.
        """
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
        """
        Cancel all remaining tasks

        **Parameters**:

        * **task_list** - A list of tasks to be cancelled
        """
        for t in task_list:
            t.cancel()

    @staticmethod
    def _build_tasks(
        pipe_list: List[Pipe],
    ) -> Tuple[Deque[List[asyncio.Task]], Set[asyncio.Queue]]:
        """
        Build all tasks for jobs needed to run the pipeline. This
        function will have all the queues needed to be awaited and
        tasks to be cancelled once the queues have processed all of
        their messages.

        **Parameters**:

        * **pipe_list** - list of pipes representing the pipeline
        """
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
        """
        Build a list of pipes by traversing the tree of jobs using
        the provided Job as the root node.

        **Parameters**:

        * **parent** - Job the pipe will use
        * **parent_queue** - Queue the parent receives messages from
        * **pipe_list** - Current pipes in the pipeline
        """
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
