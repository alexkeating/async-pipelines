import asyncio
import pytest

from hypothesis import given
from hypothesis.strategies import (
    builds,
    composite,
    data,
    integers,
    lists,
    none,
    one_of,
)

from pipelines import Job, MultiQueue, Pipe
from typing import Optional

# Test build pipeline list


# Test pipeline list
# Property would be a strategy that makes a class
# tree and the output of that should
# Pipe is number of jobs - 1
# Assumption no orphan nodes
#


class MockJob(Job):
    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        print("Running")


@composite
def job_tree(draw):
    parent = draw(builds(MockJob))
    root = parent
    depth = draw(integers(min_value=1, max_value=10))
    for i in range(depth):
        children = draw(lists(builds(MockJob)))
        for c in children:
            parent.set_downstream(c)
        if children:
            parent = children[0]
    return root


def _number_of_pipes(job):
    """
    This should be number of jobs plus
    all end nodes.
    """
    pipe_count = 0
    children = job.children

    while children:
        pipe_count += 1
        child = children.pop()
        if child.children == []:
            pipe_count += 1
        children.extend(child.children)
    if pipe_count == 0:
        return 1
    return pipe_count


@pytest.mark.asyncio
@given(job_tree())
async def test_build_test_pipeline(s):
    pipe_list = s._build_ordered_pipe_list(s, None, [])
    assert len(pipe_list) == _number_of_pipes(s)


@composite
def pipe(draw):
    job = draw(builds(MockJob))
    queue = draw(one_of(builds(asyncio.Queue), none()))
    multi_queue = draw(builds(MultiQueue))
    return Pipe(parent=job, queue=queue, subscribed_queues=multi_queue)


def _num_tasks(pipes):
    task_count = 0
    for i, p in enumerate(pipes):
        if i == 0:
            task_count += 1
            continue
        if not p.queue:
            continue
        task_count += 1
    return task_count


def _num_queues(pipes):
    queue_count = 0
    for i, p in enumerate(pipes):
        if i == 0:
            continue
        if p.queue:
            queue_count += 1
            for q in p.subscribed_queues.queues:
                queue_count += 1

    return queue_count


# This should test queue length and task length
# Which should match worker length
@pytest.mark.asyncio
@given(lists(pipe(), min_size=1), integers(min_value=0), data())
async def test__build_tasks(pipes, workers, data):
    job = data.draw(builds(MockJob))
    job.workers = workers
    tasks, queues = job._build_tasks(pipes)
    assert len(tasks) == _num_tasks(pipes)
    assert len(queues) == _num_queues(pipes)


@pytest.mark.asyncio
async def test_single_job():
    j = MockJob()
    await j.execute_jobs()


# Test build tasks
