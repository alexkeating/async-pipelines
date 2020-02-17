# Introduction

Async-pipelines is a lightweight framework that allows a developer to
apply a consumer producer pattern to their code. Some of the main benefits
of this pattern is indepedent scaling of differeny IO actions and easy
ability to break out indivdual producers and consumers into seperate jobs.

It is currently in development and gives the user the following:

* SNS support
* SQS support
* Postgres Support


## Requirements 3.7+

```shell
pip install async-pipelines
```


## Example

**example.py**

```python
import random
from typing import Optional
from pipelines import Job, Queue, MultiQueue

async def randsleep(a: int = 1, b: int = 5, caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


class ExampleProducer(Job):
    async def start(self, in_q: Optional[Queue], out_q: Optional[MultiQueue]) -> None:
        for x in range(1, 10):
            await randsleep(caller=f"Producer")
            print(f"Producer {x}")
            await out_q.put(x)


class ExampleProducerConsumer(Job):
    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        while True:
            num = await in_q.get()
            print(f"Consumer/Producer {num}")
            await out_q.put(num)
            in_q.task_done()


class FullConsumer(Job):
    workers = 10

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    async def start(
        self, in_q: Optional[asyncio.Queue], out_q: Optional[MultiQueue]
    ) -> None:
        while True:
            num = await in_q.get()
            print(f"{self.name} {num}")
            in_q.task_done()


async def main() -> None:
    # Different producers and consumers
    root = ExampleProducer()
    producer_consumer = ExampleProducerConsumer()
    consumer = FullConsumer(name="Full Consumer")
    consumer2 = FullConsumer(name="Partial Person Ser")
    consumer3 = FullConsumer(name="Second One to consume")

    # Subscribing a consumer to a producer
    root.set_downstream(producer_consumer)
    root.set_downstream(consumer2)
    producer_consumer.set_downstream(consumer)
    producer_consumer.set_downstream(consumer3)

    await root.execute_jobs()


asyncio.run(main())
```

## Modularity

Async-pipelines is meant to be modular, composable and extensible. Each
job within a pipeline should inherit from the jobs base class.










