# Use async await producer consumer pattern for queues
# Probably throw ina message processor
# 1. Batching
# 2. Lets go with class based approach

import asyncio


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


async def randsleep(a: int = 1, b: int = 5, caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


class ExampleProducer:
    async def produce(self, q):
        for x in range(1, 2):
            await randsleep(caller=f"Producer")
            print(f"Producer {x}")
            await q.put(x)


class ExampleProducerConsumer:
    q = asyncio.Queue()

    async def consume(self, q, q2):
        while True:
            num = await q.get()
            print(f"Consumer/Producer {num}")
            num += 3
            # await q2.put(num)
            q.task_done()


class FullConsumer:
    async def consume(self, q):
        while True:
            num = await q.get()
            print(f"Final consumer {num}")


async def main():
    q = asyncio.Queue()
    q2 = asyncio.Queue()
    producer = ExampleProducer()
    producer_consumer = ExampleProducerConsumer()
    consumer = FullConsumer()

    producers = [asyncio.create_task(producer.produce(q)) for n in range(10)]
    consumers1 = [
        asyncio.create_task(producer_consumer.consume(q, q2)) for n in range(10)
    ]
    # consumers2 = [
    #     asyncio.create_task(consumer.consume(producer_consumer.q)) for n in range(10)
    # ]
    await asyncio.gather(*producers)
    await q.join()  # Implicitly awaits consumers, too
    # await producer_consumer.q.join()  # Implicitly awaits consumers, too
    for c in consumers1:
        c.cancel()
    # for c in consumers2:
    #     c.cancel()


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
