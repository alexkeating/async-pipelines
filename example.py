import random


async def randsleep(a: int = 1, b: int = 5, caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


class ExampleProducer:
    async def produce(self, q):
        for x in range(1, 3):
            await randsleep(caller=f"Producer")
            print(f"Producer {x}")
            await q.put(x)


class ExampleProducerConsumer:
    async def consume(self, q, q2):
        while True:
            num = await q.get()
            print(f"Consumer/Producer {num}")
            num += 3
            await q2.put(num)
            q.task_done()


class FullConsumer:
    async def consume(self, q):
        while True:
            num = await q.get()
            print(f"Final consumer {num}")
            q.task_done()


# Queue Must live outside of worker


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
    consumers2 = [asyncio.create_task(consumer.consume(q2)) for n in range(10)]
    await asyncio.gather(*producers)
    await q.join()  # Implicitly awaits consumers, too
    await q2.join()  # Implicitly awaits consumers, too
    await producer_consumer.q.join()  # Implicitly awaits consumers, too
    for c in consumers1:
        c.cancel()
    for c in consumers2:
        c.cancel()


asyncio.run(main())
