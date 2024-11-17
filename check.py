import asyncio

async def tass():
    await asyncio.sleep(1)
    print('done')

async def worker(queue):
    while True:
        task = await queue.get()
        await task()
        queue.task_done()

async def main():
    queue = asyncio.Queue()
    # worker_task = asyncio.create_task(worker(queue))

    for i in range(10):
        await queue.put(tass)
    await queue.join()
    # worker_task.cancel()


asyncio.run(main())
