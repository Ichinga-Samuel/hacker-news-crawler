import asyncio
import time


async def tass():
    while True:
        print('running task')
        await asyncio.sleep(1000)
        time.sleep(50)
        print('done')

async def worker(queue):
    while True:
        task = await queue.get()
        await task()
        queue.task_done()

async def main():
    queue = asyncio.Queue()
    for i in range(10000):
        await queue.put(tass)
    print('joining')
    tasks = [asyncio.create_task(worker(queue)) for _ in range(10)]
    qt = asyncio.Task(queue.join())
    # qt = asyncio.create_task(queue.join())
    # asyncio.create_task(stop(qt, queue))
    st = asyncio.get_event_loop().time()
    try:
        await asyncio.wait_for(qt, timeout=20)
        ...
    except asyncio.CancelledError:
        print('end queue joining')

    except asyncio.TimeoutError:
        end = asyncio.get_event_loop().time()
        print('timeout', end - st)



async def stop(queue, q):
    await asyncio.sleep(15)
    print('stopping queue', q.qsize())
    queue.cancel()
    # signal.raise_signal(signal.SIGINT)

async def stop_queue(queue):
    queue.cancel()

asyncio.run(main())
