import asyncio
from task_queue import QueueItem


async def sleep():
    await asyncio.sleep(0)


async def man():
    q = asyncio.PriorityQueue()
    t1 = QueueItem(sleep)
    print(t1)
    # q.put_nowait((2, t1))
    # await asyncio.sleep(1)
    # t2 = QueueItem(sleep)
    # q.put_nowait((1, t2))
    # _, res = q.get_nowait()
    # print(res, t2, t1)
    # print('joining')
    # _, res = q.get_nowait()
    # print(res, q.qsize())
    # print(q.qsize())
    # _, res = q.get_nowait()
    # await q.join()
    # _, res = q.get_nowait()
    # print(res, t2, t1)


async def ma():
    t = asyncio.get_running_loop().time() + 11
    async with asyncio.timeout_at(None):
        await asyncio.sleep(10)
asyncio.run(man())
