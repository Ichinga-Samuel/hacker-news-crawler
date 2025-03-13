import asyncio
import random
async def task(tid):
    await asyncio.sleep(random.randint(10, 30))
    print('running task', 'task', tid)

async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(task(2))
        tg.create_task(task(3))
    tg.create_task(task(4))

asyncio.run(main())
