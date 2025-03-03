import asyncio
from typing import Literal
import logging

from save_to_db import SaveToDB
from task_queue import TaskQueue, QueueItem
from api import API

logger = logging.getLogger(__name__)


class AsyncQueue:
    def __init__(self, **tq_kwargs):
        self.visited = set()
        self.db = SaveToDB()
        self.db.create_tables()
        self.task_queue = TaskQueue(**tq_kwargs)
        self.api = API()

    async def get_user(self, *, user_id):
        try:
            res = await self.api.get_user(user_id=user_id)
            self.visited.add(res['id'])
            await self.db.save_user(data=res)
            if submissions := res.get('submitted'):
                [self.task_queue.add(item=QueueItem(self.get_item, item_id=item)) for item in submissions]
        except Exception as err:
            print(err)

    async def get_item(self, *, item_id):
        try:
            if item_id in self.visited:
                return
            res = await self.api.get_item(item_id=item_id)
            self.visited.add(res['id'])
            await self.db.save_data(data=res)

            if (by := res.get('by')) and by not in self.visited:
                await self.get_user(user_id=by)

            if (parent := res.get('parent')) and parent not in self.visited:
                self.task_queue.add(item=QueueItem(self.get_item, item_id=parent))

            if kids := res.get('kids'):
                [self.task_queue.add(item=QueueItem(self.get_item, item_id=item)) for item in kids if
                 item not in self.visited]

        except Exception as err:
            print(err)

    async def traverse_api(self, timeout: int = None):
        try:
            s, j, t, a, b, n = await asyncio.gather(self.api.show_stories(), self.api.job_stories(), self.api.top_stories(),
                                              self.api.ask_stories(), self.api.best_stories(), self.api.new_stories())
            stories = set(s) | set(j) | set(t) | set(a) | set(b) | set(n)
            print(f"Traversing {len(stories)} stories")
            [self.task_queue.add(item=QueueItem(self.get_item, item_id=item)) for item in stories]
            await self.task_queue.run(queue_timeout=timeout)
            print(f"Made {len(self.visited)} API calls.")
            await self.db.show()
        except Exception as err:
            print(err, 'Error in traverse_api')

    async def walk_back(self, *, amount: int = 1000, timeout: int = 0):
        largest = await self.api.max_item()
        print(f"Walking back from item {largest} to {largest - amount}")

        for item in range(largest, largest - amount, -1):
            self.task_queue.add(item=QueueItem(self.get_item, item_id=item)) if item not in self.visited else ...

        await self.task_queue.run(queue_timeout=timeout)
        print(f"Made {len(self.visited)} API calls.")
        await self.db.show()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    async def main(mode: Literal['traverse', 'walk_back'] = 'traverse'):
        async_queue = AsyncQueue(workers=2000, mode='infinite', absolute_timeout=600)

        match mode:
            case 'traverse':
                await async_queue.traverse_api()

            case 'walk_back':
                await async_queue.walk_back()

            case _:
                print('Invalid mode but running traverse')
                await async_queue.traverse_api()

    asyncio.run(main())
