import asyncio
from typing import Literal
import logging

from dict_db import DictDB
from task_queue import TaskQueue, QueueItem
from api import API

logger = logging.getLogger(__name__)


class AsyncQueue:
    def __init__(self, **tq_kwargs):
        self.visited = set()
        self.db = DictDB()
        self.task_queue = TaskQueue(**tq_kwargs)
        self.api = API()

    async def get_user(self, *, user_id):
        try:
            res = await self.api.get_user(user_id=user_id)
            self.visited.add(res['id'])
            self.task_queue.add(item=QueueItem(self.db.save_user, data=res), must_complete=True, priority=3)
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
            self.task_queue.add(item=QueueItem(self.db.save, data=res), must_complete=True, priority=3)

            if (by := res.get('by')) and by not in self.visited:
                self.task_queue.add(item=QueueItem(self.get_user, user_id=by), priority=1)

            if (parent := res.get('parent')) and parent not in self.visited:
                self.task_queue.add(item=QueueItem(self.get_item, item_id=parent), priority=2)

            if kids := res.get('kids'):
                [self.task_queue.add(item=QueueItem(self.get_item, item_id=item), priority=2) for item in kids if item not in self.visited]

        except Exception as err:
            print(err)

    async def traverse_api(self, timeout: int = None):
        try:
            s, j, t, a, b, n = await asyncio.gather(self.api.show_stories(), self.api.job_stories(), self.api.top_stories(),
                                              self.api.ask_stories(), self.api.best_stories(), self.api.new_stories())
            stories = set(s) | set(j) | set(t) | set(a) | set(b) | set(n)
            logger.info("Traversing %s stories", len(stories))
            [self.task_queue.add(item=QueueItem(self.get_item, item_id=item), priority=0) for item in stories]
            await self.task_queue.run(queue_timeout=timeout)
            print(f"Made {len(self.visited)} API calls.")
            print(self.db)
        except Exception as err:
            print(err, 'Error in traverse_api')

    async def walk_back(self, *, amount: int = 1000, timeout: int = 0):
        largest = await self.api.max_item()
        print(f"Walking back from item {largest} to {largest - amount}")

        for item in range(largest, largest - amount, -1):
            self.task_queue.add(item=QueueItem(self.get_item, item_id=item), priority=1) if item not in self.visited else ...

        await self.task_queue.run(queue_timeout=timeout)
        print(f"Made {len(self.visited)} API calls.")
        print(self.db)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    async def main(mode: Literal['traverse', 'walk_back'] = 'traverse'):
        async_queue = AsyncQueue(queue_timeout=600, workers=100, absolute_timeout=100, mode='infinite')
        match mode:
            case 'traverse':
                await async_queue.traverse_api()

            case 'walk_back':
                await async_queue.walk_back()

            case _:
                print('Invalid mode but running traverse')
                await async_queue.traverse_api()

    asyncio.run(main())
