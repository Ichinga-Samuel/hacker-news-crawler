import asyncio
import time
from typing import Literal
from logging import getLogger, basicConfig

from api import API
from save_to_db import SaveToDB

logger = getLogger(__name__)


class AsyncGroup:
    def __init__(self):
        self.api = API()
        self.db = SaveToDB()
        self.visited = set()  # keep track of visited items or users
        self.task_group = asyncio.TaskGroup()

    async def get_user(self, *, user_id):
        try:
            res = await self.api.get_user(user_id=user_id)
            self.visited.add(res["id"])
            await self.db.save_user(data=res)
            if submissions := res.get("submitted"):
                [
                    self.task_group.create_task(self.get_item(item_id=item))
                    for item in submissions
                ]
        except Exception as err:
            logger.error("%s occurred in get_user", err)

    async def get_item(self, *, item_id):
        try:
            if item_id in self.visited:
                return

            res = await self.api.get_item(item_id=item_id)
            await self.db.save_data(data=res)
            self.visited.add(item_id)

            if (by := res.get("by")) and by not in self.visited:
                self.task_group.create_task(self.get_user(user_id=by))

            # saving kids data
            if kids := res.get("kids"):
                [
                    self.task_group.create_task(self.get_item(item_id=item))
                    for item in kids
                ]

            # saving parent data
            if (parent := res.get("parent")) and parent not in self.visited:
                self.task_group.create_task(self.get_item(item_id=parent))

        except Exception as err:
            logger.warning("%s occurred in get_item", err)

    async def walk_back(self, *, amount: int = 1000, timeout: int = 60):
        largest = await self.api.max_item()
        logger.info("Walking back from item %d to %d", largest, largest - amount)
        start = time.perf_counter()
        try:
            async with asyncio.timeout(timeout):
                async with self.task_group:
                    [
                        self.task_group.create_task(self.get_item(item_id=item))
                        for item in range(largest, largest - amount, -1)
                    ]

        except asyncio.CancelledError:
            logger.warning(
                "Tasks cancelled after %d seconds", time.perf_counter() - start
            )

        except asyncio.TimeoutError:
            logger.warning("Timed out after %d seconds", time.perf_counter() - start)

        except Exception as exe:
            logger.warning("Error: %s occurred in walk_back", exe)

        finally:
            logger.info("Tasks completed in %d", time.perf_counter() - start)
            await self.db.show()

    async def traverse_api(self, timeout=10):
        s, j, n, t, a, b = await asyncio.gather(
            self.api.show_stories(),
            self.api.job_stories(),
            self.api.new_stories(),
            self.api.top_stories(),
            self.api.ask_stories(),
            self.api.best_stories(),
        )
        stories = set(s) | set(j) | set(t) | set(a) | set(b) | set(n)
        logger.info("Traversing %d stories", len(stories))
        start = time.perf_counter()
        try:
            async with asyncio.timeout(timeout):
                async with self.task_group:
                    [
                        self.task_group.create_task(self.get_item(item_id=item))
                        for item in stories
                    ]

        except asyncio.TimeoutError:
            logger.warning("Timed out after %d", time.perf_counter() - start)

        except asyncio.CancelledError:
            logger.warning("Tasks cancelled after %d", time.perf_counter() - start)

        finally:
            logger.info("Tasks completed after %d", time.perf_counter() - start)
            await self.db.show()


if __name__ == "__main__":
    basicConfig(level="INFO")

    async def main(mode: Literal["traverse", "walk_back"] = "traverse"):
        ag = AsyncGroup()
        match mode:
            case "traverse":
                await ag.traverse_api()

            case "walk_back":
                await ag.walk_back()

            case _:
                logger.warning("Invalid mode specified, but running traverse")
                await ag.traverse_api()

    asyncio.run(main())
