import asyncio
import signal
import time
from typing import Literal
from logging import getLogger, basicConfig

from api import API
from save_to_db import SaveToDB

logger = getLogger(__name__)


class AsyncGather:
    def __init__(self):
        self.api = API()
        self.db = SaveToDB()
        self.visited = set()  # keep track of visited items or users
        self.tasks: list[asyncio.Task] = []

    async def get_user(self, *, user_id):
        try:
            res = await self.api.get_user(user_id=user_id)
            self.visited.add(res["id"])
            await self.db.save_user(data=res)
            if submissions := res.get("submitted"):
                self.tasks.extend(
                    asyncio.create_task(self.get_item(item_id=item))
                    for item in submissions
                )
        except Exception as err:
            logger.error("%s occurred in get_user", err)

    async def get_item(self, *, item_id):
        try:
            if item_id in self.visited:
                return

            res = await self.api.get_item(item_id=item_id)
            await self.db.save_data(data=res)
            self.visited.add(item_id)

            # saving user data
            if (by := res.get("by")) and by not in self.visited:
                self.tasks.append(asyncio.create_task(self.get_user(user_id=by)))

            # saving kids data
            if kids := res.get("kids"):
                self.tasks.extend(
                    asyncio.create_task(self.get_item(item_id=item))
                    for item in kids
                    if item not in self.visited
                )

            # saving parent data
            if (parent := res.get("parent")) and parent not in self.visited:
                self.tasks.append(asyncio.create_task(self.get_item(item_id=parent)))

        except Exception as err:
            logger.warning("%s occurred in get_item", err)

    async def walk_back(self, *, amount: int = 1000, timeout: int = 60):
        largest = await self.api.max_item()
        logger.info("Walking back from item %d to %d", largest, largest - amount)
        start = time.perf_counter()
        try:
            self.tasks = [
                asyncio.create_task(self.get_item(item_id=item))
                for item in range(largest, largest - amount, -1)
            ]
            for task in asyncio.as_completed(self.tasks, timeout=timeout):
                try:
                    await task
                except asyncio.CancelledError:
                    ...

        except asyncio.CancelledError:
            logger.warning("Task cancelled after %d", time.perf_counter() - start)

        except Exception as exe:
            logger.error("Error: %s occurred in walk_back", exe)

        finally:
            logger.info("Task completed after %d", time.perf_counter() - start)
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
                self.tasks = [
                    asyncio.create_task(self.get_item(item_id=story))
                    for story in stories
                ]
                await asyncio.gather(*self.tasks, return_exceptions=True)

        except asyncio.TimeoutError:
            logger.warning("Timed out after %d seconds", time.perf_counter() - start)

        except asyncio.CancelledError:
            logger.warning(
                "Tasks cancelled after %d seconds", time.perf_counter() - start
            )

        finally:
            logger.info("Tasks completed after %d seconds", time.perf_counter() - start)
            await self.db.show()


if __name__ == "__main__":
    basicConfig(level="INFO")

    async def main(mode: Literal["traverse", "walk_back"] = "traverse"):
        ag = AsyncGather()
        match mode:
            case "traverse":
                await ag.traverse_api(timeout=120)

            case "walk_back":
                await ag.walk_back()

            case _:
                logger.info("Invalid mode specified, but running traverse")
                await ag.traverse_api()

    asyncio.run(main())
