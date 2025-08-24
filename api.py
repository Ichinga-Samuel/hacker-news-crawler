import http.client
import asyncio
import json


class API:
    url = "hacker-news.firebaseio.com"

    async def get(self, *, path: str) -> list | int | dict:
        url = f"{self.url}"
        path = f"/v0/{path}"
        connection = http.client.HTTPSConnection(url)
        await asyncio.to_thread(connection.request, "GET", path)
        res = await asyncio.to_thread(connection.getresponse)
        res = json.loads(res.read().decode("utf-8"))
        connection.close()
        return res

    async def get_item(self, *, item_id) -> dict:
        path = f"item/{item_id}.json"
        res = await self.get(path=path)
        return res

    async def get_user(self, *, user_id) -> dict:
        path = f"user/{user_id}.json"
        res = await self.get(path=path)
        return res

    async def max_item(self) -> int:
        path = "maxitem.json"
        res = await self.get(path=path)
        return res

    async def new_stories(self) -> list[int]:
        path = "newstories.json"
        res = await self.get(path=path)
        return res

    async def best_stories(self) -> list[int]:
        path = "beststories.json"
        res = await self.get(path=path)
        return res

    async def top_stories(self) -> list[int]:
        path = "topstories.json"
        res = await self.get(path=path)
        return res

    async def ask_stories(self) -> list[int]:
        path = "askstories.json"
        res = await self.get(path=path)
        return res

    async def job_stories(self) -> list[int]:
        path = "jobstories.json"
        res = await self.get(path=path)
        return res

    async def show_stories(self) -> list[int]:
        path = "showstories.json"
        res = await self.get(path=path)
        return res

    async def updates(self) -> dict:
        path = "updates.json"
        res = await self.get(path=path)
        return res
