"""Hacker News API client — thin async wrapper around the Firebase REST API.

The official HN API lives at https://hacker-news.firebaseio.com/v0/.
Every endpoint returns JSON and requires no authentication.

This module provides both an async client (for AsyncQueue) and a sync client
(for ThreadQueue / ProcessQueue), so every osiiso queue type can use the
transport layer that matches its execution model.
"""

from __future__ import annotations

import http.client
import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://hacker-news.firebaseio.com/v0"


# ---------------------------------------------------------------------------
# Async client — used by AsyncQueue
# ---------------------------------------------------------------------------

class AsyncHNClient:
    """Async Hacker News API client backed by httpx.

    Designed to be used as an async context manager so the underlying
    connection pool is properly closed.

    Example::

        async with AsyncHNClient() as client:
            item = await client.get_item(1)
    """

    def __init__(self, timeout: float = 15.0) -> None:
        self._client = httpx.AsyncClient(timeout=timeout, http2=False)

    async def __aenter__(self) -> AsyncHNClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.aclose()

    async def _get(self, path: str) -> Any:
        """Fetch a JSON endpoint and return the parsed response."""
        url = f"{BASE_URL}/{path}"
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def get_item(self, item_id: int) -> dict | None:
        """Fetch a single item (story, comment, job, poll, etc.)."""
        return await self._get(f"item/{item_id}.json")

    async def get_user(self, user_id: str) -> dict | None:
        """Fetch a single user profile."""
        return await self._get(f"user/{user_id}.json")

    async def top_stories(self) -> list[int]:
        """Return up to 500 top story IDs."""
        return await self._get("topstories.json")

    async def new_stories(self) -> list[int]:
        """Return up to 500 newest story IDs."""
        return await self._get("newstories.json")

    async def best_stories(self) -> list[int]:
        """Return up to 500 best story IDs."""
        return await self._get("beststories.json")

    async def ask_stories(self) -> list[int]:
        """Return up to 200 Ask HN story IDs."""
        return await self._get("askstories.json")

    async def show_stories(self) -> list[int]:
        """Return up to 200 Show HN story IDs."""
        return await self._get("showstories.json")

    async def job_stories(self) -> list[int]:
        """Return up to 200 job story IDs."""
        return await self._get("jobstories.json")

    async def max_item_id(self) -> int:
        """Return the current largest item ID."""
        return await self._get("maxitem.json")

    async def updates(self) -> dict:
        """Return recently changed items and profiles."""
        return await self._get("updates.json")


# ---------------------------------------------------------------------------
# Sync client — used by ThreadQueue and ProcessQueue
# ---------------------------------------------------------------------------

class SyncHNClient:
    """Synchronous Hacker News API client backed by http.client.

    Used by ThreadQueue and ProcessQueue where async I/O is not available.

    Example::

        client = SyncHNClient()
        item = client.get_item(1)
    """

    def __init__(self, timeout: float = 15.0) -> None:
        self._timeout = timeout

    def _get(self, path: str) -> Any:
        """Fetch a JSON endpoint using a fresh HTTPS connection."""
        conn = http.client.HTTPSConnection(
            "hacker-news.firebaseio.com", timeout=self._timeout
        )
        try:
            conn.request("GET", f"/v0/{path}")
            resp = conn.getresponse()
            data = resp.read().decode("utf-8")
            return json.loads(data)
        finally:
            conn.close()

    def get_item(self, item_id: int) -> dict | None:
        """Fetch a single item."""
        return self._get(f"item/{item_id}.json")

    def get_user(self, user_id: str) -> dict | None:
        """Fetch a single user profile."""
        return self._get(f"user/{user_id}.json")

    def top_stories(self) -> list[int]:
        """Return up to 500 top story IDs."""
        return self._get("topstories.json")

    def new_stories(self) -> list[int]:
        """Return up to 500 newest story IDs."""
        return self._get("newstories.json")

    def best_stories(self) -> list[int]:
        """Return up to 500 best story IDs."""
        return self._get("beststories.json")

    def max_item_id(self) -> int:
        """Return the current largest item ID."""
        return self._get("maxitem.json")
