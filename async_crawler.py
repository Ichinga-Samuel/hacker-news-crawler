"""Part 1 — AsyncQueue: High-throughput async story crawler.

Demonstrates osiiso's AsyncQueue for I/O-bound concurrent work.
AsyncQueue is ideal when tasks are dominated by network latency.

Key osiiso features: submit(), map(), group(), @q.task() decorator,
TaskOptions for retries/timeouts, RunSummary diagnostics.
"""

from __future__ import annotations

import asyncio
import logging
import time

import osiiso
from src.client import AsyncHNClient
from src.models import HNItem, HNUser
from src.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

storage = Storage("hn_async.db")
client: AsyncHNClient | None = None
seen_items: set[int] = set()
seen_users: set[str] = set()
_fetched = 0
_saved = 0


async def process_and_save_item(item_id: int) -> HNItem | None:
    """Fetch, parse, and save a single HN item."""
    global _saved, _fetched
    if item_id in seen_items:
        return None
    seen_items.add(item_id)
    data = await client.get_item(item_id)
    _fetched += 1
    if not data:
        return None
    item = HNItem.from_api(data)
    if item:
        storage.save_item(item)
        _saved += 1
        return item
    return None


async def process_and_save_user(user_id: str) -> HNUser | None:
    """Fetch, parse, and save a user profile."""
    if not user_id or user_id in seen_users:
        return None
    seen_users.add(user_id)
    data = await client.get_user(user_id)
    if not data:
        return None
    user = HNUser.from_api(data)
    if user:
        storage.save_user(user)
        return user
    return None


# --- Approach 1: submit() — one task at a time ---

async def crawl_with_submit(story_ids: list[int], limit: int = 30):
    """Most explicit pattern: q.submit() returns a TaskHandle per task."""
    logger.info("━━━ Approach 1: submit() — %d stories ━━━", limit)
    ids = story_ids[:limit]

    async with osiiso.AsyncQueue(workers=10) as q:
        for item_id in ids:
            q.submit(process_and_save_item, item_id,
                     retries=2, retry_delay=0.5, timeout=10.0,
                     name=f"story-{item_id}")
        summary = await q.run()

    logger.info("  → %d succeeded, %d failed", summary.succeeded, summary.failed)
    return summary


# --- Approach 2: map() — batch submission ---

async def crawl_with_map(story_ids: list[int], limit: int = 50):
    """map() submits fn(item) for every item — cleaner than a loop."""
    logger.info("━━━ Approach 2: map() — %d stories ━━━", limit)
    ids = story_ids[:limit]

    async with osiiso.AsyncQueue(workers=15) as q:
        q.map(process_and_save_item, ids,
              retries=2, retry_delay=0.5, timeout=10.0)
        summary = await q.run()

    logger.info("  → %d succeeded, %d failed", summary.succeeded, summary.failed)
    return summary


# --- Approach 3: group() — organized heterogeneous tasks ---

async def crawl_with_groups(limit: int = 30):
    """group() batches related tasks with separate wait()/RunSummary."""
    logger.info("━━━ Approach 3: group() — multi-phase crawl ━━━")

    async with osiiso.AsyncQueue(workers=20) as q:
        # Phase 1: Fetch story lists
        lists_group = q.group([
            (client.top_stories,),
            (client.new_stories,),
            (client.best_stories,),
        ], group_id="fetch-lists")

        list_summary = await lists_group.wait()
        all_ids: set[int] = set()
        for r in list_summary.results:
            if r.value:
                all_ids.update(r.value)
        target_ids = list(all_ids)[:limit]
        logger.info("  Phase 1: %d unique stories, fetching %d",
                     len(all_ids), len(target_ids))

        # Phase 2: Fetch stories
        tasks = [(process_and_save_item, iid) for iid in target_ids]
        stories_group = q.group(tasks, group_id="stories", retries=2, timeout=10.0)
        stories_summary = await stories_group.wait()

        # Phase 3: Fetch authors
        authors = {r.value.by for r in stories_summary.results
                   if r.value and r.value.by}
        if authors:
            author_tasks = [(process_and_save_user, uid) for uid in authors]
            authors_grp = q.group(author_tasks, group_id="authors", timeout=8.0)
            await authors_grp.wait()

        summary = await q.run()

    logger.info("  → Group crawl done: %d tasks", summary.total_submitted)
    return summary


# --- Approach 4: @q.task() decorator ---

async def crawl_with_decorator(story_ids: list[int], limit: int = 40):
    """Decorator binds a function to the queue with default options."""
    logger.info("━━━ Approach 4: @q.task() — %d stories ━━━", limit)
    ids = story_ids[:limit]

    async with osiiso.AsyncQueue(workers=15) as q:
        @q.task(retries=2, retry_delay=0.5, timeout=10.0)
        async def fetch_story(item_id: int):
            return await process_and_save_item(item_id)

        # Calling fetch_story() submits a task, returns a handle
        for item_id in ids:
            fetch_story(item_id)

        summary = await q.run()

    logger.info("  → %d tasks, %d succeeded", summary.total_submitted, summary.succeeded)
    return summary


async def main():
    """Run all four AsyncQueue approaches."""
    global client

    print("\n" + "=" * 70)
    print("  PART 1: AsyncQueue — Async I/O-Bound HN Crawler")
    print("=" * 70 + "\n")

    async with AsyncHNClient() as c:
        client = c
        top_ids = await client.top_stories()
        logger.info("Got %d top story IDs\n", len(top_ids))

        t0 = time.perf_counter()
        await crawl_with_submit(top_ids); print()
        await crawl_with_map(top_ids); print()
        await crawl_with_groups(); print()
        s4 = await crawl_with_decorator(top_ids); print()
        elapsed = time.perf_counter() - t0

    print("=" * 70)
    print("  RESULTS")
    print("=" * 70)
    for k, v in storage.summary().items():
        print(f"    {k:>15}: {v}")
    print(f"\n  Total time: {elapsed:.2f}s | Fetched: {_fetched} | Saved: {_saved}")
    print("=" * 70)
    s4.display()
    storage.close()


if __name__ == "__main__":
    osiiso.run(main())
