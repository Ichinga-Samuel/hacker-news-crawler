"""Part 2 — ThreadQueue: Blocking I/O crawler with thread-based concurrency.

ThreadQueue is the right choice when you need concurrency for blocking I/O
in synchronous code — no event loop required. Each task runs in a daemon
thread managed by the queue.

What this script does:
    1. Uses SyncHNClient (http.client) for blocking HTTP calls.
    2. Spawns ThreadQueue workers that each fetch items in their own thread.
    3. Demonstrates submit(), map(), group(), and the @q.task() decorator.
    4. Shows lifecycle callbacks (on_start, on_complete, on_retry).
    5. Saves everything to SQLite using the thread-safe Storage class.

When to pick ThreadQueue over AsyncQueue:
    - You're integrating with sync-only libraries (e.g. requests, selenium).
    - Your codebase is not async and you don't want an event loop.
    - The work is I/O-bound but blocking (file reads, HTTP, subprocess).
"""

from __future__ import annotations

import logging
import time

import osiiso
from src.client import SyncHNClient
from src.models import HNItem, HNUser
from src.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

storage = Storage("hn_threads.db")
sync_client = SyncHNClient()
seen_items: set[int] = set()
seen_users: set[str] = set()
_fetched = 0
_saved = 0


def fetch_and_save_item(item_id: int) -> HNItem | None:
    """Sync: fetch an item, parse it, and save to SQLite.

    This is a plain synchronous function — exactly what ThreadQueue expects.
    The queue runs it inside a managed daemon thread.
    """
    global _fetched, _saved
    if item_id in seen_items:
        return None
    seen_items.add(item_id)
    data = sync_client.get_item(item_id)
    _fetched += 1
    if not data:
        return None
    item = HNItem.from_api(data)
    if item:
        storage.save_item(item)
        _saved += 1
        return item
    return None


def fetch_and_save_user(user_id: str) -> HNUser | None:
    """Sync: fetch a user profile and save it."""
    if not user_id or user_id in seen_users:
        return None
    seen_users.add(user_id)
    data = sync_client.get_user(user_id)
    if not data:
        return None
    user = HNUser.from_api(data)
    if user:
        storage.save_user(user)
        return user
    return None


# --- Approach 1: submit() with lifecycle callbacks ---

def crawl_with_submit(story_ids: list[int], limit: int = 30):
    """ThreadQueue.submit() with on_start/on_complete callbacks."""
    logger.info("━━━ Approach 1: submit() with callbacks — %d stories ━━━", limit)

    completed = 0

    def on_complete(result: osiiso.TaskResult):
        nonlocal completed
        completed += 1
        if completed % 10 == 0:
            logger.info("  Progress: %d/%d tasks completed", completed, limit)

    def on_retry(handle: osiiso.SyncTaskHandle, exc: BaseException):
        logger.warning("  ↻ Retrying %s (attempt %d): %s",
                       handle.name, handle.attempts, exc)

    with osiiso.ThreadQueue(
        workers=8,
        on_complete=on_complete,
        on_retry=on_retry,
    ) as q:
        for item_id in story_ids[:limit]:
            q.submit(fetch_and_save_item, item_id,
                     retries=2, retry_delay=0.3, timeout=12.0,
                     name=f"item-{item_id}")
        summary = q.run()

    logger.info("  → %d succeeded, %d failed", summary.succeeded, summary.failed)
    return summary


# --- Approach 2: map() with shared TaskOptions ---

def crawl_with_map(story_ids: list[int], limit: int = 50):
    """ThreadQueue.map() with reusable TaskOptions."""
    logger.info("━━━ Approach 2: map() with TaskOptions — %d stories ━━━", limit)

    # Create a reusable options bundle
    retry_opts = osiiso.TaskOptions(retries=2, retry_delay=0.5, timeout=12.0)

    with osiiso.ThreadQueue(workers=12) as q:
        handles = q.map(fetch_and_save_item, story_ids[:limit], opts=retry_opts)
        summary = q.run()

    logger.info("  → %d mapped, %d succeeded", len(handles), summary.succeeded)
    return summary


# --- Approach 3: group() for multi-phase pipeline ---

def crawl_with_groups(limit: int = 30):
    """ThreadQueue.group() for organized, multi-phase crawling."""
    logger.info("━━━ Approach 3: group() — stories + authors ━━━")

    with osiiso.ThreadQueue(workers=10) as q:
        # Phase 1: Fetch top story IDs (single task)
        handle = q.submit(sync_client.top_stories, timeout=15.0)
        phase1 = q.run()

        if not phase1.ok:
            logger.error("  Failed to fetch story list!")
            return phase1

        story_ids = handle.value()
        target_ids = story_ids[:limit]
        logger.info("  Phase 1: Got %d story IDs, fetching %d",
                     len(story_ids), len(target_ids))

        # Reset the queue for the next phase
        q.reset()

        # Phase 2: Fetch all stories as a group
        story_tasks = [(fetch_and_save_item, sid) for sid in target_ids]
        stories_grp = q.group(story_tasks, group_id="stories",
                              retries=2, timeout=10.0)
        phase2 = q.run()

        # Phase 3: Fetch authors discovered in phase 2
        authors: set[str] = set()
        for r in phase2.results:
            if r.value and r.value.by:
                authors.add(r.value.by)

        if authors:
            q.reset()
            user_tasks = [(fetch_and_save_user, uid) for uid in authors]
            users_grp = q.group(user_tasks, group_id="authors", timeout=8.0)
            phase3 = q.run()
            logger.info("  Phase 3: %d authors, %d saved",
                        len(authors), phase3.succeeded)

    logger.info("  → Group crawl complete")
    return phase2


# --- Approach 4: @q.task() decorator ---

def crawl_with_decorator(story_ids: list[int], limit: int = 40):
    """ThreadQueue @q.task() decorator — the cleanest API."""
    logger.info("━━━ Approach 4: @q.task() — %d stories ━━━", limit)

    with osiiso.ThreadQueue(workers=10) as q:
        @q.task(retries=2, retry_delay=0.3, timeout=12.0)
        def fetch_story(item_id: int):
            return fetch_and_save_item(item_id)

        # Use the decorator's built-in .map()
        fetch_story.map(story_ids[:limit])

        summary = q.run()

    logger.info("  → %d tasks, %d succeeded", summary.total_submitted, summary.succeeded)
    return summary


def main():
    """Run all four ThreadQueue approaches."""
    print("\n" + "=" * 70)
    print("  PART 2: ThreadQueue — Threaded I/O-Bound HN Crawler")
    print("=" * 70 + "\n")

    top_ids = sync_client.top_stories()
    logger.info("Got %d top story IDs\n", len(top_ids))

    t0 = time.perf_counter()
    crawl_with_submit(top_ids); print()
    crawl_with_map(top_ids); print()
    crawl_with_groups(); print()
    s4 = crawl_with_decorator(top_ids); print()
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
    main()
