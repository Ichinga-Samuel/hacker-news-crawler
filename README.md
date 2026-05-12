# Mastering Structured Concurrency in Python: Crawling Hacker News with Osiiso

**A hands-on tutorial exploring async, threaded, and multiprocessing concurrency through a real-world web crawler.**

---

Python gives you three concurrency models—`asyncio`, `threading`, and `multiprocessing`—but using them correctly is surprisingly hard. Error handling is scattered. Task lifecycle is invisible. Cancellation is an afterthought.

[**Osiiso**](https://pypi.org/project/osiiso/) is a structured concurrency library that provides a unified API across all three models. In this tutorial, we'll build a complete Hacker News crawler and analyzer that demonstrates when and why you'd choose each one.

## What We're Building

We'll create a three-part pipeline that crawls the [Hacker News API](https://github.com/HackerNews/API):

| Part | Queue Type | Use Case | Why This Queue? |
|------|-----------|----------|----------------|
| **Part 1** | `AsyncQueue` | Fetch stories from the HN API | I/O-bound, async-native, highest throughput |
| **Part 2** | `ThreadQueue` | Fetch items using blocking HTTP | I/O-bound, sync code, no event loop needed |
| **Part 3** | `ProcessQueue` | Analyze text from crawled data | CPU-bound, bypasses GIL, true parallelism |

By the end, you'll understand:
- When to use each queue type (and why it matters)
- Four submission patterns: `submit()`, `map()`, `group()`, and `@q.task()`
- How retries, timeouts, and lifecycle callbacks work
- How to compose queues for multi-phase data pipelines

## Project Structure

```
hn_crawler/
├── src/
│   ├── __init__.py
│   ├── client.py          # Async + sync HN API clients
│   ├── models.py          # Immutable dataclasses for HN items & users
│   ├── storage.py         # Thread-safe SQLite persistence
│   └── analysis.py        # CPU-bound text analysis (for ProcessQueue)
├── 01_async_crawler.py    # Part 1: AsyncQueue demo
├── 02_thread_crawler.py   # Part 2: ThreadQueue demo
├── 03_process_analyzer.py # Part 3: ProcessQueue demo
├── pyproject.toml
└── README.md
```

## Prerequisites

- Python 3.13+
- An internet connection (to reach the HN API)

```bash
# Clone and set up the project
git clone https://github.com/Ichinga-Samuel/hacker-news-crawler
cd hacker-news-crawler

# Create a virtual environment and install dependencies
uv sync
```

The only dependency beyond the standard library is `osiiso` and `httpx`:

```toml
# pyproject.toml
[project]
dependencies = [
    "osiiso>=0.0.1a1",
    "httpx>=0.28.1",
]
```

---

## Part 1: AsyncQueue — High-Throughput Async Crawling

### When to Use AsyncQueue

**AsyncQueue** is the best choice when:
- Your tasks are **I/O-bound** (network requests, database queries)
- You're already in an **async codebase** (or want to be)
- You need the **highest throughput** for concurrent I/O
- You want **native `await` support** for task results

Under the hood, AsyncQueue runs worker coroutines inside a single event loop. There's no thread overhead—just cooperative multitasking.

### The API Client

First, we need an async HTTP client. We use `httpx` for its async support:

```python
# src/client.py
import httpx

BASE_URL = "https://hacker-news.firebaseio.com/v0"

class AsyncHNClient:
    """Async HN API client backed by httpx."""

    def __init__(self, timeout: float = 15.0):
        self._client = httpx.AsyncClient(timeout=timeout)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self._client.aclose()

    async def get_item(self, item_id: int) -> dict | None:
        resp = await self._client.get(f"{BASE_URL}/item/{item_id}.json")
        resp.raise_for_status()
        return resp.json()

    async def top_stories(self) -> list[int]:
        resp = await self._client.get(f"{BASE_URL}/topstories.json")
        return resp.json()
```

### Approach 1: `submit()` — One Task at a Time

The most explicit pattern. You submit each task individually and get back a `TaskHandle`:

```python
async with osiiso.AsyncQueue(workers=10) as q:
    # Each submit() returns a TaskHandle you can await later
    for item_id in story_ids[:30]:
        q.submit(
            process_and_save_item, item_id,
            retries=2,           # Retry up to 2 times on failure
            retry_delay=0.5,     # Wait 0.5s before first retry
            timeout=10.0,        # Kill the task after 10 seconds
            name=f"story-{item_id}",  # Human-readable name in logs
        )

    # run() blocks until all tasks complete and returns a RunSummary
    summary = await q.run()
    print(f"Succeeded: {summary.succeeded}, Failed: {summary.failed}")
```

**When to use `submit()`**: When you need fine-grained control over each task's options, or when tasks have different configurations.

### Approach 2: `map()` — Batch Submission

When every task calls the same function with different arguments, `map()` is cleaner:

```python
async with osiiso.AsyncQueue(workers=15) as q:
    # map() submits process_and_save_item(id) for each id
    handles = q.map(
        process_and_save_item,
        story_ids[:50],
        retries=2,
        retry_delay=0.5,
        timeout=10.0,
    )
    summary = await q.run()
```

**When to use `map()`**: When applying one function to a list of inputs—like `list(map(fn, items))` but concurrent.

### Approach 3: `group()` — Organized Multi-Phase Pipelines

`group()` batches tasks into a logical unit with its own `wait()` and `RunSummary`. This is powerful for multi-phase workflows:

```python
async with osiiso.AsyncQueue(workers=20) as q:
    # Phase 1: Fetch all story lists as a group
    lists_group = q.group([
        (client.top_stories,),
        (client.new_stories,),
        (client.best_stories,),
    ], group_id="fetch-lists")

    list_summary = await lists_group.wait()
    # Extract all unique story IDs from the three lists
    all_ids = set()
    for r in list_summary.results:
        if r.value:
            all_ids.update(r.value)

    # Phase 2: Fetch story details as another group
    story_tasks = [(process_and_save_item, sid) for sid in list(all_ids)[:30]]
    stories_group = q.group(story_tasks, group_id="stories", retries=2)
    stories_summary = await stories_group.wait()

    # Phase 3: Fetch authors discovered in Phase 2
    authors = {r.value.by for r in stories_summary.results if r.value and r.value.by}
    author_tasks = [(process_and_save_user, uid) for uid in authors]
    authors_group = q.group(author_tasks, group_id="authors")
    await authors_group.wait()
```

**When to use `group()`**: When you have multi-phase pipelines, heterogeneous tasks (different functions), or need to track logical batches independently.

### Approach 4: `@q.task()` — The Decorator Pattern

The cleanest API. The decorator binds a function to the queue with default options:

```python
async with osiiso.AsyncQueue(workers=15) as q:
    @q.task(retries=2, retry_delay=0.5, timeout=10.0)
    async def fetch_story(item_id: int):
        return await process_and_save_item(item_id)

    # Now calling fetch_story() submits a task, not a coroutine!
    for item_id in story_ids[:40]:
        fetch_story(item_id)  # Returns a TaskHandle

    # The decorator also adds .map() and .group()
    # fetch_story.map(story_ids[:40])  # equivalent to the loop

    summary = await q.run()
```

**When to use `@q.task()`**: When you want the most readable code and your tasks share the same configuration.

### Running Part 1

```bash
python 01_async_crawler.py
```

```
══════════════════════════════════════════════════════════════════════
  PART 1: AsyncQueue — Async I/O-Bound HN Crawler
══════════════════════════════════════════════════════════════════════

━━━ Approach 1: submit() — 30 stories ━━━
  → 30 succeeded, 0 failed

━━━ Approach 2: map() — 50 stories ━━━
  → 50 succeeded, 0 failed

━━━ Approach 3: group() — multi-phase crawl ━━━
  Phase 1: 687 unique stories, fetching 30
  → Group crawl done

━━━ Approach 4: @q.task() — 40 stories ━━━
  → 40 tasks, 40 succeeded

  RESULTS
══════════════════════════════════════════════════════════════════════
          stories: 77
         comments: 0
             jobs: 1
            users: 28
       total_items: 78
  Total time: 26.64s
```

---

## Part 2: ThreadQueue — Blocking I/O with Thread-Based Concurrency

### When to Use ThreadQueue

**ThreadQueue** is the right choice when:
- You're working with **sync-only libraries** (requests, selenium, file I/O)
- Your codebase **isn't async** and you don't want to add an event loop
- Tasks are **I/O-bound but blocking** (network calls, disk reads)
- You need **real threads** for legacy library compatibility

ThreadQueue uses daemon threads with a priority queue. No event loop, no `async/await`—just plain Python functions.

### The Sync Client

For ThreadQueue, we use `http.client` from the standard library:

```python
# src/client.py
import http.client
import json

class SyncHNClient:
    """Synchronous HN API client for use with ThreadQueue."""

    def get_item(self, item_id: int) -> dict | None:
        conn = http.client.HTTPSConnection("hacker-news.firebaseio.com")
        try:
            conn.request("GET", f"/v0/item/{item_id}.json")
            resp = conn.getresponse()
            return json.loads(resp.read().decode("utf-8"))
        finally:
            conn.close()
```

### Lifecycle Callbacks

ThreadQueue supports `on_start`, `on_complete`, and `on_retry` callbacks for real-time monitoring:

```python
def on_complete(result: osiiso.TaskResult):
    completed += 1
    if completed % 10 == 0:
        print(f"Progress: {completed} tasks done")

def on_retry(handle, exc):
    print(f"Retrying {handle.name} (attempt {handle.attempts}): {exc}")

with osiiso.ThreadQueue(
    workers=8,
    on_complete=on_complete,
    on_retry=on_retry,
) as q:
    for item_id in story_ids[:30]:
        q.submit(fetch_and_save_item, item_id,
                 retries=2, retry_delay=0.3, timeout=12.0)
    summary = q.run()
```

### Reusable TaskOptions

Instead of repeating the same options on every `submit()`, create a `TaskOptions` bundle:

```python
# Create once, reuse everywhere
retry_opts = osiiso.TaskOptions(retries=2, retry_delay=0.5, timeout=12.0)

with osiiso.ThreadQueue(workers=12) as q:
    q.map(fetch_and_save_item, story_ids[:50], opts=retry_opts)
    summary = q.run()
```

### Multi-Phase with reset()

ThreadQueue is synchronous, so you can call `run()` multiple times by using `reset()` between phases:

```python
with osiiso.ThreadQueue(workers=10) as q:
    # Phase 1
    q.submit(sync_client.top_stories)
    phase1 = q.run()
    story_ids = phase1.values[0]

    # Reset clears internal state for the next run
    q.reset()

    # Phase 2: Fetch stories using group()
    tasks = [(fetch_and_save_item, sid) for sid in story_ids[:30]]
    q.group(tasks, group_id="stories", retries=2)
    phase2 = q.run()
```

### Running Part 2

```bash
python 02_thread_crawler.py
```

---

## Part 3: ProcessQueue — CPU-Bound Analysis with Multiprocessing

### When to Use ProcessQueue

**ProcessQueue** is essential when:
- Tasks are **CPU-bound** (text processing, number crunching, image analysis)
- You need **true parallelism** across multiple cores
- The GIL is your bottleneck
- Tasks are **independent** (no shared mutable state)

ProcessQueue spawns actual subprocesses. Each task runs in complete isolation with its own Python interpreter and memory space.

### The Constraint: Everything Must Be Pickleable

This is the most important thing to understand about ProcessQueue:

```python
# ✅ Module-level function — pickleable
def analyze_text(text: str) -> TextStats:
    words = tokenize(strip_html(text))
    counter = Counter(words)
    return TextStats(word_count=len(words), ...)

# ❌ Lambda — NOT pickleable
analyze = lambda text: TextStats(...)

# ❌ Closure — NOT pickleable
def make_analyzer(stop_words):
    def analyze(text):  # captures stop_words from outer scope
        ...
    return analyze

# ❌ Instance method — NOT pickleable (bound to self)
class Analyzer:
    def analyze(self, text): ...
```

**Rule of thumb**: If `pickle.dumps(fn)` works, ProcessQueue can use it.

### Text Analysis Pipeline

We define CPU-intensive functions at module level in `src/analysis.py`:

```python
# src/analysis.py
from collections import Counter
import re

def analyze_text(text: str) -> TextStats:
    """Word frequency + readability analysis — CPU-intensive."""
    clean = strip_html(text)
    words = tokenize(clean)
    counter = Counter(words)

    # Simplified Flesch readability score
    avg_len = sum(len(w) for w in words) / len(words)
    readability = 206.835 - 1.015 * avg_sentence_length - 84.6 * (avg_len / 3.0)

    return TextStats(
        word_count=len(words),
        unique_words=len(counter),
        top_words=tuple(counter.most_common(10)),
        readability_score=round(readability, 1),
    )

def rank_stories(stories: list[dict]) -> list[dict]:
    """Rank stories by engagement score — CPU-intensive."""
    for story in stories:
        age_hours = (time.time() - story["time"]) / 3600
        story["engagement_score"] = compute_engagement_score(
            story["score"], story["descendants"], age_hours
        )
    return sorted(stories, key=lambda x: x["engagement_score"], reverse=True)
```

### Submitting CPU-Bound Work

```python
# Each comment is analyzed in a separate subprocess
with osiiso.ProcessQueue(workers=4) as q:
    for text in comment_texts:
        q.submit(analyze_text, text, timeout=60.0)
    summary = q.run()
```

### Heterogeneous Groups: Text + Ranking

Groups shine when you have different types of CPU work to parallelize:

```python
with osiiso.ProcessQueue(workers=4) as q:
    # Group 1: Text analysis per comment
    text_tasks = [(analyze_text, t) for t in comment_texts]
    text_grp = q.group(text_tasks, group_id="text-analysis")

    # Group 2: Story ranking (chunked for parallelism)
    chunk_size = len(stories) // 4
    chunks = [stories[i:i+chunk_size] for i in range(0, len(stories), chunk_size)]
    rank_tasks = [(rank_stories, chunk) for chunk in chunks]
    rank_grp = q.group(rank_tasks, group_id="story-ranking")

    summary = q.run()

    # Results are tagged by group_id
    text_results = [r for r in summary.successes() if r.group_id == "text-analysis"]
    rank_results = [r for r in summary.successes() if r.group_id == "story-ranking"]
```

### Composing Queue Types: ThreadQueue → ProcessQueue

The real power comes from combining queue types. Use ThreadQueue for I/O, then ProcessQueue for computation:

```python
# Step 1: Fetch data with ThreadQueue (I/O-bound)
with osiiso.ThreadQueue(workers=8) as tq:
    tq.map(fetch_item, top_ids[:30], retries=2, timeout=10.0)
    fetch_summary = tq.run()

# Step 2: Analyze with ProcessQueue (CPU-bound)
texts = load_comments_from_db()
with osiiso.ProcessQueue(workers=4) as pq:
    pq.map(analyze_text, texts, timeout=60.0)
    analysis_summary = pq.run()
```

### Running Part 3

```bash
# Run Part 1 or 2 first to populate the database, then:
python 03_process_analyzer.py
```

---

## The RunSummary: Your Execution Dashboard

Every `run()` and `group.wait()` returns a `RunSummary`—an immutable snapshot of what happened:

```python
summary = await q.run()

# High-level stats
print(summary.total_submitted)  # Total tasks
print(summary.succeeded)        # Successful count
print(summary.failed)           # Failed count
print(summary.cancelled)        # Cancelled count
print(summary.duration)         # Wall-clock time in seconds
print(summary.ok)               # True if no failures

# Access results
summary.values          # Return values of succeeded tasks
summary.errors          # TaskResult objects for failed tasks
summary.by_group()      # Results grouped by group_id
summary.by_name()       # Results grouped by task name

# Pretty-print
summary.display()
# ----------------------------------------
#   Run Summary: PASSED
# ----------------------------------------
#   Total tasks : 40
#   Succeeded   : 40
#   Failed      : 0
#   Cancelled   : 0
#   Duration    : 3.142s
# ----------------------------------------

# Strict mode: raise if anything failed
summary = await q.run(strict=True)  # Raises ExecutionError on failure
```

---

## Choosing the Right Queue

```
              ┌─────────────────────────────────────────────┐
              │           What kind of work?                │
              └─────────┬───────────────────┬───────────────┘
                        │                   │
                   I/O-bound            CPU-bound
                        │                   │
              ┌─────────┴────────┐          │
              │  Async codebase? │          │
              └────┬────────┬────┘    ┌─────┴──────┐
                   │        │        │ ProcessQueue │
                  Yes       No       │  (workers=N) │
                   │        │        └──────────────┘
            ┌──────┴──────┐ │
            │  AsyncQueue  │ │
            │ (workers=20) │ │
            └──────────────┘ │
                      ┌──────┴──────┐
                      │ ThreadQueue  │
                      │  (workers=8) │
                      └──────────────┘
```

| Feature | AsyncQueue | ThreadQueue | ProcessQueue |
|---------|-----------|-------------|-------------|
| **Best for** | Async I/O | Blocking I/O | CPU-bound |
| **Concurrency** | Coroutines | OS threads | Subprocesses |
| **GIL** | N/A (cooperative) | Limited by GIL | Bypasses GIL |
| **Context manager** | `async with` | `with` | `with` |
| **Task functions** | Async or sync | Sync only | Sync (pickleable) |
| **Handle type** | `TaskHandle` (awaitable) | `SyncTaskHandle` (blocking) | `SyncTaskHandle` (blocking) |
| **Shared state** | Yes (same process) | Yes (with locks) | No (separate processes) |
| **Overhead** | Minimal | Low | High (process spawn) |

---

## Key Osiiso Concepts

### TaskOptions — Reusable Configuration

```python
opts = osiiso.TaskOptions(
    priority=1,           # Lower = higher priority (default: 3)
    retries=3,            # Retry up to 3 times on failure
    retry_delay=1.0,      # Wait 1s before first retry
    backoff=2.0,          # Double the delay on each retry
    timeout=30.0,         # Kill after 30 seconds
    must_complete=True,    # Don't cancel during shutdown
    name="critical-task",  # Human-readable name
)

# Use as a base, override per-task
q.submit(fn, arg, opts=opts, priority=0)  # Override priority
```

### TaskHandle — Task Lifecycle

```python
handle = q.submit(fetch, url)

handle.status      # "pending" → "running" → "succeeded"/"failed"/"cancelled"
handle.done()      # True when finished
handle.cancel()    # Request cancellation
handle.attempts    # Number of execution attempts
result = await handle  # Await the result (AsyncQueue)
result = handle.wait() # Block for result (ThreadQueue/ProcessQueue)
```

### Error Handling

```python
# Option 1: Inspect the summary
summary = await q.run()
for error in summary.errors:
    print(f"Task {error.name} failed: {error.exception}")

# Option 2: Strict mode raises immediately
try:
    summary = await q.run(strict=True)
except osiiso.ExecutionError as e:
    for result in e.results:
        print(f"Failed: {result.name}")

# Option 3: fail_first policy cancels remaining tasks
async with osiiso.AsyncQueue(fail_policy="fail_first") as q:
    ...  # First failure cancels everything
```

---

## Running the Complete Pipeline

```bash
# Step 1: Crawl with AsyncQueue (fastest for I/O)
python 01_async_crawler.py

# Step 2: Crawl with ThreadQueue (sync alternative)
python 02_thread_crawler.py

# Step 3: Analyze crawled data with ProcessQueue (CPU-bound)
python 03_process_analyzer.py
```

Each script is self-contained and can be run independently. Part 3 will automatically detect and use databases created by Parts 1 or 2.

---

## Conclusion

Osiiso gives you **one API** across three concurrency models:

- **`AsyncQueue`** for high-throughput async I/O — use it when you need speed and you're in an async context.
- **`ThreadQueue`** for blocking I/O in sync code — use it when your libraries don't support async.
- **`ProcessQueue`** for CPU-bound computation — use it when the GIL is your bottleneck.

The unified `submit()`/`map()`/`group()`/`@task()` API means you can switch concurrency models by changing one line of code. The `RunSummary` gives you instant visibility into what happened. And structured concurrency via context managers ensures nothing leaks.

**Install osiiso and start building:**

```bash
pip install osiiso
```

Happy crawling! 🚀