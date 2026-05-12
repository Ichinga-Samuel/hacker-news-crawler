"""Part 3 — ProcessQueue: CPU-bound text analysis with multiprocessing.

ProcessQueue runs each task in a separate subprocess, bypassing the GIL.
This makes it the right choice for CPU-intensive computation that benefits
from true parallelism across multiple cores.

What this script does:
    1. Reads previously-crawled HN data from SQLite (from Parts 1 or 2).
    2. Uses ProcessQueue to analyze comment text in parallel subprocesses:
       - Word frequency analysis
       - Readability scoring
       - Engagement ranking
    3. Demonstrates submit(), map(), and group() for CPU-bound work.

When to pick ProcessQueue:
    - Tasks are CPU-bound (text processing, image resizing, number crunching).
    - You need true parallelism, not just concurrency.
    - Tasks are independent and don't share mutable state.

Important constraints:
    - All functions must be module-level (pickleable).
    - All arguments must be serializable.
    - No shared mutable state between processes.
"""

import logging
import os
import sqlite3
import time

import osiiso
from src.analysis import (
    TextStats,
    analyze_batch,
    analyze_text,
    compute_engagement_score,
    rank_stories,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Data loaders ────────────────────────────────────────────────────

def load_items_from_db(db_path, item_type=None, limit=200):
    """Load items from SQLite as plain dicts (for process serialization).

    Only selects columns relevant for analysis to keep the serialized
    payload small for cross-process transfer.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cols = "id, type, by, time, title, text, url, score, descendants"
    if item_type:
        rows = conn.execute(
            "SELECT {} FROM items WHERE type = ? LIMIT ?".format(cols),
            (item_type, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT {} FROM items LIMIT ?".format(cols), (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_comments_text(db_path, limit=100):
    """Load comment text bodies from the database."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT text FROM items WHERE type = 'comment' AND text != '' LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ─── Approach 1: submit() ────────────────────────────────────────────

def analyze_with_submit(db_path, limit=50):
    """Submit individual text analysis tasks to ProcessQueue.

    Each comment's text is analyzed in a separate subprocess.
    """
    logger.info("━━━ Approach 1: submit() — analyzing %d comments ━━━", limit)

    texts = load_comments_text(db_path, limit=limit)
    if not texts:
        logger.warning("  No comments found. Run Part 1 or 2 first!")
        return None

    logger.info("  Loaded %d comment texts", len(texts))

    with osiiso.ProcessQueue(workers=4) as q:
        for i, text in enumerate(texts):
            q.submit(analyze_text, text,
                     name="analyze-{}".format(i), timeout=120.0)
        summary = q.run()

    logger.info("  -> %d analyzed, %d failed", summary.succeeded, summary.failed)

    successes = summary.successes()
    if successes:
        stats = successes[0].value
        logger.info(
            "  Sample result: %d words, readability=%.1f, top=%s",
            stats.word_count,
            stats.readability_score,
            [w for w, _ in stats.top_words[:3]],
        )
    return summary


# ─── Approach 2: map() ───────────────────────────────────────────────

def analyze_with_map(db_path, limit=80):
    """map() submits analyze_text for every comment — one line."""
    logger.info("━━━ Approach 2: map() — %d comments ━━━", limit)

    texts = load_comments_text(db_path, limit=limit)
    if not texts:
        logger.warning("  No comments found!")
        return None

    with osiiso.ProcessQueue(workers=4) as q:
        q.map(analyze_text, texts, timeout=120.0)
        summary = q.run()

    logger.info("  -> %d analyzed, %d failed", summary.succeeded, summary.failed)

    total_words = sum(
        r.value.word_count for r in summary.successes() if r.value
    )
    logger.info("  Total words across all comments: %d", total_words)
    return summary


# ─── Approach 3: group() ─────────────────────────────────────────────

def analyze_with_groups(db_path, limit=50):
    """group() organizes different CPU-bound analyses as logical units.

    We create two groups:
    1. Text analysis group — word frequency + readability per comment.
    2. Ranking group — engagement scoring + ranking for stories.
    """
    logger.info("━━━ Approach 3: group() — text + ranking ━━━")

    texts = load_comments_text(db_path, limit=limit)
    stories = load_items_from_db(db_path, item_type="story", limit=limit)

    if not texts and not stories:
        logger.warning("  No data found!")
        return None

    with osiiso.ProcessQueue(workers=4) as q:
        # Group 1: Individual text analysis per comment
        if texts:
            text_tasks = [(analyze_text, t) for t in texts]
            q.group(text_tasks, group_id="text-analysis", timeout=120.0)
            logger.info("  Text group: %d tasks", len(text_tasks))

        # Group 2: Story ranking
        if stories:
            q.group(
                [(rank_stories, stories)],
                group_id="story-ranking",
                timeout=120.0,
            )
            logger.info("  Ranking group: 1 task with %d stories", len(stories))

        summary = q.run()

    logger.info(
        "  -> %d total tasks, %d succeeded",
        summary.total_submitted,
        summary.succeeded,
    )

    # Show the top-ranked story
    if stories:
        rank_results = [
            r
            for r in summary.successes()
            if r.group_id == "story-ranking" and r.value
        ]
        if rank_results:
            ranked = rank_results[0].value
            if ranked:
                top = ranked[0]
                logger.info(
                    "  Top story: '%s' (score=%.4f)",
                    top.get("title", "?")[:60],
                    top.get("engagement_score", 0),
                )

    return summary


# ─── Fallback: Fetch + Analyze ────────────────────────────────────────

def fetch_and_analyze(limit=30):
    """If no pre-crawled data exists, fetch fresh data and analyze it.

    Uses ThreadQueue to fetch (I/O) and ProcessQueue to analyze (CPU),
    demonstrating how the two queue types complement each other.
    """
    logger.info("━━━ Bonus: ThreadQueue fetch -> ProcessQueue analyze ━━━")

    from src.client import SyncHNClient
    from src.models import HNItem
    from src.storage import Storage

    sync_client = SyncHNClient()
    db = Storage("hn_process.db")

    # Step 1: Fetch items with ThreadQueue
    logger.info("  Step 1: Fetching %d stories with ThreadQueue...", limit)

    def fetch_item(item_id):
        data = sync_client.get_item(item_id)
        if data:
            item = HNItem.from_api(data)
            if item:
                db.save_item(item)
        return data

    top_ids = sync_client.top_stories()[:limit]

    with osiiso.ThreadQueue(workers=8) as tq:
        tq.map(fetch_item, top_ids, retries=2, timeout=10.0)
        fetch_summary = tq.run()

    logger.info("  Fetched %d items", fetch_summary.succeeded)

    # Step 2: Analyze with ProcessQueue
    texts = load_comments_text("hn_process.db", limit=50)
    stories = load_items_from_db("hn_process.db", item_type="story", limit=50)

    if texts:
        logger.info(
            "  Step 2: Analyzing %d texts with ProcessQueue...", len(texts)
        )
        with osiiso.ProcessQueue(workers=4) as pq:
            pq.map(analyze_text, texts, timeout=120.0)
            if stories:
                pq.submit(rank_stories, stories, timeout=120.0,
                          name="rank-stories")
            analysis_summary = pq.run()

        logger.info("  Analysis: %d succeeded", analysis_summary.succeeded)
        analysis_summary.display()
    else:
        logger.info("  No text to analyze (stories may not have comments)")

    db.close()


# ─── Entry point ──────────────────────────────────────────────────────

def main():
    """Run all ProcessQueue approaches."""
    print("\n" + "=" * 70)
    print("  PART 3: ProcessQueue — CPU-Bound Text Analysis")
    print("=" * 70 + "\n")

    # Try to use data from previous runs
    db_path = None
    for candidate in ["hn_async.db", "hn_threads.db", "hn_process.db"]:
        if os.path.exists(candidate):
            db_path = candidate
            break

    t0 = time.perf_counter()

    if db_path:
        logger.info("Using existing database: %s\n", db_path)
        analyze_with_submit(db_path)
        print()
        analyze_with_map(db_path)
        print()
        s3 = analyze_with_groups(db_path)
        print()
    else:
        logger.info("No existing data found — fetching fresh data...\n")
        fetch_and_analyze(limit=30)
        print()
        db_path = "hn_process.db"
        s3 = analyze_with_groups(db_path)

    elapsed = time.perf_counter() - t0

    print("=" * 70)
    print("  RESULTS")
    print("=" * 70)
    print("  Total time: {:.2f}s".format(elapsed))
    if s3:
        s3.display()
    print("=" * 70)


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
