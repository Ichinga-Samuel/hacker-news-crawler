"""Storage layer — thread-safe SQLite persistence for crawled data.

Uses a single WAL-mode SQLite database with separate tables for each item
type and users. The storage class is designed to be shared safely across
threads (for ThreadQueue) while remaining usable from async contexts.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .models import HNItem, HNUser

logger = logging.getLogger(__name__)

_CREATE_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS items (
    id          INTEGER PRIMARY KEY,
    type        TEXT    NOT NULL DEFAULT 'story',
    by          TEXT    NOT NULL DEFAULT '',
    time        INTEGER NOT NULL DEFAULT 0,
    title       TEXT    NOT NULL DEFAULT '',
    text        TEXT    NOT NULL DEFAULT '',
    url         TEXT    NOT NULL DEFAULT '',
    score       INTEGER NOT NULL DEFAULT 0,
    descendants INTEGER NOT NULL DEFAULT 0,
    parent      INTEGER,
    kids        TEXT    NOT NULL DEFAULT '[]',
    dead        INTEGER NOT NULL DEFAULT 0,
    deleted     INTEGER NOT NULL DEFAULT 0
);
"""

_CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id      TEXT    PRIMARY KEY,
    created INTEGER NOT NULL DEFAULT 0,
    karma   INTEGER NOT NULL DEFAULT 0,
    about   TEXT    NOT NULL DEFAULT ''
);
"""

_INSERT_ITEM = """
INSERT OR IGNORE INTO items (id, type, by, time, title, text, url, score,
    descendants, parent, kids, dead, deleted)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

_INSERT_USER = """
INSERT OR IGNORE INTO users (id, created, karma, about)
VALUES (?, ?, ?, ?);
"""


class Storage:
    """Thread-safe SQLite storage for Hacker News data.

    Uses Write-Ahead Logging (WAL) mode for better concurrent read
    performance. The connection is per-instance and protected by a lock
    for multi-threaded writes.

    Args:
        db_path: Path to the SQLite database file.

    Example::

        storage = Storage("hn_data.db")
        storage.save_item(item)
        print(storage.count_items())
    """

    def __init__(self, db_path: str | Path = "hn_data.db") -> None:
        self._db_path = str(db_path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self) -> None:
        """Create all tables if they don't exist."""
        with self._lock:
            self._conn.execute(_CREATE_ITEMS_TABLE)
            self._conn.execute(_CREATE_USERS_TABLE)
            self._conn.commit()

    def save_item(self, item: HNItem) -> bool:
        """Persist an HNItem. Returns True if inserted, False if duplicate."""
        with self._lock:
            cursor = self._conn.execute(
                _INSERT_ITEM,
                (
                    item.id, item.type, item.by, item.time,
                    item.title, item.text, item.url, item.score,
                    item.descendants, item.parent,
                    json.dumps(list(item.kids)),
                    int(item.dead), int(item.deleted),
                ),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def save_user(self, user: HNUser) -> bool:
        """Persist an HNUser. Returns True if inserted, False if duplicate."""
        with self._lock:
            cursor = self._conn.execute(
                _INSERT_USER,
                (user.id, user.created, user.karma, user.about),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def save_items_batch(self, items: list[HNItem]) -> int:
        """Batch-insert multiple items. Returns count of new rows inserted."""
        rows = [
            (
                it.id, it.type, it.by, it.time,
                it.title, it.text, it.url, it.score,
                it.descendants, it.parent,
                json.dumps(list(it.kids)),
                int(it.dead), int(it.deleted),
            )
            for it in items
        ]
        with self._lock:
            cursor = self._conn.executemany(_INSERT_ITEM, rows)
            self._conn.commit()
            return cursor.rowcount

    def save_users_batch(self, users: list[HNUser]) -> int:
        """Batch-insert multiple users. Returns count of new rows inserted."""
        rows = [(u.id, u.created, u.karma, u.about) for u in users]
        with self._lock:
            cursor = self._conn.executemany(_INSERT_USER, rows)
            self._conn.commit()
            return cursor.rowcount

    def count_items(self, item_type: str | None = None) -> int:
        """Count stored items, optionally filtered by type."""
        if item_type:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM items WHERE type = ?", (item_type,)
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) FROM items").fetchone()
        return row[0] if row else 0

    def count_users(self) -> int:
        """Count stored user profiles."""
        row = self._conn.execute("SELECT COUNT(*) FROM users").fetchone()
        return row[0] if row else 0

    def get_item_ids(self) -> set[int]:
        """Return the set of all stored item IDs (for deduplication)."""
        rows = self._conn.execute("SELECT id FROM items").fetchall()
        return {r[0] for r in rows}

    def get_user_ids(self) -> set[str]:
        """Return the set of all stored user IDs (for deduplication)."""
        rows = self._conn.execute("SELECT id FROM users").fetchall()
        return {r[0] for r in rows}

    def summary(self) -> dict[str, int]:
        """Return a human-friendly summary of stored data."""
        return {
            "stories": self.count_items("story"),
            "comments": self.count_items("comment"),
            "jobs": self.count_items("job"),
            "polls": self.count_items("poll"),
            "pollopts": self.count_items("pollopt"),
            "users": self.count_users(),
            "total_items": self.count_items(),
        }

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __repr__(self) -> str:
        return f"Storage({self._db_path!r})"
