"""Data models — clean dataclasses for Hacker News entities.

These are pure data containers with no database logic. They handle the
messy, nullable JSON coming from the HN API and normalize it into typed
Python objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal


@dataclass(frozen=True, slots=True)
class HNItem:
    """A single Hacker News item (story, comment, job, poll, or pollopt).

    Attributes:
        id: The item's unique integer ID.
        type: One of ``story``, ``comment``, ``job``, ``poll``, ``pollopt``.
        by: Username of the author (empty string for deleted items).
        time: Unix timestamp of creation.
        title: Title text (stories, jobs, polls only).
        text: Body text (comments, Ask HN, polls).
        url: URL of the linked page (stories only).
        score: Upvote score.
        descendants: Total comment count (stories, polls).
        parent: Parent item ID (comments only).
        kids: Child comment IDs in ranked display order.
        dead: True if the item is flagged dead.
        deleted: True if the item was deleted.
    """

    id: int
    type: Literal["story", "comment", "job", "poll", "pollopt"] = "story"
    by: str = ""
    time: int = 0
    title: str = ""
    text: str = ""
    url: str = ""
    score: int = 0
    descendants: int = 0
    parent: int | None = None
    kids: tuple[int, ...] = ()
    dead: bool = False
    deleted: bool = False

    @classmethod
    def from_api(cls, data: dict) -> HNItem | None:
        """Build an HNItem from raw API JSON, returning None for invalid data."""
        if not data or "id" not in data:
            return None
        return cls(
            id=data["id"],
            type=data.get("type", "story"),
            by=data.get("by", ""),
            time=data.get("time", 0),
            title=data.get("title", ""),
            text=data.get("text", ""),
            url=data.get("url", ""),
            score=data.get("score", 0),
            descendants=data.get("descendants", 0),
            parent=data.get("parent"),
            kids=tuple(data.get("kids", [])),
            dead=data.get("dead", False),
            deleted=data.get("deleted", False),
        )

    @property
    def created_at(self) -> datetime:
        """Return the creation timestamp as a timezone-aware datetime."""
        return datetime.fromtimestamp(self.time, tz=timezone.utc)

    @property
    def domain(self) -> str:
        """Extract the domain from the URL, or return empty string."""
        if not self.url:
            return ""
        try:
            from urllib.parse import urlparse
            return urlparse(self.url).netloc
        except Exception:
            return ""


@dataclass(frozen=True, slots=True)
class HNUser:
    """A Hacker News user profile.

    Attributes:
        id: The user's unique username.
        created: Unix timestamp of account creation.
        karma: The user's karma score.
        about: Self-description in HTML.
        submitted: List of the user's submitted item IDs.
    """

    id: str
    created: int = 0
    karma: int = 0
    about: str = ""
    submitted: tuple[int, ...] = ()

    @classmethod
    def from_api(cls, data: dict) -> HNUser | None:
        """Build an HNUser from raw API JSON, returning None for invalid data."""
        if not data or "id" not in data:
            return None
        return cls(
            id=data["id"],
            created=data.get("created", 0),
            karma=data.get("karma", 0),
            about=data.get("about", ""),
            submitted=tuple(data.get("submitted", [])),
        )

    @property
    def created_at(self) -> datetime:
        """Return account creation as a timezone-aware datetime."""
        return datetime.fromtimestamp(self.created, tz=timezone.utc)
