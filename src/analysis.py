"""Text analysis utilities — CPU-bound processing for ProcessQueue demos.

These functions perform genuine computation on HN data: text tokenization,
word frequency analysis, and readability scoring. They are designed to be
pickleable (module-level functions with serializable arguments) so they
work correctly with ProcessQueue's subprocess execution model.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from html import unescape


# ---------------------------------------------------------------------------
# Text cleaning
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    return _WHITESPACE_RE.sub(" ", unescape(_TAG_RE.sub(" ", text))).strip()


def tokenize(text: str) -> list[str]:
    """Split text into lowercase alpha tokens (words)."""
    return re.findall(r"[a-zA-Z]{2,}", text.lower())


# ---------------------------------------------------------------------------
# CPU-bound analysis functions (for ProcessQueue)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class TextStats:
    """Result of analyzing a block of text.

    Attributes:
        word_count: Total number of words.
        unique_words: Number of distinct words.
        avg_word_length: Mean character count per word.
        top_words: The 10 most common words and their counts.
        readability_score: A simplified Flesch-like readability estimate.
        sentence_count: Approximate number of sentences.
    """
    word_count: int = 0
    unique_words: int = 0
    avg_word_length: float = 0.0
    top_words: tuple[tuple[str, int], ...] = ()
    readability_score: float = 0.0
    sentence_count: int = 0


def analyze_text(text: str) -> TextStats:
    """Perform word-frequency and readability analysis on a text block.

    This is intentionally CPU-intensive — perfect for ProcessQueue.

    Args:
        text: Raw text (may contain HTML).

    Returns:
        A TextStats dataclass with the analysis results.
    """
    clean = strip_html(text)
    words = tokenize(clean)

    if not words:
        return TextStats()

    counter = Counter(words)
    total = len(words)
    unique = len(counter)
    avg_len = sum(len(w) for w in words) / total

    # Approximate sentence count by splitting on sentence-ending punctuation
    sentences = re.split(r"[.!?]+", clean)
    sentence_count = max(1, len([s for s in sentences if s.strip()]))

    # Simplified readability score (higher = easier to read)
    avg_sentence_length = total / sentence_count
    avg_syllables = avg_len / 3.0  # rough approximation
    readability = max(0.0, 206.835 - 1.015 * avg_sentence_length - 84.6 * avg_syllables)

    return TextStats(
        word_count=total,
        unique_words=unique,
        avg_word_length=round(avg_len, 2),
        top_words=tuple(counter.most_common(10)),
        readability_score=round(readability, 1),
        sentence_count=sentence_count,
    )


def analyze_batch(texts: list[str]) -> dict[str, int | float]:
    """Analyze multiple text blocks and return aggregate statistics.

    Args:
        texts: List of raw text strings.

    Returns:
        Dict with aggregate word counts, unique word counts, and
        overall readability.
    """
    all_words: list[str] = []
    total_sentences = 0

    for text in texts:
        clean = strip_html(text)
        words = tokenize(clean)
        all_words.extend(words)
        sentences = re.split(r"[.!?]+", clean)
        total_sentences += max(1, len([s for s in sentences if s.strip()]))

    if not all_words:
        return {"total_words": 0, "unique_words": 0, "readability": 0.0}

    counter = Counter(all_words)
    avg_len = sum(len(w) for w in all_words) / len(all_words)
    avg_sentence_length = len(all_words) / max(1, total_sentences)
    avg_syllables = avg_len / 3.0
    readability = max(0.0, 206.835 - 1.015 * avg_sentence_length - 84.6 * avg_syllables)

    return {
        "total_words": len(all_words),
        "unique_words": len(counter),
        "top_words": counter.most_common(20),
        "readability": round(readability, 1),
    }


def compute_engagement_score(score: int, descendants: int, age_hours: float) -> float:
    """Compute a weighted engagement score for a story.

    Uses a time-decay formula similar to HN's own ranking algorithm.
    This is a CPU-bound calculation suitable for ProcessQueue.

    Args:
        score: The story's upvote score.
        descendants: Total comment count.
        age_hours: Hours since the story was posted.

    Returns:
        A float engagement score (higher = more engaging).
    """
    # Gravity factor: older posts decay faster
    gravity = 1.8
    points = score + (descendants * 0.5)
    denominator = (age_hours + 2) ** gravity
    return round(points / denominator, 4) if denominator > 0 else 0.0


def rank_stories(stories: list[dict]) -> list[dict]:
    """Rank a batch of stories by engagement score.

    Each dict must have keys: 'id', 'score', 'descendants', 'time'.

    Args:
        stories: List of story dicts from the API.

    Returns:
        The same list sorted by computed engagement score (descending),
        with the score added as 'engagement_score'.
    """
    import time as time_mod
    now = time_mod.time()
    results = []

    for story in stories:
        age_hours = (now - story.get("time", now)) / 3600.0
        eng = compute_engagement_score(
            story.get("score", 0),
            story.get("descendants", 0),
            age_hours,
        )
        results.append({**story, "engagement_score": eng})

    results.sort(key=lambda x: x["engagement_score"], reverse=True)
    return results
