#!/usr/bin/env python3
"""
Tier 1 filter: only accept reviews from popular/fringe sources if the book
(matched by author last name + fuzzy title) already has at least one review
from a Tier 2 source elsewhere in the DB.

Used by:
- Scrapers (during insert) — filter out non-philosophy books at scrape time
- Backfill cleanup — periodic sweep to remove existing Tier 1 entries that no
  longer pass the filter

Configuration: TIER1_SOURCES is the set of `publication_source` values whose
reviews must pass the filter. All other sources are treated as Tier 2 and
their reviews populate the reference set unconditionally.
"""

import os
import re
import sqlite3
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db

# Sources whose reviews require an existing matching book in the DB
TIER1_SOURCES = frozenset([
    # Popular commentary magazines
    "Quillette",
    "Jacobin",
    "Reason",
    "Law & Liberty",
    "Public Discourse",
    "The Unpopulist",
    "Claremont Review of Books",
    "Liberal Currents",
    "Libertarianism.org",
    # Fringe academic journals (conservative starter set)
    "Journal of the History of Biology",
    "History and Philosophy of the Life Sciences",
    "Modern Theology",
    # Interdisciplinary humanities journal — mostly literary/cultural/art/film
    # theory, not philosophy. Keep only books already covered elsewhere in the DB.
    "Critical Inquiry",
])


_TITLE_STOPWORDS = frozenset([
    "a", "an", "the", "and", "of", "in", "on", "to", "for", "with", "from",
    "by", "is", "as", "at", "or", "be", "are", "was", "were",
    "book", "review", "reviews", "essay", "essays", "introduction",
])


def _significant_words(t):
    """Return set of lowercased significant words (>=4 chars, not stopwords)
    from the title, with HTML stripped."""
    if not t:
        return set()
    t = re.sub(r"<[^>]+>", " ", t).lower()
    words = re.findall(r"[a-z][a-z']{3,}", t)
    return {w.rstrip("'s") for w in words if w not in _TITLE_STOPWORDS}


def build_reference_index(conn):
    """Build several reference sets from non-Tier 1 entries.

    Returns a dict with three keys:
        'titles': {last_name_lower: [set_of_words, ...]} for title-overlap match
        'authors': set of last_name_lower with >= 2 distinct Tier 2 books
                   (signals an established philosophy book author)
        'reviewers': set of (last_name_lower, first_initial) with >= 3 Tier 2
                     reviews (signals an established philosophy reviewer)
    """
    placeholders = ",".join("?" * len(TIER1_SOURCES))
    rows = conn.execute(
        f"""
        SELECT book_author_last_name, book_title,
               reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE publication_source NOT IN ({placeholders})
        """,
        tuple(TIER1_SOURCES),
    ).fetchall()

    titles = defaultdict(list)
    author_book_counts = defaultdict(set)  # last_name → set of distinct titles
    reviewer_counts = defaultdict(int)      # (last, normalized_first) → count

    for ba_last, btitle, rev_first, rev_last in rows:
        if ba_last and btitle:
            key = ba_last.lower().strip()
            words = _significant_words(btitle)
            if key and words:
                titles[key].append(words)
                # Use normalized title prefix as a "distinct book" key
                norm = re.sub(r"[^a-z0-9]", "", btitle.lower())[:40]
                if norm:
                    author_book_counts[key].add(norm)
        if rev_last:
            reviewer_counts[_reviewer_key(rev_first, rev_last)] += 1

    authors = {k for k, v in author_book_counts.items() if len(v) >= 2}
    reviewers = {k for k, c in reviewer_counts.items() if c >= 3}

    return {"titles": titles, "authors": authors, "reviewers": reviewers}


def _reviewer_key(first, last):
    """Stable identifier for a reviewer.

    Uses the full normalized first name (lowercased, periods stripped, spaces
    collapsed) plus last name. Distinguishes "J. Wall" from "J. L. Wall" from
    "John Wall" — the trade-off is that the same person using both forms
    won't be matched, but that's better than conflating different people.
    """
    f = re.sub(r"\.", "", (first or "")).strip().lower()
    f = re.sub(r"\s+", " ", f)
    return ((last or "").strip().lower(), f)


# Fringe academic journals where the author/reviewer-only signals are too
# permissive — a reviewer who works in both history of science AND philosophy of
# science can trigger the "established reviewer" rule even when the specific
# book is pure history of discipline. For these journals, require the stronger
# Signal 1 (same author + title overlap with a Tier 2 review).
STRICT_TITLE_OVERLAP_REQUIRED = frozenset([
    "Journal of the History of Biology",
    "History and Philosophy of the Life Sciences",
    # Modern Theology stays out — its theology authors often don't have
    # other DB coverage but the books are legitimate philosophy of religion.
    # Quillette: a culture magazine that occasionally reviews philosophy books.
    # The reviewer-only and author-only signals are too permissive (Shakespeare
    # as "author" passes via Signal 2 even when the article is about a film).
    # Require title overlap with an existing Tier 2 review of the same book.
    "Quillette",
    # Critical Inquiry: literary/cultural theorists routinely review across
    # fields, so author/reviewer-only signals over-admit. Require the actual
    # book to already be covered (Signal 1).
    "Critical Inquiry",
])


def book_passes_filter(book_author_last_name, book_title, reference_index,
                       reviewer_first_name="", reviewer_last_name="",
                       publication_source=""):
    """Return True if the review passes any of three Tier 2 signals.

    1. Same author + 2+ shared significant title words (or 1 distinctive
       8+ char word) with a Tier 2 review by the same author.
    2. The book author has >= 2 distinct books reviewed in Tier 2
       sources — signals an established philosophy author (e.g. Macedo).
    3. The reviewer has >= 3 reviews in Tier 2 sources — signals an
       established philosophy reviewer (e.g. Feser at CRB).

    For STRICT_TITLE_OVERLAP_REQUIRED journals, only Signal 1 counts —
    Signals 2/3 alone are too permissive because reviewers and authors
    often work across philosophy / history-of-discipline boundaries.
    """
    titles_idx = reference_index["titles"]
    authors_set = reference_index["authors"]
    reviewers_set = reference_index["reviewers"]

    last = (book_author_last_name or "").lower().strip()
    is_strict = publication_source in STRICT_TITLE_OVERLAP_REQUIRED

    # Signals 2/3 only apply for non-strict sources
    if not is_strict:
        if reviewer_last_name:
            if _reviewer_key(reviewer_first_name, reviewer_last_name) in reviewers_set:
                return True
        if last and last in authors_set:
            return True

    # Signal 1: same author + title overlap
    if not last or not book_title:
        return False
    if last not in titles_idx:
        return False
    target_words = _significant_words(book_title)
    if not target_words:
        return False
    for ref_words in titles_idx[last]:
        overlap = target_words & ref_words
        if len(overlap) >= 2:
            return True
        if any(len(w) >= 8 for w in overlap):
            return True
    return False


def cleanup_tier1(dry_run=False):
    """Remove existing Tier 1 entries that don't pass the filter.

    Returns dict with counts and details.
    """
    conn = sqlite3.connect(db.DB_PATH)
    print("Building reference index from Tier 2 sources...")
    ref = build_reference_index(conn)
    print(f"Reference index: {len(ref)} unique authors, "
          f"{sum(len(v) for v in ref.values())} (author, title) pairs")

    placeholders = ",".join("?" * len(TIER1_SOURCES))
    rows = conn.execute(
        f"""
        SELECT id, book_title, book_author_last_name, publication_source,
               reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE publication_source IN ({placeholders})
        """,
        tuple(TIER1_SOURCES),
    ).fetchall()

    by_source_pass = defaultdict(int)
    by_source_fail = defaultdict(int)
    fail_ids = []
    fail_samples = defaultdict(list)

    for rid, title, last, source, rev_first, rev_last in rows:
        if book_passes_filter(last, title, ref, rev_first, rev_last, source):
            by_source_pass[source] += 1
        else:
            by_source_fail[source] += 1
            fail_ids.append(rid)
            if len(fail_samples[source]) < 3:
                fail_samples[source].append((rid, title or "", last or ""))

    sources = sorted(set(by_source_pass) | set(by_source_fail))
    print(f"\n{'Source':<45s} {'Pass':>6s} {'Fail':>6s}")
    for s in sources:
        p, f = by_source_pass[s], by_source_fail[s]
        print(f"  {s[:43]:<43s} {p:>6d} {f:>6d}")

    print(f"\nTotal: {len(rows)} entries, {len(fail_ids)} would be deleted")
    print("\nSample fails:")
    for s in sources:
        if fail_samples[s]:
            print(f"\n  [{s}]")
            for rid, t, l in fail_samples[s]:
                print(f"    id={rid} '{t[:50]}' by ?? {l}")

    if not dry_run and fail_ids:
        # Batch delete
        for i in range(0, len(fail_ids), 500):
            batch = fail_ids[i:i + 500]
            conn.execute(
                f"DELETE FROM reviews WHERE id IN ({','.join('?' * len(batch))})",
                batch,
            )
        conn.commit()
        print(f"\nDeleted {len(fail_ids)} entries")
    elif dry_run:
        print(f"\n[DRY RUN] would delete {len(fail_ids)} entries")

    conn.close()
    return {"deleted": len(fail_ids) if not dry_run else 0, "checked": len(rows)}


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    cleanup_tier1(dry_run=dry_run)
