"""Materialise a `books` table: one row per distinct book, many reviews each.

The schema has no book entity — a book exists only as (title, author) repeated
across review rows. Grouping at request time over 227k rows is too slow for a
page, so the grouping is materialised here and rebuilt after each weekly run.

Identity is the normalised full title plus the author's surname. Subtitles are
deliberately NOT dropped: doing so merges "Collected Papers: Volume 1" with
"Volume 2". Under-merging costs a duplicate page; over-merging puts reviews of
one book on the page of another, which is a correctness bug. Title variants of
the same book are handled by reconcile_titles.py, which is human-reviewed.
"""
import os
import re
import sqlite3
import sys
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db

_SCHEMA = """
CREATE TABLE IF NOT EXISTS books (
    book_key     TEXT PRIMARY KEY,
    slug         TEXT NOT NULL UNIQUE,
    title        TEXT NOT NULL,
    author_first TEXT,
    author_last  TEXT,
    review_count INTEGER NOT NULL DEFAULT 0,
    first_year   TEXT,
    last_year    TEXT
);
CREATE INDEX IF NOT EXISTS idx_books_count ON books(review_count DESC);
"""

_STOP_SLUG = {"the", "a", "an", "of", "and", "or", "in", "on", "to", "for", "with"}


def _fold(s):
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def book_key(title, author_last):
    """Stable identity for a book. Empty title -> None (never grouped)."""
    t = re.sub(r"[^a-z0-9]+", " ", _fold(title).lower()).strip()
    a = re.sub(r"[^a-z0-9]+", " ", _fold(author_last or "").lower()).strip()
    if not t:
        return None
    a = a.split()[-1] if a else ""
    return f"{t[:120]}|{a}"


def make_slug(title, author_last, taken):
    """Readable, unique, URL-safe. Collisions get a numeric suffix."""
    words = [w for w in re.split(r"[^a-z0-9]+", _fold(title).lower()) if w]
    kept, out = 0, []
    for w in words:
        if kept >= 9:
            break
        if w in _STOP_SLUG and out:
            out.append(w)
            continue
        out.append(w)
        kept += 1
    base = "-".join(out).strip("-")[:80].strip("-")
    surname = re.sub(r"[^a-z0-9]+", "", _fold(author_last or "").lower())
    if surname:
        base = f"{base}-{surname}"[:96].strip("-")
    base = base or "untitled"
    slug, n = base, 2
    while slug in taken:
        slug = f"{base}-{n}"
        n += 1
    taken.add(slug)
    return slug


def rebuild(verbose=False):
    conn = sqlite3.connect(db.DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    # Keep slugs stable across rebuilds — a changing URL loses its ranking.
    existing = {r["book_key"]: r["slug"] for r in conn.execute("SELECT book_key, slug FROM books")}
    taken = set(existing.values())

    # "Author"/"Editor" in the author field are parse residue, not people; a
    # book page for them would be nonsense.
    role_words = ("author", "authors", "editor", "editors", "eds", "ed")

    groups = {}
    for r in conn.execute("""SELECT book_title t, book_author_first_name f,
                                    book_author_last_name l, publication_date d
                             FROM reviews WHERE COALESCE(TRIM(book_title),'') <> ''"""):
        if (r["l"] or "").strip().lower() in role_words:
            continue
        key = book_key(r["t"], r["l"])
        if not key:
            continue
        g = groups.get(key)
        year = (r["d"] or "")[:4]
        if g is None:
            groups[key] = {"title": r["t"], "f": r["f"] or "", "l": r["l"] or "",
                           "n": 1, "lo": year, "hi": year}
        else:
            g["n"] += 1
            # keep the longest title seen — usually the one with its subtitle
            if len(r["t"]) > len(g["title"]):
                g["title"], g["f"], g["l"] = r["t"], r["f"] or "", r["l"] or ""
            if year:
                g["lo"] = min(g["lo"] or year, year)
                g["hi"] = max(g["hi"] or year, year)

    rows = []
    for key, g in groups.items():
        slug = existing.get(key) or make_slug(g["title"], g["l"], taken)
        rows.append((key, slug, g["title"], g["f"], g["l"], g["n"], g["lo"], g["hi"]))

    # Stamp the key onto each review: the page must be one indexed lookup,
    # not a scan and re-normalise of 227k rows per request.
    cols = [c[1] for c in conn.execute("PRAGMA table_info(reviews)")]
    if "book_key" not in cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN book_key TEXT")
    updates = []
    for r in conn.execute(
            "SELECT id, book_title t, book_author_last_name l FROM reviews "
            "WHERE COALESCE(TRIM(book_title),'') <> ''"):
        if (r["l"] or "").strip().lower() in role_words:
            continue
        k = book_key(r["t"], r["l"])
        if k:
            updates.append((k, r["id"]))
    conn.executemany("UPDATE reviews SET book_key = ? WHERE id = ?", updates)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_book_key ON reviews(book_key)")

    conn.execute("DELETE FROM books")
    conn.executemany("""INSERT INTO books
        (book_key, slug, title, author_first, author_last, review_count, first_year, last_year)
        VALUES (?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    stats = {
        "books": len(rows),
        "multi_review": sum(1 for r in rows if r[5] > 1),
        "slugs_reused": sum(1 for k in groups if k in existing),
        "reviews_keyed": len(updates),
    }
    if verbose:
        print(stats)
    conn.close()
    return stats


if __name__ == "__main__":
    print(rebuild(verbose=True))
