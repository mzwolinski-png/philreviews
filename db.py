"""
PhilReviews SQLite database interface.
Single module for all database operations — replaces Airtable.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_title TEXT,
    book_author_first_name TEXT,
    book_author_last_name TEXT,
    reviewer_first_name TEXT,
    reviewer_last_name TEXT,
    publication_source TEXT,
    publication_date TEXT,
    review_link TEXT,
    review_summary TEXT,
    access_type TEXT,
    doi TEXT,
    entry_type TEXT DEFAULT 'review',
    symposium_group TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_doi
    ON reviews(doi) WHERE doi IS NOT NULL AND doi != '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_link
    ON reviews(review_link) WHERE review_link IS NOT NULL AND review_link != '';
"""


def _connect():
    return sqlite3.connect(DB_PATH)


def _migrate(conn):
    """Add new columns if they don't exist (for existing databases)."""
    cursor = conn.execute("PRAGMA table_info(reviews)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if 'entry_type' not in existing_cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN entry_type TEXT DEFAULT 'review'")
    if 'symposium_group' not in existing_cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN symposium_group TEXT")
    if 'subfield_primary' not in existing_cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN subfield_primary TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subfield ON reviews(subfield_primary)")
    if 'subfield_secondary' not in existing_cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN subfield_secondary TEXT")


def init_db():
    """Create the reviews table if it doesn't exist."""
    with _connect() as conn:
        conn.executescript(_SCHEMA)
        _migrate(conn)


def insert_review(fields: dict):
    """INSERT OR IGNORE a single review."""
    cols = [
        "book_title", "book_author_first_name", "book_author_last_name",
        "reviewer_first_name", "reviewer_last_name", "publication_source",
        "publication_date", "review_link", "review_summary", "access_type", "doi",
        "entry_type", "symposium_group", "subfield_primary", "subfield_secondary",
    ]
    values = [fields.get(c, "") for c in cols]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    with _connect() as conn:
        conn.execute(
            f"INSERT OR IGNORE INTO reviews ({col_names}) VALUES ({placeholders})",
            values,
        )


def insert_reviews(records: list[dict]):
    """Batch insert reviews (INSERT OR IGNORE)."""
    cols = [
        "book_title", "book_author_first_name", "book_author_last_name",
        "reviewer_first_name", "reviewer_last_name", "publication_source",
        "publication_date", "review_link", "review_summary", "access_type", "doi",
        "entry_type", "symposium_group", "subfield_primary", "subfield_secondary",
    ]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    rows = [[r.get(c, "") for c in cols] for r in records]
    with _connect() as conn:
        conn.executemany(
            f"INSERT OR IGNORE INTO reviews ({col_names}) VALUES ({placeholders})",
            rows,
        )


def doi_exists(doi: str) -> bool:
    """Check whether a DOI already exists in the database."""
    if not doi:
        return False
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM reviews WHERE doi = ? LIMIT 1", (doi,)
        ).fetchone()
        return row is not None


def review_link_exists(url: str) -> bool:
    """Check whether a review link already exists in the database."""
    if not url:
        return False
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM reviews WHERE review_link = ? LIMIT 1", (url,)
        ).fetchone()
        return row is not None


def get_all_reviews() -> list[dict]:
    """Return every review as a list of dicts."""
    with _connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM reviews ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def get_reviews_missing_authors() -> list[dict]:
    """Return reviews where both author first and last names are empty."""
    with _connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM reviews "
            "WHERE (book_author_first_name IS NULL OR book_author_first_name = '') "
            "  AND (book_author_last_name IS NULL OR book_author_last_name = '')"
        ).fetchall()
        return [dict(r) for r in rows]


def update_author(review_link: str, first: str, last: str):
    """Update the book author on a review identified by its link."""
    with _connect() as conn:
        conn.execute(
            "UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ? "
            "WHERE review_link = ?",
            (first, last, review_link),
        )


# Auto-init on import
init_db()
