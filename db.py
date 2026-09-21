"""
PhilReviews SQLite database interface.
Single module for all database operations — replaces Airtable.
"""

import re
import sqlite3
import os
import time

DB_PATH = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db"),
)

# Subscribers live in a separate DB so they survive reviews.db syncs
SUBSCRIBERS_DB_PATH = os.environ.get(
    "SUBSCRIBERS_DB_PATH",
    os.path.join(os.path.dirname(DB_PATH), "subscribers.db"),
)

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
    """Open a new SQLite connection with WAL mode."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except sqlite3.DatabaseError:
        # Corrupt DB — move it aside and create a fresh one
        import logging
        log = logging.getLogger(__name__)
        log.error(f"Database corrupt at {DB_PATH}, moving aside and creating fresh DB")
        bad_path = DB_PATH + ".corrupt"
        if os.path.exists(DB_PATH):
            os.rename(DB_PATH, bad_path)
        for ext in ("-wal", "-shm"):
            p = DB_PATH + ext
            if os.path.exists(p):
                os.remove(p)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


# Shared read-only connection for queries (thread-safe in WAL mode)
_read_conn = None


def _get_read_conn():
    """Return a shared read connection, creating it if needed."""
    global _read_conn
    if _read_conn is None:
        _read_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _read_conn.execute("PRAGMA journal_mode=WAL")
        _read_conn.execute("PRAGMA query_only=ON")
    return _read_conn


_REVIEW_FLAGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS review_flags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved INTEGER DEFAULT 0
);
"""


_REJECTION_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS rejection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER,
    book_title TEXT,
    book_author TEXT,
    publication_source TEXT,
    doi TEXT,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


_SUBSCRIBERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    token TEXT UNIQUE NOT NULL,
    verified INTEGER DEFAULT 0,
    subfields TEXT DEFAULT 'all',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_sent_date TEXT
);
CREATE TABLE IF NOT EXISTS follows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    follow_type TEXT NOT NULL,
    follow_value TEXT NOT NULL,
    norm_value TEXT NOT NULL,
    token TEXT UNIQUE NOT NULL,
    verified INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(email, follow_type, norm_value)
);
"""


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
    if 'reviewed' not in existing_cols:
        conn.execute("ALTER TABLE reviews ADD COLUMN reviewed INTEGER DEFAULT 0")
        # Grandfather all pre-existing entries as already reviewed; only entries
        # inserted after this migration enter the review queue (default 0).
        conn.execute("UPDATE reviews SET reviewed = 1")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviewed ON reviews(reviewed)")
    # Subscribers + review-flag + rejection-log tables
    conn.executescript(_SUBSCRIBERS_SCHEMA)
    conn.executescript(_REVIEW_FLAGS_SCHEMA)
    conn.executescript(_REJECTION_LOG_SCHEMA)


def _create_indexes(conn):
    """Create indexes for common query patterns."""
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pub_source ON reviews(publication_source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pub_date ON reviews(publication_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entry_type ON reviews(entry_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_access_type ON reviews(access_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symposium_group ON reviews(symposium_group)")
    # Covering index for default sort (date desc, id desc) — avoids full table scan
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_id ON reviews(publication_date DESC, id DESC)")
    # Composite index for type-filtered queries sorted by date
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type_date ON reviews(entry_type, publication_date DESC, id DESC)")
    # Journal and subfield listing pages filter on one column and sort by date.
    # With only the single-column index SQLite sorted every matching row in a
    # temp B-tree before it could page: measured 10.2 ms on journal page 40 and
    # 31.1 ms on subfield page 50, against 0.2 ms with these (audit 2026-09-21).
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source_date "
                 "ON reviews(publication_source, publication_date DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subfield_date "
                 "ON reviews(subfield_primary, publication_date DESC, id DESC)")


def init_fts(conn):
    """Create FTS5 virtual table and sync triggers."""
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS reviews_fts USING fts5(
            book_title,
            book_author_first_name,
            book_author_last_name,
            reviewer_first_name,
            reviewer_last_name,
            publication_source,
            content='reviews',
            content_rowid='id'
        )
    """)
    # Triggers to keep FTS in sync
    conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS reviews_ai AFTER INSERT ON reviews BEGIN
            INSERT INTO reviews_fts(rowid, book_title, book_author_first_name,
                book_author_last_name, reviewer_first_name, reviewer_last_name,
                publication_source)
            VALUES (new.id, new.book_title, new.book_author_first_name,
                new.book_author_last_name, new.reviewer_first_name,
                new.reviewer_last_name, new.publication_source);
        END;

        CREATE TRIGGER IF NOT EXISTS reviews_ad AFTER DELETE ON reviews BEGIN
            INSERT INTO reviews_fts(reviews_fts, rowid, book_title,
                book_author_first_name, book_author_last_name,
                reviewer_first_name, reviewer_last_name, publication_source)
            VALUES ('delete', old.id, old.book_title, old.book_author_first_name,
                old.book_author_last_name, old.reviewer_first_name,
                old.reviewer_last_name, old.publication_source);
        END;

        CREATE TRIGGER IF NOT EXISTS reviews_au AFTER UPDATE ON reviews BEGIN
            INSERT INTO reviews_fts(reviews_fts, rowid, book_title,
                book_author_first_name, book_author_last_name,
                reviewer_first_name, reviewer_last_name, publication_source)
            VALUES ('delete', old.id, old.book_title, old.book_author_first_name,
                old.book_author_last_name, old.reviewer_first_name,
                old.reviewer_last_name, old.publication_source);
            INSERT INTO reviews_fts(rowid, book_title, book_author_first_name,
                book_author_last_name, reviewer_first_name, reviewer_last_name,
                publication_source)
            VALUES (new.id, new.book_title, new.book_author_first_name,
                new.book_author_last_name, new.reviewer_first_name,
                new.reviewer_last_name, new.publication_source);
        END;
    """)


def rebuild_fts():
    """Rebuild the FTS index from scratch (use after bulk imports)."""
    with _connect() as conn:
        init_fts(conn)
        conn.execute("INSERT INTO reviews_fts(reviews_fts) VALUES('rebuild')")


def init_db():
    """Create the reviews table if it doesn't exist."""
    with _connect() as conn:
        conn.executescript(_SCHEMA)
        _migrate(conn)
        _create_indexes(conn)
        try:
            init_fts(conn)
            # Auto-rebuild FTS if reviews exist but FTS is empty or corrupted
            review_count = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
            if review_count > 0:
                try:
                    fts_count = conn.execute("SELECT COUNT(*) FROM reviews_fts").fetchone()[0]
                except sqlite3.DatabaseError:
                    fts_count = 0
                    conn.execute("DROP TABLE IF EXISTS reviews_fts")
                    init_fts(conn)
                if fts_count == 0:
                    conn.execute("INSERT INTO reviews_fts(reviews_fts) VALUES('rebuild')")
        except sqlite3.DatabaseError:
            # FTS is unrecoverable (deeply corrupted DB) — skip FTS init,
            # app will still boot and serve queries without full-text search
            import logging
            logging.getLogger(__name__).warning("FTS init failed (corrupted DB?), skipping")


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
    """Check whether a DOI already exists in the database, or has been excluded
    as a known false positive."""
    if not doi:
        return False
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM reviews WHERE doi = ? LIMIT 1", (doi,)
        ).fetchone()
        if row is not None:
            return True
        # Check the excluded_dois table — DOIs of items previously identified
        # as false positives that we don't want to re-scrape.
        row = conn.execute(
            "SELECT 1 FROM excluded_dois WHERE doi = ? LIMIT 1", (doi,)
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


# ---------------------------------------------------------------------------
# Server-side search & metadata
# ---------------------------------------------------------------------------

_SORT_COLUMNS = {
    "title": "book_title",
    "author": "book_author_last_name",
    "reviewer": "reviewer_last_name",
    "journal": "publication_source",
    "date": "publication_date",
}

# Cache for default query (no filters, page 1, date desc) and total count
_default_cache = {"data": None, "expires": 0}
_count_cache = {"total": None, "expires": 0}
# General query result cache keyed by (where_clause, params, sort, page)
_query_cache = {}
_QUERY_CACHE_TTL = 600  # 10 minutes
_QUERY_CACHE_MAX = 50   # max cached queries


def _get_total_count():
    """Return cached total review count (refreshes every 10 minutes)."""
    now = time.time()
    if _count_cache["total"] is not None and now < _count_cache["expires"]:
        return _count_cache["total"]
    conn = _get_read_conn()
    total = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    _count_cache["total"] = total
    _count_cache["expires"] = now + 600
    return total


def _is_default_query(q, title, author, reviewer, journals, subfields,
                      year_from, year_to, access, entry_type,
                      sort, sort_dir, page, per_page):
    """Check if this is the default landing page query."""
    return (not q and not title and not author and not reviewer
            and not journals and not subfields
            and not year_from and not year_to
            and not access and not entry_type
            and sort == "date" and sort_dir == "desc"
            and page == 1 and per_page == 25)


def invalidate_caches():
    """Clear query caches (call after inserts/updates)."""
    global _read_conn
    _default_cache["data"] = None
    _default_cache["expires"] = 0
    _count_cache["total"] = None
    _count_cache["expires"] = 0
    _metadata_cache["data"] = None
    _metadata_cache["expires"] = 0
    _query_cache.clear()
    # Close shared read connection so it picks up new data
    if _read_conn is not None:
        try:
            _read_conn.close()
        except Exception:
            pass
        _read_conn = None


def search_reviews(q=None, title=None, author=None, reviewer=None,
                   journals=None, subfields=None, year_from=None, year_to=None,
                   access=None, entry_type=None,
                   sort="date", sort_dir="desc", page=1, per_page=25):
    """Server-side search with FTS5, filters, sorting, and pagination.

    Returns dict: {reviews, total, page, per_page, total_pages}
    """
    # Fast path: return cached result for default landing page query
    now = time.time()
    if _is_default_query(q, title, author, reviewer, journals, subfields,
                         year_from, year_to, access, entry_type,
                         sort, sort_dir, page, per_page):
        if _default_cache["data"] and now < _default_cache["expires"]:
            return _default_cache["data"]

    conditions = []
    params = []

    # FTS5 full-text search
    fts_ids = None
    if q:
        # Escape FTS special chars and build prefix query
        fts_q = _fts_escape(q)
        conn = _get_read_conn()
        rows = conn.execute(
            "SELECT rowid FROM reviews_fts WHERE reviews_fts MATCH ?",
            (fts_q,),
        ).fetchall()
        fts_ids = {r[0] for r in rows}
        if not fts_ids:
            return {"reviews": [], "total": 0, "page": page,
                    "per_page": per_page, "total_pages": 0}

    # Field-specific filters
    if title:
        conditions.append("book_title LIKE ?")
        params.append(f"%{title}%")
    if author:
        conditions.append(
            "(book_author_first_name LIKE ? OR book_author_last_name LIKE ? "
            "OR (book_author_first_name || ' ' || book_author_last_name) LIKE ?)"
        )
        params.extend([f"%{author}%", f"%{author}%", f"%{author}%"])
    if reviewer:
        conditions.append(
            "(reviewer_first_name LIKE ? OR reviewer_last_name LIKE ? "
            "OR (reviewer_first_name || ' ' || reviewer_last_name) LIKE ?)"
        )
        params.extend([f"%{reviewer}%", f"%{reviewer}%", f"%{reviewer}%"])
    if journals:
        placeholders = ", ".join("?" for _ in journals)
        conditions.append(f"publication_source IN ({placeholders})")
        params.extend(journals)
    if subfields:
        placeholders = ", ".join("?" for _ in subfields)
        conditions.append(
            f"(subfield_primary IN ({placeholders}) OR subfield_secondary IN ({placeholders}))"
        )
        params.extend(subfields)
        params.extend(subfields)
    if year_from:
        conditions.append("CAST(SUBSTR(publication_date, 1, 4) AS INTEGER) >= ?")
        params.append(year_from)
    if year_to:
        conditions.append("CAST(SUBSTR(publication_date, 1, 4) AS INTEGER) <= ?")
        params.append(year_to)
    if access:
        # Case-insensitive: stored values have drifted between 'Open'/'open'
        # (and historically 'Paywalled'/'Subscription'), while the filter UI
        # sends lowercase — an exact match silently returned a subset.
        conditions.append("lower(access_type) = lower(?)")
        params.append(access)
    if entry_type:
        conditions.append("entry_type = ?")
        params.append(entry_type)

    # Build WHERE clause
    if fts_ids is not None:
        placeholders = ", ".join("?" for _ in fts_ids)
        conditions.append(f"id IN ({placeholders})")
        params.extend(fts_ids)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    # Sort
    sort_col = _SORT_COLUMNS.get(sort, "publication_date")
    direction = "ASC" if sort_dir == "asc" else "DESC"
    order = f"ORDER BY {sort_col} {direction}, id DESC"

    # Check general query cache
    cache_key = (where, tuple(params), sort_col, direction, page, per_page)
    now = time.time()
    cached = _query_cache.get(cache_key)
    if cached and now < cached["expires"]:
        return cached["data"]

    # Count — use cached total for unfiltered queries
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    if not conditions:
        total = _get_total_count()
    else:
        total = conn.execute(f"SELECT COUNT(*) FROM reviews{where}", params).fetchone()[0]

    total_pages = max(1, (total + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * per_page

    rows = conn.execute(
        f"SELECT * FROM reviews{where} {order} LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    reviews = [dict(r) for r in rows]

    result = {
        "reviews": reviews,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }

    # Cache the result (evict oldest if too many)
    if len(_query_cache) >= _QUERY_CACHE_MAX:
        oldest_key = min(_query_cache, key=lambda k: _query_cache[k]["expires"])
        del _query_cache[oldest_key]
    _query_cache[cache_key] = {"data": result, "expires": now + _QUERY_CACHE_TTL}

    # Also cache in default cache if applicable
    if _is_default_query(q, title, author, reviewer, journals, subfields,
                         year_from, year_to, access, entry_type,
                         sort, sort_dir, page, per_page):
        _default_cache["data"] = result
        _default_cache["expires"] = now + 600

    return result


def get_symposium_peers(symposium_group: str, exclude_id: int) -> list[dict]:
    """Get other entries in the same symposium group."""
    if not symposium_group:
        return []
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM reviews WHERE symposium_group = ? AND id != ?",
        (symposium_group, exclude_id),
    ).fetchall()
    return [dict(r) for r in rows]


def get_symposium_peers_batch(groups: dict[str, int]) -> dict[str, list[dict]]:
    """Batch fetch peers for multiple symposium groups.

    groups: {symposium_group: exclude_id}
    Returns: {symposium_group: [peer_dicts]}
    """
    if not groups:
        return {}
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    placeholders = ", ".join("?" for _ in groups)
    rows = conn.execute(
        f"SELECT * FROM reviews WHERE symposium_group IN ({placeholders})",
        list(groups.keys()),
    ).fetchall()
    result = {g: [] for g in groups}
    for r in rows:
        row = dict(r)
        grp = row["symposium_group"]
        if grp in groups and row["id"] != groups[grp]:
            result[grp].append(row)
    return result


# Metadata cache
_metadata_cache = {"data": None, "expires": 0}


def get_metadata():
    """Return aggregate metadata (journals, subfields, year range, total).
    Cached in memory for 10 minutes.
    """
    now = time.time()
    if _metadata_cache["data"] and now < _metadata_cache["expires"]:
        return _metadata_cache["data"]

    conn = _get_read_conn()
    total = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

    journals_rows = conn.execute(
        "SELECT publication_source, COUNT(*) as cnt FROM reviews "
        "WHERE publication_source IS NOT NULL AND publication_source != '' "
        "GROUP BY publication_source ORDER BY cnt DESC"
    ).fetchall()
    journals = [{"name": r[0], "count": r[1]} for r in journals_rows]

    year_row = conn.execute(
        "SELECT MIN(CAST(SUBSTR(publication_date, 1, 4) AS INTEGER)), "
        "MAX(CAST(SUBSTR(publication_date, 1, 4) AS INTEGER)) "
        "FROM reviews WHERE LENGTH(publication_date) >= 4 "
        "AND SUBSTR(publication_date, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'"
    ).fetchone()
    min_year = year_row[0] or 2000
    max_year = year_row[1] or 2026

    subfield_rows = conn.execute(
        "SELECT subfield_primary, COUNT(*) as cnt FROM reviews "
        "WHERE subfield_primary IS NOT NULL AND subfield_primary != '' "
        "GROUP BY subfield_primary ORDER BY cnt DESC"
    ).fetchall()
    subfields = [{"code": r[0], "count": r[1]} for r in subfield_rows]

    data = {
        "total": total,
        "min_year": min_year,
        "max_year": max_year,
        "journals": journals,
        "subfields": subfields,
    }
    _metadata_cache["data"] = data
    _metadata_cache["expires"] = now + 600
    return data


def _fts_escape(q: str) -> str:
    """Escape an FTS5 query string for safe matching.

    Wraps each token in double quotes to avoid FTS syntax errors,
    then appends * for prefix matching on the last token.
    """
    # Remove FTS special characters
    tokens = q.strip().split()
    if not tokens:
        return '""'
    escaped = []
    for t in tokens:
        # Strip characters that are special in FTS5 (but keep apostrophes
        # so possessive searches like "Kant's" work correctly)
        clean = t.replace('"', '').replace('*', '')
        if clean:
            escaped.append(f'"{clean}"')
    if not escaped:
        return '""'
    # Add prefix match on last token for partial matching
    last = escaped[-1]
    escaped[-1] = last[:-1] + '*"'  # "word" -> "word*"
    return " ".join(escaped)


def get_journal_reviews(journal_name, page=1, per_page=50):
    """Return reviews for a specific journal with pagination and year stats."""
    conn = _get_read_conn()
    total = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE publication_source = ?",
        (journal_name,)
    ).fetchone()[0]
    if total == 0:
        return None
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * per_page
    rows = conn.execute(
        "SELECT * FROM reviews WHERE publication_source = ? "
        "ORDER BY publication_date DESC, id DESC LIMIT ? OFFSET ?",
        (journal_name, per_page, offset)
    ).fetchall()
    year_row = conn.execute(
        "SELECT MIN(CAST(SUBSTR(publication_date,1,4) AS INTEGER)), "
        "MAX(CAST(SUBSTR(publication_date,1,4) AS INTEGER)) "
        "FROM reviews WHERE publication_source = ? "
        "AND LENGTH(publication_date) >= 4",
        (journal_name,)
    ).fetchone()
    return {
        "reviews": [dict(r) for r in rows],
        "total": total, "page": page, "per_page": per_page,
        "total_pages": total_pages,
        "min_year": year_row[0] or 0, "max_year": year_row[1] or 0,
    }


def get_subfield_reviews(subfield_code, page=1, per_page=50):
    """Return reviews for a specific subfield with pagination."""
    conn = _get_read_conn()
    total = conn.execute(
        "SELECT COUNT(*) FROM reviews "
        "WHERE subfield_primary = ? OR subfield_secondary = ?",
        (subfield_code, subfield_code)
    ).fetchone()[0]
    if total == 0:
        return None
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * per_page
    rows = conn.execute(
        "SELECT * FROM reviews "
        "WHERE subfield_primary = ? OR subfield_secondary = ? "
        "ORDER BY publication_date DESC, id DESC LIMIT ? OFFSET ?",
        (subfield_code, subfield_code, per_page, offset)
    ).fetchall()
    return {
        "reviews": [dict(r) for r in rows],
        "total": total, "page": page, "per_page": per_page,
        "total_pages": total_pages,
    }


# --- Subscriber functions (separate DB) ---

def _sub_connect():
    """Open a connection to the subscribers database."""
    conn = sqlite3.connect(SUBSCRIBERS_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SUBSCRIBERS_SCHEMA)
    return conn


def get_verified_subscriber_count() -> int:
    """Return the number of verified subscribers."""
    with _sub_connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM subscribers WHERE verified = 1"
        ).fetchone()[0]


def add_subscriber(email: str, subfields: str, token: str) -> bool:
    """Add a new subscriber. Returns True if created, False if email already exists."""
    with _sub_connect() as conn:
        try:
            conn.execute(
                "INSERT INTO subscribers (email, subfields, token) VALUES (?, ?, ?)",
                (email.lower().strip(), subfields, token),
            )
            return True
        except sqlite3.IntegrityError:
            # Email already exists — update preferences and re-send verification
            conn.execute(
                "UPDATE subscribers SET subfields = ?, token = ?, verified = 0 WHERE email = ?",
                (subfields, token, email.lower().strip()),
            )
            return True


def verify_subscriber(token: str) -> bool:
    """Mark a subscriber as verified if the token is valid and the signup is no
    more than 14 days old. Returns True if a row was verified. (Unsubscribe
    still works on the same token indefinitely — only verification expires.)"""
    with _sub_connect() as conn:
        cur = conn.execute(
            "UPDATE subscribers SET verified = 1 "
            "WHERE token = ? AND created_at >= datetime('now', '-14 days')",
            (token,),
        )
        return cur.rowcount > 0


def unsubscribe(token: str) -> bool:
    """Remove a subscriber by token. Returns True if found."""
    with _sub_connect() as conn:
        cur = conn.execute("DELETE FROM subscribers WHERE token = ?", (token,))
        return cur.rowcount > 0


def get_verified_subscribers() -> list[dict]:
    """Return all verified subscribers."""
    conn = sqlite3.connect(SUBSCRIBERS_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM subscribers WHERE verified = 1"
    ).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def norm_follow_value(s: str) -> str:
    """Normalize a book title or author name for follow matching:
    diacritics stripped, lowercase, alphanumerics + single spaces only.
    'Susana Monsó' -> 'susana monso'."""
    import unicodedata
    s = unicodedata.normalize('NFD', s or '')
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9 ]', ' ', s.lower())
    return re.sub(r'\s+', ' ', s).strip()


def add_follow(email: str, follow_type: str, value: str, token: str) -> str:
    """Register a follow (book title or author). Returns one of:
    'active'  — the email already has a verified subscription/follow, so the
                new follow is live immediately;
    'pending' — new email, verification required (send them the token link);
    'exists'  — this exact follow is already registered for this email."""
    email = email.lower().strip()
    norm = norm_follow_value(value)
    with _sub_connect() as conn:
        dup = conn.execute(
            "SELECT verified FROM follows WHERE email=? AND follow_type=? AND norm_value=?",
            (email, follow_type, norm)).fetchone()
        if dup:
            return 'exists' if dup[0] else 'pending'
        known = conn.execute(
            "SELECT 1 FROM subscribers WHERE email=? AND verified=1 "
            "UNION SELECT 1 FROM follows WHERE email=? AND verified=1",
            (email, email)).fetchone()
        conn.execute(
            "INSERT INTO follows (email, follow_type, follow_value, norm_value, token, verified) "
            "VALUES (?,?,?,?,?,?)",
            (email, follow_type, value.strip(), norm, token, 1 if known else 0))
        return 'active' if known else 'pending'


def pending_token_exists(token: str, kind: str) -> bool:
    """Does this token match a real, still-unverified signup? Used so the
    confirm page 404s on bogus tokens instead of rendering a form that can
    never succeed. kind is 'subscriber' or 'follow'."""
    table = "subscribers" if kind == "subscriber" else "follows"
    with _sub_connect() as conn:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE token = ? "
            f"AND created_at >= datetime('now', '-14 days')", (token,)).fetchone()
        return row is not None


def verify_follow(token: str) -> bool:
    """Verify the follow with this token — and any other pending follows the
    same address registered (one click confirms the address)."""
    with _sub_connect() as conn:
        row = conn.execute(
            "SELECT email FROM follows WHERE token=? "
            "AND created_at >= datetime('now', '-14 days')", (token,)).fetchone()
        if not row:
            return False
        conn.execute("UPDATE follows SET verified=1 WHERE email=?", (row[0],))
        return True


def unfollow(token: str) -> bool:
    """Remove a single follow by its token."""
    with _sub_connect() as conn:
        cur = conn.execute("DELETE FROM follows WHERE token=?", (token,))
        return cur.rowcount > 0


def get_verified_follows() -> list[dict]:
    """All verified follows (for the weekly alert matcher)."""
    conn = sqlite3.connect(SUBSCRIBERS_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM follows WHERE verified=1").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_book(slug: str):
    """One book and every review of it, for /book/<slug>.

    Reviews carry the same normalised key the books table was built from, so a
    book page can never show another book's reviews. Single indexed lookup.
    """
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    book = conn.execute("SELECT * FROM books WHERE slug = ?", (slug,)).fetchone()
    if not book:
        return None
    reviews = conn.execute(
        """SELECT * FROM reviews WHERE book_key = ?
           ORDER BY publication_date DESC, id DESC""",
        (book["book_key"],)).fetchall()
    return {"book": dict(book), "reviews": [dict(r) for r in reviews]}


def get_books_by_author(author_last: str, exclude_slug: str = "", limit: int = 8):
    """Other books by the same author, for cross-linking book pages.

    Surname-only, which is how the author is stored; a common surname will mix
    two people, so this is presented as a browse aid rather than a claim.
    """
    if not (author_last or "").strip():
        return []
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(
        """SELECT slug, title, review_count FROM books
           WHERE lower(author_last) = lower(?) AND slug <> ?
           ORDER BY review_count DESC, title LIMIT ?""",
        (author_last.strip(), exclude_slug, limit))]


def get_books_for_sitemap(min_reviews: int = 1, limit: int = 0, offset: int = 0):
    """Slugs for the sitemap, most-reviewed first so crawl budget lands well."""
    conn = _get_read_conn()
    conn.row_factory = sqlite3.Row
    sql = ("SELECT slug, review_count, last_year FROM books WHERE review_count >= ? "
           "ORDER BY review_count DESC, slug")
    params = [min_reviews]
    if limit:
        sql += " LIMIT ? OFFSET ?"
        params += [limit, offset]
    return [dict(r) for r in conn.execute(sql, params)]


def slugs_for_reviews(book_keys):
    """Map book_key -> slug for a page of results, in one query.

    Search results link to book pages, and looking each up individually would
    be 50 round trips per page.
    """
    keys = [k for k in dict.fromkeys(book_keys) if k]
    if not keys:
        return {}
    conn = _get_read_conn()
    out = {}
    for i in range(0, len(keys), 400):
        chunk = keys[i:i + 400]
        ph = ",".join("?" * len(chunk))
        for k, slug in conn.execute(
                f"SELECT book_key, slug FROM books WHERE book_key IN ({ph})", chunk):
            out[k] = slug
    return out


def get_total_reviews() -> int:
    return _get_total_count()


def count_sources() -> int:
    conn = _get_read_conn()
    return conn.execute("SELECT COUNT(DISTINCT publication_source) FROM reviews").fetchone()[0]


def count_books(min_reviews: int = 1) -> int:
    conn = _get_read_conn()
    return conn.execute("SELECT COUNT(*) FROM books WHERE review_count >= ?",
                        (min_reviews,)).fetchone()[0]


def update_subscriber_last_sent(subscriber_id: int, date_str: str):
    """Update the last_sent_date for a subscriber."""
    with _sub_connect() as conn:
        conn.execute(
            "UPDATE subscribers SET last_sent_date = ? WHERE id = ?",
            (date_str, subscriber_id),
        )


# Auto-init on import
init_db()

# Warm up caches so first request is fast
try:
    get_metadata()
    search_reviews()  # cache default landing page query
except Exception:
    pass
