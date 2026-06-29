#!/usr/bin/env python3
"""
Philosopher's Index (EBSCO) Import Pipeline

Imports book review metadata from PI CSV exports into the PhilReviews database.
Designed to be resumable — state is tracked in a pi_staging SQLite table.

Stages:
  --ingest   Load CSVs into staging table (dedup by accession number)
  --match    Match staging rows against existing DB reviews
  --resolve  Resolve DOIs via Crossref and insert new reviews
  --classify Run subfield classification on new entries

Usage:
  python pi_import.py --ingest /path/to/*.csv
  python pi_import.py --match
  python pi_import.py --resolve
  python pi_import.py --classify
"""

import argparse
import csv
import glob
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db")

# ---------------------------------------------------------------------------
# Journal name normalization: PI name → DB publication_source name
# Only entries where the names differ need to be listed.
# Exact matches (e.g. "Analysis" → "Analysis") are handled automatically.
# ---------------------------------------------------------------------------

PI_JOURNAL_MAP = {
    # Subtitle stripping
    "Acta Analytica: International Periodical for Philosophy in the Analytical Tradition": "Acta Analytica",
    "Constellations: An International Journal of Critical and Democratic Theory": "Constellations",
    "Dao: A Journal of Comparative Philosophy": "Dao",
    "Dialectica: International Journal of Philosophy of Knowledge": "Dialectica",
    "Environmental Ethics: An Interdisciplinary Journal Dedicated to the Philosophical Aspects of Environmental Problems": "Environmental Ethics",
    "Environmental Philosophy: The Journal of the International Association for Environmental Philosophy": "Environmental Philosophy",
    "Erkenntnis: An International Journal of Analytic Philosophy": "Erkenntnis",
    "Ethical Perspectives: Journal of the European Ethics Network": "Ethical Perspectives",
    "Faith and Philosophy: Journal of the Society of Christian Philosophers": "Faith and Philosophy",
    "Filosofia Theoretica: Journal of African Philosophy, Culture and Religions": "Filosofia Theoretica",
    "Grazer Philosophische Studien: Internationale Zeitschrift für Analytische Philosophie": "Grazer Philosophische Studien",
    "Human Studies: A Journal for Philosophy and the Social Sciences": "Human Studies",
    "Inquiry: An Interdisciplinary Journal of Philosophy": "Inquiry",
    "Journal of Medical Ethics: The Journal of the Institute of Medical Ethics": "Journal of Medical Ethics",
    "Journal of Speculative Philosophy: A Quarterly Journal of History, Criticism, and Imagination": "The Journal of Speculative Philosophy",
    "Jurisprudence: An International Journal of Legal and Political Thought": "Jurisprudence",
    "Medicine, Health Care and Philosophy: A European Journal": "Medicine, Health Care and Philosophy",
    "Metascience: An International Review Journal for the History, Philosophy and Social Studies of Science": "Metascience",
    "Political Theory: An International Journal of Political Philosophy": "Political Theory",
    "Reason Papers: A Journal of Interdisciplinary Normative Studies": "Reason Papers",
    "Religious Studies: An International Journal for the Philosophy of Religion": "Religious Studies",
    "Sophia: International Journal for Philosophy of Religion, Metaphysical Theology and Ethics": "Sophia",
    "Studia Leibnitiana: Zeitschrift fuer Geschichte der Philosophie und der Wissenschaften": "Studia Leibnitiana",
    "Studia Logica: An International Journal for Symbolic Logic": "Studia Logica",
    "Synthese: An International Journal for Epistemology, Methodology and Philosophy of Science": "Synthese",
    "The Review of Metaphysics: A Philosophical Quarterly": "The Review of Metaphysics",
    "Theoretical Medicine and Bioethics: Philosophy of Medical Research and Practice": "Theoretical Medicine and Bioethics",
    "Topoi: An International Review of Philosophy": "Topoi",
    "Philosophy: The Journal of the Royal Institute of Philosophy": "Philosophy",

    # & vs "and"
    "American Journal of Theology and Philosophy": "American Journal of Theology & Philosophy",
    "Arabic Sciences & Philosophy": "Arabic Sciences and Philosophy",
    "Biology & Philosophy": "Biology and Philosophy",
    "Business and Professional Ethics Journal": "Business & Professional Ethics Journal",
    "Critical Review of International Social & Political Philosophy": "Critical Review of International Social and Political Philosophy",
    "Economics & Philosophy": "Economics and Philosophy",
    "Educational Philosophy & Theory": "Educational Philosophy and Theory",
    "Ethical Theory & Moral Practice": "Ethical Theory and Moral Practice",
    "History & Philosophy of Logic": "History and Philosophy of Logic",
    "History of Philosophy & Logical Analysis": "History of Philosophy and Logical Analysis",
    "Law & Philosophy": "Law and Philosophy",
    "Mind and Language": "Mind & Language",
    "Minds & Machines": "Minds and Machines",
    "Philosophy & Phenomenological Research": "Philosophy and Phenomenological Research",
    "Philosophy East & West: A Quarterly of Comparative Philosophy": "Philosophy East and West",
    "Philosophy and Rhetoric": "Philosophy & Rhetoric",
    "Philosophy and Social Criticism": "Philosophy & Social Criticism",
    "Science & Engineering Ethics": "Science and Engineering Ethics",
    "Social Theory & Practice": "Social Theory and Practice",
    "Studies in Philosophy & Education": "Studies in Philosophy and Education",

    # "The" prefix differences
    "British Journal for the Philosophy of Science": "The British Journal for the Philosophy of Science",
    "British Journal of Aesthetics": "The British Journal of Aesthetics",
    "Heythrop Journal": "The Heythrop Journal",
    "Journal of Aesthetics & Art Criticism": "The Journal of Aesthetics and Art Criticism",
    "Journal of Ethics": "The Journal of Ethics",
    "Journal of Philosophy": "The Journal of Philosophy",
    "Journal of Religious Ethics": "The Journal of Religious Ethics",
    "Philosophical Forum": "The Philosophical Forum",
    "Philosophical Quarterly": "The Philosophical Quarterly",
    "Thomist: A Speculative Quarterly Review": "The Thomist: A Speculative Quarterly Review",

    # Parenthetical/ISSN removal
    "Mind (0026-4423)": "Mind",

    # Subtitle + prefix combos
    "Canadian Journal of Law and Jurisprudence: An International Journal of Legal Thought": "The Canadian Journal of Law and Jurisprudence",
    "Dialogue: Canadian Philosophical Review": "Dialogue",
    "HOPOS: The International Society for the History of Philosophy of Science": "HOPOS: The Journal of the International Society for the History of Philosophy of Science",
    "Iyyun: Journal of Philosophy": "Iyyun: The Jerusalem Philosophical Quarterly / עיון: רבעון פילוסופי",

    # Philosophia variants — map the Israel-based quarterly (most reviews); others stay as-is
    "Philosophia: Philosophical Quarterly of Israel": "Philosophia",

    # Theoria — the social/political theory one matches our DB
    "Theoria: A Journal of Social and Political Theory": "Theoria",

    # Res Publica — the legal/social philosophy one matches our DB
    "Res Publica: A Journal of Legal and Social Philosophy": "Res Publica",

    # WRONG MATCHES to exclude — these are different journals:
    # "Inquiry: Critical Thinking across the Disciplines" is NOT our "Inquiry"
    # "Journal of Philosophy: A Cross-Disciplinary Inquiry" is NOT "The Journal of Philosophy"
    # "Philosophical Review (1015-8995)" is NOT "The Philosophical Review"
    # "philoSOPHIA: A Journal of Continental Feminism" is NOT "Philosophia"
    # "Dialogue: Journal of Phi Sigma Tau" is NOT our "Dialogue" (Canadian Phil Review)
    # These are handled by NOT including them in the map, so they stay as-is.

    # Erkenntnis alternate subtitle
    "Erkenntnis: An International Journal of Scientific Philosophy": "Erkenntnis",
}

# Journals from PI that are definitely NOT the same as our DB journals
# even though normalization would make them look alike.
PI_JOURNAL_EXCLUDE_MAP = {
    "Inquiry: Critical Thinking across the Disciplines",
    "Journal of Philosophy: A Cross-Disciplinary Inquiry",
    "Philosophical Review (1015-8995)",
    "philoSOPHIA: A Journal of Continental Feminism",
    "Dialogue: Journal of Phi Sigma Tau",
    "Philosophia: Anuario de Filosofía",
    "Philosophia: International Journal of Philosophy",
    "Philosophia: Yearbook of the Research Center for Greek Philosophy at the Academy of Athens",
    "Theoria: A Swedish Journal of Philosophy",
    "Theoria: Revista de Teoria, Historia y Fundamentos de la Ciencia",
    "Res Publica: Revista de Historia de las Ideas Políticas",
}


def get_conn():
    return sqlite3.connect(DB_PATH)


def normalize_journal(raw_name: str) -> str:
    """Map a PI journal name to its DB equivalent, or return as-is."""
    name = raw_name.strip()
    if name in PI_JOURNAL_MAP:
        return PI_JOURNAL_MAP[name]
    return name


def normalize_title_for_match(title: str) -> str:
    """Normalize a book title for fuzzy matching."""
    t = title.lower().strip()
    # Remove common subtitle separators and everything after
    # (but keep it for storage — only strip for matching)
    t = re.sub(r'<[^>]+>', '', t)  # strip HTML tags
    t = re.sub(r'[^a-z0-9 ]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def parse_contributors(contributors: str):
    """
    Parse PI contributors field into book author(s) and reviewer.

    Format: "BookAuthor1, First ; [BookAuthor2, First ;] Reviewer, First"
    Last entry = reviewer, all others = book author(s).
    Single entry = book author only (reviewer unknown).

    Returns: (book_author_first, book_author_last, reviewer_first, reviewer_last)
    """
    if not contributors or not contributors.strip():
        return ('', '', '', '')

    parts = [p.strip() for p in contributors.split(' ; ')]

    if len(parts) == 1:
        # Single contributor = book author, no reviewer
        first, last = _parse_name(parts[0])
        return (first, last, '', '')

    # Last entry = reviewer
    reviewer_first, reviewer_last = _parse_name(parts[-1])

    # First entry = primary book author (we only store one in the DB)
    author_first, author_last = _parse_name(parts[0])

    return (author_first, author_last, reviewer_first, reviewer_last)


def _parse_name(name_str: str):
    """Parse 'Last, First' into (first, last). Handle edge cases."""
    name_str = name_str.strip()
    if not name_str:
        return ('', '')

    if ',' in name_str:
        parts = name_str.split(',', 1)
        last = parts[0].strip()
        first = parts[1].strip() if len(parts) > 1 else ''
        return (first, last)

    # No comma — might be a single name or "First Last"
    words = name_str.split()
    if len(words) == 1:
        return ('', words[0])
    # Assume last word is the last name
    return (' '.join(words[:-1]), words[-1])


def parse_date(date_str: str) -> str:
    """Parse YYYYMMDD → YYYY-MM-DD."""
    date_str = date_str.strip()
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    if len(date_str) == 6 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-01"
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return date_str


def parse_pages(page_str: str) -> str:
    """Clean page number string."""
    return page_str.strip() if page_str else ''


# ---------------------------------------------------------------------------
# Staging table management
# ---------------------------------------------------------------------------

_STAGING_SCHEMA = """
CREATE TABLE IF NOT EXISTS pi_staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    an TEXT UNIQUE,
    book_title TEXT,
    book_author_first TEXT,
    book_author_last TEXT,
    reviewer_first TEXT,
    reviewer_last TEXT,
    journal_name TEXT,
    journal_name_raw TEXT,
    issn TEXT,
    isbn TEXT,
    pub_date TEXT,
    volume TEXT,
    issue TEXT,
    page_start TEXT,
    page_end TEXT,
    publisher TEXT,
    plink TEXT,
    status TEXT DEFAULT 'pending',
    match_id INTEGER,
    doi_resolved TEXT,
    review_url_resolved TEXT
);
CREATE INDEX IF NOT EXISTS idx_pi_status ON pi_staging(status);
CREATE INDEX IF NOT EXISTS idx_pi_journal ON pi_staging(journal_name);
"""


def init_staging(conn):
    """Create the pi_staging table if it doesn't exist."""
    conn.executescript(_STAGING_SCHEMA)


# ---------------------------------------------------------------------------
# Stage 1: Ingest
# ---------------------------------------------------------------------------

def stage_ingest(csv_paths: list[str]):
    """Load PI CSV files into the staging table."""
    conn = get_conn()
    init_staging(conn)

    # Count existing staging rows
    existing = conn.execute("SELECT COUNT(*) FROM pi_staging").fetchone()[0]
    print(f"Existing staging rows: {existing}")

    total_read = 0
    total_inserted = 0
    total_skipped = 0

    for csv_path in csv_paths:
        if not os.path.exists(csv_path):
            print(f"  SKIP: {csv_path} not found")
            continue

        print(f"  Loading {os.path.basename(csv_path)}...")
        rows_in_file = 0
        inserted_from_file = 0

        with open(csv_path, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or 'an' not in reader.fieldnames:
                print(f"    SKIP: missing 'an' column")
                continue

            batch = []
            for row in reader:
                rows_in_file += 1
                an = row.get('an', '').strip()
                if not an:
                    continue

                title = row.get('title', '').strip()
                if not title:
                    continue

                contributors = row.get('contributors', '').strip()
                author_first, author_last, rev_first, rev_last = parse_contributors(contributors)

                raw_journal = row.get('source', '').strip()
                journal = normalize_journal(raw_journal)
                pub_date = parse_date(row.get('publicationDate', ''))
                issn = row.get('issns', '').strip()
                isbn = row.get('isbns', '').strip()
                volume = row.get('volume', '').strip()
                issue = row.get('issue', '').strip()
                page_start = parse_pages(row.get('pageStart', ''))
                page_end = parse_pages(row.get('pageEnd', ''))
                publisher = row.get('publisher', '').strip()
                plink = row.get('plink', '').strip()

                batch.append((
                    an, title, author_first, author_last,
                    rev_first, rev_last, journal, raw_journal,
                    issn, isbn, pub_date, volume, issue,
                    page_start, page_end, publisher, plink,
                ))

                if len(batch) >= 1000:
                    ins = _insert_staging_batch(conn, batch)
                    inserted_from_file += ins
                    batch = []

            if batch:
                ins = _insert_staging_batch(conn, batch)
                inserted_from_file += ins

        total_read += rows_in_file
        total_inserted += inserted_from_file
        skipped = rows_in_file - inserted_from_file
        total_skipped += skipped
        print(f"    {rows_in_file} rows read, {inserted_from_file} inserted, {skipped} skipped (dupes)")

    conn.commit()
    conn.close()

    final_count = _count_staging()
    print(f"\nIngest complete: {total_read} rows read, {total_inserted} new, {total_skipped} skipped")
    print(f"Total staging rows: {final_count}")

    # Print status breakdown
    _print_status_counts()


def _insert_staging_batch(conn, batch):
    """Insert a batch of rows, ignoring duplicate ANs. Returns count inserted."""
    cursor = conn.executemany(
        """INSERT OR IGNORE INTO pi_staging
           (an, book_title, book_author_first, book_author_last,
            reviewer_first, reviewer_last, journal_name, journal_name_raw,
            issn, isbn, pub_date, volume, issue,
            page_start, page_end, publisher, plink)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch,
    )
    return cursor.rowcount


def _count_staging():
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM pi_staging").fetchone()[0]
    conn.close()
    return count


def _print_status_counts():
    conn = get_conn()
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM pi_staging GROUP BY status ORDER BY status"
    ).fetchall()
    conn.close()
    print("Status breakdown:")
    for status, count in rows:
        print(f"  {status}: {count}")


# ---------------------------------------------------------------------------
# Stage 2: Match
# ---------------------------------------------------------------------------

def stage_match():
    """Match staging rows against existing reviews in the DB."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    init_staging(conn)

    pending = conn.execute(
        "SELECT * FROM pi_staging WHERE status = 'pending'"
    ).fetchall()
    print(f"Pending rows to match: {len(pending)}")

    if not pending:
        print("Nothing to match.")
        return

    # Build an index of existing reviews by normalized journal name
    # for faster lookups
    reviews = conn.execute(
        "SELECT id, book_title, book_author_first_name, book_author_last_name, "
        "reviewer_first_name, reviewer_last_name, publication_source, publication_date "
        "FROM reviews"
    ).fetchall()

    # Index by publication_source (lowercased) for fast lookup
    review_index = {}
    for r in reviews:
        src = (r['publication_source'] or '').lower()
        if src not in review_index:
            review_index[src] = []
        review_index[src].append(dict(r))

    print(f"Indexed {len(reviews)} existing reviews across {len(review_index)} journals")

    matched = 0
    unmatched = 0
    corrections = 0
    batch_size = 500

    for i, row in enumerate(pending):
        row = dict(row)
        journal_lower = (row['journal_name'] or '').lower()

        # Skip if journal is in the exclude list (different journal, same normalized name)
        if row['journal_name_raw'] in PI_JOURNAL_EXCLUDE_MAP:
            # These are distinct journals that shouldn't match our DB entries
            # but we still want to import them as new entries
            conn.execute(
                "UPDATE pi_staging SET status = 'unmatched' WHERE id = ?",
                (row['id'],)
            )
            unmatched += 1
            if (i + 1) % batch_size == 0:
                conn.commit()
                print(f"  Processed {i + 1}/{len(pending)} ({matched} matched, {unmatched} unmatched)...")
            continue

        candidates = review_index.get(journal_lower, [])
        best_match = None
        best_score = 0

        if candidates:
            pi_title_norm = normalize_title_for_match(row['book_title'])
            pi_title_40 = pi_title_norm[:40]

            for cand in candidates:
                cand_title_norm = normalize_title_for_match(cand['book_title'] or '')
                score = 0

                # Title matching
                if pi_title_norm and cand_title_norm:
                    if pi_title_norm == cand_title_norm:
                        score += 10
                    elif pi_title_norm in cand_title_norm or cand_title_norm in pi_title_norm:
                        score += 7
                    elif pi_title_40 and cand_title_norm[:40] == pi_title_40:
                        score += 5
                    else:
                        continue  # No title match at all, skip

                # Author last name match
                if (row['book_author_last'] and cand['book_author_last_name']
                        and row['book_author_last'].lower() == cand['book_author_last_name'].lower()):
                    score += 3

                # Year proximity
                pi_year = row['pub_date'][:4] if row['pub_date'] else ''
                cand_year = (cand['publication_date'] or '')[:4]
                if pi_year and cand_year:
                    try:
                        if abs(int(pi_year) - int(cand_year)) <= 1:
                            score += 2
                    except ValueError:
                        pass

                if score > best_score:
                    best_score = score
                    best_match = cand

        if best_score >= 7 and best_match:
            matched += 1
            conn.execute(
                "UPDATE pi_staging SET status = 'matched', match_id = ? WHERE id = ?",
                (best_match['id'], row['id'])
            )

            # Apply corrections: fill gaps or correct names in the DB
            corrections += _apply_corrections(conn, row, best_match)
        else:
            unmatched += 1
            conn.execute(
                "UPDATE pi_staging SET status = 'unmatched' WHERE id = ?",
                (row['id'],)
            )

        if (i + 1) % batch_size == 0:
            conn.commit()
            print(f"  Processed {i + 1}/{len(pending)} ({matched} matched, {unmatched} unmatched, {corrections} corrections)...")

    conn.commit()
    conn.close()

    print(f"\nMatch complete: {matched} matched, {unmatched} unmatched, {corrections} corrections applied")
    _print_status_counts()


def _apply_corrections(conn, pi_row, db_row) -> int:
    """Compare PI data with DB data and apply corrections. Returns count of corrections.

    Prefers PI data for names (overwrites, not just gap-filling).
    Preserves symposia classifications (entry_type, symposium_group)
    and bracket labels in book_title ([Précis], [Author's Reply]).
    """
    updates = {}
    corrections = 0

    # Overwrite reviewer with PI data (prefer PI names)
    if pi_row['reviewer_last']:
        db_rev = (db_row['reviewer_last_name'] or '').strip()
        pi_rev = pi_row['reviewer_last'].strip()
        if not db_rev or db_rev != pi_rev:
            updates['reviewer_first_name'] = pi_row['reviewer_first']
            updates['reviewer_last_name'] = pi_row['reviewer_last']

    # Overwrite book author with PI data (prefer PI names)
    if pi_row['book_author_last']:
        db_auth = (db_row['book_author_last_name'] or '').strip()
        pi_auth = pi_row['book_author_last'].strip()
        if not db_auth or db_auth != pi_auth:
            updates['book_author_first_name'] = pi_row['book_author_first']
            updates['book_author_last_name'] = pi_row['book_author_last']

    # NOTE: We do NOT update book_title (preserves [Précis]/[Author's Reply] labels),
    # entry_type (preserves 'symposium'), or symposium_group.

    if updates:
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [db_row['id']]
        conn.execute(
            f"UPDATE reviews SET {set_clause} WHERE id = ?",
            values,
        )
        corrections = 1

    return corrections


# ---------------------------------------------------------------------------
# Stage 3: Resolve & Insert
# ---------------------------------------------------------------------------

def stage_resolve(dry_run=False):
    """Resolve DOIs via Crossref and insert unmatched entries as new reviews."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    init_staging(conn)

    unmatched = conn.execute(
        "SELECT * FROM pi_staging WHERE status = 'unmatched' ORDER BY id"
    ).fetchall()
    print(f"Unmatched rows to resolve: {len(unmatched)}")

    if not unmatched:
        print("Nothing to resolve.")
        return

    crossref_email = os.getenv('CROSSREF_EMAIL', 'user@example.com')
    session = requests.Session()
    session.headers.update({
        'User-Agent': f'PhilReviews/2.0 (mailto:{crossref_email})',
    })

    inserted = 0
    resolved_dois = 0
    skipped_dupe = 0
    failed_resolve = 0
    total = len(unmatched)

    for i, row in enumerate(unmatched):
        row = dict(row)

        # Try Crossref DOI resolution
        doi = None
        review_url = None

        if not dry_run:
            doi = _resolve_doi_crossref(
                session, crossref_email,
                row['book_title'], row['issn'],
                row['volume'], row['issue'], row['page_start'],
                row['journal_name'],
            )

        if doi:
            resolved_dois += 1
            review_url = f"https://doi.org/{doi}"
            # Check if this DOI already exists in the DB
            existing = conn.execute(
                "SELECT 1 FROM reviews WHERE doi = ? LIMIT 1", (doi,)
            ).fetchone()
            if existing:
                # Already in DB — mark as matched (we missed it in stage 2)
                skipped_dupe += 1
                conn.execute(
                    "UPDATE pi_staging SET status = 'matched', doi_resolved = ? WHERE id = ?",
                    (doi, row['id'])
                )
                if (i + 1) % 500 == 0:
                    conn.commit()
                    _print_resolve_progress(i + 1, total, inserted, resolved_dois, skipped_dupe)
                continue
        else:
            # Use EBSCO plink as fallback URL
            review_url = row['plink'] or ''
            if review_url:
                # Check if this plink already exists
                existing = conn.execute(
                    "SELECT 1 FROM reviews WHERE review_link = ? LIMIT 1", (review_url,)
                ).fetchone()
                if existing:
                    skipped_dupe += 1
                    conn.execute(
                        "UPDATE pi_staging SET status = 'matched' WHERE id = ?",
                        (row['id'],)
                    )
                    if (i + 1) % 500 == 0:
                        conn.commit()
                        _print_resolve_progress(i + 1, total, inserted, resolved_dois, skipped_dupe)
                    continue

        if dry_run:
            conn.execute(
                "UPDATE pi_staging SET status = 'dry_run' WHERE id = ?",
                (row['id'],)
            )
            continue

        # Insert into reviews table
        pub_date = row['pub_date'] or ''
        # Normalize to YYYY-MM-DD or just YYYY
        if len(pub_date) >= 10:
            pub_date = pub_date[:10]

        conn.execute(
            """INSERT OR IGNORE INTO reviews
               (book_title, book_author_first_name, book_author_last_name,
                reviewer_first_name, reviewer_last_name,
                publication_source, publication_date, review_link,
                doi, entry_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'review')""",
            (
                row['book_title'],
                row['book_author_first'],
                row['book_author_last'],
                row['reviewer_first'],
                row['reviewer_last'],
                row['journal_name'],
                pub_date,
                review_url,
                doi or '',
            )
        )
        inserted += 1

        # Update staging row
        conn.execute(
            "UPDATE pi_staging SET status = 'inserted', doi_resolved = ?, review_url_resolved = ? WHERE id = ?",
            (doi or '', review_url or '', row['id'])
        )

        # Checkpoint every 500 entries
        if (i + 1) % 500 == 0:
            conn.commit()
            _print_resolve_progress(i + 1, total, inserted, resolved_dois, skipped_dupe)

        # Rate limit: ~10 req/sec for Crossref
        if doi is not None or (i + 1) % 10 == 0:
            time.sleep(0.1)

    conn.commit()
    conn.close()

    print(f"\nResolve complete:")
    print(f"  {inserted} new reviews inserted")
    print(f"  {resolved_dois} DOIs resolved via Crossref")
    print(f"  {skipped_dupe} duplicates skipped (already in DB)")
    print(f"  {total - inserted - skipped_dupe} unresolved (inserted with plink)")
    _print_status_counts()


def _print_resolve_progress(i, total, inserted, resolved, skipped):
    pct = i / total * 100
    print(f"  [{i}/{total} ({pct:.1f}%)] inserted={inserted} doi_resolved={resolved} skipped_dupe={skipped}")


def _resolve_doi_crossref(session, email, title, issn, volume, issue, page_start, journal_name):
    """
    Try to find a DOI for this review via Crossref API.
    Returns DOI string or None.
    """
    if not title:
        return None

    params = {
        'query.bibliographic': title,
        'rows': 5,
        'mailto': email,
    }
    if issn:
        # Use first ISSN if multiple
        first_issn = issn.split(';')[0].strip().split(',')[0].strip()
        if first_issn:
            params['filter'] = f'issn:{first_issn}'

    try:
        resp = session.get(
            'https://api.crossref.org/works',
            params=params,
            timeout=30,
        )
        if resp.status_code == 429:
            # Rate limited — wait and skip
            time.sleep(5)
            return None
        resp.raise_for_status()
        data = resp.json()
        items = data.get('message', {}).get('items', [])
    except Exception:
        return None

    if not items:
        return None

    # Score each candidate
    title_norm = normalize_title_for_match(title)
    best_doi = None
    best_score = 0

    for item in items:
        item_title = (item.get('title', ['']) or [''])[0]
        item_title_norm = normalize_title_for_match(item_title)

        score = 0

        # Title similarity
        if title_norm and item_title_norm:
            if title_norm == item_title_norm:
                score += 10
            elif title_norm[:40] == item_title_norm[:40] and len(title_norm) >= 10:
                score += 7
            elif title_norm in item_title_norm or item_title_norm in title_norm:
                score += 6
            else:
                # Word overlap
                t_words = set(title_norm.split())
                i_words = set(item_title_norm.split())
                if t_words and i_words:
                    overlap = len(t_words & i_words) / max(len(t_words), len(i_words))
                    if overlap >= 0.7:
                        score += 5
                    else:
                        continue
                else:
                    continue

        # Volume match
        item_vol = item.get('volume', '')
        if volume and item_vol and volume == item_vol:
            score += 2

        # Page match
        item_page = item.get('page', '')
        if page_start and item_page and page_start in item_page:
            score += 2

        if score > best_score:
            best_score = score
            best_doi = item.get('DOI')

    if best_score >= 7 and best_doi:
        return best_doi

    return None


# ---------------------------------------------------------------------------
# Stage 4: Classify
# ---------------------------------------------------------------------------

def stage_classify():
    """Run subfield classification on newly inserted entries."""
    try:
        from classify_subfields import classify_new_reviews
        print("Running subfield classification on new entries...")
        classify_new_reviews()
        print("Classification complete.")
    except ImportError:
        print("ERROR: classify_subfields.py not found. Skipping classification.")
    except Exception as e:
        print(f"ERROR during classification: {e}")


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------

def verify_ingest():
    """Print verification stats after ingest."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    init_staging(conn)

    total = conn.execute("SELECT COUNT(*) FROM pi_staging").fetchone()[0]
    print(f"\n=== Ingest Verification ===")
    print(f"Total staging rows: {total}")

    # Journal distribution (top 20)
    journals = conn.execute(
        "SELECT journal_name, COUNT(*) as cnt FROM pi_staging "
        "GROUP BY journal_name ORDER BY cnt DESC LIMIT 20"
    ).fetchall()
    print(f"\nTop 20 journals:")
    for j in journals:
        print(f"  {j['cnt']:>5}  {j['journal_name']}")

    # Date range
    dates = conn.execute(
        "SELECT MIN(pub_date), MAX(pub_date) FROM pi_staging WHERE pub_date != ''"
    ).fetchone()
    print(f"\nDate range: {dates[0]} to {dates[1]}")

    # Missing fields
    no_author = conn.execute(
        "SELECT COUNT(*) FROM pi_staging WHERE book_author_last = '' OR book_author_last IS NULL"
    ).fetchone()[0]
    no_reviewer = conn.execute(
        "SELECT COUNT(*) FROM pi_staging WHERE reviewer_last = '' OR reviewer_last IS NULL"
    ).fetchone()[0]
    print(f"Missing book author: {no_author}")
    print(f"Missing reviewer: {no_reviewer}")

    # Spot check 10 entries
    samples = conn.execute(
        "SELECT an, book_title, book_author_first, book_author_last, "
        "reviewer_first, reviewer_last, journal_name, pub_date "
        "FROM pi_staging ORDER BY RANDOM() LIMIT 10"
    ).fetchall()
    print(f"\nSpot check (10 random entries):")
    for s in samples:
        print(f"  [{s['an']}] \"{s['book_title'][:60]}\"")
        print(f"    Author: {s['book_author_first']} {s['book_author_last']} | "
              f"Reviewer: {s['reviewer_first']} {s['reviewer_last']}")
        print(f"    Journal: {s['journal_name']} | Date: {s['pub_date']}")

    conn.close()


def verify_match():
    """Print verification stats after matching."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    init_staging(conn)

    print(f"\n=== Match Verification ===")
    _print_status_counts()

    # Sample matched entries
    matched = conn.execute(
        "SELECT s.book_title as pi_title, s.journal_name as pi_journal, "
        "r.book_title as db_title, r.publication_source as db_journal "
        "FROM pi_staging s JOIN reviews r ON s.match_id = r.id "
        "WHERE s.status = 'matched' ORDER BY RANDOM() LIMIT 10"
    ).fetchall()

    if matched:
        print(f"\nSample matches (10 random):")
        for m in matched:
            print(f"  PI:  \"{m['pi_title'][:60]}\" ({m['pi_journal']})")
            print(f"  DB:  \"{m['db_title'][:60]}\" ({m['db_journal']})")
            print()

    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Philosopher's Index Import Pipeline")
    parser.add_argument('--ingest', nargs='*', metavar='CSV',
                        help='Ingest CSV files into staging table')
    parser.add_argument('--match', action='store_true',
                        help='Match staging rows against existing reviews')
    parser.add_argument('--resolve', action='store_true',
                        help='Resolve DOIs and insert new reviews')
    parser.add_argument('--classify', action='store_true',
                        help='Run subfield classification on new entries')
    parser.add_argument('--verify', choices=['ingest', 'match'],
                        help='Run verification checks')
    parser.add_argument('--status', action='store_true',
                        help='Print staging table status counts')
    parser.add_argument('--dry-run', action='store_true',
                        help='Resolve stage: find DOIs but do not insert')

    args = parser.parse_args()

    if args.status:
        _print_status_counts()
        return

    if args.verify:
        if args.verify == 'ingest':
            verify_ingest()
        elif args.verify == 'match':
            verify_match()
        return

    ran_something = False

    if args.ingest is not None:
        csv_paths = args.ingest
        if not csv_paths:
            # Default: find PI CSV files in Downloads
            csv_paths = _find_pi_csvs()
        if not csv_paths:
            print("No CSV files specified or found. Use: --ingest file1.csv file2.csv ...")
            return
        print(f"=== Stage 1: Ingest ({len(csv_paths)} files) ===")
        stage_ingest(csv_paths)
        ran_something = True

    if args.match:
        print(f"\n=== Stage 2: Match ===")
        stage_match()
        ran_something = True

    if args.resolve:
        print(f"\n=== Stage 3: Resolve & Insert ===")
        stage_resolve(dry_run=args.dry_run)
        ran_something = True

    if args.classify:
        print(f"\n=== Stage 4: Classify ===")
        stage_classify()
        ran_something = True

    if not ran_something:
        parser.print_help()


def _find_pi_csvs():
    """Auto-detect PI CSV files in Downloads folder."""
    downloads = os.path.expanduser("~/Downloads")
    candidates = []
    for f in glob.glob(os.path.join(downloads, "*.csv")):
        try:
            with open(f, encoding='utf-8-sig') as fh:
                reader = csv.DictReader(fh)
                if reader.fieldnames and 'an' in reader.fieldnames and 'shortDBName' in reader.fieldnames:
                    candidates.append(f)
        except Exception:
            pass
    if candidates:
        print(f"Auto-detected {len(candidates)} PI CSV files in {downloads}")
    return sorted(candidates)


if __name__ == '__main__':
    main()
