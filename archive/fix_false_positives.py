#!/usr/bin/env python3
"""
Systematic false positive cleanup: check Crossref type for recent entries
and delete those that are regular articles, not book reviews.

The possessive-title parser in crossref_scraper.py was too aggressive,
matching article titles like "Author's Topic..." as book reviews.
"""

import re
import sqlite3
import sys
import time
import os

import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import db

DB_PATH = db.DB_PATH
SESSION = requests.Session()
SESSION.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

# Crossref types that indicate a real book review
REVIEW_TYPES = {'book-review', 'peer-review'}


def crossref_type(doi):
    """Look up a DOI on Crossref and return just the type."""
    url = f"https://api.crossref.org/works/{doi}"
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get('message', {})
            return data.get('type', ''), data.get('title', [''])[0]
        elif resp.status_code == 404:
            return None, None
        else:
            return None, None
    except Exception:
        return None, None


def title_looks_like_review(title):
    """Check if a Crossref title looks like a book review."""
    if not title:
        return False
    if re.match(r'(?:book\s+)?reviews?\b', title, re.I):
        return True
    if re.search(r'reviewed?\s+works?', title, re.I):
        return True
    # Compound title (two parts without space)
    if re.search(r'[a-z][A-Z][a-z]', title) and len(title) > 30:
        return True
    # Publisher/bibliographic info
    if re.search(r'(?:University Press|Routledge|Springer|Clarendon|Macmillan|'
                 r'Pp\.\s*\d|pp\.\s*\d|\$\d|\d+\s*pp\.?)', title):
        return True
    # Period + "By Author"
    if re.search(r'\.\s+(?:By|by)\s+[A-Z]', title):
        return True
    # Period + Author initials
    if re.search(r'\.\s+[A-Z]\.\s*[A-Z]', title):
        return True
    # "(City: Publisher, Year)"
    if re.search(r'\([A-Z][a-z]+:\s+[A-Z]', title):
        return True
    return False


def scan_and_clean(min_id=0, dry_run=False):
    """Scan entries from min_id onwards and delete false positives."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get all entries with DOIs from the target range
    rows = conn.execute("""
        SELECT id, book_title, book_author_first_name, book_author_last_name,
               reviewer_first_name, reviewer_last_name, doi, publication_source
        FROM reviews
        WHERE id > ? AND doi IS NOT NULL AND doi != ''
        ORDER BY id
    """, (min_id,)).fetchall()

    print(f"Scanning {len(rows)} entries (id > {min_id})...")

    delete_ids = []
    keep_count = 0
    failed = 0
    journal_stats = {}  # journal → {delete: N, keep: N}

    for i, row in enumerate(rows):
        if i > 0 and i % 10 == 0:
            time.sleep(1)
        if i % 100 == 0:
            print(f"  Processing {i}/{len(rows)}...")

        doi = row['doi']
        cr_type, cr_title = crossref_type(doi)

        if cr_type is None:
            failed += 1
            continue

        journal = row['publication_source']
        if journal not in journal_stats:
            journal_stats[journal] = {'delete': 0, 'keep': 0}

        is_review = (cr_type in REVIEW_TYPES
                     or title_looks_like_review(cr_title or ''))

        if is_review:
            keep_count += 1
            journal_stats[journal]['keep'] += 1
        else:
            delete_ids.append(row['id'])
            journal_stats[journal]['delete'] += 1
            if len(delete_ids) <= 30:
                print(f"  DELETE #{row['id']}: [{journal}] '{row['book_title'][:60]}' "
                      f"(cr_type={cr_type})")

    print(f"\nResults:")
    print(f"  {len(delete_ids)} false positives to delete")
    print(f"  {keep_count} legitimate reviews to keep")
    print(f"  {failed} Crossref lookups failed")

    print(f"\nPer-journal breakdown:")
    for j, stats in sorted(journal_stats.items(), key=lambda x: x[1]['delete'], reverse=True):
        if stats['delete'] > 0:
            print(f"  {j}: {stats['delete']} delete, {stats['keep']} keep")

    if not dry_run and delete_ids:
        for start in range(0, len(delete_ids), 500):
            batch = delete_ids[start:start+500]
            placeholders = ','.join('?' * len(batch))
            conn.execute(f"DELETE FROM reviews WHERE id IN ({placeholders})", batch)
        conn.commit()
        print(f"\nDeleted {len(delete_ids)} false positives.")
    elif dry_run:
        print("\n(dry run — no changes written)")

    # Also delete author=reviewer entries (precis/replies, not reviews)
    precis = conn.execute("""
        SELECT id, book_title, book_author_first_name, book_author_last_name, publication_source
        FROM reviews
        WHERE book_author_last_name = reviewer_last_name
        AND book_author_first_name = reviewer_first_name
        AND book_author_last_name IS NOT NULL AND book_author_last_name != ''
    """).fetchall()

    if precis:
        print(f"\nAuthor=Reviewer entries (precis/replies): {len(precis)}")
        for r in precis[:10]:
            print(f"  #{r['id']}: '{r['book_title'][:50]}' by {r['book_author_first_name']} {r['book_author_last_name']} [{r['publication_source']}]")
        if not dry_run:
            precis_ids = [r['id'] for r in precis]
            placeholders = ','.join('?' * len(precis_ids))
            conn.execute(f"DELETE FROM reviews WHERE id IN ({placeholders})", precis_ids)
            conn.commit()
            print(f"  Deleted {len(precis_ids)} precis/reply entries.")
        else:
            print("  (dry run — not deleted)")

    conn.close()
    return len(delete_ids)


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv

    # Default: scan entries from the recent Crossref import batch
    min_id = 86000
    for arg in sys.argv[1:]:
        if arg.startswith('--min-id='):
            min_id = int(arg.split('=')[1])

    if dry_run:
        print("=== DRY RUN ===\n")

    deleted = scan_and_clean(min_id=min_id, dry_run=dry_run)

    print(f"\nTotal DB size: ", end="")
    conn = sqlite3.connect(DB_PATH)
    print(conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0])
    conn.close()
