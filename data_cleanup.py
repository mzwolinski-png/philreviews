#!/usr/bin/env python3
"""
One-time data quality sweep:
1. Re-fetch raw Crossref titles for entries with missing authors
2. Re-parse with improved parser
3. Clean overly long titles (strip bibliographic metadata)
4. Update the database
"""

import re
import sys
import time
import sqlite3
import requests
from crossref_scraper import parse_review_title, _looks_like_author_name, _extract_first_author

DB_PATH = 'reviews.db'
SESSION = requests.Session()
SESSION.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'


def get_missing_author_entries():
    """Get all entries with missing book authors."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, doi, book_title, book_author_first_name, book_author_last_name, "
        "publication_source FROM reviews "
        "WHERE (book_author_last_name IS NULL OR book_author_last_name = '')"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_crossref_title(doi):
    """Fetch the raw title from Crossref for a given DOI."""
    try:
        resp = SESSION.get(f'https://api.crossref.org/works/{doi}', timeout=15)
        if resp.status_code == 200:
            data = resp.json()['message']
            raw_title = data.get('title', [''])[0]
            subtitle = data.get('subtitle', [''])[0] if data.get('subtitle') else ''
            return raw_title, subtitle, data
        return None, None, None
    except Exception as e:
        print(f'  Error fetching {doi}: {e}')
        return None, None, None


def clean_title(title):
    """Strip bibliographic metadata from a book title."""
    if not title:
        return title

    original = title

    # Strip "By Author Name" suffixes (with credentials)
    # e.g. "Title. By Frank Plumpton Ramsey M.A., Fellow and Director..."
    title = re.sub(
        r'\.\s+By\s+[A-Z][a-zA-Z.\s,]+(?:M\.A\.|Ph\.D\.|D\.Phil\.|Fellow|Lecturer|Professor|Director).*$',
        '', title
    ).strip()

    # Strip publisher info after title: ". Publisher, City, Year"
    # Common pattern: "Title. Publisher Name, Year" or "Title (Publisher, Year)"
    title = re.sub(r'\.\s+(?:Franciscan Institute|Cambridge University|Oxford University|Princeton University|Routledge|Macmillan|Blackwell|Springer|Penguin|Harvard|Yale|MIT|Clarendon|Wiley).*$', '', title, flags=re.IGNORECASE).strip()

    # Strip parenthetical publisher info: "(Publisher, Year, Pages)"
    title = re.sub(r'\s*\([A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?::|,)\s+\d{4}[^)]*\)\s*$', '', title).strip()

    # Strip trailing price: "$12.95" / "£44.50"
    title = re.sub(r'\s*[\$£]\d+[.\d]*\s*$', '', title).strip()

    # Strip trailing ISBN: "(ISBN xxx)"
    title = re.sub(r'\s*\(ISBN[^)]+\)\s*$', '', title, flags=re.IGNORECASE).strip()

    # Strip trailing page info: "Pp. xxx" / "xxx pages" / "xii + 472"
    title = re.sub(r'\s*Pp\.?\s+[xivlc\d+\s]+\s*$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'\s*,?\s*\d+\s+pages?\s*\.?\s*$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'\s*,?\s*[xivlc]+\s*\+?\s*\d+\s*(?:pp?\.?)?\s*$', '', title, flags=re.IGNORECASE).strip()

    # Strip trailing year after period: ". 1977"
    title = re.sub(r'\.\s+\d{4}\s*$', '', title).strip()

    # Strip trailing city/publisher fragments: ", New York" / ". London:"
    title = re.sub(r'[,.]?\s+(?:New York|London|Cambridge|Oxford|Princeton|Chicago|Boston|Berkeley|Dordrecht|Leiden|The Hague|Ithaca|Toronto|Paris|Amsterdam|Berlin|Bloomington|Indianapolis|Philadelphia|Pittsburgh|Notre Dame|Englewood Cliffs|West Lafayette|St\. Bonaventure)[,:\s].*$', '', title, flags=re.IGNORECASE).strip()

    # Clean trailing punctuation
    title = title.rstrip('.,;: ')

    return title


def update_entry(entry_id, book_title=None, first=None, last=None):
    """Update a review entry by ID."""
    conn = sqlite3.connect(DB_PATH)
    if book_title is not None and first is not None:
        conn.execute(
            "UPDATE reviews SET book_title = ?, book_author_first_name = ?, "
            "book_author_last_name = ? WHERE id = ?",
            (book_title, first, last, entry_id)
        )
    elif book_title is not None:
        conn.execute(
            "UPDATE reviews SET book_title = ? WHERE id = ?",
            (book_title, entry_id)
        )
    elif first is not None:
        conn.execute(
            "UPDATE reviews SET book_author_first_name = ?, "
            "book_author_last_name = ? WHERE id = ?",
            (first, last, entry_id)
        )
    conn.commit()
    conn.close()


def fix_missing_authors():
    """Re-fetch and re-parse entries with missing authors."""
    entries = get_missing_author_entries()
    print(f'Found {len(entries)} entries with missing authors')

    with_doi = [e for e in entries if e['doi']]
    without_doi = [e for e in entries if not e['doi']]
    print(f'  With DOI (can re-fetch): {len(with_doi)}')
    print(f'  Without DOI (cannot re-fetch): {len(without_doi)}')

    fixed = 0
    failed = 0
    by_journal = {}

    for i, entry in enumerate(with_doi):
        raw_title, subtitle, crossref_data = fetch_crossref_title(entry['doi'])
        if not raw_title:
            failed += 1
            continue

        result = parse_review_title(raw_title, subtitle or '', crossref_data)
        if result and result.get('book_author_last'):
            # Clean the title too
            clean_book_title = clean_title(result['book_title'])
            update_entry(
                entry['id'],
                book_title=clean_book_title,
                first=result['book_author_first'],
                last=result['book_author_last'],
            )
            fixed += 1
            journal = entry['publication_source']
            by_journal[journal] = by_journal.get(journal, 0) + 1
        else:
            failed += 1

        # Rate limiting: ~10/sec
        if (i + 1) % 10 == 0:
            time.sleep(1)

        if (i + 1) % 100 == 0:
            print(f'  Processed {i + 1}/{len(with_doi)}: {fixed} fixed, {failed} failed')

    print(f'\nAuthor fix results:')
    print(f'  Fixed: {fixed}')
    print(f'  Still missing: {failed + len(without_doi)}')
    print(f'  By journal:')
    for journal, count in sorted(by_journal.items(), key=lambda x: -x[1]):
        print(f'    {journal}: {count}')

    return fixed


def fix_long_titles():
    """Clean bibliographic metadata from overly long titles."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, book_title, publication_source FROM reviews "
        "WHERE LENGTH(book_title) > 150"
    ).fetchall()
    conn.close()

    print(f'\nFound {len(rows)} titles longer than 150 chars')

    fixed = 0
    for row in rows:
        row = dict(row)
        cleaned = clean_title(row['book_title'])
        if cleaned != row['book_title'] and len(cleaned) < len(row['book_title']):
            print(f'  [{row["publication_source"]}]')
            print(f'    Before: {row["book_title"][:100]}...')
            print(f'    After:  {cleaned[:100]}')
            update_entry(row['id'], book_title=cleaned)
            fixed += 1

    print(f'Cleaned {fixed} long titles')
    return fixed


def fix_all_titles_with_metadata():
    """Clean bibliographic metadata from ALL titles, not just long ones."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Look for titles with common bibliographic patterns
    rows = conn.execute(
        "SELECT id, book_title FROM reviews "
        "WHERE book_title LIKE '%Pp.%' "
        "   OR book_title LIKE '%pages%' "
        "   OR book_title LIKE '%ISBN%' "
        "   OR book_title LIKE '%$%' "
        "   OR book_title LIKE '%£%' "
        "   OR book_title LIKE '%. By %M.A.%' "
        "   OR book_title LIKE '%. By %Ph.D.%'"
    ).fetchall()
    conn.close()

    print(f'\nFound {len(rows)} titles with potential bibliographic metadata')

    fixed = 0
    for row in rows:
        row = dict(row)
        cleaned = clean_title(row['book_title'])
        if cleaned != row['book_title']:
            update_entry(row['id'], book_title=cleaned)
            fixed += 1

    print(f'Cleaned {fixed} titles with bibliographic metadata')
    return fixed


def main():
    print('=== PhilReviews Data Quality Sweep ===\n')

    # Check baseline
    conn = sqlite3.connect(DB_PATH)
    missing_before = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE book_author_last_name IS NULL OR book_author_last_name = ''"
    ).fetchone()[0]
    long_before = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE LENGTH(book_title) > 200"
    ).fetchone()[0]
    conn.close()
    print(f'Baseline: {missing_before} missing authors, {long_before} titles >200 chars\n')

    # Step 1: Fix missing authors
    print('--- Step 1: Fix missing authors via Crossref re-fetch ---')
    fix_missing_authors()

    # Step 2: Clean long titles
    print('\n--- Step 2: Clean long titles ---')
    fix_long_titles()

    # Step 3: Clean bibliographic metadata from all titles
    print('\n--- Step 3: Clean bibliographic metadata from all titles ---')
    fix_all_titles_with_metadata()

    # Final check
    conn = sqlite3.connect(DB_PATH)
    missing_after = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE book_author_last_name IS NULL OR book_author_last_name = ''"
    ).fetchone()[0]
    long_after = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE LENGTH(book_title) > 200"
    ).fetchone()[0]
    conn.close()

    print(f'\n=== Summary ===')
    print(f'Missing authors: {missing_before} → {missing_after} (fixed {missing_before - missing_after})')
    print(f'Long titles (>200): {long_before} → {long_after} (fixed {long_before - long_after})')


if __name__ == '__main__':
    main()
