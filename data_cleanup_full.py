#!/usr/bin/env python3
"""
Full data cleanup pass for PhilReviews database.

Phases 1-6: Offline fixes (no API calls)
Phases 7-8: Crossref DOI enrichment for missing authors/reviewers

Usage:
    python3 data_cleanup_full.py                  # all phases
    python3 data_cleanup_full.py --dry-run        # preview only
    python3 data_cleanup_full.py --phase 1-6      # offline only
    python3 data_cleanup_full.py --phase 7-8      # Crossref enrichment only
"""

import argparse
import html
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests

DB_PATH = Path(__file__).parent / 'reviews.db'

# ---------------------------------------------------------------------------
# Phase 1: Non-review patterns to delete
# ---------------------------------------------------------------------------
NON_REVIEW_SQL = """
    SELECT id, book_title, publication_source FROM reviews WHERE
    LOWER(book_title) LIKE '%list of members%'
    OR LOWER(book_title) LIKE '%descriptive notices%'
    OR (LOWER(book_title) LIKE 'booknotes%' AND publication_source = 'Philosophy')
    OR (book_title = 'Book Notes' AND publication_source IN ('Ethics', 'Hypatia', 'Phronesis'))
    OR (book_title = 'Aristotle Book Notes' AND publication_source = 'Phronesis')
    OR (book_title LIKE 'Editor''s Notes' AND publication_source = 'Phronesis')
    OR LOWER(book_title) LIKE 'corrigendum:%'
    OR LOWER(book_title) LIKE 'correction to:%'
    OR LOWER(book_title) LIKE '%erratum%'
    OR (LOWER(book_title) LIKE '%errata%' AND LOWER(book_title) NOT LIKE '%de erratas%')
    OR LOWER(book_title) = 'in memoriam'
    OR LOWER(book_title) LIKE '%announcement by the editor%'
    OR LOWER(book_title) LIKE '%american council%learned%'
    OR (book_title LIKE 'Fall 19%' AND book_title LIKE '%Through%')
    OR (book_title LIKE 'Fall 196%' AND book_title LIKE '%Summer%')
    OR book_title LIKE 'BOOK NOTICE%'
    OR book_title = '[Book Review - title not specified in metadata]'
    OR LOWER(book_title) = 'books received'
    OR (book_title LIKE 'Book Reviews (Vol%' AND publication_source = 'Ethical Perspectives')
    OR (book_title LIKE 'Chronicle%Book Reviews%' AND publication_source = 'Ethical Perspectives')
    OR (book_title LIKE 'Index for Volume%' AND publication_source = 'The Journal of Aesthetics and Art Criticism')
    OR (LOWER(book_title) LIKE 'fe de erratas%' AND publication_source LIKE '%Cr_tica%')
    OR (book_title LIKE 'Report of the%Conference%' AND publication_source = 'International Journal of Ethics')
    OR (book_title LIKE 'Septimana Spinozana%' AND publication_source = 'Philosophy')
"""

# ---------------------------------------------------------------------------
# Phase 2: Irrecoverable placeholder patterns
# ---------------------------------------------------------------------------
PLACEHOLDER_SQL = """
    SELECT id, book_title, publication_source FROM reviews WHERE
    LOWER(book_title) IN ('book review', 'book reviews', 'reviews', 'review', 'book notes')
    AND (book_author_last_name IS NULL OR book_author_last_name = '')
"""

# ---------------------------------------------------------------------------
# Phase 4: Journal name normalization mapping (wrong → correct)
# ---------------------------------------------------------------------------
JOURNAL_FIXES = {
    'Times LIterary Supplement': 'The Times Literary Supplement',
    'The TImes Literary Supplement': 'The Times Literary Supplement',
    'The London Review of Books': 'London Review of Books',
    'The Boston Review': 'Boston Review',
    'Boston Globe': 'The Boston Globe',
    'Hedgehog Review': 'The Hedgehog Review',
    'Financial Times': 'The Financial Times',
    'New York Journal of Books': 'The New York Journal of Books',
    'Christian Science Monitor': 'The Christian Science Monitor',
    'ARC Digital': 'Arc Digital',
    'British Society for Philosophy of Science': 'The British Society for Philosophy of Science',
    'British Society for the Philosophy of Science': 'The British Society for the Philosophy of Science',
}

# ---------------------------------------------------------------------------
# Phase 5: "Book Review:" prefix regex
# ---------------------------------------------------------------------------
BOOK_REVIEW_PREFIX_RE = re.compile(
    r'^(?:BOOK\s+REVIEW|Book\s+[Rr]eview)\s*[-:]\s*', re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def phase_header(num, name):
    print(f'\n{"="*60}')
    print(f'Phase {num}: {name}')
    print('='*60)


# ---------------------------------------------------------------------------
# Phase implementations
# ---------------------------------------------------------------------------

def phase1_delete_non_reviews(conn, dry_run):
    """Delete entries that are clearly not book reviews."""
    phase_header(1, 'Delete non-reviews')
    rows = conn.execute(NON_REVIEW_SQL).fetchall()
    print(f'Found {len(rows)} non-review entries')
    for r in rows:
        print(f'  DELETE id={r["id"]}: "{r["book_title"][:70]}" | {r["publication_source"]}')
    if not dry_run and rows:
        ids = [r['id'] for r in rows]
        conn.executemany('DELETE FROM reviews WHERE id = ?', [(i,) for i in ids])
        conn.commit()
        print(f'Deleted {len(rows)} entries')
    return len(rows)


def phase2_delete_placeholders(conn, dry_run):
    """Delete placeholder-title entries with no recoverable metadata."""
    phase_header(2, 'Delete irrecoverable placeholders')
    rows = conn.execute(PLACEHOLDER_SQL).fetchall()
    print(f'Found {len(rows)} placeholder entries')
    for r in rows[:20]:
        print(f'  DELETE id={r["id"]}: "{r["book_title"]}" | {r["publication_source"]}')
    if len(rows) > 20:
        print(f'  ... and {len(rows) - 20} more')
    if not dry_run and rows:
        ids = [r['id'] for r in rows]
        conn.executemany('DELETE FROM reviews WHERE id = ?', [(i,) for i in ids])
        conn.commit()
        print(f'Deleted {len(rows)} entries')
    return len(rows)


def phase3_fix_html_entities(conn, dry_run):
    """Decode HTML entities in all text fields."""
    phase_header(3, 'Fix HTML entities')
    fields = ['book_title', 'book_author_first_name', 'book_author_last_name',
              'reviewer_first_name', 'reviewer_last_name']
    total_fixed = 0
    for field in fields:
        rows = conn.execute(
            f"SELECT id, {field} FROM reviews WHERE {field} LIKE '%&#%' "
            f"OR {field} LIKE '%&amp;%' OR {field} LIKE '%&quot;%' "
            f"OR {field} LIKE '%&lt;%' OR {field} LIKE '%&gt;%'"
        ).fetchall()
        if not rows:
            continue
        print(f'\n  {field}: {len(rows)} entries')
        updates = []
        for r in rows:
            old_val = r[field]
            new_val = html.unescape(old_val)
            # Strip trailing comma/period from names
            if 'name' in field:
                new_val = new_val.rstrip(',. ')
            if old_val != new_val:
                print(f'    id={r["id"]}: "{old_val}" → "{new_val}"')
                updates.append((new_val, r['id']))
        if not dry_run and updates:
            conn.executemany(f'UPDATE reviews SET {field} = ? WHERE id = ?', updates)
            conn.commit()
        total_fixed += len(updates)
    print(f'\nTotal HTML entity fixes: {total_fixed}')
    return total_fixed


def phase4_normalize_journals(conn, dry_run):
    """Normalize inconsistent journal names."""
    phase_header(4, 'Normalize journal names')
    total_fixed = 0
    for wrong, correct in JOURNAL_FIXES.items():
        rows = conn.execute(
            'SELECT COUNT(*) as cnt FROM reviews WHERE publication_source = ?',
            (wrong,)
        ).fetchone()
        cnt = rows['cnt']
        if cnt > 0:
            print(f'  "{wrong}" → "{correct}" ({cnt} entries)')
            if not dry_run:
                conn.execute(
                    'UPDATE reviews SET publication_source = ? WHERE publication_source = ?',
                    (correct, wrong)
                )
            total_fixed += cnt
    if not dry_run and total_fixed:
        conn.commit()
    print(f'Total journal name fixes: {total_fixed}')
    return total_fixed


def phase5_strip_prefixes(conn, dry_run):
    """Strip 'Book Review:' prefixes from titles."""
    phase_header(5, 'Strip "Book Review:" prefixes')
    rows = conn.execute("""
        SELECT id, book_title FROM reviews WHERE
        book_title LIKE 'Book Review:%' OR book_title LIKE 'BOOK REVIEW:%'
        OR book_title LIKE 'Book review:%' OR book_title LIKE 'BOOK REVIEW -%'
        OR book_title LIKE 'Book Review -%'
    """).fetchall()
    updates = []
    for r in rows:
        old = r['book_title']
        new = BOOK_REVIEW_PREFIX_RE.sub('', old).strip()
        if new != old and len(new) > 3:
            print(f'  id={r["id"]}: "{old[:80]}" → "{new[:80]}"')
            updates.append((new, r['id']))
    if not dry_run and updates:
        conn.executemany('UPDATE reviews SET book_title = ? WHERE id = ?', updates)
        conn.commit()
    print(f'Total prefix strips: {len(updates)}')
    return len(updates)


def phase6_fix_corruption(conn, dry_run):
    """Fix known data corruption patterns."""
    phase_header(6, 'Fix known data corruption')
    total_fixed = 0

    # 6a: "Confusion*1" → "Confusion"
    rows = conn.execute("SELECT id FROM reviews WHERE book_title = 'Confusion*1'").fetchall()
    if rows:
        print(f'  "Confusion*1" → "Confusion": {len(rows)} entries')
        if not dry_run:
            conn.execute("UPDATE reviews SET book_title = 'Confusion' WHERE book_title = 'Confusion*1'")
        total_fixed += len(rows)

    # 6b: Malformed reviewer names — strip parenthetical notes
    paren_fixes = [
        (11724, 'Kathryn Sophia', 'Belle'),
        (13320, 'Mark', 'Siderits'),
        (29937, 'Alex', 'Alraf'),
    ]
    for rid, first, last in paren_fixes:
        row = conn.execute('SELECT reviewer_first_name, reviewer_last_name FROM reviews WHERE id = ?', (rid,)).fetchone()
        if row and (')' in (row['reviewer_last_name'] or '') or ')' in (row['reviewer_first_name'] or '')):
            print(f'  id={rid}: reviewer → "{first} {last}"')
            if not dry_run:
                conn.execute(
                    'UPDATE reviews SET reviewer_first_name = ?, reviewer_last_name = ? WHERE id = ?',
                    (first, last, rid)
                )
            total_fixed += 1

    # 6c: Dollar-sign titles in Dialogue (need Crossref to fix properly)
    dollar_rows = conn.execute("SELECT id, book_title, doi FROM reviews WHERE book_title LIKE '$%'").fetchall()
    if dollar_rows:
        print(f'  Dollar-sign titles found: {len(dollar_rows)} (will fix in phase 7 via Crossref)')

    # 6d: "Apology:" entry in Philosophy that's actually a review
    apology_row = conn.execute(
        "SELECT id, book_title FROM reviews WHERE id = 57978"
    ).fetchone()
    if apology_row and apology_row['book_title'].startswith('Apology:'):
        new_title = apology_row['book_title'].replace('Apology: ', '')
        print(f'  id=57978: strip "Apology: " prefix → "{new_title[:60]}"')
        if not dry_run:
            conn.execute('UPDATE reviews SET book_title = ? WHERE id = ?', (new_title, 57978))
        total_fixed += 1

    if not dry_run:
        conn.commit()
    print(f'Total corruption fixes: {total_fixed}')
    return total_fixed


def phase7_enrich_authors(conn, dry_run):
    """Use Crossref DOI lookups to recover missing book authors."""
    phase_header(7, 'Crossref enrichment: missing book authors')

    # Import parse_review_title from crossref_scraper
    sys.path.insert(0, str(Path(__file__).parent))
    from crossref_scraper import parse_review_title, _extract_first_author, _looks_like_author_name

    rows = conn.execute("""
        SELECT id, book_title, doi, publication_source FROM reviews
        WHERE (book_author_last_name IS NULL OR book_author_last_name = '')
        AND doi IS NOT NULL AND doi != ''
        ORDER BY id
    """).fetchall()

    print(f'Found {len(rows)} entries with DOI but no book author')
    if dry_run:
        print('(dry-run: showing first 10 only)')
        rows = rows[:10]

    fixed = 0
    skipped = 0
    errors = 0
    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    for i, r in enumerate(rows):
        if (i + 1) % 100 == 0:
            print(f'  Progress: {i+1}/{len(rows)} (fixed: {fixed}, skipped: {skipped})')

        doi = r['doi']
        try:
            resp = session.get(
                f'https://api.crossref.org/works/{doi}',
                timeout=15
            )
            if resp.status_code == 404:
                skipped += 1
                continue
            resp.raise_for_status()
            data = resp.json()['message']
        except Exception as e:
            errors += 1
            if errors > 20:
                print(f'  Too many errors ({errors}), stopping')
                break
            time.sleep(2)
            continue

        # Try to extract book author from Crossref metadata
        cr_title = ''
        if data.get('title'):
            cr_title = data['title'][0]
        cr_subtitle = ''
        if data.get('subtitle'):
            cr_subtitle = data['subtitle'][0]

        parsed = parse_review_title(cr_title, cr_subtitle, crossref_data=data)
        if parsed and parsed.get('book_author_last') and not parsed.get('needs_doi_scrape'):
            new_first = parsed['book_author_first']
            new_last = parsed['book_author_last']
            new_title = parsed.get('book_title', '')

            # Validate: reject single-word "authors" that are really book subjects
            # For enrichment, require first+last (not just a last name) since
            # single-word parses are often misidentified book topics
            full_name = (new_first + ' ' + new_last).strip() if new_first else new_last
            if not new_first or not _looks_like_author_name(full_name):
                skipped += 1
                continue

            # Also update book_title if we got a better one and current one is generic
            old_title = r['book_title']
            update_title = False
            if new_title and len(new_title) > 5:
                if old_title.lower() in ('book review', 'book reviews', 'review', 'reviews') or len(old_title) < 5:
                    update_title = True

            if dry_run:
                extra = f', title → "{new_title[:50]}"' if update_title else ''
                print(f'  id={r["id"]}: → {new_first} {new_last}{extra} | {r["publication_source"]}')
            else:
                if update_title:
                    conn.execute(
                        'UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ?, book_title = ? WHERE id = ?',
                        (new_first, new_last, new_title, r['id'])
                    )
                else:
                    conn.execute(
                        'UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ? WHERE id = ?',
                        (new_first, new_last, r['id'])
                    )
            fixed += 1
        else:
            # Try reviewer extraction from Crossref author field
            # (reviewer is the author of the review article)
            skipped += 1

        # Rate limit: be polite to Crossref
        if (i + 1) % 20 == 0:
            time.sleep(1)

    if not dry_run:
        conn.commit()

    print(f'\nResults: {fixed} authors recovered, {skipped} skipped, {errors} errors')
    return fixed


def phase8_enrich_reviewers(conn, dry_run):
    """Use Crossref DOI lookups to recover missing reviewer names."""
    phase_header(8, 'Crossref enrichment: missing reviewers')

    rows = conn.execute("""
        SELECT id, book_title, doi, reviewer_first_name, reviewer_last_name FROM reviews
        WHERE (reviewer_last_name IS NULL OR reviewer_last_name = '')
        AND doi IS NOT NULL AND doi != ''
        ORDER BY id
    """).fetchall()

    print(f'Found {len(rows)} entries with DOI but no reviewer')
    if dry_run:
        print('(dry-run: showing first 10 only)')
        rows = rows[:10]

    fixed = 0
    skipped = 0
    errors = 0
    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    for i, r in enumerate(rows):
        if (i + 1) % 100 == 0:
            print(f'  Progress: {i+1}/{len(rows)} (fixed: {fixed}, skipped: {skipped})')

        doi = r['doi']
        try:
            resp = session.get(
                f'https://api.crossref.org/works/{doi}',
                timeout=15
            )
            if resp.status_code == 404:
                skipped += 1
                continue
            resp.raise_for_status()
            data = resp.json()['message']
        except Exception as e:
            errors += 1
            if errors > 20:
                print(f'  Too many errors ({errors}), stopping')
                break
            time.sleep(2)
            continue

        # Reviewer = the Crossref "author" of the review article
        authors = data.get('author', [])
        if authors:
            first_author = authors[0]
            new_first = first_author.get('given', '')
            new_last = first_author.get('family', '')
            if new_last:
                if dry_run:
                    print(f'  id={r["id"]}: reviewer → {new_first} {new_last} | "{r["book_title"][:50]}"')
                else:
                    conn.execute(
                        'UPDATE reviews SET reviewer_first_name = ?, reviewer_last_name = ? WHERE id = ?',
                        (new_first, new_last, r['id'])
                    )
                fixed += 1
            else:
                skipped += 1
        else:
            skipped += 1

        if (i + 1) % 20 == 0:
            time.sleep(1)

    if not dry_run:
        conn.commit()

    print(f'\nResults: {fixed} reviewers recovered, {skipped} skipped, {errors} errors')
    return fixed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASES = {
    1: ('Delete non-reviews', phase1_delete_non_reviews),
    2: ('Delete irrecoverable placeholders', phase2_delete_placeholders),
    3: ('Fix HTML entities', phase3_fix_html_entities),
    4: ('Normalize journal names', phase4_normalize_journals),
    5: ('Strip "Book Review:" prefixes', phase5_strip_prefixes),
    6: ('Fix known data corruption', phase6_fix_corruption),
    7: ('Crossref enrichment: missing authors', phase7_enrich_authors),
    8: ('Crossref enrichment: missing reviewers', phase8_enrich_reviewers),
}


def parse_phase_range(phase_str):
    """Parse '1-6' or '7' or '1-8' into a list of phase numbers."""
    if '-' in phase_str:
        start, end = phase_str.split('-', 1)
        return list(range(int(start), int(end) + 1))
    return [int(phase_str)]


def main():
    parser = argparse.ArgumentParser(description='PhilReviews full data cleanup')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes only')
    parser.add_argument('--phase', type=str, default='1-8', help='Phase range to run (e.g. 1-6, 7-8, 3)')
    args = parser.parse_args()

    phases_to_run = parse_phase_range(args.phase)
    print(f'PhilReviews Data Cleanup {"(DRY RUN)" if args.dry_run else ""}')
    print(f'Phases: {phases_to_run}')
    print(f'Database: {DB_PATH}')

    conn = get_conn()
    total_before = conn.execute('SELECT COUNT(*) FROM reviews').fetchone()[0]
    print(f'Total entries before: {total_before:,}')

    results = {}
    for phase_num in phases_to_run:
        if phase_num not in PHASES:
            print(f'Unknown phase {phase_num}, skipping')
            continue
        name, func = PHASES[phase_num]
        results[phase_num] = func(conn, args.dry_run)

    total_after = conn.execute('SELECT COUNT(*) FROM reviews').fetchone()[0]
    conn.close()

    print(f'\n{"="*60}')
    print('SUMMARY')
    print('='*60)
    for phase_num in sorted(results):
        name = PHASES[phase_num][0]
        print(f'  Phase {phase_num} ({name}): {results[phase_num]}')
    print(f'\nEntries before: {total_before:,}')
    print(f'Entries after:  {total_after:,}')
    print(f'Net change:     {total_after - total_before:+,}')


if __name__ == '__main__':
    main()
