#!/usr/bin/env python3
"""
Data cleanup pass #2 for PhilReviews database.

All phases are offline (no API calls). Focuses on field normalization,
name cleaning, and deduplication.

Usage:
    python3 data_cleanup_pass2.py                  # all phases
    python3 data_cleanup_pass2.py --dry-run        # preview only
    python3 data_cleanup_pass2.py --phase 1-3      # specific phases
"""

import argparse
import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'reviews.db'

NAME_FIELDS = [
    'book_author_first_name', 'book_author_last_name',
    'reviewer_first_name', 'reviewer_last_name',
]

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
# Phase 1: Normalize whitespace-only fields to NULL
# ---------------------------------------------------------------------------

def phase1_whitespace_to_null(conn, dry_run):
    """Replace whitespace-only or empty string name fields with NULL."""
    phase_header(1, 'Normalize whitespace-only fields to NULL')
    total = 0
    for field in NAME_FIELDS:
        rows = conn.execute(
            f"SELECT COUNT(*) as cnt FROM reviews "
            f"WHERE {field} IS NOT NULL AND TRIM({field}) = ''"
        ).fetchone()
        cnt = rows['cnt']
        if cnt > 0:
            print(f'  {field}: {cnt} entries')
            if not dry_run:
                conn.execute(
                    f"UPDATE reviews SET {field} = NULL "
                    f"WHERE {field} IS NOT NULL AND TRIM({field}) = ''"
                )
            total += cnt
    if not dry_run and total:
        conn.commit()
    print(f'Total whitespace→NULL: {total}')
    return total


# ---------------------------------------------------------------------------
# Phase 2: Standardize access_type and entry_type
# ---------------------------------------------------------------------------

def phase2_standardize_types(conn, dry_run):
    """Lowercase access_type values; set empty entry_type to 'review'."""
    phase_header(2, 'Standardize access_type and entry_type')
    total = 0

    # 2a: Lowercase access_type
    for wrong, correct in [('Open', 'open'), ('Restricted', 'restricted'), ('Paywalled', 'paywalled')]:
        rows = conn.execute(
            'SELECT COUNT(*) as cnt FROM reviews WHERE access_type = ?', (wrong,)
        ).fetchone()
        cnt = rows['cnt']
        if cnt > 0:
            print(f'  access_type "{wrong}" → "{correct}": {cnt} entries')
            if not dry_run:
                conn.execute(
                    'UPDATE reviews SET access_type = ? WHERE access_type = ?',
                    (correct, wrong)
                )
            total += cnt

    # 2b: Empty entry_type → 'review'
    rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM reviews WHERE entry_type IS NULL OR entry_type = ''"
    ).fetchone()
    cnt = rows['cnt']
    if cnt > 0:
        print(f'  entry_type empty → "review": {cnt} entries')
        if not dry_run:
            conn.execute(
                "UPDATE reviews SET entry_type = 'review' "
                "WHERE entry_type IS NULL OR entry_type = ''"
            )
        total += cnt

    if not dry_run and total:
        conn.commit()
    print(f'Total type standardizations: {total}')
    return total


# ---------------------------------------------------------------------------
# Phase 3: Strip trailing commas from names
# ---------------------------------------------------------------------------

def phase3_strip_trailing_commas(conn, dry_run):
    """Strip trailing commas from all name fields."""
    phase_header(3, 'Strip trailing commas from names')
    total = 0
    for field in NAME_FIELDS:
        rows = conn.execute(
            f"SELECT id, {field} FROM reviews WHERE {field} LIKE '%,'"
        ).fetchall()
        if not rows:
            continue
        updates = []
        for r in rows:
            old = r[field]
            new = old.rstrip(',').rstrip()
            if new != old and new:
                updates.append((new, r['id']))
        if updates:
            print(f'  {field}: {len(updates)} entries')
            for u in updates[:5]:
                print(f'    "{conn.execute(f"SELECT {field} FROM reviews WHERE id = ?", (u[1],)).fetchone()[0]}" → "{u[0]}"')
            if len(updates) > 5:
                print(f'    ... and {len(updates) - 5} more')
            if not dry_run:
                conn.executemany(
                    f'UPDATE reviews SET {field} = ? WHERE id = ?', updates
                )
            total += len(updates)
    if not dry_run and total:
        conn.commit()
    print(f'Total trailing commas stripped: {total}')
    return total


# ---------------------------------------------------------------------------
# Phase 4: Clean "eds."/"ed." from author names
# ---------------------------------------------------------------------------

ED_SUFFIX_RE = re.compile(r',?\s*\(?\beds\.?\)?\.?$', re.IGNORECASE)
ED_SINGLE_RE = re.compile(r',?\s*\(?\bed\.?\)?\.?$', re.IGNORECASE)

def phase4_clean_editor_markers(conn, dry_run):
    """Strip editor markers and extract first author from multi-author jams."""
    phase_header(4, 'Clean "eds."/"ed." from author names')
    total = 0

    # 4a: Strip eds./ed. from last names
    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE book_author_last_name LIKE '%eds%'
           OR book_author_last_name LIKE '%ed.'
           OR book_author_last_name LIKE '%ed)'
           OR book_author_last_name LIKE '%(ed%'
    """).fetchall()
    updates = []
    for r in rows:
        old = r['book_author_last_name']
        new = ED_SUFFIX_RE.sub('', old).strip()
        new = ED_SINGLE_RE.sub('', new).strip()
        if new != old and new:
            updates.append((new, r['id']))
    if updates:
        print(f'  Last name eds./ed. cleanup: {len(updates)} entries')
        for u in updates[:5]:
            row = conn.execute(
                'SELECT book_author_last_name FROM reviews WHERE id = ?', (u[1],)
            ).fetchone()
            print(f'    "{row[0]}" → "{u[0]}"')
        if len(updates) > 5:
            print(f'    ... and {len(updates) - 5} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_last_name = ? WHERE id = ?', updates
            )
        total += len(updates)

    # 4b: Strip eds./ed. from first names
    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE book_author_first_name LIKE '%eds%'
           OR book_author_first_name LIKE '%ed.'
           OR book_author_first_name LIKE '%(ed%'
    """).fetchall()
    updates = []
    for r in rows:
        old = r['book_author_first_name']
        new = ED_SUFFIX_RE.sub('', old).strip()
        new = ED_SINGLE_RE.sub('', new).strip()
        if new != old and new:
            updates.append((new, r['id']))
    if updates:
        print(f'  First name eds./ed. cleanup: {len(updates)} entries')
        for u in updates[:5]:
            row = conn.execute(
                'SELECT book_author_first_name FROM reviews WHERE id = ?', (u[1],)
            ).fetchone()
            print(f'    "{row[0]}" → "{u[0]}"')
        if len(updates) > 5:
            print(f'    ... and {len(updates) - 5} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_first_name = ? WHERE id = ?', updates
            )
        total += len(updates)

    # 4c: Multi-author jams in first name (contains " and ")
    # Only fix when last name has an editor marker — otherwise the " and "
    # is likely part of a book title or organization name in garbled metadata
    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE book_author_first_name LIKE '% and %'
        AND (book_author_last_name LIKE '%eds%'
             OR book_author_last_name LIKE '%ed.'
             OR book_author_last_name LIKE '%(ed%')
    """).fetchall()
    updates = []
    for r in rows:
        first = r['book_author_first_name']
        last = r['book_author_last_name']
        # Pattern: "Alison M. Jaggar and Iris Marion Young" in first, "eds." in last
        # Extract just the first author's given names
        parts = re.split(r'\s+and\s+', first, maxsplit=1)
        if len(parts) == 2:
            first_author_given = parts[0].strip()
            words = first_author_given.split()
            if len(words) <= 3:
                new_first = first_author_given
                new_first = ED_SUFFIX_RE.sub('', new_first).strip()
                new_first = ED_SINGLE_RE.sub('', new_first).strip()
                if new_first != first and new_first:
                    updates.append((new_first, r['id']))
    if updates:
        print(f'  Multi-author jam cleanup: {len(updates)} entries')
        for u in updates[:5]:
            row = conn.execute(
                'SELECT book_author_first_name, book_author_last_name FROM reviews WHERE id = ?',
                (u[1],)
            ).fetchone()
            print(f'    first="{row[0]}" → "{u[0]}" (last="{row[1]}")')
        if len(updates) > 5:
            print(f'    ... and {len(updates) - 5} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_first_name = ? WHERE id = ?', updates
            )
        total += len(updates)

    if not dry_run and total:
        conn.commit()
    print(f'Total editor marker fixes: {total}')
    return total


# ---------------------------------------------------------------------------
# Phase 5: Fix publisher metadata in author fields
# ---------------------------------------------------------------------------

def _is_publisher_metadata(first, last):
    """Check if the combined author name fields look like publisher metadata.

    Returns True for clear metadata like "Cambridge: Cambridge University Press"
    but False for legitimate surnames like "Sara Brill" or "K. Blackwell".
    """
    combined = f"{first or ''} {last or ''}".strip()

    # "University Press" is always metadata (no human is named this)
    if re.search(r'University\s+Press', combined, re.IGNORECASE):
        return True
    # "MIT Press" same
    if re.search(r'MIT\s+Press', combined, re.IGNORECASE):
        return True
    # "Clarendon Press" same
    if re.search(r'Clarendon\s+Press', combined, re.IGNORECASE):
        return True
    # City: Publisher pattern (e.g. "Cambridge: Cambridge", "Oxford: Oxford")
    if re.search(r'(?:Cambridge|Oxford|Princeton|London|New York|Dordrecht|Leiden):', combined):
        return True
    # Contains year + publisher name (e.g. "2024 MatthewCongdon Oxford: ...")
    if re.search(r'\d{4}.*(?:University|Press|Routledge|Springer|Palgrave|Wiley|Blackwell)', combined, re.IGNORECASE):
        return True
    # Publisher name with additional metadata context (year, pages, city)
    if re.search(r'(?:Routledge|Springer|Palgrave|Macmillan|Wiley)\b', combined, re.IGNORECASE):
        # Only flag if combined has additional metadata signals
        if re.search(r'\d{4}|pp\.|Vol\.|:\s|,\s*\d', combined):
            return True
        # Or if it's just the publisher name alone (very short, no given name)
        if combined.strip() in ('Routledge', 'Springer', 'Palgrave', 'Palgrave Macmillan',
                                 'Wiley', 'Springer-Verlag'):
            return True
    return False


def phase5_publisher_metadata(conn, dry_run):
    """Null out author names that are actually publisher/city metadata."""
    phase_header(5, 'Fix publisher metadata in author fields')

    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE book_author_first_name IS NOT NULL
           OR book_author_last_name IS NOT NULL
    """).fetchall()

    updates = []
    for r in rows:
        if _is_publisher_metadata(r['book_author_first_name'], r['book_author_last_name']):
            updates.append(r['id'])

    if updates:
        print(f'  Publisher metadata in author fields: {len(updates)} entries')
        for uid in updates[:15]:
            row = conn.execute(
                'SELECT book_author_first_name, book_author_last_name FROM reviews WHERE id = ?',
                (uid,)
            ).fetchone()
            print(f'    id={uid}: first="{row[0]}" last="{row[1]}" → NULL')
        if len(updates) > 15:
            print(f'    ... and {len(updates) - 15} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_first_name = NULL, '
                'book_author_last_name = NULL WHERE id = ?',
                [(uid,) for uid in updates]
            )
            conn.commit()

    print(f'Total publisher metadata blanked: {len(updates)}')
    return len(updates)


# ---------------------------------------------------------------------------
# Phase 6: Blank garbled long author names
# ---------------------------------------------------------------------------

def phase6_garbled_long_names(conn, dry_run):
    """Blank author/reviewer names that are clearly garbled metadata."""
    phase_header(6, 'Blank garbled long author names')
    total = 0

    # 6a: book_author_first_name > 50 chars (always metadata)
    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE LENGTH(book_author_first_name) > 50
    """).fetchall()
    if rows:
        print(f'  book_author_first_name > 50 chars: {len(rows)} entries')
        for r in rows[:10]:
            print(f'    id={r["id"]}: "{r["book_author_first_name"][:60]}..." → NULL')
        if len(rows) > 10:
            print(f'    ... and {len(rows) - 10} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_first_name = NULL, '
                'book_author_last_name = NULL WHERE id = ?',
                [(r['id'],) for r in rows]
            )
        total += len(rows)

    # 6b: book_author_first_name > 20 chars with digits (embedded metadata)
    rows = conn.execute("""
        SELECT id, book_author_first_name, book_author_last_name
        FROM reviews
        WHERE LENGTH(book_author_first_name) > 20
        AND book_author_first_name GLOB '*[0-9]*'
        AND id NOT IN (SELECT id FROM reviews WHERE LENGTH(book_author_first_name) > 50)
    """).fetchall()
    if rows:
        print(f'  book_author_first_name > 20 chars with digits: {len(rows)} entries')
        for r in rows[:10]:
            print(f'    id={r["id"]}: "{r["book_author_first_name"][:60]}" → NULL')
        if len(rows) > 10:
            print(f'    ... and {len(rows) - 10} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET book_author_first_name = NULL, '
                'book_author_last_name = NULL WHERE id = ?',
                [(r['id'],) for r in rows]
            )
        total += len(rows)

    # 6c: reviewer names with same patterns
    rows = conn.execute("""
        SELECT id, reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE LENGTH(reviewer_first_name) > 50
    """).fetchall()
    if rows:
        print(f'  reviewer_first_name > 50 chars: {len(rows)} entries')
        for r in rows[:5]:
            print(f'    id={r["id"]}: "{r["reviewer_first_name"][:60]}..." → NULL')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET reviewer_first_name = NULL, '
                'reviewer_last_name = NULL WHERE id = ?',
                [(r['id'],) for r in rows]
            )
        total += len(rows)

    rows = conn.execute("""
        SELECT id, reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE LENGTH(reviewer_first_name) > 20
        AND reviewer_first_name GLOB '*[0-9]*'
        AND id NOT IN (SELECT id FROM reviews WHERE LENGTH(reviewer_first_name) > 50)
    """).fetchall()
    if rows:
        print(f'  reviewer_first_name > 20 chars with digits: {len(rows)} entries')
        for r in rows[:5]:
            print(f'    id={r["id"]}: "{r["reviewer_first_name"][:60]}" → NULL')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET reviewer_first_name = NULL, '
                'reviewer_last_name = NULL WHERE id = ?',
                [(r['id'],) for r in rows]
            )
        total += len(rows)

    if not dry_run and total:
        conn.commit()
    print(f'Total garbled names blanked: {total}')
    return total


# ---------------------------------------------------------------------------
# Phase 7: Deduplicate non-symposium entries
# ---------------------------------------------------------------------------

def phase7_deduplicate(conn, dry_run):
    """Remove duplicate entries (same book, journal, date) keeping the best one."""
    phase_header(7, 'Deduplicate non-symposium entries')

    # Find duplicates: same book_title, publication_source, publication_date
    # with no symposium_group, and same reviewer (or both NULL)
    dupes = conn.execute("""
        SELECT book_title, publication_source, publication_date,
               GROUP_CONCAT(id) as ids,
               COUNT(*) as cnt
        FROM reviews
        WHERE symposium_group IS NULL
        GROUP BY book_title, publication_source, publication_date
        HAVING cnt > 1
    """).fetchall()

    total_deleted = 0
    for d in dupes:
        ids = [int(x) for x in d['ids'].split(',')]
        # Fetch full rows
        rows = conn.execute(
            f"SELECT * FROM reviews WHERE id IN ({','.join('?' * len(ids))})",
            ids
        ).fetchall()

        # Check if all reviewers are the same (or all NULL)
        reviewers = set()
        for r in rows:
            rev = (r['reviewer_first_name'] or '', r['reviewer_last_name'] or '')
            reviewers.add(rev)
        # Skip if different reviewers — legitimate multi-reviews
        if len(reviewers) > 1:
            continue

        # Score entries: more non-NULL fields = better
        def score(row):
            s = 0
            for key in ['book_author_first_name', 'book_author_last_name',
                        'reviewer_first_name', 'reviewer_last_name',
                        'doi', 'review_link', 'review_summary']:
                if row[key]:
                    s += 1
            # Prefer longer titles
            if row['book_title']:
                s += len(row['book_title']) / 1000
            return s

        scored = sorted(rows, key=score, reverse=True)
        keep = scored[0]
        delete = scored[1:]

        if delete:
            print(f'  "{d["book_title"][:60]}" | {d["publication_source"]} | {d["publication_date"]}')
            print(f'    Keep id={keep["id"]} (score={score(keep):.1f}), delete {len(delete)}: {[r["id"] for r in delete]}')
            if not dry_run:
                conn.executemany(
                    'DELETE FROM reviews WHERE id = ?',
                    [(r['id'],) for r in delete]
                )
            total_deleted += len(delete)

    if not dry_run and total_deleted:
        conn.commit()
    print(f'Total duplicates removed: {total_deleted}')
    return total_deleted


# ---------------------------------------------------------------------------
# Phase 8: Fix truncated reviewer names
# ---------------------------------------------------------------------------

def phase8_truncated_names(conn, dry_run):
    """Null out single-character reviewer last names (truncation artifacts)."""
    phase_header(8, 'Fix truncated reviewer names')

    rows = conn.execute("""
        SELECT id, reviewer_first_name, reviewer_last_name, book_title, publication_source
        FROM reviews
        WHERE LENGTH(reviewer_last_name) = 1
        AND reviewer_last_name GLOB '[A-Z]'
    """).fetchall()

    if rows:
        print(f'  Single-character reviewer last names: {len(rows)} entries')
        for r in rows[:10]:
            print(f'    id={r["id"]}: reviewer="{r["reviewer_first_name"]} {r["reviewer_last_name"]}" | '
                  f'"{r["book_title"][:40]}" | {r["publication_source"]}')
        if len(rows) > 10:
            print(f'    ... and {len(rows) - 10} more')
        if not dry_run:
            conn.executemany(
                'UPDATE reviews SET reviewer_first_name = NULL, '
                'reviewer_last_name = NULL WHERE id = ?',
                [(r['id'],) for r in rows]
            )
            conn.commit()

    print(f'Total truncated names fixed: {len(rows)}')
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASES = {
    1: ('Normalize whitespace→NULL', phase1_whitespace_to_null),
    2: ('Standardize access_type/entry_type', phase2_standardize_types),
    3: ('Strip trailing commas', phase3_strip_trailing_commas),
    4: ('Clean eds./ed. markers', phase4_clean_editor_markers),
    5: ('Fix publisher metadata in names', phase5_publisher_metadata),
    6: ('Blank garbled long names', phase6_garbled_long_names),
    7: ('Deduplicate non-symposium entries', phase7_deduplicate),
    8: ('Fix truncated reviewer names', phase8_truncated_names),
}


def parse_phase_range(phase_str):
    """Parse '1-6' or '7' or '1-8' into a list of phase numbers."""
    if '-' in phase_str:
        start, end = phase_str.split('-', 1)
        return list(range(int(start), int(end) + 1))
    return [int(phase_str)]


def main():
    parser = argparse.ArgumentParser(description='PhilReviews data cleanup pass #2')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes only')
    parser.add_argument('--phase', type=str, default='1-8', help='Phase range (e.g. 1-3, 4, 1-8)')
    args = parser.parse_args()

    phases_to_run = parse_phase_range(args.phase)
    print(f'PhilReviews Data Cleanup Pass #2 {"(DRY RUN)" if args.dry_run else ""}')
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
