#!/usr/bin/env python3
"""
Backfill reviewer names for Journal of Markets & Morality entries.
Fetches author_info from the Scholastica API for each article.
"""

import re
import sqlite3
import time
import requests
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db")
API_BASE = 'https://www.marketsandmorality.com/api/v1/articles'

SESSION = requests.Session()
SESSION.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'


def get_jmm_entries_missing_reviewers():
    """Get JMM entries with no reviewer name."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, book_title, review_link, reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE publication_source = 'Journal of Markets and Morality'
        AND (reviewer_last_name IS NULL OR reviewer_last_name = '')
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def extract_article_id(url):
    """Extract article ID from URL like https://www.marketsandmorality.com/article/145795"""
    m = re.search(r'/article/(\d+)', url)
    return m.group(1) if m else None


def parse_author_name(author_info):
    """Parse 'First Last' or 'First M. Last' into (first, last)."""
    if not author_info or not author_info.strip():
        return None, None

    name = author_info.strip()
    # Remove any trailing credentials
    name = re.sub(r',?\s*(Ph\.?D\.?|M\.?A\.?|M\.?D\.?|S\.?J\.?|O\.?P\.?)$', '', name).strip()

    parts = name.split()
    if len(parts) >= 2:
        return ' '.join(parts[:-1]), parts[-1]
    elif len(parts) == 1:
        return '', parts[0]
    return None, None


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Backfill JMM reviewer names')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=0, help='Max entries to process (0=all)')
    args = parser.parse_args()

    entries = get_jmm_entries_missing_reviewers()
    print(f"Found {len(entries)} JMM entries missing reviewer names")

    if args.limit:
        entries = entries[:args.limit]
        print(f"Processing first {args.limit}")

    fixed = 0
    failed = 0
    errors = 0

    conn = sqlite3.connect(DB_PATH)

    for i, entry in enumerate(entries):
        article_id = extract_article_id(entry['review_link'])
        if not article_id:
            failed += 1
            continue

        try:
            resp = SESSION.get(f'{API_BASE}/{article_id}', timeout=15)
            if resp.status_code != 200:
                errors += 1
                if resp.status_code == 429:
                    print(f"  Rate limited at entry {i+1}, waiting 30s...")
                    time.sleep(30)
                continue

            data = resp.json()
            author_info = data.get('author_info', '')

            if not author_info:
                # Try authors array
                authors = data.get('authors', [])
                if authors:
                    author = authors[0]
                    first = author.get('first_name', '')
                    last = author.get('last_name', '')
                else:
                    failed += 1
                    continue
            else:
                first, last = parse_author_name(author_info)

            if first is not None and last:
                if not args.dry_run:
                    conn.execute(
                        "UPDATE reviews SET reviewer_first_name = ?, reviewer_last_name = ? WHERE id = ?",
                        (first, last, entry['id']))
                fixed += 1
                if fixed <= 10:
                    print(f"  [{entry['id']}] '{entry['book_title'][:50]}' -> reviewer: {first} {last}")
            else:
                failed += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error for article {article_id}: {e}")

        # Rate limiting: ~5 req/sec
        if (i + 1) % 5 == 0:
            time.sleep(1)

        if (i + 1) % 100 == 0:
            if not args.dry_run:
                conn.commit()
            print(f"  Processed {i+1}/{len(entries)}: {fixed} fixed, {failed} no data, {errors} errors")

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"JMM Reviewer Backfill {'(DRY RUN)' if args.dry_run else 'Results'}")
    print(f"{'='*50}")
    print(f"Processed:  {len(entries)}")
    print(f"Fixed:      {fixed}")
    print(f"No data:    {failed}")
    print(f"Errors:     {errors}")


if __name__ == '__main__':
    main()
