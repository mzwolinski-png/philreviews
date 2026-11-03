#!/usr/bin/env python3
"""What did the weekly scan miss that Philosopher's Index caught?

Run after `pi_import.py --resolve`. Every PI row inserted as new is a review
our own scrapers never found. Grouped by journal, those rows say where the
scan is blind:

  - unscanned journal whose reviews have Crossref DOIs -> candidate for journals.py
  - unscanned journal without DOIs                     -> PI (or a TOC alert) is the only route
  - scanned journal                                    -> a detection or parsing gap;
    each missed DOI is re-run through is_book_review / extract_review and
    tagged with the reason it slipped through

Usage:
  python3 scripts/pi_gap_report.py                # rows inserted by the latest resolve
  python3 scripts/pi_gap_report.py --since 2026-11-03
  python3 scripts/pi_gap_report.py --min 5        # only journals with >= 5 misses
"""
import argparse
import collections
import os
import sqlite3
import sys
import time

import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, ROOT)

import crossref_parsing as cp  # noqa: E402
from journals import JOURNALS  # noqa: E402

DB = os.path.join(ROOT, 'reviews.db')
MAILTO = os.getenv('CROSSREF_EMAIL', 'mzwolinski@gmail.com')


def _why_missed(scraper, doi, cfg):
    """Re-run the scan's own logic on one Crossref record."""
    try:
        r = requests.get(f'https://api.crossref.org/works/{doi}',
                         params={'mailto': MAILTO}, timeout=20)
        if r.status_code != 200:
            return 'not in Crossref', ''
        item = r.json()['message']
    except requests.RequestException:
        return 'fetch failed', ''
    title = (item.get('title') or [''])[0].replace('\n', ' ')
    container = (item.get('container-title') or [''])[0]
    floor = cfg.get('reviews_from')
    parts = (item.get('issued') or {}).get('date-parts') or [[0]]
    issued = '-'.join(f'{p:02d}' if i else str(p) for i, p in enumerate(parts[0]))
    if floor and issued < floor:
        return 'before reviews_from floor', title
    if container and container not in JOURNALS:
        return f'container-title differs ({container})', title
    if not cfg.get('all_reviews') and not cp.is_book_review(item, cfg.get('detection_mode', 'all')):
        return f"not detected ({cfg.get('detection_mode', 'all')})", title
    if not scraper.extract_review(item):
        return 'detected, parse failed', title
    return 'detected and parsed (scan timing?)', title


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', help='created_at date of the inserted rows (default: latest resolve)')
    ap.add_argument('--min', type=int, default=3, help='smallest journal gap to report')
    ap.add_argument('--samples', type=int, default=4, help='DOIs to diagnose per scanned journal')
    args = ap.parse_args()

    conn = sqlite3.connect(DB)
    since = args.since or conn.execute(
        "SELECT date(max(r.created_at)) FROM reviews r JOIN pi_staging s "
        "ON s.status = 'inserted' AND r.review_link = s.review_url_resolved").fetchone()[0]
    rows = conn.execute(
        "SELECT s.journal_name, s.doi_resolved, s.book_title "
        "FROM pi_staging s JOIN reviews r ON r.review_link = s.review_url_resolved "
        "WHERE s.status = 'inserted' AND r.created_at >= ?", (since,)).fetchall()
    print(f'PI rows inserted since {since}: {len(rows)}\n')

    by_journal = collections.defaultdict(list)
    for j, doi, title in rows:
        by_journal[j].append((doi, title))

    scanned, add_candidates, pi_only = [], [], []
    for j, items in sorted(by_journal.items(), key=lambda kv: -len(kv[1])):
        if len(items) < args.min:
            continue
        n_doi = sum(1 for d, _ in items if d)
        if j in JOURNALS:
            scanned.append((j, items))
        elif n_doi >= len(items) / 2:
            add_candidates.append((j, len(items), n_doi))
        else:
            pi_only.append((j, len(items), n_doi))

    print('== Unscanned, reviews have Crossref DOIs: candidates for journals.py ==')
    for j, n, d in add_candidates:
        print(f'  {n:4d} missed  {d:4d} with DOI  {j}')
    print('\n== Unscanned, few or no DOIs: PI or TOC alerts are the route ==')
    for j, n, d in pi_only:
        print(f'  {n:4d} missed  {d:4d} with DOI  {j}')

    print('\n== Scanned journals with misses: why the scan skipped them ==')
    if scanned:
        import crossref_scraper as cs
        scraper = cs.CrossrefReviewScraper()
    for j, items in scanned:
        cfg = JOURNALS[j]
        with_doi = [(d, t) for d, t in items if d]
        reasons = collections.Counter()
        examples = {}
        for doi, _ in with_doi[:args.samples]:
            why, title = _why_missed(scraper, doi, cfg)
            reasons[why] += 1
            examples.setdefault(why, (doi, title[:80]))
            time.sleep(0.2)
        print(f'\n  {j}: {len(items)} missed, {len(with_doi)} with DOI '
              f"(mode {cfg.get('detection_mode', 'all')})")
        for why, n in reasons.most_common():
            doi, title = examples[why]
            print(f'    {n} x {why}   e.g. {doi}  "{title}"')
        if not with_doi:
            print('    no DOIs: reviews not deposited in Crossref')


if __name__ == '__main__':
    main()
