#!/usr/bin/env python3
"""
Scraper for The Independent Review book reviews.

Scrapes the live site at independent.org. Auto-discovers issue slugs from the
main journal page and supports incremental mode (only recent issues) for
weekly integration.

Each issue page contains book-review cards with:
  - Book title (h3.pc-title)
  - Book author ("By Author Name")
  - Reviewer ("Reviewed by Reviewer Name")
  - Link to the review article
"""

import re
import sys
import time
import sqlite3
import requests
from datetime import datetime

import db

JOURNAL_NAME = 'The Independent Review'
BASE_URL = 'https://www.independent.org/research/independent-review/'
ISSUE_URL = BASE_URL + 'issue/'

SEASON_TO_MONTH = {
    'spring': '03', 'summer': '06', 'fall': '09', 'winter': '12',
}


def slug_to_date(slug: str) -> str:
    """Convert an issue slug like '2025-fall' to a date string like '2025-09-01'."""
    parts = slug.split('-')
    year = parts[0]
    season = parts[-1]
    month = SEASON_TO_MONTH.get(season, '01')
    return f'{year}-{month}-01'


class SiteBlocked(Exception):
    """independent.org is behind a bot challenge and cannot be scraped."""


def discover_issue_slugs(session: requests.Session) -> list:
    """Fetch all issue slugs from the main TIR page, sorted chronologically.

    independent.org sits behind a Cloudflare challenge as of 2026-08-30
    (`cf-mitigated: challenge` on every path, including /robots.txt and the
    RSS feed, for any user-agent). TIR deposits no Crossref DOIs, so there is
    no metadata fallback; raise SiteBlocked so the caller can report the cause
    instead of a bare 403 traceback.
    """
    resp = session.get(BASE_URL, timeout=30)
    if resp.status_code in (403, 429, 503) and (
            "cf-mitigated" in {k.lower() for k in resp.headers}
            or "just a moment" in resp.text[:600].lower()):
        raise SiteBlocked(
            f"independent.org returned {resp.status_code} behind a Cloudflare "
            f"challenge; TIR cannot be scraped until that is lifted")
    resp.raise_for_status()
    slugs = set(re.findall(r'/research/independent-review/issue/([^/\"]+)/', resp.text))
    return sorted(slugs, key=lambda s: slug_to_date(s))


def fetch_issue_page(slug: str, session: requests.Session) -> str:
    """Fetch an issue page from the live site."""
    url = f'{ISSUE_URL}{slug}/'
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        return ''
    except Exception as e:
        print(f'  Error fetching {slug}: {e}')
        return ''


def extract_reviews(html: str, slug: str) -> list:
    """Extract book review data from an issue page."""
    reviews = []
    pub_date = slug_to_date(slug)

    cards = list(re.finditer(
        r'class="pc-card\s+tir-article\s+book-review[^"]*">(.*?)(?=<div\s+class="pc-card|<footer|</main)',
        html, re.DOTALL
    ))

    for m in cards:
        content = m.group(1)

        # Title from h3.pc-title link text
        title_m = re.search(r'<h[23][^>]*class="pc-title[^"]*"[^>]*><a[^>]*>([^<]+)</a>', content)
        if not title_m:
            title_m = re.search(r'<h[23][^>]*class="pc-title[^"]*"[^>]*>([^<]+)<', content)
        book_title = title_m.group(1).strip() if title_m else ''
        if not book_title:
            continue

        book_title = re.sub(r'\s+', ' ', book_title).strip()

        # Link
        link_m = re.search(r'<h[23][^>]*><a\s+href="([^"]+)"', content)
        if not link_m:
            link_m = re.search(r'href="([^"]*(?:independent\.org|/tir/)[^"]+)"', content)
        link = link_m.group(1) if link_m else ''

        # Book author: "By Author Name"
        by_m = re.search(r'<div class="pc-author">By ([^<]+)</div>', content)
        book_author = by_m.group(1).strip() if by_m else ''

        book_first, book_last = '', ''
        if book_author:
            parts = book_author.split()
            if len(parts) >= 2:
                book_last = parts[-1]
                book_first = ' '.join(parts[:-1])
            elif len(parts) == 1:
                book_last = parts[0]

        # Reviewer: "Reviewed by Name"
        reviewer_m = re.search(
            r'Reviewed by.*?class="pc-author"[^>]*>([^<]+)<', content, re.DOTALL
        )
        if not reviewer_m:
            reviewer_m = re.search(r'Reviewed by\s+([^<]+)<', content)
        reviewer = reviewer_m.group(1).strip() if reviewer_m else ''

        reviewer_first, reviewer_last = '', ''
        if reviewer:
            parts = reviewer.split()
            if len(parts) >= 2:
                reviewer_last = parts[-1]
                reviewer_first = ' '.join(parts[:-1])
            elif len(parts) == 1:
                reviewer_last = parts[0]

        reviews.append({
            'book_title': book_title,
            'book_author_first_name': book_first,
            'book_author_last_name': book_last,
            'reviewer_first_name': reviewer_first,
            'reviewer_last_name': reviewer_last,
            'publication_source': JOURNAL_NAME,
            'publication_date': pub_date,
            'review_link': link,
            'review_summary': '',
            'access_type': 'Open',
            'doi': '',
        })

    return reviews


class TIRScraper:
    """Scraper with incremental mode for weekly integration."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    def run(self, dry_run=False, recent_issues=4):
        """Incremental run: check the most recent N issues for new reviews."""
        try:
            all_slugs = discover_issue_slugs(self.session)
        except SiteBlocked as e:
            # Not a scraper bug — report and skip rather than failing the run.
            print(f'TIR: SKIPPED — {e}')
            return {'found': 0, 'new': 0, 'uploaded': 0, 'blocked': True}
        if not all_slugs:
            print('TIR: Could not discover issue slugs')
            return {'found': 0, 'new': 0, 'uploaded': 0}

        # Only check the most recent issues
        slugs_to_check = all_slugs[-recent_issues:]
        print(f'TIR: checking {len(slugs_to_check)} recent issues: {", ".join(slugs_to_check)}')

        all_reviews = []
        for slug in slugs_to_check:
            html = fetch_issue_page(slug, self.session)
            if not html:
                continue
            reviews = extract_reviews(html, slug)
            all_reviews.extend(reviews)
            print(f'  [{slug}] {len(reviews)} book reviews')
            time.sleep(2)

        # Filter to new reviews only
        new_reviews = [r for r in all_reviews if not db.review_link_exists(r['review_link'])]

        if new_reviews and not dry_run:
            db.insert_reviews(new_reviews)

        print(f'TIR: {len(all_reviews)} found, {len(new_reviews)} new')
        return {
            'found': len(all_reviews),
            'new': len(new_reviews),
            'uploaded': len(new_reviews),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Scrape The Independent Review book reviews')
    parser.add_argument('--dry-run', action='store_true', help='Print results without inserting')
    parser.add_argument('--max-issues', type=int, default=0, help='Max issues to scrape (0=all)')
    parser.add_argument('--recent', type=int, default=0, help='Only check N most recent issues')
    parser.add_argument('--issues', nargs='+', help='Specific issue slugs to scrape')
    args = parser.parse_args()

    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    if args.issues:
        slugs = args.issues
    else:
        print('Discovering issues from TIR main page...')
        slugs = discover_issue_slugs(session)
        print(f'Found {len(slugs)} issues')

    if args.recent:
        slugs = slugs[-args.recent:]
    elif args.max_issues:
        slugs = slugs[:args.max_issues]

    print(f'Scraping {len(slugs)} issues from The Independent Review...')
    all_reviews = []
    failed_issues = []

    for i, slug in enumerate(slugs):
        html = fetch_issue_page(slug, session)
        if not html:
            failed_issues.append(slug)
            continue

        if 'Just a moment' in html or 'cf_chl' in html:
            print(f'  [{slug}] Cloudflare challenge — skipping')
            failed_issues.append(slug)
            continue

        reviews = extract_reviews(html, slug)
        all_reviews.extend(reviews)
        print(f'  [{slug}] {len(reviews)} book reviews')

        if (i + 1) % 5 == 0:
            time.sleep(2)
        else:
            time.sleep(1)

        if (i + 1) % 20 == 0:
            print(f'  Progress: {i + 1}/{len(slugs)} issues, {len(all_reviews)} reviews so far')

    # Summary
    print(f'\n{"=" * 60}')
    print(f'RESULTS')
    print(f'{"=" * 60}')
    print(f'Issues scraped: {len(slugs) - len(failed_issues)}/{len(slugs)}')
    print(f'Failed issues: {len(failed_issues)}')
    print(f'Total book reviews: {len(all_reviews)}')

    with_author = sum(1 for r in all_reviews if r['book_author_last_name'])
    with_reviewer = sum(1 for r in all_reviews if r['reviewer_last_name'])
    print(f'With book author: {with_author}/{len(all_reviews)}')
    print(f'With reviewer: {with_reviewer}/{len(all_reviews)}')

    if failed_issues:
        print(f'\nFailed issues: {", ".join(failed_issues[:20])}')

    print(f'\nSample reviews:')
    for r in all_reviews[:5]:
        print(f'  "{r["book_title"][:50]}" by {r["book_author_first_name"]} {r["book_author_last_name"]}')
        print(f'    Reviewed by {r["reviewer_first_name"]} {r["reviewer_last_name"]}')
        print(f'    {r["publication_date"]} | {r["review_link"][:80]}')
        print()

    if not args.dry_run and all_reviews:
        new_reviews = [r for r in all_reviews if not db.review_link_exists(r['review_link'])]
        print(f'\nInserting {len(new_reviews)} new reviews ({len(all_reviews) - len(new_reviews)} duplicates skipped)...')
        if new_reviews:
            db.insert_reviews(new_reviews)
            print(f'Done.')
    elif args.dry_run:
        print('\nDry run — skipping database insert')


if __name__ == '__main__':
    main()
