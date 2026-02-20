#!/usr/bin/env python3
"""
Scraper for The Independent Review book reviews.

Since the live site (independent.org) is behind Cloudflare, this scraper
uses the Wayback Machine to access cached issue pages.

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
WAYBACK_PREFIX = 'https://web.archive.org/web/'
BASE_URL = 'https://www.independent.org/research/independent-review/issue/'

# All issue slugs from 1996 to 2025
ISSUE_SLUGS = [
    '1996-spring', '1996-fall', '1996-97-winter',
    '1997-spring', '1997-summer', '1997-fall', '1997-98-winter',
    '1998-spring', '1998-summer', '1998-fall', '1998-99-winter',
    '1999-spring', '1999-summer', '1999-fall', '1999-00-winter',
    '2000-spring', '2000-summer', '2000-fall', '2000-01-winter',
    '2001-spring', '2001-summer', '2001-fall', '2001-02-winter',
    '2002-spring', '2002-summer', '2002-fall', '2002-03-winter',
    '2003-spring', '2003-summer', '2003-fall', '2003-04-winter',
    '2004-spring', '2004-summer', '2004-fall', '2004-05-winter',
    '2005-spring', '2005-summer', '2005-fall', '2005-06-winter',
    '2006-spring', '2006-summer', '2006-fall', '2006-07-winter',
    '2007-spring', '2007-summer', '2007-fall', '2007-08-winter',
    '2008-spring', '2008-summer', '2008-fall', '2008-09-winter',
    '2009-spring', '2009-summer', '2009-fall', '2009-10-winter',
    '2010-spring', '2010-summer', '2010-fall', '2010-11-winter',
    '2011-spring', '2011-summer', '2011-fall', '2011-12-winter',
    '2012-spring', '2012-summer', '2012-fall', '2012-13-winter',
    '2013-spring', '2013-summer', '2013-fall', '2013-14-winter',
    '2014-spring', '2014-summer', '2014-fall', '2014-15-winter',
    '2015-spring', '2015-summer', '2015-fall', '2015-16-winter',
    '2016-spring', '2016-summer', '2016-fall', '2016-17-winter',
    '2017-spring', '2017-summer', '2017-fall', '2017-18-winter',
    '2018-spring', '2018-summer', '2018-fall', '2018-19-winter',
    '2019-spring', '2019-summer', '2019-fall', '2019-20-winter',
    '2020-spring', '2020-summer', '2020-fall', '2020-21-winter',
    '2021-spring', '2021-summer', '2021-fall', '2021-22-winter',
    '2022-spring', '2022-summer', '2022-fall', '2022-23-winter',
    '2023-spring', '2023-summer', '2023-fall', '2023-24-winter',
    '2024-spring', '2024-summer', '2024-fall', '2024-25-winter',
    '2025-spring', '2025-summer', '2025-fall',
]

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


def fetch_issue_page(slug: str, session: requests.Session) -> str:
    """Fetch an issue page via the Wayback Machine."""
    url = f'{BASE_URL}{slug}/'
    wayback_url = f'{WAYBACK_PREFIX}2025/{url}'
    try:
        resp = session.get(wayback_url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        # Try page 2 as well (some issues split content across pages)
        return ''
    except Exception as e:
        print(f'  Error fetching {slug}: {e}')
        return ''


def extract_reviews(html: str, slug: str) -> list:
    """Extract book review data from an issue page."""
    reviews = []
    pub_date = slug_to_date(slug)

    # Find book-review cards
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

        # Clean extra whitespace
        book_title = re.sub(r'\s+', ' ', book_title).strip()

        # Link
        link_m = re.search(r'<h[23][^>]*><a\s+href="([^"]+)"', content)
        if not link_m:
            link_m = re.search(r'href="([^"]*(?:independent\.org|/tir/)[^"]+)"', content)
        link = link_m.group(1) if link_m else ''
        # Strip Wayback prefix
        link = re.sub(r'https://web\.archive\.org/web/\d+/', '', link)

        # Book author: "By Author Name"
        by_m = re.search(r'<div class="pc-author">By ([^<]+)</div>', content)
        book_author = by_m.group(1).strip() if by_m else ''

        # Split author into first/last
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


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Scrape The Independent Review book reviews')
    parser.add_argument('--dry-run', action='store_true', help='Print results without inserting')
    parser.add_argument('--max-issues', type=int, default=0, help='Max issues to scrape (0=all)')
    parser.add_argument('--issues', nargs='+', help='Specific issue slugs to scrape')
    args = parser.parse_args()

    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    slugs = args.issues if args.issues else ISSUE_SLUGS
    if args.max_issues:
        slugs = slugs[:args.max_issues]

    print(f'Scraping {len(slugs)} issues from The Independent Review...')
    all_reviews = []
    failed_issues = []

    for i, slug in enumerate(slugs):
        html = fetch_issue_page(slug, session)
        if not html:
            failed_issues.append(slug)
            continue

        # Check if it's a Cloudflare challenge
        if 'Just a moment' in html or 'cf_chl' in html:
            print(f'  [{slug}] Cloudflare challenge — skipping')
            failed_issues.append(slug)
            continue

        reviews = extract_reviews(html, slug)

        # Also check page 2 if it exists
        html2 = ''
        try:
            wayback_url2 = f'{WAYBACK_PREFIX}2025/{BASE_URL}{slug}/page/2/'
            resp2 = session.get(wayback_url2, timeout=30)
            if resp2.status_code == 200 and 'pc-card' in resp2.text:
                reviews2 = extract_reviews(resp2.text, slug)
                reviews.extend(reviews2)
        except Exception:
            pass

        all_reviews.extend(reviews)
        print(f'  [{slug}] {len(reviews)} book reviews')

        # Rate limiting: be respectful to Wayback Machine
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

    # Show samples
    print(f'\nSample reviews:')
    for r in all_reviews[:5]:
        print(f'  "{r["book_title"][:50]}" by {r["book_author_first_name"]} {r["book_author_last_name"]}')
        print(f'    Reviewed by {r["reviewer_first_name"]} {r["reviewer_last_name"]}')
        print(f'    {r["publication_date"]} | {r["review_link"][:80]}')
        print()

    # Insert into database
    if not args.dry_run and all_reviews:
        # Filter out duplicates by review_link
        new_reviews = [r for r in all_reviews if not db.review_link_exists(r['review_link'])]
        print(f'\nInserting {len(new_reviews)} new reviews ({len(all_reviews) - len(new_reviews)} duplicates skipped)...')
        if new_reviews:
            db.insert_reviews(new_reviews)
            print(f'Done.')
    elif args.dry_run:
        print('\nDry run — skipping database insert')


if __name__ == '__main__':
    main()
