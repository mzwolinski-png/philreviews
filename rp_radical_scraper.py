#!/usr/bin/env python3
"""
Scraper for Radical Philosophy book reviews.

Scrapes the /category/reviews pages at radicalphilosophy.com.
Structured entries (from RP 2.01, Feb 2018 onwards) have subtitles like:
  "Review of Author, <em>Book Title</em> (City: Publisher, Year)"

Older entries are review compilations without individual book metadata
and are skipped.
"""

import re
import sys
import time
import requests
from datetime import datetime

import db

JOURNAL_NAME = 'Radical Philosophy'
BASE_URL = 'https://www.radicalphilosophy.com/category/reviews'

# Map issue references to approximate dates
# RP 2.XX uses season/year format: "RP 2.20 (Winter 2026)"
SEASON_MONTH = {
    'winter': '01', 'spring': '04', 'summer': '07', 'autumn': '10',
    'february': '02', 'march': '03', 'april': '04', 'may': '05',
    'june': '06', 'july': '07', 'august': '08', 'september': '09',
    'october': '10', 'november': '11', 'december': '12',
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'may/june': '05', 'jul/aug': '07', 'sept/oct': '09', 'nov/dec': '11',
    'jan/feb': '01', 'mar/apr': '03', 'may/jun': '05', 'jul/august': '07',
    'sep/oct': '09',
}


def parse_issue_date(issue_str: str) -> str:
    """Parse an issue string like 'RP 2.20 (Winter 2026)' into a date."""
    # Extract year and season/month from parentheses
    m = re.search(r'\(([^)]+)\)', issue_str)
    if not m:
        return ''
    content = m.group(1).strip()

    # Find year
    year_m = re.search(r'(\d{4})', content)
    year = year_m.group(1) if year_m else ''
    if not year:
        return ''

    # Find season/month
    content_lower = content.lower()
    month = '01'
    for key, val in SEASON_MONTH.items():
        if key in content_lower:
            month = val
            break

    return f'{year}-{month}-01'


def parse_subtitle(subtitle_html: str) -> dict:
    """Parse a review subtitle to extract book author and title.

    Expected format: "Review of Author, <em>Book Title</em> (City: Publisher, Year)"
    """
    if not subtitle_html:
        return {}

    # Extract book title from <em> tags
    em_m = re.search(r'<em>([^<]+)</em>', subtitle_html)
    book_title = em_m.group(1).strip() if em_m else ''

    # Extract author: between "Review of" / "Reivew of" and the first <em>
    # or between "Review of" and the comma before the title
    clean = re.sub(r'<[^>]+>', '', subtitle_html).strip()

    # Handle "Review of Author, Title (Publisher)" or "Review of Author, Title"
    author_m = re.match(r'(?:Re(?:view|ivew)\s+of\s+)(.+?)(?:,\s+' + re.escape(book_title) + '|$)', clean)
    if not author_m and book_title:
        # Try splitting at the book title
        parts = clean.split(book_title)
        if parts:
            before = parts[0].strip()
            before = re.sub(r'^Re(?:view|ivew)\s+of\s+', '', before).strip().rstrip(',').strip()
            if before:
                author_m = type('', (), {'group': lambda self, n: before})()

    book_author = ''
    if author_m:
        book_author = author_m.group(1).strip().rstrip(',').strip()
        # Clean "eds." / "ed." suffix
        book_author = re.sub(r',?\s*\(?eds?\.?\)?\s*$', '', book_author, flags=re.IGNORECASE).strip()

    # Split author into first/last
    first, last = '', ''
    if book_author:
        # Handle "and" for multiple authors — take first author only
        book_author = re.split(r'\s+and\s+', book_author, maxsplit=1)[0].strip()
        parts = book_author.split()
        if len(parts) >= 2:
            last = parts[-1]
            first = ' '.join(parts[:-1])
        elif len(parts) == 1:
            last = parts[0]

    return {
        'book_title': book_title,
        'book_author_first': first,
        'book_author_last': last,
    }


def scrape_page(page_num: int, session: requests.Session) -> list:
    """Scrape one page of reviews. Returns list of review dicts."""
    url = BASE_URL if page_num == 1 else f'{BASE_URL}/page/{page_num}'
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return []
    except Exception as e:
        print(f'  Error fetching page {page_num}: {e}')
        return []

    html = resp.text
    articles = re.findall(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)

    reviews = []
    for art in articles:
        # Check for subtitle (structured entry)
        sub_m = re.search(r"class=['\"]subtitle['\"]>(.*?)</div>", art, re.DOTALL)
        subtitle_html = sub_m.group(1).strip() if sub_m else ''
        if not subtitle_html:
            continue  # Skip unstructured review compilations

        # Parse book info from subtitle
        book_info = parse_subtitle(subtitle_html)
        if not book_info.get('book_title'):
            continue

        # Review link and title
        link_m = re.search(r"<h3[^>]*><a\s+href='([^']+)'", art)
        if not link_m:
            link_m = re.search(r'<h3[^>]*><a\s+href="([^"]+)"', art)
        link = link_m.group(1) if link_m else ''

        title_m = re.search(r'<h3[^>]*><a[^>]+>([^<]+)</a>', art)
        review_title = title_m.group(1).strip() if title_m else ''

        # Reviewer
        reviewer_m = re.search(r'class="author url fn"[^>]*>([^<]+)<', art)
        reviewer = reviewer_m.group(1).strip() if reviewer_m else ''

        reviewer_first, reviewer_last = '', ''
        if reviewer:
            parts = reviewer.split()
            if len(parts) >= 2:
                reviewer_last = parts[-1]
                reviewer_first = ' '.join(parts[:-1])
            elif len(parts) == 1:
                reviewer_last = parts[0]

        # Issue/date
        issue_m = re.search(r"class=['\"]issue-link['\"]><a[^>]+>([^<]+)<", art)
        issue_str = issue_m.group(1).strip() if issue_m else ''
        pub_date = parse_issue_date(issue_str)

        reviews.append({
            'book_title': book_info['book_title'],
            'book_author_first_name': book_info.get('book_author_first', ''),
            'book_author_last_name': book_info.get('book_author_last', ''),
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
    parser = argparse.ArgumentParser(description='Scrape Radical Philosophy book reviews')
    parser.add_argument('--dry-run', action='store_true', help='Print results without inserting')
    parser.add_argument('--max-pages', type=int, default=0, help='Max pages to scrape (0=all)')
    args = parser.parse_args()

    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    print('Scraping Radical Philosophy reviews...')
    all_reviews = []
    page = 1

    while True:
        if args.max_pages and page > args.max_pages:
            break

        reviews = scrape_page(page, session)
        if not reviews:
            # Check if page exists at all
            url = BASE_URL if page == 1 else f'{BASE_URL}/page/{page}'
            try:
                resp = session.get(url, timeout=10)
                if resp.status_code == 404:
                    break
                # Page exists but no structured reviews — we've hit the older entries
                if page > 1:
                    # Check if there are any articles at all
                    articles = re.findall(r'<article[^>]*>', resp.text)
                    if not articles:
                        break
                    # If there are articles but none have subtitles, we're past structured reviews
                    print(f'  Page {page}: 0 structured reviews (reached older compilations)')
                    page += 1
                    time.sleep(1)
                    continue
            except Exception:
                break

        all_reviews.extend(reviews)
        print(f'  Page {page}: {len(reviews)} reviews (total: {len(all_reviews)})')
        page += 1
        time.sleep(1)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'RESULTS')
    print(f'{"=" * 60}')
    print(f'Pages scraped: {page - 1}')
    print(f'Total book reviews: {len(all_reviews)}')

    with_author = sum(1 for r in all_reviews if r['book_author_last_name'])
    with_reviewer = sum(1 for r in all_reviews if r['reviewer_last_name'])
    print(f'With book author: {with_author}/{len(all_reviews)}')
    print(f'With reviewer: {with_reviewer}/{len(all_reviews)}')

    # Samples
    print(f'\nSample reviews:')
    for r in all_reviews[:5]:
        print(f'  "{r["book_title"][:50]}" by {r["book_author_first_name"]} {r["book_author_last_name"]}')
        print(f'    Reviewed by {r["reviewer_first_name"]} {r["reviewer_last_name"]}')
        print(f'    {r["publication_date"]} | {r["review_link"][:80]}')
        print()

    # Insert
    if not args.dry_run and all_reviews:
        new_reviews = [r for r in all_reviews if not db.review_link_exists(r['review_link'])]
        print(f'\nInserting {len(new_reviews)} new reviews ({len(all_reviews) - len(new_reviews)} duplicates skipped)...')
        if new_reviews:
            db.insert_reviews(new_reviews)
            print('Done.')
    elif args.dry_run:
        print('\nDry run — skipping database insert')


if __name__ == '__main__':
    main()
