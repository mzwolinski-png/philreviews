#!/usr/bin/env python3
"""
Scraper for Journal of Markets & Morality book reviews.

Uses the Scholastica API at marketsandmorality.com to fetch all reviews
from section ID 5169 (Reviews, 604 published articles).

Title format: "Review of _Title_ by Author" or "Review of _Title_ by Author (Editor)"
"""

import re
import sys
import time
import requests
from datetime import datetime

import db

JOURNAL_NAME = 'Journal of Markets and Morality'
API_BASE = 'https://www.marketsandmorality.com/api/v1/articles'
SECTION_ID = 5169  # Reviews section
PER_PAGE = 100

# Titles to skip (not actual book reviews)
SKIP_TITLES = {
    'Other Books of Interest',
    'Books Received',
}


def parse_review_title(title: str) -> dict:
    """Parse a review title to extract book title and author.

    Formats:
      "Review of _Book Title_ by Author Name"
      "Review of _Book Title_ by Author Name (Editor)"
      "Review: \"Book Title\" by Author Name"
    """
    result = {
        'book_title': '',
        'book_author_first': '',
        'book_author_last': '',
    }

    # Skip non-review entries
    if not title or title.strip() in SKIP_TITLES:
        return result

    # Strip "Review of " or "Review: " prefix
    m = re.match(r'^Review\s+of\s+(.+)', title, re.IGNORECASE)
    if not m:
        m = re.match(r'^Review:\s+(.+)', title, re.IGNORECASE)
    if not m:
        return result

    remainder = m.group(1).strip()

    # Extract book title from _underscores_ or "quotes"
    title_m = re.match(r'_(.+?)_\s*(.*)$', remainder)
    if not title_m:
        title_m = re.match(r'"(.+?)"\s*(.*)$', remainder)
    if not title_m:
        # Try without markers — "Review of Title by Author"
        title_m = re.match(r'(.+?)\s+by\s+(.+)$', remainder, re.IGNORECASE)
        if title_m:
            result['book_title'] = title_m.group(1).strip()
            author_str = title_m.group(2).strip()
        else:
            # Just a title with no author
            result['book_title'] = remainder.strip()
            return result
    else:
        result['book_title'] = title_m.group(1).strip()
        author_str = title_m.group(2).strip()

    if not author_str:
        return result

    # Strip "by " prefix
    author_str = re.sub(r'^by\s+', '', author_str, flags=re.IGNORECASE).strip()

    # Strip "(Editor)" / "(Editors)" / "ed." / "eds."
    author_str = re.sub(r'\s*\((?:Editor|Editors|Ed\.|Eds\.)\)\s*$', '', author_str, flags=re.IGNORECASE).strip()
    author_str = re.sub(r',?\s*(?:eds?\.)?\s*$', '', author_str, flags=re.IGNORECASE).strip()

    # Take first author if multiple (split on " and ")
    author_str = re.split(r'\s+and\s+', author_str, maxsplit=1)[0].strip()

    # Split into first/last
    if author_str:
        parts = author_str.split()
        if len(parts) >= 2:
            result['book_author_last'] = parts[-1]
            result['book_author_first'] = ' '.join(parts[:-1])
        elif len(parts) == 1:
            result['book_author_last'] = parts[0]

    return result


def fetch_reviews(session: requests.Session) -> list:
    """Fetch all reviews from the API."""
    all_articles = []
    page = 1

    while True:
        url = f'{API_BASE}?section_id={SECTION_ID}&per_page={PER_PAGE}&page={page}'
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code != 200:
                print(f'  Error on page {page}: HTTP {resp.status_code}')
                break

            data = resp.json()
            articles = data.get('articles', [])
            if not articles:
                break

            all_articles.extend(articles)
            print(f'  Page {page}: {len(articles)} articles (total: {len(all_articles)})')

            if len(articles) < PER_PAGE:
                break

            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f'  Error on page {page}: {e}')
            break

    return all_articles


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Scrape Journal of Markets & Morality reviews')
    parser.add_argument('--dry-run', action='store_true', help='Print results without inserting')
    args = parser.parse_args()

    session = requests.Session()
    session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    print('Fetching Journal of Markets & Morality reviews...')
    articles = fetch_reviews(session)

    reviews = []
    skipped = 0
    for art in articles:
        title = art.get('title', '').strip()

        # Skip non-review entries
        if title in SKIP_TITLES or not title:
            skipped += 1
            continue

        # Skip "Note on..." entries
        if title.startswith('Note on'):
            skipped += 1
            continue

        book_info = parse_review_title(title)
        if not book_info.get('book_title'):
            skipped += 1
            continue

        # Extract reviewer from authors array or title
        # API returns empty authors array in list view, so we parse from abstract if needed
        pub_date = art.get('published_at', '')
        if pub_date:
            # Parse ISO date to YYYY-MM-DD
            try:
                dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                pub_date = dt.strftime('%Y-%m-%d')
            except Exception:
                pub_date = ''

        # Build review link
        review_link = f'https://www.marketsandmorality.com/article/{art["id"]}'

        reviews.append({
            'book_title': book_info['book_title'],
            'book_author_first_name': book_info.get('book_author_first', ''),
            'book_author_last_name': book_info.get('book_author_last', ''),
            'reviewer_first_name': '',
            'reviewer_last_name': '',
            'publication_source': JOURNAL_NAME,
            'publication_date': pub_date,
            'review_link': review_link,
            'review_summary': '',
            'access_type': 'Open',
            'doi': '',
        })

    # Summary
    print(f'\n{"=" * 60}')
    print(f'RESULTS')
    print(f'{"=" * 60}')
    print(f'Total API articles: {len(articles)}')
    print(f'Skipped: {skipped}')
    print(f'Book reviews: {len(reviews)}')

    with_author = sum(1 for r in reviews if r['book_author_last_name'])
    print(f'With book author: {with_author}/{len(reviews)}')

    # Samples
    print(f'\nSample reviews:')
    for r in reviews[:5]:
        print(f'  "{r["book_title"][:60]}" by {r["book_author_first_name"]} {r["book_author_last_name"]}')
        print(f'    {r["publication_date"]} | {r["review_link"]}')
        print()

    # Insert
    if not args.dry_run and reviews:
        new_reviews = [r for r in reviews if not db.review_link_exists(r['review_link'])]
        print(f'\nInserting {len(new_reviews)} new reviews ({len(reviews) - len(new_reviews)} duplicates skipped)...')
        if new_reviews:
            db.insert_reviews(new_reviews)
            print('Done.')
    elif args.dry_run:
        print('\nDry run — skipping database insert')


if __name__ == '__main__':
    main()
