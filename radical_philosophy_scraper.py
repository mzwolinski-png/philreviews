#!/usr/bin/env python3
"""
Scraper for Radical Philosophy book reviews.

Uses the WordPress REST API to discover review posts, then fetches each
review page to parse structured book metadata.

Review pages have a consistent format:
    [Review Title]
    Review of [Book Author],
    [Book Title]
    ([City]: [Publisher], [Year])
    [Reviewer Name]
    RP [issue] ([Season] [Year])
"""

import re
import sys
import time
import requests

import db

JOURNAL_NAME = 'Radical Philosophy'
WP_API = 'https://www.radicalphilosophy.com/wp-json/wp/v2/posts'
REVIEW_CATEGORY_ID = 10090  # "Review" category in WP

SEASON_TO_MONTH = {
    'spring': '03', 'summer': '06', 'autumn': '09', 'winter': '12',
}


def parse_review_page(html: str):
    """Parse book metadata from a review page's article element."""
    article = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
    if not article:
        return None

    text = re.sub(r'<[^>]+>', '\n', article.group(1))
    lines = [l.strip() for l in text.split('\n') if l.strip()]

    if len(lines) < 4:
        return None

    # Find the "Review of" line
    review_of_idx = None
    for i, line in enumerate(lines):
        if line.lower().startswith('review of '):
            review_of_idx = i
            break

    if review_of_idx is None:
        return None

    # Book author: "Review of Author," or "Review of Author and Author,"
    book_author_line = lines[review_of_idx]
    book_author = re.sub(r'^[Rr]eview of\s+', '', book_author_line).rstrip(',').strip()
    # Handle "edited by" or "ed."
    book_author = re.sub(r'\s*\(eds?\.?\)', '', book_author).strip()

    # Book title: next line(s) until we hit a parenthetical (publisher) or reviewer
    title_lines = []
    j = review_of_idx + 1
    while j < len(lines):
        if lines[j].startswith('(') or re.match(r'^RP\s', lines[j]):
            break
        # Stop if we hit the tilde separator
        if lines[j] == '~':
            break
        title_lines.append(lines[j])
        j += 1

    book_title = ' '.join(title_lines).strip()
    # Clean up publisher info that might be appended
    book_title = re.sub(r'\s*\([^)]*\d{4}[^)]*\)\s*$', '', book_title).strip()

    if not book_title:
        return None

    # Publisher/year: look for "(City: Publisher, Year)" pattern
    pub_year = ''
    for line in lines[review_of_idx:review_of_idx + 6]:
        year_m = re.search(r'\b((?:19|20)\d{2})\b', line)
        if year_m and '(' in line:
            pub_year = year_m.group(1)
            break

    # Reviewer name: line before "RP X.XX" issue reference
    reviewer = ''
    for i, line in enumerate(lines):
        if re.match(r'^RP\s+\d', line):
            if i > 0 and not lines[i - 1].startswith('('):
                reviewer = lines[i - 1].strip()
            break

    # Issue/date: "RP 2.20 (Winter 2026)" or "RP 214 (Autumn 2019)"
    pub_date = ''
    for line in lines:
        m = re.match(r'RP\s+[\d.]+\s*\((\w+)\s+(\d{4})\)', line)
        if m:
            season = m.group(1).lower()
            year = m.group(2)
            month = SEASON_TO_MONTH.get(season, '01')
            pub_date = f'{year}-{month}-01'
            break

    if not pub_date and pub_year:
        pub_date = f'{pub_year}-01-01'

    # Split names
    book_first, book_last = split_name(book_author)
    rev_first, rev_last = split_name(reviewer)

    return {
        'book_title': book_title,
        'book_author_first_name': book_first,
        'book_author_last_name': book_last,
        'reviewer_first_name': rev_first,
        'reviewer_last_name': rev_last,
        'publication_source': JOURNAL_NAME,
        'publication_date': pub_date,
        'review_summary': '',
        'access_type': 'Subscription',
        'doi': '',
    }


def split_name(name: str) -> tuple:
    if not name:
        return '', ''
    parts = name.split()
    if len(parts) >= 2:
        return ' '.join(parts[:-1]), parts[-1]
    return '', parts[0] if parts else ''


class RadicalPhilosophyScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    def get_review_urls(self, max_pages=2) -> list:
        """Get recent review post URLs from WP REST API."""
        urls = []
        for page in range(1, max_pages + 1):
            try:
                resp = self.session.get(WP_API, params={
                    'categories': REVIEW_CATEGORY_ID,
                    'per_page': 100,
                    'page': page,
                    'orderby': 'date',
                    'order': 'desc',
                }, timeout=30)
                if resp.status_code != 200:
                    break
                posts = resp.json()
                if not posts:
                    break
                for p in posts:
                    urls.append(p.get('link', ''))
            except Exception as e:
                print(f'  Error fetching page {page}: {e}')
                break
        return [u for u in urls if u]

    def run(self, dry_run=False, max_pages=2):
        """Incremental run: check recent review pages for new entries."""
        urls = self.get_review_urls(max_pages=max_pages)
        print(f'Radical Philosophy: {len(urls)} review URLs from API')

        # Filter to new URLs only
        new_urls = [u for u in urls if not db.review_link_exists(u)]
        print(f'  {len(new_urls)} new (not yet in DB)')

        if not new_urls:
            return {'found': len(urls), 'new': 0, 'uploaded': 0}

        reviews = []
        for i, url in enumerate(new_urls):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
                parsed = parse_review_page(resp.text)
                if parsed:
                    parsed['review_link'] = url
                    reviews.append(parsed)
                time.sleep(1)
            except Exception as e:
                print(f'  Error fetching {url}: {e}')
                time.sleep(2)

            if (i + 1) % 20 == 0:
                print(f'  Progress: {i + 1}/{len(new_urls)} pages fetched')

        if reviews and not dry_run:
            db.insert_reviews(reviews)

        print(f'  Parsed {len(reviews)} reviews from {len(new_urls)} pages')
        return {'found': len(urls), 'new': len(reviews), 'uploaded': len(reviews)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Scrape Radical Philosophy reviews')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--max-pages', type=int, default=44, help='API pages to fetch (44=all)')
    args = parser.parse_args()

    scraper = RadicalPhilosophyScraper()
    stats = scraper.run(dry_run=args.dry_run, max_pages=args.max_pages)
    print(f'\nResults: {stats}')


if __name__ == '__main__':
    main()
