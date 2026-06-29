#!/usr/bin/env python3
"""
Scraper for Journal of Markets & Morality book reviews.

Uses the sitemap to find review URLs (slug contains "review-of" or
"review-essay"), then reads citation meta tags from each page.

Title format: Review of "Book Title" by Book Author
Meta tags: citation_author (reviewer), citation_publication_date
"""

import re
import sys
import time
import html as html_mod
import requests

import db

JOURNAL_NAME = 'Journal of Markets and Morality'
SITEMAP_URL = 'https://www.marketsandmorality.com/sitemap.xml'
RSS_URL = 'https://www.marketsandmorality.com/feed'


def parse_review_title(title: str) -> dict:
    """Parse book title and author from review title string."""
    title = html_mod.unescape(title)

    # Pattern: Review of "Book Title" by Book Author
    m = re.match(
        r'^Review of\s+["\u201c](.+?)["\u201d]\s+by\s+(.+?)$',
        title, re.IGNORECASE,
    )
    if m:
        return {'book_title': m.group(1).strip(), 'book_author': m.group(2).strip()}

    # Pattern: Review Essay: Title
    m = re.match(r'^Review Essay:\s+(.+?)$', title, re.IGNORECASE)
    if m:
        return {'book_title': m.group(1).strip(), 'book_author': ''}

    return {}


def parse_review_page(html: str, url: str) -> dict:
    """Extract review metadata from a page's meta tags."""
    def meta(name):
        m = re.search(rf'name="{name}"\s+content="([^"]*)"', html)
        if not m:
            m = re.search(rf'content="([^"]*)"\s+name="{name}"', html)
        return html_mod.unescape(m.group(1)) if m else ''

    title = meta('citation_title')
    reviewer = meta('citation_author')
    date_str = meta('citation_publication_date')

    if not title:
        return {}

    parsed = parse_review_title(title)
    if not parsed:
        return {}

    # Parse date: "2026/3/25" or "2004/1/1"
    pub_date = ''
    if date_str:
        parts = date_str.split('/')
        if len(parts) >= 2:
            pub_date = f'{parts[0]}-{int(parts[1]):02d}-01'

    book_author = parsed.get('book_author', '')
    book_first, book_last = split_name(book_author)
    rev_first, rev_last = split_name(reviewer)

    return {
        'book_title': parsed.get('book_title', ''),
        'book_author_first_name': book_first,
        'book_author_last_name': book_last,
        'reviewer_first_name': rev_first,
        'reviewer_last_name': rev_last,
        'publication_source': JOURNAL_NAME,
        'publication_date': pub_date,
        'review_link': url,
        'review_summary': '',
        'access_type': 'Open',
        'doi': '',
    }


def split_name(name: str) -> tuple:
    if not name:
        return '', ''
    # Handle "First Last and First Last" (take first author only)
    name = re.split(r'\s+and\s+', name)[0].strip()
    parts = name.split()
    if len(parts) >= 2:
        return ' '.join(parts[:-1]), parts[-1]
    return '', parts[0] if parts else ''


class MMScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    def get_review_urls_from_sitemap(self) -> list:
        """Get all review URLs from the sitemap."""
        resp = self.session.get(SITEMAP_URL, timeout=30)
        resp.raise_for_status()
        urls = re.findall(r'<loc>(.*?)</loc>', resp.text)
        return [u for u in urls if 'review-of' in u.lower() or 'review-essay' in u.lower()]

    def get_review_urls_from_rss(self) -> list:
        """Get recent review URLs from RSS feed."""
        resp = self.session.get(RSS_URL, timeout=30)
        items = re.findall(r'<item>(.*?)</item>', resp.text, re.DOTALL)
        urls = []
        for item in items:
            title = re.search(r'<title>(.*?)</title>', item)
            link = re.search(r'<link>(.*?)</link>', item)
            if title and link:
                t = title.group(1).lower()
                if 'review of' in t or 'review essay' in t:
                    urls.append(link.group(1))
        return urls

    def run(self, dry_run=False, full_scan=False):
        """Run scraper. Default: RSS only. full_scan: all sitemap URLs."""
        if full_scan:
            urls = self.get_review_urls_from_sitemap()
            print(f'Markets & Morality: {len(urls)} review URLs from sitemap')
        else:
            urls = self.get_review_urls_from_rss()
            print(f'Markets & Morality: {len(urls)} review URLs from RSS')

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
                parsed = parse_review_page(resp.text, url)
                if parsed and parsed.get('book_title'):
                    reviews.append(parsed)
                time.sleep(1)
            except Exception as e:
                print(f'  Error fetching {url}: {e}')
                time.sleep(2)

            if (i + 1) % 50 == 0:
                print(f'  Progress: {i + 1}/{len(new_urls)} pages fetched')

        if reviews and not dry_run:
            db.insert_reviews(reviews)

        print(f'  Parsed {len(reviews)} reviews from {len(new_urls)} pages')
        return {'found': len(urls), 'new': len(reviews), 'uploaded': len(reviews)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Scrape Journal of Markets & Morality reviews')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--full', action='store_true', help='Full sitemap scan (not just RSS)')
    args = parser.parse_args()

    scraper = MMScraper()
    stats = scraper.run(dry_run=args.dry_run, full_scan=args.full)
    print(f'\nResults: {stats}')


if __name__ == '__main__':
    main()
