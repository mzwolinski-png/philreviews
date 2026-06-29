#!/usr/bin/env python3
"""
APA Blog "Recently Published Book Spotlight" scraper.

Scrapes https://blog.apaonline.org/category/recently-published-books/ — the
American Philosophical Association's Book Spotlight series, where newly
published philosophy book authors write about their own books.

These are author-presentations, not traditional reviews, but they serve
as a useful index of new philosophy books with brief introductions.

The "reviewer" field holds the first author of the post (who is also
the book author in most cases).

Usage:
    python3 apa_blog_scraper.py                  # incremental (first 2 pages)
    python3 apa_blog_scraper.py --bulk           # full backfill
    python3 apa_blog_scraper.py --dry-run        # parse only
"""

import argparse
import html
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://blog.apaonline.org"
ARCHIVE_URL = f"{BASE_URL}/category/recently-published-books/"
SOURCE_NAME = "APA Blog"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


class APABlogScraper(BaseScraper):
    """Scrapes Book Spotlight posts from the APA Blog."""

    name = "apa_blog"
    default_delay = 1.5

    def __init__(self):
        super().__init__()
        self.session.headers.update(HEADERS)
        self.stats = {
            "pages_scanned": 0,
            "urls_found": 0,
            "reviews_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def fetch_spotlight_urls(self, max_pages=None):
        """Scan category pages for spotlight URLs. Returns list of
        (url, title_hint) tuples."""
        items = []
        page = 1
        limit = max_pages or 30

        while page <= limit:
            url = ARCHIVE_URL if page == 1 else f"{ARCHIVE_URL}page/{page}/"
            try:
                r = self.get_with_retry(url, timeout=30)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    break
                raise
            if not r or r.status_code != 200:
                break

            # Find bookmark links (each post)
            links = re.findall(
                r'<a href="(https?://blog\.apaonline\.org/\d{4}/\d{2}/\d{2}/[^"]+)"[^>]*rel="bookmark"[^>]*>([^<]+)</a>',
                r.text
            )
            new_items = []
            seen = set()
            for href, title in links:
                if href in seen:
                    continue
                seen.add(href)
                new_items.append((href, html.unescape(title.strip())))

            if not new_items:
                break

            items.extend(new_items)
            self.stats["pages_scanned"] += 1
            self.log.info(f"  Page {page}: {len(new_items)} posts")
            page += 1
            self.sleep()

        return items

    def extract_review(self, url, title_hint=''):
        """Fetch a post page and extract book metadata."""
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return None

        html_text = r.text

        record = {
            'Book Title': '',
            'Book Author First Name': '',
            'Book Author Last Name': '',
            'Reviewer First Name': '',
            'Reviewer Last Name': '',
            'Publication Source': SOURCE_NAME,
            'Publication Date': '',
            'Review Link': url,
            'Review Summary': '',
            'Access Type': 'Open',
            'DOI': '',
        }

        # Book title: from h1, strip "Recently Published Book Spotlight:" prefix
        soup = BeautifulSoup(html_text, 'html.parser')
        h1 = soup.find('h1')
        prefix_re = (r'^(?:Recently\s+Published\s+Book(?:\s+Spotlight)?|'
                     r'New\s+Book\s+Spotlight|Book\s+Spotlight|Spotlight)\s*:\s*')
        if h1:
            raw = h1.get_text(strip=True)
            record['Book Title'] = re.sub(prefix_re, '', raw, flags=re.I).strip()
        elif title_hint:
            record['Book Title'] = re.sub(prefix_re, '', title_hint, flags=re.I).strip()

        # Extract publication date from URL (/YYYY/MM/DD/...)
        date_m = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
        if date_m:
            record['Publication Date'] = f"{date_m.group(1)}-{date_m.group(2)}-{date_m.group(3)}"

        # Book author from first meta name="author"
        # (APA posts have multiple author metas; the first is the book author)
        authors = re.findall(r'<meta name="author" content="([^"]+)"', html_text)
        # Filter out combined entries (contain "&" or "&amp;")
        single_authors = [html.unescape(a).replace('\xa0', ' ').strip()
                          for a in authors if '&' not in a and 'and' not in a.lower().split()]
        if single_authors:
            # First author is typically the book author
            author_name = single_authors[0]
            # Take first author for multi-author (defensive)
            author_name = re.split(r'\s+and\s+|\s+&\s+|,', author_name, maxsplit=1)[0].strip()
            parts = author_name.split()
            if len(parts) >= 2:
                record['Book Author First Name'] = ' '.join(parts[:-1])
                record['Book Author Last Name'] = parts[-1]
                # Use same name as reviewer (it's a self-presentation)
                record['Reviewer First Name'] = record['Book Author First Name']
                record['Reviewer Last Name'] = record['Book Author Last Name']
            elif parts:
                record['Book Author Last Name'] = parts[0]
                record['Reviewer Last Name'] = parts[0]

        if not record['Book Title']:
            self.stats["parse_errors"] += 1
            return None

        self.stats["reviews_parsed"] += 1
        return record

    def upload_to_db(self, records):
        uploaded = 0
        for rec in records:
            link = rec.get('Review Link', '')
            if link and db.review_link_exists(link):
                self.stats["duplicates_skipped"] += 1
                continue
            db.insert_review({
                'book_title': rec['Book Title'],
                'book_author_first_name': rec['Book Author First Name'],
                'book_author_last_name': rec['Book Author Last Name'],
                'reviewer_first_name': rec['Reviewer First Name'],
                'reviewer_last_name': rec['Reviewer Last Name'],
                'publication_source': rec['Publication Source'],
                'publication_date': rec['Publication Date'],
                'review_link': rec['Review Link'],
                'review_summary': rec['Review Summary'],
                'access_type': rec['Access Type'],
                'doi': rec['DOI'],
            })
            if link and db.review_link_exists(link):
                uploaded += 1
        self.stats["uploaded"] = uploaded
        return uploaded

    def run(self, dry_run=False, bulk=False, max_pages=None, limit=None):
        pages = None if bulk else (max_pages or 2)
        self.log.info(f"Scanning APA Blog Book Spotlights...")
        items = self.fetch_spotlight_urls(max_pages=pages)
        self.stats["urls_found"] = len(items)
        self.log.info(f"Found {len(items)} spotlight URLs")

        if limit:
            items = items[:limit]

        new_items = [i for i in items if not db.review_link_exists(i[0])]
        self.log.info(f"{len(new_items)} new (skipping {len(items) - len(new_items)} existing)")

        records = []
        for i, (url, title_hint) in enumerate(new_items):
            record = self.extract_review(url, title_hint)
            if record:
                records.append(record)
            if (i + 1) % 25 == 0:
                self.log.info(f"  Processed {i + 1}/{len(new_items)}")
            self.sleep()

        self.log.info(f"Extracted {len(records)} records")

        if dry_run:
            self.log.info("[DRY RUN] Sample:")
            for rec in records[:10]:
                print(f"  {rec['Book Title'][:50]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | {rec['Publication Date']}")
        elif records:
            self.upload_to_db(records)
            self.log.info(f"Uploaded {self.stats['uploaded']} new (skipped "
                          f"{self.stats['duplicates_skipped']} dupes)")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="APA Blog Book Spotlight scraper")
    parser.add_argument("--bulk", action="store_true", help="Scan all pages")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages")
    parser.add_argument("--limit", type=int, default=None, help="Limit URLs")
    args = parser.parse_args()

    scraper = APABlogScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk,
                        max_pages=args.max_pages, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
