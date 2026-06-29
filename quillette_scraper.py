#!/usr/bin/env python3
"""
Quillette book reviews scraper.

Scrapes https://quillette.com/tag/books/ — heterodox intellectual magazine
with ~226 book reviews across multiple pages. Covers philosophy, politics,
science, and culture.

Architecture:
- Tag pages list review URLs (URL pattern: /YYYY/MM/DD/slug/)
- description meta tag has "{Author}'s {Book Title}" pattern
- JSON-LD has article author (reviewer)

Usage:
    python3 quillette_scraper.py                  # incremental (first 2 pages)
    python3 quillette_scraper.py --bulk           # full backfill
    python3 quillette_scraper.py --dry-run        # parse only
"""

import argparse
import html
import json as json_module
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://quillette.com"
ARCHIVE_URL = f"{BASE_URL}/tag/books/"
SOURCE_NAME = "Quillette"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


class QuilletteScraper(BaseScraper):
    """Scrapes book reviews from Quillette."""

    name = "quillette"
    default_delay = 2.0

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
            "skipped_non_reviews": 0,
        }

    def fetch_review_urls(self, max_pages=None):
        """Scan tag pages and collect review URLs."""
        urls = []
        page = 1
        limit = max_pages or 30  # safety limit

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

            # Extract review URLs (pattern: /YYYY/MM/DD/slug/)
            rel_urls = re.findall(
                r'href="(/\d{4}/\d{2}/\d{2}/[^/]+/)"',
                r.text
            )
            page_urls = [BASE_URL + u for u in rel_urls]
            # Dedupe within page
            new = []
            seen_on_page = set()
            for u in page_urls:
                if u not in seen_on_page:
                    seen_on_page.add(u)
                    new.append(u)

            if not new:
                break

            urls.extend(new)
            self.stats["pages_scanned"] += 1
            self.log.info(f"  Page {page}: {len(new)} URLs")
            page += 1
            self.sleep()

        # Dedupe across all pages
        seen = set()
        unique = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        return unique

    def extract_review(self, url):
        """Fetch a review page and extract book metadata."""
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return None

        soup = BeautifulSoup(r.text, 'html.parser')
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

        # Publication date from meta
        m = re.search(r'<meta property="article:published_time" content="([^"]+)"', html_text)
        if m:
            record['Publication Date'] = m.group(1)[:10]

        # Reviewer from JSON-LD or byline
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json_module.loads(script.string or '{}')
                if isinstance(data, dict) and data.get('@type') == 'Article':
                    author = data.get('author', {})
                    if isinstance(author, dict):
                        name = author.get('name', '')
                    elif isinstance(author, list) and author:
                        name = author[0].get('name', '') if isinstance(author[0], dict) else ''
                    else:
                        name = ''
                    if name:
                        self._split_name(name, 'Reviewer', record)
                        # Date from JSON-LD if meta missing
                        if not record['Publication Date']:
                            pub = data.get('datePublished', '')
                            if pub:
                                record['Publication Date'] = pub[:10]
                        break
            except (json_module.JSONDecodeError, AttributeError):
                continue

        # Fallback reviewer from byline
        if not record['Reviewer Last Name']:
            byline = soup.select_one('a[rel="author"], .author-name, .byline a')
            if byline:
                self._split_name(byline.get_text(strip=True), 'Reviewer', record)

        # Book title from og:title (format: "Book Title—Book Review" or "Book Title - Book Review")
        og_title_m = re.search(r'<meta property="og:title" content="([^"]+)"', html_text)
        if og_title_m:
            og_title = html.unescape(og_title_m.group(1))
            # Strip "—Book Review" / " - Book Review" / ": Review" suffixes
            book_title = re.sub(
                r'\s*[—\-:]\s*(?:Book\s+Review|Review)\s*$',
                '', og_title, flags=re.I
            ).strip()
            # Strip article-title prefix if present: "Heir Jordan — The Most American King..."
            # If still ends/starts with review indicator, ignore
            if book_title and book_title.lower() not in ('book review', 'review'):
                record['Book Title'] = book_title

        # Try description for book author (most reliable when present)
        desc_m = re.search(r'<meta name="description" content="([^"]+)"', html_text)
        desc = html.unescape(desc_m.group(1)) if desc_m else ''
        og_desc_m = re.search(r'<meta property="og:description" content="([^"]+)"', html_text)
        og_desc = html.unescape(og_desc_m.group(1)) if og_desc_m else ''

        # Pattern: "{First Last}'s..." or "{First Last} has/wrote..."
        # Require 2-4 word name starting with proper first name
        name_words = r"[A-Z][a-z]+(?:[.\-\u2019'][A-Za-z]+)*"
        name_pattern = rf"{name_words}(?:\s+{name_words}){{1,3}}"
        for text in [desc, og_desc]:
            if not text:
                continue
            m = re.match(
                rf"^({name_pattern})(?:['\u2019]s\s|\s+(?:has|have|wrote|writes|published|argues|examines|tells|presents|offers|reveals|provides|makes|challenges|traces|explores|is\s+|was\s+|reconsiders|contrasts))",
                text
            )
            if m:
                author_str = m.group(1).strip()
                # Validate: must not start with common non-name words
                first_word = author_str.split()[0].lower()
                if first_word not in ('a', 'the', 'an', 'two', 'three', 'some', 'new', 'this', 'that', 'these', 'those', 'review', 'book'):
                    self._set_book_author(record, author_str)
                    break

        # Queue for Haiku if book_title or book_author still missing
        if record['Book Title'] and not record['Book Author Last Name']:
            # Get article body for Haiku
            body = soup.select_one('article') or soup.select_one('main') or soup.find('body')
            if body:
                for tag in body.find_all(['script', 'style']):
                    tag.decompose()
                record['_body_text'] = body.get_text(' ', strip=True)[:2500]
                record['_needs_author'] = True

        # Skip if no book title
        if not record['Book Title']:
            self.stats["parse_errors"] += 1
            return None

        self.stats["reviews_parsed"] += 1
        return record

    def _split_name(self, full, kind, record):
        """Split full name into first/last, taking first author for multi-author."""
        full = re.split(r'\s+and\s+|\s+&\s+', full, maxsplit=1)[0]
        if full.count(',') >= 2:
            full = full.split(',')[0].strip()
        parts = full.strip().split()
        if len(parts) >= 2:
            record[f'{kind} First Name'] = ' '.join(parts[:-1])
            record[f'{kind} Last Name'] = parts[-1]
        elif parts:
            record[f'{kind} Last Name'] = parts[0]

    def _set_book_author(self, record, author_str):
        """Set book author fields from a name string."""
        author_str = re.sub(r'\s*\((?:eds?\.?|editor[s]?)\)\s*', '', author_str, flags=re.I).strip()
        author_str = re.split(r'\s+and\s+|\s+&\s+', author_str, maxsplit=1)[0]
        parts = author_str.strip().split()
        if len(parts) >= 2:
            record['Book Author First Name'] = ' '.join(parts[:-1])
            record['Book Author Last Name'] = parts[-1]
        elif parts:
            record['Book Author Last Name'] = parts[0]

    def enrich_authors_with_haiku(self, records):
        """Fill in missing book authors via Haiku."""
        needs = [r for r in records if r.get('_needs_author')]
        if not needs:
            return
        try:
            from anthropic import Anthropic
            client = Anthropic()
        except Exception as e:
            self.log.warning(f"Haiku unavailable: {e}")
            return

        self.log.info(f"Enriching {len(needs)} records with Haiku...")
        import json as json_module

        for i in range(0, len(needs), 10):
            batch = needs[i:i+10]
            items_text = ""
            for j, rec in enumerate(batch):
                items_text += f"\n--- Item {j+1} ---\nBook title: {rec['Book Title']}\nArticle: {rec['_body_text']}\n"
            prompt = f"""These are book reviews. For each, identify the book's author (NOT reviewer).

Output ONLY JSON: [{{"item": 1, "author": "First Last"}}, ...]
Use null if unclear.
{items_text}"""
            try:
                resp = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = resp.content[0].text
                match = re.search(r'\[.*\]', text, re.DOTALL)
                if match:
                    results = json_module.loads(match.group())
                    for result in results:
                        idx = result.get('item', 0) - 1
                        if 0 <= idx < len(batch) and result.get('author'):
                            self._set_book_author(batch[idx], result['author'])
            except Exception as e:
                self.log.warning(f"Haiku batch failed: {e}")

        for rec in records:
            rec.pop('_needs_author', None)
            rec.pop('_body_text', None)

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
        self.log.info(f"Scanning Quillette books tag ({'bulk' if bulk else f'first {pages} pages'})...")
        urls = self.fetch_review_urls(max_pages=pages)
        self.stats["urls_found"] = len(urls)
        self.log.info(f"Found {len(urls)} review URLs")

        if limit:
            urls = urls[:limit]

        new_urls = [u for u in urls if not db.review_link_exists(u)]
        self.log.info(f"{len(new_urls)} new (skipping {len(urls) - len(new_urls)} existing)")

        records = []
        for i, url in enumerate(new_urls):
            record = self.extract_review(url)
            if record:
                records.append(record)
            if (i + 1) % 25 == 0:
                self.log.info(f"  Processed {i + 1}/{len(new_urls)}")
            self.sleep()

        self.log.info(f"Extracted {len(records)} records")

        # Enrich with Haiku
        self.enrich_authors_with_haiku(records)

        if dry_run:
            self.log.info("[DRY RUN] Sample:")
            for rec in records[:10]:
                print(f"  {rec['Book Title'][:50]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | reviewer: "
                      f"{rec['Reviewer First Name']} {rec['Reviewer Last Name']} | "
                      f"{rec['Publication Date']}")
        elif records:
            self.upload_to_db(records)
            self.log.info(f"Uploaded {self.stats['uploaded']} new reviews "
                          f"(skipped {self.stats['duplicates_skipped']} dupes)")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="Quillette book review scraper")
    parser.add_argument("--bulk", action="store_true", help="Scan all pages")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages")
    parser.add_argument("--limit", type=int, default=None, help="Limit URLs")
    args = parser.parse_args()

    scraper = QuilletteScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk,
                        max_pages=args.max_pages, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
