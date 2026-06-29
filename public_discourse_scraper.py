#!/usr/bin/env python3
"""
Public Discourse book review scraper.

Scrapes https://www.thepublicdiscourse.com — the Witherspoon Institute's
online journal. Covers natural law, ethics, political theory, religious
philosophy (~456 book reviews across 19 pages).

Uses the RSS feed pagination to bulk-fetch reviews with full HTML bodies.
Book title/author extracted from italicized Amazon link in body.

Usage:
    python3 public_discourse_scraper.py                # incremental (latest page)
    python3 public_discourse_scraper.py --bulk         # full backfill
    python3 public_discourse_scraper.py --dry-run      # parse only
"""

import argparse
import html
import os
import re
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://www.thepublicdiscourse.com"
SOURCE_NAME = "Public Discourse"
FEED_URL = f"{BASE_URL}/category/book-reviews/feed/"


class PublicDiscourseScraper(BaseScraper):
    """Scrapes book reviews from Public Discourse via RSS feed."""

    name = "public_discourse"
    default_delay = 2.0

    def __init__(self):
        super().__init__()
        self.stats = {
            "feed_pages": 0,
            "items_processed": 0,
            "reviews_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "not_reviews": 0,
            "parse_errors": 0,
        }

    def fetch_feed_page(self, page_num):
        """Fetch one page of the RSS feed. Returns list of items."""
        url = f"{FEED_URL}?paged={page_num}"
        try:
            r = self.get_with_retry(url, timeout=60)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise
        if not r or r.status_code != 200:
            return None
        # Parse with BeautifulSoup (handles CDATA)
        soup = BeautifulSoup(r.text, 'xml')
        return soup.find_all('item')

    def extract_book_from_item(self, item):
        """Extract review data from an RSS feed <item>."""
        link = item.find('link').get_text(strip=True) if item.find('link') else ''
        # Strip UTM tracking params (both ?utm_ and &utm_ forms)
        link = re.sub(r'[?&]utm_[^&]+(?:&utm_[^&]+)*', '', link).rstrip('?&')

        title_elem = item.find('title')
        headline = title_elem.get_text(strip=True) if title_elem else ''

        creator = item.find('dc:creator')
        reviewer_name = creator.get_text(strip=True) if creator else ''

        pub_date_elem = item.find('pubDate')
        pub_date = ''
        if pub_date_elem:
            try:
                dt = parsedate_to_datetime(pub_date_elem.get_text(strip=True))
                pub_date = dt.strftime('%Y-%m-%d')
            except Exception:
                pass

        # Description = kicker/excerpt
        desc_elem = item.find('description')
        kicker = desc_elem.get_text(strip=True) if desc_elem else ''

        # content:encoded = full article body HTML
        content_elem = item.find('content:encoded')
        content_html = content_elem.get_text() if content_elem else ''

        record = {
            'Book Title': '',
            'Book Author First Name': '',
            'Book Author Last Name': '',
            'Reviewer First Name': '',
            'Reviewer Last Name': '',
            'Publication Source': SOURCE_NAME,
            'Publication Date': pub_date,
            'Review Link': link,
            'Review Summary': kicker[:500] if kicker else '',
            'Access Type': 'Open',
            'DOI': '',
        }

        # Split reviewer into first/last
        if reviewer_name:
            parts = reviewer_name.split()
            if len(parts) >= 2:
                record['Reviewer First Name'] = ' '.join(parts[:-1])
                record['Reviewer Last Name'] = parts[-1]
            elif parts:
                record['Reviewer Last Name'] = parts[0]

        # Get article body plain text (reused for regex + Haiku fallback)
        body_text = re.sub(r'<[^>]+>', ' ', content_html)
        body_text = re.sub(r'\s+', ' ', body_text).strip()

        # Strategy 1: kicker has "review of Author's <i>Title</i>"
        m = re.search(r"review of\s+(.+?)['\u2019]s\s+<i>([^<]+?)</i>",
                      kicker, re.IGNORECASE)
        if m:
            author_str = html.unescape(m.group(1).strip())
            book_title = html.unescape(m.group(2).strip())
            record['Book Title'] = book_title
            self._set_author(record, author_str)
            return record

        # Strategy 2: first italicized link in article body (usually the book)
        # Match <a href="..."><i|em>TITLE</i|em></a> optionally with nested <span>
        m = re.search(
            r'<a\s+href="(https?://[^"]*)"[^>]*>\s*<(i|em)>\s*(?:<span[^>]*>)?([^<]+?)(?:</span>)?\s*</(?:i|em)>\s*</a>',
            content_html
        )
        if m:
            book_title = html.unescape(m.group(3).strip())
            record['Book Title'] = book_title

            # Author extraction: try multiple regex patterns on surrounding text
            idx = content_html.find(m.group(0))
            surrounding = re.sub(r'<[^>]+>', ' ', content_html)
            surrounding = re.sub(r'\s+', ' ', surrounding).strip()
            # Find index in plain text
            title_idx = surrounding.find(book_title)

            if title_idx > 0:
                pre_text = surrounding[:title_idx]
                post_text = surrounding[title_idx + len(book_title):title_idx + len(book_title) + 400]
                search_region_pre = pre_text[-400:] if len(pre_text) > 400 else pre_text

                # Pattern A: "Name's new/recent/latest book/work" before title
                # Pattern B: "Name argues/claims/writes in [title]" before title
                # Pattern C: "published by Name" or "by Name, published" after title
                # Pattern D: "(title), originally published ... by Name"

                name_ptn = r"([A-Z][a-zA-Z.\-\u2019']+(?:\s+[A-Z][a-zA-Z.\-\u2019']+){1,4})"
                author_str = None

                # Pre-title possessive: "Name's book/work"
                ma = re.search(rf"{name_ptn}['\u2019]s\s+(?:new|recent|latest|first|most recent)?\s*(?:book|work|volume|study|argument|treatise|volume|collection)",
                               search_region_pre)
                if ma:
                    author_str = ma.group(1)

                # Post-title "by Name"
                if not author_str:
                    mb = re.search(rf"\bby\s+{name_ptn}", post_text)
                    if mb:
                        author_str = mb.group(1)

                # Pre-title "Name makes/argues in" pattern
                if not author_str:
                    mc = re.search(rf"{name_ptn}\s+(?:makes|argues|claims|writes|contends|suggests|argued|offers|presents|defends)\s+(?:in|that)\s*$",
                                   search_region_pre)
                    if mc:
                        author_str = mc.group(1)

                if author_str:
                    self._set_author(record, author_str.strip())

        # If book_title extracted but no author, queue for Haiku extraction
        if record['Book Title'] and not record['Book Author Last Name']:
            record['_body_text'] = body_text[:2000]
            record['_needs_author'] = True

        if record['Book Title']:
            return record
        return None

    def _set_author(self, record, author_str):
        """Set book author first/last from a name string."""
        # Strip common suffixes/prefixes
        author_str = re.sub(r'\s*\((?:eds?\.?|editor[s]?)\)\s*', '', author_str, flags=re.I).strip()
        # For multi-author: take first
        author_str = re.split(r'\s+and\s+|,\s*', author_str, maxsplit=1)[0].strip()
        parts = author_str.split()
        if len(parts) >= 2:
            record['Book Author First Name'] = ' '.join(parts[:-1])
            record['Book Author Last Name'] = parts[-1]
        elif parts:
            record['Book Author Last Name'] = parts[0]

    def enrich_authors_with_haiku(self, records):
        """Use Haiku to extract book authors for records that need them."""
        needs_enrichment = [r for r in records if r.get('_needs_author')]
        if not needs_enrichment:
            return
        try:
            from anthropic import Anthropic
        except ImportError:
            self.log.warning("anthropic package not installed, skipping Haiku enrichment")
            return
        try:
            client = Anthropic()
        except Exception as e:
            self.log.warning(f"Anthropic client failed: {e}")
            return

        self.log.info(f"Enriching {len(needs_enrichment)} records with Haiku...")

        import json as json_module
        # Batch 10 at a time
        for i in range(0, len(needs_enrichment), 10):
            batch = needs_enrichment[i:i+10]
            items_text = ""
            for j, rec in enumerate(batch):
                items_text += f"\n--- Item {j+1} ---\nBook title: {rec['Book Title']}\nArticle excerpt: {rec['_body_text']}\n"

            prompt = f"""These are excerpts from book reviews. For each, identify the book's author(s) (NOT the reviewer).

Output ONLY a JSON array with the author name for each item:
[{{"item": 1, "author": "First Last"}}, ...]

If you can't determine the author, use null.
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
                            self._set_author(batch[idx], result['author'])
            except Exception as e:
                self.log.warning(f"Haiku batch failed: {e}")

        # Clean up temporary fields
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

    def run(self, dry_run=False, bulk=False, max_pages=None):
        """Main entry point.

        Args:
            dry_run: don't write to DB.
            bulk: scan all feed pages (default: first 2 pages = ~48 items).
            max_pages: explicit page count limit.
        """
        if bulk:
            pages = max_pages or 20  # 19 is current; include safety margin
        else:
            pages = max_pages or 2

        all_records = []
        for page_num in range(1, pages + 1):
            self.log.info(f"Fetching feed page {page_num}...")
            items = self.fetch_feed_page(page_num)
            if items is None or len(items) == 0:
                self.log.info(f"  Page {page_num}: empty, stopping")
                break
            self.stats["feed_pages"] += 1
            self.log.info(f"  Page {page_num}: {len(items)} items")

            for item in items:
                self.stats["items_processed"] += 1
                try:
                    record = self.extract_book_from_item(item)
                    if record:
                        all_records.append(record)
                        self.stats["reviews_parsed"] += 1
                    else:
                        # Not a book review (most PD articles aren't) — expected,
                        # not an error.
                        self.stats["not_reviews"] += 1
                except Exception as e:
                    self.log.exception(f"Parse error: {e}")
                    self.stats["parse_errors"] += 1
            self.sleep()

        self.log.info(f"Extracted {len(all_records)} records from {self.stats['feed_pages']} feed pages "
                      f"({self.stats['not_reviews']} non-reviews skipped, "
                      f"{self.stats['parse_errors']} parse errors)")

        # Enrich missing authors via Haiku
        self.enrich_authors_with_haiku(all_records)

        if dry_run:
            self.log.info("[DRY RUN] Sample:")
            for rec in all_records[:10]:
                print(f"  {rec['Book Title'][:50]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | reviewer: "
                      f"{rec['Reviewer First Name']} {rec['Reviewer Last Name']} | "
                      f"{rec['Publication Date']}")
        elif all_records:
            self.upload_to_db(all_records)
            self.log.info(f"Uploaded {self.stats['uploaded']} new reviews "
                          f"(skipped {self.stats['duplicates_skipped']} dupes)")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="Public Discourse book review scraper")
    parser.add_argument("--bulk", action="store_true", help="Full backfill")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages")
    args = parser.parse_args()

    scraper = PublicDiscourseScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk, max_pages=args.max_pages)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
