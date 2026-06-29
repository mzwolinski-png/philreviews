#!/usr/bin/env python3
"""
BJPS Review of Books scraper.

Scrapes https://www.thebsps.org/reviewofbooks/ — the British Society for the
Philosophy of Science book review site. ~155 rigorous academic reviews.

The og:description meta tag has the exact format:
  "{Reviewer} reviews {Book Title}, by {Book Author}"

Usage:
    python3 bjps_scraper.py                  # full backfill
    python3 bjps_scraper.py --dry-run        # parse only
"""

import argparse
import os
import re
from datetime import datetime

import requests

import db
from scraper_base import BaseScraper

BASE_URL = "https://www.thebsps.org"
ARCHIVE_URL = f"{BASE_URL}/reviewofbooks/"
SOURCE_NAME = "BJPS Review of Books"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PhilReviews/2.0)"}


class BJPSScraper(BaseScraper):
    """Scrapes book reviews from BJPS Review of Books."""

    name = "bjps"
    default_delay = 1.5

    def __init__(self):
        super().__init__()
        self.session.headers.update(HEADERS)
        self.stats = {
            "urls_found": 0,
            "reviews_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def fetch_review_urls(self):
        """Get all review URLs from the single archive page."""
        r = self.get_with_retry(ARCHIVE_URL, timeout=30)
        if not r or r.status_code != 200:
            return []
        # Find all /reviewofbooks/{slug}/ URLs (excluding /feed/)
        urls = re.findall(r'href=[\'"](https://www\.thebsps\.org/reviewofbooks/[a-z0-9-]+/)[\'"]', r.text)
        # Dedupe
        seen = set()
        unique = []
        for u in urls:
            if u not in seen and '/feed/' not in u:
                seen.add(u)
                unique.append(u)
        return unique

    def extract_review(self, url):
        """Fetch a review page and extract book metadata."""
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return None

        html = r.text
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

        # Primary: og:description has format:
        #   "{Reviewer} reviews {Title}, by {Author}"   (most common)
        #   "{Reviewer1} and {Reviewer2} review {Title}, by {Author}"   (co-reviewers)
        #   "{Reviewer} reviews {Title} by {Author}"   (no comma before 'by')
        #   "{Reviewer} reviews {Title}, edited by {Author}"   (edited volumes)
        m = re.search(r'<meta property="og:description" content="([^"]+)"', html)
        if m:
            desc = m.group(1).strip()
            desc = desc.replace('&amp;', '&').replace('&#039;', "'").replace('&quot;', '"')
            # Try patterns in order
            parsed = re.match(
                r'^(.+?)\s+(?:reviews?|review)\s+(.+?)(?:,\s+|\s+)(?:edited\s+)?by\s+(.+?)\.?$',
                desc
            )
            if parsed:
                reviewer = parsed.group(1).strip()
                book_title = parsed.group(2).strip()
                book_author = parsed.group(3).strip()
                # Handle co-reviewers (take first)
                reviewer = re.split(r'\s+and\s+', reviewer, maxsplit=1)[0].strip()
                self._split_name(reviewer, record, 'Reviewer')
                self._split_name(book_author, record, 'Book Author')
                record['Book Title'] = book_title

        # Fallback: JSON-LD headline
        if not record['Book Title']:
            m = re.search(r'"headline":\s*"([^"]+)"', html)
            if m:
                headline = m.group(1).strip().replace('\\u2019', "'")
                # Format: "{Book Author}, {Book Title} // Reviewed by {Reviewer}"
                parsed = re.match(r'^(.+?),\s+(.+?)\s+//\s+Reviewed by\s+(.+?)$', headline)
                if parsed:
                    self._split_name(parsed.group(1).strip(), record, 'Book Author')
                    record['Book Title'] = parsed.group(2).strip()
                    self._split_name(parsed.group(3).strip(), record, 'Reviewer')

        # Publication date
        m = re.search(r'<meta property="article:published_time" content="([^"]+)"', html)
        if m:
            record['Publication Date'] = m.group(1)[:10]

        if not record['Book Title']:
            self.stats["parse_errors"] += 1
            return None

        self.stats["reviews_parsed"] += 1
        return record

    def _split_name(self, full_name, record, kind):
        """Split 'First Last' into first/last fields. Take first author only for multi-author."""
        # Strip multi-author suffixes: "A and B", "A & B", "A, B, and C", "A, B"
        full_name = re.split(r'\s+and\s+|\s+&\s+', full_name, maxsplit=1)[0]
        # Handle comma-separated list: take first author only (but preserve "Last, First")
        if full_name.count(',') >= 2:
            full_name = full_name.split(',')[0].strip()
        parts = full_name.strip().split()
        if len(parts) >= 2:
            record[f'{kind} First Name'] = ' '.join(parts[:-1])
            record[f'{kind} Last Name'] = parts[-1]
        elif parts:
            record[f'{kind} Last Name'] = parts[0]

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

    def run(self, dry_run=False, limit=None):
        self.log.info("Fetching BJPS review URLs...")
        urls = self.fetch_review_urls()
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
    parser = argparse.ArgumentParser(description="BJPS Review of Books scraper")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--limit", type=int, default=None, help="Scan only first N")
    args = parser.parse_args()

    scraper = BJPSScraper()
    stats = scraper.run(dry_run=args.dry_run, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
