#!/usr/bin/env python3
"""
Syndicate Network philosophy symposia scraper.

Scrapes https://syndicate.network/symposia/philosophy/ — academic book
symposia with introduction, responses, and author replies.

Each symposium page contains all posts inline (intro + responses + replies).
Tagged as entry_type='symposium' with shared symposium_group.

Usage:
    python3 syndicate_scraper.py                  # scan all symposia
    python3 syndicate_scraper.py --dry-run        # parse only
"""

import argparse
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://syndicate.network"
ARCHIVE_URL = f"{BASE_URL}/symposia/philosophy/"
BACKLIST_URL = f"{BASE_URL}/backlist/"
SOURCE_NAME = "Syndicate"


class SyndicateScraper(BaseScraper):
    """Scrapes book symposia from Syndicate Network."""

    name = "syndicate"
    default_delay = 2.0

    def __init__(self):
        super().__init__()
        self.stats = {
            "symposia_found": 0,
            "symposia_processed": 0,
            "posts_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def fetch_symposium_urls(self):
        """Collect all philosophy symposium URLs from archive + backlist."""
        urls = set()
        for page_url in [ARCHIVE_URL, BACKLIST_URL]:
            r = self.get_with_retry(page_url, timeout=30)
            if not r or r.status_code != 200:
                continue
            found = re.findall(
                r'href="(https://syndicate\.network/symposia/philosophy/[a-z0-9-]+/)"',
                r.text
            )
            for u in found:
                if not u.endswith('/feed/'):
                    urls.add(u)
            self.sleep()
        return sorted(urls)

    def _parse_date(self, date_str):
        """Parse Syndicate date format 'M.D.YY |' → YYYY-MM-DD."""
        if not date_str:
            return ''
        clean = date_str.strip().rstrip('|').strip()
        # Format: M.D.YY (e.g., 3.9.26)
        m = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2})$', clean)
        if m:
            month, day, yy = m.groups()
            year = f"20{yy}"
            return f"{year}-{int(month):02d}-{int(day):02d}"
        return ''

    def _split_name(self, full, kind, record):
        # Take first author for multi-author names
        full = re.split(r'\s+and\s+|\s+&\s+', full, maxsplit=1)[0]
        if full.count(',') >= 2:
            full = full.split(',')[0].strip()
        full = full.rstrip(', ').strip()
        parts = full.split()
        if len(parts) >= 2:
            record[f'{kind} First Name'] = ' '.join(parts[:-1])
            record[f'{kind} Last Name'] = parts[-1]
        elif parts:
            record[f'{kind} Last Name'] = parts[0]

    def process_symposium(self, url):
        """Fetch a symposium page and extract all posts."""
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        slug = url.rstrip('/').rsplit('/', 1)[-1]
        symposium_group = f"Syndicate|{slug}"

        # Book title
        h1 = soup.select_one('h1.post-title')
        book_title = h1.get_text(strip=True) if h1 else ''
        if not book_title:
            return []

        # Book author (primary: structured h5.book-author element)
        book_author_elem = soup.select_one('h5.book-author span.author-name')
        book_author = book_author_elem.get_text(strip=True) if book_author_elem else ''

        # Fallback: extract from symposium intro text ("Author's BookTitle" pattern)
        if not book_author:
            intro = soup.select_one('div.symposium-intro')
            if intro:
                intro_text = intro.get_text(' ', strip=True)[:500]
                # Match possessive name at start of intro, or followed by book title
                m = re.match(
                    r"^([A-Z][a-zA-Z\u00c0-\u017f.\-']+(?:\s+[A-Z][a-zA-Z\u00c0-\u017f.\-']+){0,3})['\u2019]s",
                    intro_text
                )
                if m:
                    book_author = m.group(1).strip()

        records = []

        # Symposium intro (first h2.intro-author in the main body, not in a section.commentary)
        intro_section = soup.select_one('div.symposium-intro')
        if intro_section:
            # The intro-author is usually sibling of symposium-intro
            intro_author_elem = None
            for sib in intro_section.find_all_previous('h2', class_='intro-author', limit=1):
                intro_author_elem = sib
                break
            if intro_author_elem:
                record = self._build_record(
                    intro_author_elem, symposium_group, book_title, book_author, url, slug,
                    post_title="[Symposium Introduction]", is_intro=True
                )
                if record:
                    records.append(record)

        # Responses are in commentary-block, author Replies are in response-block
        for section in soup.select('section.commentary'):
            for block_type, block_class in [('Response', 'div.commentary-block'),
                                             ('Reply', 'div.response-block')]:
                block = section.select_one(block_class)
                if not block:
                    continue
                anchor = block.get('id', '') or section.get('id', '')
                author_elem = block.select_one('h2.intro-author')
                if not author_elem:
                    continue
                # Post title
                post_title_elem = block.select_one('h3.post-title')
                post_title = post_title_elem.get_text(strip=True) if post_title_elem else block_type

                record = self._build_record(
                    author_elem, symposium_group, book_title, book_author,
                    f"{url}#{anchor}" if anchor else url, slug,
                    post_title=post_title,
                    kind=block_type,
                )
                if record:
                    records.append(record)

        self.stats["posts_parsed"] += len(records)
        return records

    def _build_record(self, author_elem, symposium_group, book_title, book_author,
                       url, slug, post_title="", is_intro=False, kind="Response"):
        """Build a review record from an author element."""
        # Reviewer name
        name_elem = author_elem.select_one('span.intro-author-name a')
        if not name_elem:
            name_elem = author_elem.select_one('span.intro-author-name')
        reviewer_name = name_elem.get_text(strip=True) if name_elem else ''

        # Date
        date_elem = author_elem.select_one('span.post-date')
        pub_date = self._parse_date(date_elem.get_text(strip=True)) if date_elem else ''

        if not reviewer_name:
            return None

        record = {
            'Book Title': book_title,
            'Book Author First Name': '',
            'Book Author Last Name': '',
            'Reviewer First Name': '',
            'Reviewer Last Name': '',
            'Publication Source': SOURCE_NAME,
            'Publication Date': pub_date,
            'Review Link': url,
            'Review Summary': '',
            'Access Type': 'Open',
            'DOI': '',
            '_entry_type': 'symposium',
            '_symposium_group': symposium_group,
        }
        self._split_name(reviewer_name, 'Reviewer', record)
        if book_author:
            self._split_name(book_author, 'Book Author', record)

        # Check if this is the book author's reply
        is_author_reply = (
            book_author and reviewer_name and
            book_author.lower().strip() == reviewer_name.lower().strip()
        )
        if is_author_reply:
            record['Book Title'] = f"{book_title} [Author's Reply]"
        elif is_intro:
            record['Book Title'] = f"{book_title} [Symposium Introduction]"

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
                'entry_type': rec['_entry_type'],
                'symposium_group': rec['_symposium_group'],
            })
            if link and db.review_link_exists(link):
                uploaded += 1
        self.stats["uploaded"] = uploaded
        return uploaded

    def run(self, dry_run=False, limit=None):
        self.log.info("Fetching Syndicate symposium URLs...")
        urls = self.fetch_symposium_urls()
        self.stats["symposia_found"] = len(urls)
        self.log.info(f"Found {len(urls)} symposia")

        if limit:
            urls = urls[:limit]

        all_records = []
        for i, url in enumerate(urls):
            records = self.process_symposium(url)
            all_records.extend(records)
            self.stats["symposia_processed"] += 1
            if (i + 1) % 10 == 0:
                self.log.info(f"  {i + 1}/{len(urls)} symposia, {len(all_records)} posts so far")
            self.sleep()

        self.log.info(f"Extracted {len(all_records)} posts from {len(urls)} symposia")

        if dry_run:
            self.log.info("[DRY RUN] Sample:")
            for rec in all_records[:10]:
                print(f"  {rec['Book Title'][:55]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | reviewer: "
                      f"{rec['Reviewer First Name']} {rec['Reviewer Last Name']} | "
                      f"{rec['Publication Date']}")
        elif all_records:
            self.upload_to_db(all_records)
            self.log.info(f"Uploaded {self.stats['uploaded']} new posts "
                          f"(skipped {self.stats['duplicates_skipped']} dupes)")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="Syndicate philosophy symposia scraper")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--limit", type=int, default=None, help="Scan only first N symposia")
    args = parser.parse_args()

    scraper = SyndicateScraper()
    stats = scraper.run(dry_run=args.dry_run, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
