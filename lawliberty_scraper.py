#!/usr/bin/env python3
"""
Law & Liberty book review scraper.

Scrapes https://lawliberty.org — a Liberty Fund publication covering
political/legal philosophy, intellectual history, and classical liberalism.

Architecture:
- Bulk: single AJAX POST gets all review URLs at once
- Each review page has a structured "book widget" with title, author,
  Amazon link (ISBN in URL)
- Incremental mode uses the site's RSS feed

Usage:
    python3 lawliberty_scraper.py                  # incremental (new reviews)
    python3 lawliberty_scraper.py --bulk           # full backfill
    python3 lawliberty_scraper.py --dry-run        # parse only
    python3 lawliberty_scraper.py --limit 20       # first N for testing
"""

import argparse
import json
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://lawliberty.org"
SOURCE_NAME = "Law & Liberty"
AJAX_URL = f"{BASE_URL}/wp/wp-admin/admin-ajax.php"
RSS_URL = f"{BASE_URL}/book-reviews/feed/"

STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "lawliberty_state.json"
)


class LawLibertyScraper(BaseScraper):
    """Scrapes book reviews from Law & Liberty."""

    name = "lawliberty"
    default_delay = 1.5  # be polite

    def __init__(self):
        super().__init__()
        self.stats = {
            "urls_found": 0,
            "reviews_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return {"last_run": None, "known_urls": []}
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            return {"last_run": None, "known_urls": []}

    def _save_state(self, state):
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)

    def fetch_all_review_urls(self):
        """Get all review URLs via the AJAX endpoint (single request)."""
        data = {
            "action": "get_posts",
            "page": "1",
            "max": "200",
            "pt": "book_review",
            "orderby": "date",
            "order": "DESC",
            "ppp": "2000",
            "offset": "0",
        }
        r = requests.post(AJAX_URL, data=data, timeout=60,
                          headers={"User-Agent": "Mozilla/5.0 (compatible; PhilReviews/2.0)"})
        r.raise_for_status()
        resp = r.json()
        posts = resp.get("posts", [])
        urls = []
        for html_chunk in posts:
            # Each chunk is a card; extract the review URL
            m = re.search(r'href="(https://lawliberty\.org/book-review/[^"]+/)"', html_chunk)
            if m:
                urls.append(m.group(1))
        # Dedupe while preserving order
        seen = set()
        unique_urls = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                unique_urls.append(u)
        return unique_urls

    def fetch_rss_urls(self):
        """Get recent review URLs via the RSS feed."""
        r = self.get_with_retry(RSS_URL, timeout=30)
        if not r or r.status_code != 200:
            return []
        urls = re.findall(r'<link>(https://lawliberty\.org/book-review/[^<]+/)</link>', r.text)
        return urls

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

        # Reviewer from the structured widget
        m = re.search(r'gf__widget-authors-author[^<]*<a href="[^"]+">([^<]+)</a>', html)
        if m:
            full = m.group(1).strip()
            parts = full.split()
            if len(parts) >= 2:
                record['Reviewer First Name'] = ' '.join(parts[:-1])
                record['Reviewer Last Name'] = parts[-1]
            elif parts:
                record['Reviewer Last Name'] = parts[0]

        # Book title from the book widget
        m = re.search(r'class="gf__widget-book-title-link"[^>]*>([^<]+)</a>', html)
        if m:
            record['Book Title'] = m.group(1).strip()

        # Book author
        m = re.search(r'class="gf__widget-book-author[^"]*">\s*(.+?)\s*</div>', html)
        if m:
            author_str = re.sub(r'^by\s+', '', m.group(1).strip(), flags=re.I).strip()
            # Strip any HTML tags
            author_str = re.sub(r'<[^>]+>', '', author_str).strip()
            # Strip parenthetical role markers: "(translator)", "(author)", "(editor)", "(ed. XYZ)"
            author_str = re.sub(r'\s*\([^)]*\)\s*', ' ', author_str).strip()
            # Take first author for multi-author books
            author_str = re.split(r'\s+and\s+|\s+&\s+', author_str, maxsplit=1)[0]
            if author_str.count(',') >= 2:
                author_str = author_str.split(',')[0].strip()
            author_str = author_str.rstrip(', ').strip()
            # Strip editor markers
            author_str = re.sub(r'[,()]*\s*(?:eds?\.?|Eds?\.?|editor[s]?)\s*[,()]*\s*$', '', author_str).strip()
            # Strip honorific suffixes (Jr., Sr., II, III, S.J., etc.)
            author_str = re.sub(r',?\s*(?:Jr\.?|Sr\.?|I{2,4}|IV|S\.?J\.?|O\.?P\.?|OSB|Ph\.?D\.?|M\.?D\.?)\s*$', '', author_str).strip()
            author_str = author_str.rstrip(',').strip()
            parts = author_str.split()
            if len(parts) >= 2:
                record['Book Author First Name'] = ' '.join(parts[:-1])
                record['Book Author Last Name'] = parts[-1]
            elif parts:
                record['Book Author Last Name'] = parts[0]

        # Publication date (prefer ISO from JSON-LD)
        m = re.search(r'"datePublished":"([^"]+)"', html)
        if m:
            record['Publication Date'] = m.group(1)[:10]  # YYYY-MM-DD
        else:
            # Fallback to human-readable
            m = re.search(
                r'gf__single-article-meta-info-published[^>]*>\s*([^<]+)<',
                html
            )
            if m:
                try:
                    d = datetime.strptime(m.group(1).strip(), '%B %d, %Y')
                    record['Publication Date'] = d.strftime('%Y-%m-%d')
                except ValueError:
                    pass

        if not record['Book Title']:
            self.stats["parse_errors"] += 1
            return None

        self.stats["reviews_parsed"] += 1
        return record

    def upload_to_db(self, records):
        """Insert records, skipping duplicates."""
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

    def run(self, dry_run=False, bulk=False, limit=None):
        """Main entry point.

        Args:
            dry_run: if True, don't write to DB.
            bulk: if True, full backfill via AJAX.
            limit: scan only first N URLs (for testing).
        """
        if bulk:
            self.log.info("Fetching all review URLs via AJAX...")
            urls = self.fetch_all_review_urls()
        else:
            self.log.info("Fetching recent review URLs via RSS...")
            urls = self.fetch_rss_urls()

        self.stats["urls_found"] = len(urls)
        self.log.info(f"Found {len(urls)} review URLs")

        if limit:
            urls = urls[:limit]
            self.log.info(f"Limited to first {limit}")

        # Filter to URLs not already in DB (saves page fetches)
        new_urls = [u for u in urls if not db.review_link_exists(u)]
        self.log.info(f"{len(new_urls)} new (skipping {len(urls) - len(new_urls)} existing)")

        records = []
        for i, url in enumerate(new_urls):
            record = self.extract_review(url)
            if record:
                records.append(record)
            if (i + 1) % 50 == 0:
                self.log.info(f"  Processed {i + 1}/{len(new_urls)} URLs")
            self.sleep()

        self.log.info(f"Extracted {len(records)} records")

        if dry_run:
            self.log.info("[DRY RUN] Sample:")
            for rec in records[:5]:
                print(f"  {rec['Book Title'][:60]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | reviewer: "
                      f"{rec['Reviewer First Name']} {rec['Reviewer Last Name']} | "
                      f"{rec['Publication Date']}")
        elif records:
            self.upload_to_db(records)
            state = self._load_state()
            state["last_run"] = datetime.now().isoformat()
            self._save_state(state)
            self.log.info(f"Uploaded {self.stats['uploaded']} new reviews "
                          f"(skipped {self.stats['duplicates_skipped']} dupes)")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="Law & Liberty book review scraper")
    parser.add_argument("--bulk", action="store_true",
                        help="Full backfill via AJAX endpoint")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse only, don't write to database")
    parser.add_argument("--limit", type=int, default=None,
                        help="Scan only first N reviews (for testing)")
    args = parser.parse_args()

    scraper = LawLibertyScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
