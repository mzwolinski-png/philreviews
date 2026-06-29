#!/usr/bin/env python3
"""
Philosophy Now scraper for PhilReviews.

Scrapes book reviews from Philosophy Now (philosophynow.org), a bimonthly
magazine of accessible philosophy articles. Each issue has a REVIEWS section
with ~3-6 book reviews.

URL structure:
    /issues/{N}/Book_Title_by_Author  ← review page
    /issues/{N}                        ← issue landing page (lists reviews)

Usage:
    python3 philosophy_now_scraper.py                  # incremental (new issues)
    python3 philosophy_now_scraper.py --bulk           # all issues (backfill)
    python3 philosophy_now_scraper.py --dry-run        # parse only
    python3 philosophy_now_scraper.py --issues 170-173 # specific range
"""

import argparse
import json
import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://philosophynow.org"
SOURCE_NAME = "Philosophy Now"
STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "philosophy_now_state.json"
)

# Map issue number → approximate month-year. Philosophy Now is bimonthly.
# Issue 1 = 1991 Summer. From 1993 onwards it became bimonthly starting
# Jan/Feb. We use a simple formula: issue N → ~(1991 + N * 2/12)
# More accurate mapping is extracted from the issue page title.
MONTH_PATTERNS = [
    ("January", "01"), ("February", "02"), ("March", "03"), ("April", "04"),
    ("May", "05"), ("June", "06"), ("July", "07"), ("August", "08"),
    ("September", "09"), ("October", "10"), ("November", "11"), ("December", "12"),
]


class PhilosophyNowScraper(BaseScraper):
    """Scrapes book reviews from Philosophy Now magazine."""

    name = "philosophy_now"
    default_delay = 1.0

    def __init__(self):
        super().__init__()
        self.stats = {
            "issues_checked": 0,
            "reviews_found": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return {"last_issue": 0}
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            return {"last_issue": 0}

    def _save_state(self, state):
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)

    def _get_current_issue(self):
        """Find the current (latest) issue number from the site."""
        # /issues uses JS rendering, so scrape the homepage instead.
        # Filter out URLs like /issues/{article_id}/ by capping at 500
        # (Philosophy Now is bimonthly so issue 500 is decades away).
        r = self.get_with_retry(f"{BASE_URL}/", timeout=30)
        if not r or r.status_code != 200:
            return None
        nums = [int(n) for n in re.findall(r'/issues/(\d+)/', r.text)]
        valid = [n for n in nums if n <= 500]
        return max(valid, default=None)

    def _parse_issue_date(self, issue_num, html=None):
        """Extract the publication date from an issue page title.

        Formats:
            'Issue 173: April/May 2026' → '2026-04-01'
            'Issue 1: Summer 1991' → '1991-06-01'
        Returns YYYY-MM-DD string or None.
        """
        if html is None:
            r = self.get_with_retry(f"{BASE_URL}/issues/{issue_num}", timeout=30)
            if not r or r.status_code != 200:
                return None
            html = r.text
        # Look for pattern like "Issue 173: April/May 2026" or "Issue 1: Summer 1991"
        m = re.search(r'Issue\s+\d+:\s*(\w+)(?:/\w+)?\s+(\d{4})', html)
        if m:
            token = m.group(1)
            year = m.group(2)
            # Try month name first
            for name, num in MONTH_PATTERNS:
                if name.lower() == token.lower():
                    return f"{year}-{num}-01"
            # Try season
            season_map = {
                'spring': '03', 'summer': '06', 'autumn': '09',
                'fall': '09', 'winter': '12',
            }
            if token.lower() in season_map:
                return f"{year}-{season_map[token.lower()]}-01"
        return None

    def _fetch_issue_reviews(self, issue_num):
        """Fetch book review URLs from a single issue's landing page."""
        r = self.get_with_retry(f"{BASE_URL}/issues/{issue_num}", timeout=30)
        if not r or r.status_code != 200:
            self.log.warning(f"Issue {issue_num}: HTTP {r.status_code if r else '?'}")
            return [], None

        # Get issue date from this page (save a request)
        pub_date = self._parse_issue_date(issue_num, html=r.text)

        soup = BeautifulSoup(r.text, 'html.parser')

        # Find the REVIEWS section: <h3>REVIEWS</h3> followed by book review h2s
        review_urls = []
        reviews_heading = None
        for h in soup.find_all(['h3']):
            if h.get_text(strip=True).upper() == 'REVIEWS':
                reviews_heading = h
                break

        if reviews_heading:
            # Walk siblings until next h3
            for elem in reviews_heading.find_next_siblings():
                if elem.name == 'h3':
                    break
                # Extract review links from this element
                if hasattr(elem, 'find_all'):
                    for a in elem.find_all('a', href=True):
                        href = a['href']
                        # Match book review URL pattern (has "_by_" for the author)
                        if re.match(rf'/issues/{issue_num}/[^/]+_by_[^/]+$', href):
                            full_url = f"{BASE_URL}{href}"
                            if full_url not in review_urls:
                                review_urls.append(full_url)
        else:
            # Fallback: scan all anchors in the page
            for a in soup.find_all('a', href=True):
                href = a['href']
                if re.match(rf'/issues/{issue_num}/[^/]+_by_[^/]+$', href):
                    full_url = f"{BASE_URL}{href}"
                    if full_url not in review_urls:
                        review_urls.append(full_url)

        return review_urls, pub_date

    def _extract_review(self, url, pub_date):
        """Fetch a review page and extract book metadata.

        Returns a dict record or None on parse failure.
        """
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return None

        soup = BeautifulSoup(r.text, 'html.parser')

        # Title & book author from h1: <em>Title</em> by Author
        h1 = soup.find('h1')
        if not h1:
            self.stats["parse_errors"] += 1
            return None

        em = h1.find('em')
        if em:
            book_title = em.get_text(strip=True)
            # Author is the text after " by " in the h1
            h1_text = h1.get_text(strip=True)
            by_match = re.search(r'\bby\s+(.+?)$', h1_text.replace(book_title, '', 1))
            book_author = by_match.group(1).strip() if by_match else ''
        else:
            # Fallback: parse the whole h1
            h1_text = h1.get_text(strip=True)
            m = re.match(r'^(.+?)\s+by\s+(.+?)$', h1_text)
            if not m:
                self.stats["parse_errors"] += 1
                return None
            book_title, book_author = m.group(1).strip(), m.group(2).strip()

        # Reviewer from meta description: "FirstName LastName [verb] ..."
        desc = soup.find('meta', {'name': 'description'})
        reviewer_first, reviewer_last = '', ''
        if desc and desc.get('content'):
            content = desc['content'].strip()
            # First 2-4 capitalized words before a lowercase verb
            m = re.match(r'^((?:[A-Z][\w.\-\u2019\']*\s+){1,3}[A-Z][\w.\-\u2019\']*)\s+[a-z]',
                         content)
            if m:
                full_name = m.group(1).strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    reviewer_first = ' '.join(parts[:-1])
                    reviewer_last = parts[-1]

        # Split book_author into first/last
        author_parts = book_author.split()
        book_author_first = ' '.join(author_parts[:-1]) if len(author_parts) >= 2 else ''
        book_author_last = author_parts[-1] if author_parts else ''

        return {
            'Book Title': book_title,
            'Book Author First Name': book_author_first,
            'Book Author Last Name': book_author_last,
            'Reviewer First Name': reviewer_first,
            'Reviewer Last Name': reviewer_last,
            'Publication Source': SOURCE_NAME,
            'Publication Date': pub_date or '',
            'Review Link': url,
            'Review Summary': '',
            'Access Type': 'Open',
            'DOI': '',
        }

    def process_issue(self, issue_num):
        """Fetch and extract all book reviews from one issue."""
        review_urls, pub_date = self._fetch_issue_reviews(issue_num)
        if not review_urls:
            return []

        records = []
        for url in review_urls:
            record = self._extract_review(url, pub_date)
            if record and record.get('Book Title'):
                records.append(record)
            self.sleep()

        self.stats["reviews_found"] += len(records)
        self.log.info(f"Issue {issue_num} ({pub_date or '?'}): {len(records)} book reviews")
        return records

    def upload_to_db(self, records):
        """Insert records into the DB, skipping duplicates by review_link."""
        uploaded = 0
        for rec in records:
            link = rec.get('Review Link', '')
            if link and db.review_link_exists(link):
                self.stats["duplicates_skipped"] += 1
                continue
            # Insert via db helper (returns None; use existence check to confirm)
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

    def run(self, dry_run=False, bulk=False, issue_range=None):
        """Main entry point.

        Args:
            dry_run: if True, don't write to DB.
            bulk: if True, scan from issue 1 (full backfill).
            issue_range: (start, end) tuple to scan specific issues.
        """
        state = self._load_state()
        current = self._get_current_issue()
        if not current:
            self.log.error("Could not determine current issue number")
            return self.stats

        if issue_range:
            start, end = issue_range
        elif bulk:
            start, end = 1, current
        else:
            # Incremental: scan from last_issue + 1 through current
            start = max(state.get("last_issue", 0) + 1, 1)
            end = current

        if start > end:
            self.log.info(f"No new issues (last scanned: {state.get('last_issue')}, "
                          f"current: {current})")
            return self.stats

        self.log.info(f"Scanning issues {start}-{end}")
        all_records = []
        for issue_num in range(start, end + 1):
            try:
                records = self.process_issue(issue_num)
                all_records.extend(records)
                self.stats["issues_checked"] += 1
            except Exception as e:
                self.log.exception(f"Error processing issue {issue_num}: {e}")
                self.stats["parse_errors"] += 1
            self.sleep()

        if all_records and not dry_run:
            self.upload_to_db(all_records)
            # Update state
            state["last_issue"] = end
            state["last_run"] = datetime.now().isoformat()
            self._save_state(state)
            self.log.info(f"Uploaded {self.stats['uploaded']} new reviews "
                          f"(skipped {self.stats['duplicates_skipped']} dupes)")
        elif dry_run:
            self.log.info(f"[DRY RUN] Would upload {len(all_records)} records")
            for rec in all_records[:10]:
                print(f"  {rec['Book Title'][:50]} | by {rec['Book Author First Name']} "
                      f"{rec['Book Author Last Name']} | reviewer: "
                      f"{rec['Reviewer First Name']} {rec['Reviewer Last Name']} | "
                      f"{rec['Publication Date']}")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description="Philosophy Now book review scraper")
    parser.add_argument("--bulk", action="store_true",
                        help="Scan all issues from 1 to current (backfill)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse only, don't write to database")
    parser.add_argument("--issues", type=str, default=None,
                        help="Specific issue range, e.g. '170-173'")
    args = parser.parse_args()

    issue_range = None
    if args.issues:
        m = re.match(r'^(\d+)(?:-(\d+))?$', args.issues)
        if m:
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else start
            issue_range = (start, end)

    scraper = PhilosophyNowScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk, issue_range=issue_range)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
