#!/usr/bin/env python3
"""
Claremont Review of Books scraper.

Scrapes https://claremontreviewofbooks.com/articles/book-reviews/ — the
Claremont Institute's quarterly journal covering political philosophy,
intellectual history, and Straussian thought (~1,100 reviews across 123 pages).

Approach:
- Use the RSS feed for full text (bulk + incremental)
- Parse book title from h3.book-title structured markup on review pages
- Book author not in structured data — Haiku extraction from article body

Usage:
    python3 crb_scraper.py                  # incremental (RSS feed)
    python3 crb_scraper.py --bulk           # full backfill (scans archive pages)
    python3 crb_scraper.py --dry-run        # parse only
"""

import argparse
import os
import re
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

import db
from scraper_base import BaseScraper

BASE_URL = "https://claremontreviewofbooks.com"
ARCHIVE_URL = f"{BASE_URL}/articles/book-reviews/"
FEED_URL = f"{BASE_URL}/articles/book-reviews/feed/"
SOURCE_NAME = "Claremont Review of Books"

# Issue season → approximate month
SEASON_MONTH = {
    'winter': '12', 'spring': '03', 'summer': '06', 'fall': '09', 'autumn': '09',
}


class CRBScraper(BaseScraper):
    """Scrapes book reviews from Claremont Review of Books."""

    name = "crb"
    default_delay = 1.5

    def __init__(self):
        super().__init__()
        self.stats = {
            "pages_scanned": 0,
            "urls_found": 0,
            "reviews_parsed": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
        }

    def fetch_archive_urls(self, max_pages=None):
        """Scan archive pages for review URLs."""
        urls = []
        page = 1
        # Detect total pages from first page
        r = self.get_with_retry(ARCHIVE_URL, timeout=30)
        if not r:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')
        total_elem = soup.select_one('span.comp__pagination-breadcrumbs-total')
        try:
            total_pages = int(total_elem.get_text(strip=True)) if total_elem else 123
        except ValueError:
            total_pages = 123
        if max_pages:
            total_pages = min(total_pages, max_pages)
        self.log.info(f"Scanning {total_pages} archive pages")

        while page <= total_pages:
            page_url = ARCHIVE_URL if page == 1 else f"{ARCHIVE_URL}page/{page}/"
            try:
                r = self.get_with_retry(page_url, timeout=30)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    break
                raise
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, 'html.parser')
            count = 0
            for art in soup.select('article.post.post-type__post'):
                a = art.select_one('h1.post-title > a')
                if a and a.get('href'):
                    href = a['href']
                    if href.startswith('/'):
                        href = BASE_URL + href
                    urls.append(href)
                    count += 1
            self.log.info(f"  Page {page}/{total_pages}: {count} reviews")
            self.stats["pages_scanned"] += 1
            page += 1
            self.sleep()
        return urls

    def fetch_feed_urls(self):
        """Get recent review URLs + metadata from RSS feed."""
        r = self.get_with_retry(FEED_URL, timeout=30)
        if not r:
            return []
        soup = BeautifulSoup(r.text, 'xml')
        items = soup.find_all('item')
        results = []
        for item in items:
            link_elem = item.find('link')
            url = link_elem.get_text(strip=True) if link_elem else ''
            creator_elem = item.find('dc:creator')
            reviewer = creator_elem.get_text(strip=True) if creator_elem else ''
            pub_date_elem = item.find('pubDate')
            pub_date = ''
            if pub_date_elem:
                try:
                    dt = parsedate_to_datetime(pub_date_elem.get_text(strip=True))
                    pub_date = dt.strftime('%Y-%m-%d')
                except Exception:
                    pass
            results.append({'url': url, 'reviewer': reviewer, 'pub_date': pub_date})
        return results

    def extract_review(self, url, reviewer_hint='', pub_date_hint=''):
        """Fetch a review page and extract book metadata."""
        r = self.get_with_retry(url, timeout=30)
        if not r or r.status_code != 200:
            self.stats["parse_errors"] += 1
            return None

        soup = BeautifulSoup(r.text, 'html.parser')

        record = {
            'Book Title': '',
            'Book Author First Name': '',
            'Book Author Last Name': '',
            'Reviewer First Name': '',
            'Reviewer Last Name': '',
            'Publication Source': SOURCE_NAME,
            'Publication Date': pub_date_hint,
            'Review Link': url,
            'Review Summary': '',
            'Access Type': 'Restricted',
            'DOI': '',
        }

        # Reviewer
        reviewer_elem = soup.select_one('div.box__name > a[href*="/author/"]')
        reviewer_name = reviewer_elem.get_text(strip=True) if reviewer_elem else reviewer_hint
        if reviewer_name:
            parts = reviewer_name.split()
            if len(parts) >= 2:
                record['Reviewer First Name'] = ' '.join(parts[:-1])
                record['Reviewer Last Name'] = parts[-1]
            elif parts:
                record['Reviewer Last Name'] = parts[0]

        # Book title
        book_elem = soup.select_one('h3.book-title > a')
        if not book_elem:
            book_elem = soup.select_one('h3.book-title')
        if book_elem:
            record['Book Title'] = book_elem.get_text(strip=True)

        # If no publication date, derive from issue
        if not record['Publication Date']:
            # Try the modern issue breadcrumb
            issue_elem = soup.select_one('a.breadcrumbs-meta-issue')
            # Or the legacy format: "Vol. VIII Number 3, Summer 2008"
            if not issue_elem:
                issue_elem = soup.select_one('.issue, .volume, .post-related a[href*="/issue/"]')
            if issue_elem:
                issue_text = issue_elem.get_text(strip=True).lower()
                # Match "Vol. X Number Y, Season YYYY" or plain "Season YYYY"
                m = re.search(r'(winter|spring|summer|fall|autumn)\s+(\d{4})', issue_text)
                if m:
                    season, year = m.groups()
                    record['Publication Date'] = f"{year}-{SEASON_MONTH[season]}-01"

        # Grab body text for Haiku author enrichment
        body_elem = soup.select_one('div.entry-content, div.post-content, div.article-body, article')
        if body_elem:
            # Strip scripts/styles
            for tag in body_elem.find_all(['script', 'style']):
                tag.decompose()
            body_text = body_elem.get_text(' ', strip=True)
            record['_body_text'] = body_text[:2500]

        if not record['Book Title']:
            self.stats["parse_errors"] += 1
            return None

        self.stats["reviews_parsed"] += 1
        return record

    def enrich_authors_with_haiku(self, records):
        """Use Haiku to extract book authors for records."""
        needs_enrichment = [r for r in records if not r['Book Author Last Name'] and r.get('_body_text')]
        if not needs_enrichment:
            return
        try:
            from anthropic import Anthropic
            client = Anthropic()
        except Exception as e:
            self.log.warning(f"Haiku unavailable: {e}")
            return

        self.log.info(f"Enriching {len(needs_enrichment)} records with Haiku...")
        import json as json_module

        for i in range(0, len(needs_enrichment), 10):
            batch = needs_enrichment[i:i+10]
            items_text = ""
            for j, rec in enumerate(batch):
                items_text += f"\n--- Item {j+1} ---\nBook title: {rec['Book Title']}\nArticle excerpt: {rec['_body_text']}\n"
            prompt = f"""These are excerpts from book reviews. For each, identify the book's author(s) (NOT the reviewer).

Output ONLY a JSON array:
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
                            name = result['author']
                            # Take first author only
                            name = re.split(r'\s+and\s+|,\s*', name, maxsplit=1)[0].strip()
                            parts = name.split()
                            if len(parts) >= 2:
                                batch[idx]['Book Author First Name'] = ' '.join(parts[:-1])
                                batch[idx]['Book Author Last Name'] = parts[-1]
                            elif parts:
                                batch[idx]['Book Author Last Name'] = parts[0]
            except Exception as e:
                self.log.warning(f"Haiku batch failed: {e}")

        for rec in records:
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
        if bulk:
            self.log.info("Bulk mode: scanning archive pages...")
            urls = self.fetch_archive_urls(max_pages=max_pages)
            items = [{'url': u, 'reviewer': '', 'pub_date': ''} for u in urls]
        else:
            self.log.info("Incremental mode: fetching RSS feed...")
            items = self.fetch_feed_urls()

        self.stats["urls_found"] = len(items)
        self.log.info(f"Found {len(items)} review URLs")

        if limit:
            items = items[:limit]

        new_items = [i for i in items if not db.review_link_exists(i['url'])]
        self.log.info(f"{len(new_items)} new (skipping {len(items) - len(new_items)} existing)")

        records = []
        for i, item in enumerate(new_items):
            record = self.extract_review(item['url'], item.get('reviewer', ''),
                                          item.get('pub_date', ''))
            if record:
                records.append(record)
            if (i + 1) % 50 == 0:
                self.log.info(f"  Processed {i + 1}/{len(new_items)}")
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
    parser = argparse.ArgumentParser(description="Claremont Review of Books scraper")
    parser.add_argument("--bulk", action="store_true", help="Scan all archive pages")
    parser.add_argument("--dry-run", action="store_true", help="Parse only")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit archive pages")
    parser.add_argument("--limit", type=int, default=None, help="Limit reviews")
    args = parser.parse_args()

    scraper = CRBScraper()
    stats = scraper.run(dry_run=args.dry_run, bulk=args.bulk,
                        max_pages=args.max_pages, limit=args.limit)
    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
