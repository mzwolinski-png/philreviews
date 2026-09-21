#!/usr/bin/env python3
"""
Backfill missing reviewer names for mainstream outlet entries.
Fetches each article URL and extracts the reviewer from page metadata.
"""

import json
import re
import sqlite3
import time
import os
from html.parser import HTMLParser

import requests

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviews.db")

SESSION = requests.Session()
SESSION.headers['User-Agent'] = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

# Outlets to process (excluding Kirkus which is anonymous)
TARGET_OUTLETS = [
    'The New York Times', 'New York Review of Books',
    'The Times Literary Supplement', 'Los Angeles Review of Books',
    'Boston Review', 'The Guardian', 'The Wall Street Journal',
    'The New Yorker', 'Literary Review', 'The Washington Post',
    'The Nation', 'The Atlantic', 'The Telegraph',
]


class MetaExtractor(HTMLParser):
    """Extract author info from HTML meta tags and JSON-LD."""

    def __init__(self):
        super().__init__()
        self.authors = []
        self.in_script = False
        self.script_data = ''
        self._current_tag = None

    def handle_starttag(self, tag, attrs):
        self._current_tag = tag
        if tag == 'meta':
            d = dict(attrs)
            name = d.get('name', '').lower()
            prop = d.get('property', '').lower()
            content = d.get('content', '')

            if content and name in ('author', 'sailthru.author', 'byl',
                                     'dc.creator', 'citation_author'):
                self.authors.append(content)
            elif content and prop in ('article:author', 'og:article:author',
                                       'author'):
                self.authors.append(content)
        elif tag == 'script':
            d = dict(attrs)
            if d.get('type') == 'application/ld+json':
                self.in_script = True
                self.script_data = ''

    def handle_data(self, data):
        if self.in_script:
            self.script_data += data

    def handle_endtag(self, tag):
        if tag == 'script' and self.in_script:
            self.in_script = False
            try:
                ld = json.loads(self.script_data)
                self._extract_ld_author(ld)
            except (json.JSONDecodeError, TypeError):
                pass

    def _extract_ld_author(self, data):
        if isinstance(data, list):
            for item in data:
                self._extract_ld_author(item)
            return
        if not isinstance(data, dict):
            return
        # Check @graph
        if '@graph' in data:
            self._extract_ld_author(data['@graph'])
        author = data.get('author')
        if author:
            if isinstance(author, str):
                self.authors.append(author)
            elif isinstance(author, dict):
                name = author.get('name', '')
                if name:
                    self.authors.append(name)
            elif isinstance(author, list):
                for a in author:
                    if isinstance(a, str):
                        self.authors.append(a)
                    elif isinstance(a, dict) and a.get('name'):
                        self.authors.append(a['name'])


def clean_author_name(raw: str) -> tuple:
    """Clean and split author name into (first, last)."""
    name = raw.strip()
    # Remove "By " prefix
    name = re.sub(r'^By\s+', '', name, flags=re.IGNORECASE).strip()
    # Remove URLs
    if name.startswith('http'):
        return None, None
    # Remove outlet-specific noise
    name = re.sub(r'\s*\|.*$', '', name).strip()
    name = re.sub(r',?\s*(staff|correspondent|editor|contributor|reviewer).*$',
                  '', name, flags=re.IGNORECASE).strip()
    # Remove trailing dates
    name = re.sub(r',?\s*\w+\s+\d{1,2},?\s*\d{4}\s*$', '', name).strip()

    if not name or len(name) < 3:
        return None, None

    # Split
    parts = name.split()
    if len(parts) >= 2:
        return ' '.join(parts[:-1]), parts[-1]
    elif len(parts) == 1:
        return '', parts[0]
    return None, None


def extract_inline_author(html_text: str) -> str:
    """Extract author from inline JSON/JS data in the page."""
    # TLS: "byline":{"text":"Author Name"}
    m = re.search(r'"byline"\s*:\s*\{\s*"text"\s*:\s*"([^"]+)"', html_text)
    if m:
        return m.group(1)

    # LARB and others: "author":[{"name":"Author Name"}] or "author":{"name":"..."}
    m = re.search(r'"author"\s*:\s*\[\s*\{[^}]*"name"\s*:\s*"([^"]+)"', html_text)
    if m:
        return m.group(1)
    m = re.search(r'"author"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"', html_text)
    if m:
        return m.group(1)

    # Generic byline class: <span class="byline">By Author Name</span>
    m = re.search(r'class="[^"]*byline[^"]*"[^>]*>(?:By\s+)?([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', html_text)
    if m:
        return m.group(1)

    return ''


# Outlets known to block bot requests
SKIP_OUTLETS = {'The New York Times'}  # Uses Datadome CAPTCHA


def fetch_reviewer(url: str, outlet: str = '') -> tuple:
    """Fetch a URL and extract the reviewer name."""
    if outlet in SKIP_OUTLETS:
        return None, None, 'blocked (CAPTCHA)'

    try:
        resp = SESSION.get(url, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return None, None, f'HTTP {resp.status_code}'

        html_text = resp.text[:80000]

        # Try structured metadata first (meta tags + JSON-LD)
        extractor = MetaExtractor()
        try:
            extractor.feed(html_text)
        except Exception:
            pass

        # Filter and deduplicate authors
        seen = set()
        for author in extractor.authors:
            author = author.strip()
            if not author or author.lower() in seen:
                continue
            seen.add(author.lower())
            # Skip URLs (Guardian returns profile URLs in article:author)
            if author.startswith('http'):
                continue
            first, last = clean_author_name(author)
            if first is not None and last:
                return first, last, None

        # Fallback: try inline JSON/JS extraction
        inline = extract_inline_author(html_text)
        if inline:
            first, last = clean_author_name(inline)
            if first is not None and last:
                return first, last, None

        return None, None, 'no author found'

    except requests.exceptions.Timeout:
        return None, None, 'timeout'
    except requests.exceptions.ConnectionError:
        return None, None, 'connection error'
    except Exception as e:
        return None, None, str(e)


def get_missing_reviewer_entries():
    """Get mainstream outlet entries missing reviewer names."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    placeholders = ','.join('?' * len(TARGET_OUTLETS))
    rows = conn.execute(f"""
        SELECT id, book_title, review_link, publication_source
        FROM reviews
        WHERE (reviewer_last_name IS NULL OR reviewer_last_name = '')
        AND publication_source IN ({placeholders})
        ORDER BY publication_source
    """, TARGET_OUTLETS).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fix missing reviewers for mainstream outlets')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()

    entries = get_missing_reviewer_entries()
    print(f"Found {len(entries)} mainstream entries missing reviewers")

    if args.limit:
        entries = entries[:args.limit]

    fixed = 0
    failed = 0
    errors = 0
    by_outlet = {}

    conn = sqlite3.connect(DB_PATH)

    for i, entry in enumerate(entries):
        url = entry['review_link']
        outlet = entry['publication_source']

        if not url:
            failed += 1
            continue

        first, last, err = fetch_reviewer(url, outlet)

        if first is not None and last:
            if not args.dry_run:
                conn.execute(
                    "UPDATE reviews SET reviewer_first_name = ?, reviewer_last_name = ? WHERE id = ?",
                    (first, last, entry['id']))
            fixed += 1
            by_outlet[outlet] = by_outlet.get(outlet, 0) + 1
            if fixed <= 15:
                print(f"  [{outlet}] '{entry['book_title'][:40]}' -> {first} {last}")
        else:
            failed += 1
            if failed <= 10:
                print(f"  MISS [{outlet}] '{entry['book_title'][:40]}' ({err})")

        # Rate limit: 2 req/sec
        time.sleep(0.5)

        if (i + 1) % 50 == 0:
            if not args.dry_run:
                conn.commit()
            print(f"  Processed {i+1}/{len(entries)}: {fixed} fixed, {failed} failed")

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"Mainstream Reviewer Fix {'(DRY RUN)' if args.dry_run else 'Results'}")
    print(f"{'='*50}")
    print(f"Processed: {len(entries)}")
    print(f"Fixed:     {fixed}")
    print(f"Failed:    {failed}")
    if by_outlet:
        print(f"By outlet:")
        for outlet, count in sorted(by_outlet.items(), key=lambda x: -x[1]):
            print(f"  {outlet}: {count}")


if __name__ == '__main__':
    main()
