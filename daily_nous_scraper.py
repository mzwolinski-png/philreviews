#!/usr/bin/env python3
"""
Daily Nous scraper for PhilReviews.

Scrapes the "Book Reviews" section from Daily Nous "Online Philosophy
Resources Weekly Update" posts to find reviews of philosophy books in
non-academic media (The Atlantic, NYT, TLS, etc.) and open-access
academic journals.

Uses the WordPress REST API to fetch posts — no web scraping needed
for post discovery.

Usage:
    python3 daily_nous_scraper.py                  # bulk import (all posts)
    python3 daily_nous_scraper.py --recent         # latest 5 posts only
    python3 daily_nous_scraper.py --dry-run        # parse only, no DB writes
    python3 daily_nous_scraper.py --limit 10       # first N posts only
"""

import argparse
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup, Tag

import db

WP_API = "https://dailynous.com/wp-json/wp/v2/posts"
SLUG_PREFIX = "online-philosophy-resources-weekly-update"
USER_AGENT = (
    "PhilReviews/2.0 (academic research aggregator; mailto:mzwolinski@sandiego.edu)"
)

# Patterns for the section header
HEADER_PATTERNS = [
    re.compile(r"Book Reviews\*?", re.IGNORECASE),
    re.compile(
        r"Recent Philosophy Book Reviews in Non-Academic Media", re.IGNORECASE
    ),
]


class DailyNousScraper:
    """Scrapes book review listings from Daily Nous weekly update posts."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.stats = {
            "posts_fetched": 0,
            "posts_with_reviews": 0,
            "reviews_parsed": 0,
            "multi_book_entries": 0,
            "parse_errors": 0,
            "uploaded": 0,
            "duplicates_skipped": 0,
        }

    def log(self, msg, level="INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {level}: {msg}")

    # ── Post discovery via WP REST API ─────────────────────────────

    def _api_get(self, params, max_pages=None):
        """Fetch posts from WP REST API with pagination."""
        all_posts = []
        page = 1
        while True:
            params_page = {**params, "page": page}
            resp = self.session.get(WP_API, params=params_page, timeout=30)
            if resp.status_code == 400:
                # WP returns 400 when page > total_pages
                break
            resp.raise_for_status()
            posts = resp.json()
            if not posts:
                break
            all_posts.extend(posts)
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            if max_pages and page >= max_pages:
                break
            page += 1
            time.sleep(1)
        return all_posts

    def fetch_all_posts(self):
        """Fetch all Weekly Update posts (bulk import)."""
        self.log("Fetching all Weekly Update posts via WP API...")
        posts = self._api_get(
            {
                "search": "online philosophy resources weekly update",
                "per_page": 100,
                "orderby": "date",
                "order": "asc",
                "_fields": "id,date,slug,link,content",
            }
        )
        # Filter to only weekly update slugs
        posts = [p for p in posts if p.get("slug", "").startswith(SLUG_PREFIX)]
        self.log(f"Found {len(posts)} Weekly Update posts")
        return posts

    def fetch_recent_posts(self, count=5):
        """Fetch the N most recent Weekly Update posts (incremental)."""
        self.log(f"Fetching {count} most recent Weekly Update posts...")
        posts = self._api_get(
            {
                "search": "online philosophy resources weekly update",
                "per_page": count,
                "orderby": "date",
                "order": "desc",
                "_fields": "id,date,slug,link,content",
            },
            max_pages=1,
        )
        posts = [p for p in posts if p.get("slug", "").startswith(SLUG_PREFIX)]
        return posts

    # ── HTML parsing ───────────────────────────────────────────────

    def _extract_reviews_ol(self, html):
        """Find the Book Reviews <ol> from post content HTML.

        Returns the <ol> BeautifulSoup element, or None if not found.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Strategy: find any element whose text matches a header pattern,
        # then grab the next <ol> sibling.
        for el in soup.find_all(["h2", "h3", "h4", "p", "strong"]):
            text = el.get_text(strip=True)
            for pat in HEADER_PATTERNS:
                if pat.search(text):
                    # Found the header — look for the next <ol>
                    # Walk siblings from the header element (or its parent <p>)
                    start = el
                    if el.name == "strong" and el.parent and el.parent.name == "p":
                        start = el.parent
                    for sib in start.next_siblings:
                        if isinstance(sib, Tag) and sib.name == "ol":
                            return sib
                    return None
        return None

    def _parse_li(self, li, post_date):
        """Parse a single <li> element into a list of review dicts.

        Returns a list because multi-book reviews produce multiple records.
        """
        text = li.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        # Normalize smart quotes to straight quotes for consistent parsing
        text = text.replace("\u2019", "'").replace("\u2018", "'")
        text = text.replace("\u201c", '"').replace("\u201d", '"')

        if not text or len(text) < 10:
            return []

        # Find all <a> elements and their hrefs
        all_links = li.find_all("a", href=True)

        # Separate Amazon links from review/venue links, skip punctuation-only links
        non_amazon = [
            a
            for a in all_links
            if "amazon.com" not in a.get("href", "")
            and a.get_text(strip=True).strip(". ,;") != ""
        ]

        if not non_amazon:
            return []

        # Find all <em> elements and their cleaned texts
        all_em = li.find_all("em")
        em_texts = [em.get_text(strip=True).strip("., ") for em in all_em]
        em_texts = [t for t in em_texts if t]  # drop empty

        if not em_texts:
            return []

        # Venue detection: the venue is always the LAST <em> text
        # (venue names like "The Atlantic", "TLS" always appear at the end)
        venue_name = em_texts[-1]

        # Build venue_links: non-Amazon links whose <em> child matches a venue
        venue_links = []  # [(url, venue_name)]
        venue_names = set()
        for a in non_amazon:
            em = a.find("em")
            if em:
                vn = em.get_text(strip=True).strip("., ")
                # Only count as venue link if the <em> text matches a known
                # venue (appears at end) or is short enough to be a venue
                if vn and vn not in em_texts[:-1]:
                    # It's a venue, not a book title in the middle
                    pass
                if vn == venue_name:
                    venue_links.append((a.get("href", "").strip(), vn))
                    venue_names.add(vn)

        # Check for multi-venue: multiple non-Amazon links with <em> children
        # that match different venue names at the end of the text
        if not venue_links:
            # No link wraps the venue — the review URL is on the book title link
            # Use the first (or only) non-Amazon link as the review URL
            review_url = non_amazon[0].get("href", "").strip()
            venue_links = [(review_url, venue_name)]
            venue_names.add(venue_name)
        else:
            # Check if there are ADDITIONAL venue links (multi-venue)
            for a in non_amazon:
                em = a.find("em")
                if em:
                    vn = em.get_text(strip=True).strip("., ")
                    if vn and vn != venue_name and vn in em_texts:
                        # Check if this <em> appears at/near the end (likely a venue)
                        # by seeing if it's one of the last 2 em_texts
                        idx = em_texts.index(vn) if vn in em_texts else -1
                        if idx >= len(em_texts) - 2:
                            venue_links.insert(0, (a.get("href", "").strip(), vn))
                            venue_names.add(vn)

        # Book titles: <em> texts that are NOT venue names and look like real titles
        book_titles = [
            t
            for t in em_texts
            if t and t not in venue_names and len(t) > 5
        ]

        if not book_titles:
            return []

        # For multi-venue entries, parse each reviewer-venue pair
        if len(venue_links) > 1:
            records = self._parse_multi_venue(
                text, book_titles, venue_links, post_date
            )
            if records:
                return records

        # Single-venue
        review_url, venue_name = venue_links[0]
        records = self._parse_text(
            text, book_titles, venue_name, review_url, post_date
        )
        return records

    def _find_venue_and_url(self, li, non_amazon_links, text):
        """Identify the review URL and venue name from a <li>.

        Returns (review_url, venue_name) or (None, None).
        For multi-venue entries, returns the last venue link.
        """
        if not non_amazon_links:
            return None, None

        # Heuristic: the venue link is the last non-Amazon <a> in the <li>.
        venue_link = non_amazon_links[-1]
        review_url = venue_link.get("href", "").strip()

        # Try to get venue name from the <em> child of the venue link
        venue_em = venue_link.find("em")
        if venue_em:
            venue_name = venue_em.get_text(strip=True).strip(". ")
        else:
            # Try the link text itself
            link_text = venue_link.get_text(strip=True).strip(". ")
            # If link text looks like a venue (short, capitalized), use it
            if link_text and len(link_text) < 60 and not link_text.startswith("http"):
                venue_name = link_text
            else:
                venue_name = self._extract_venue_from_text(text)

        return review_url, venue_name

    def _extract_venue_from_text(self, text):
        """Extract venue name from text after the last 'at' keyword."""
        # Find all "at VenueName" matches and take the last one
        matches = list(re.finditer(r"\bat\s+([A-Z][^.]+?)\.?\s*$", text))
        if matches:
            return matches[-1].group(1).strip()
        # Fallback: find last "at CapitalWord..."
        m = re.search(r"\bat\s+([A-Z]\w[\w\s]*?)\.?\s*$", text)
        if m:
            return m.group(1).strip()
        return ""

    def _parse_multi_venue(self, text, book_titles, venue_links, post_date):
        """Parse entries like: 'Title by Author is reviewed by R1 at V1, and by R2 at V2'.

        Each venue_link is (url, venue_name).
        """
        title = book_titles[0] if book_titles else ""
        if not title:
            return []

        title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())

        # Extract author
        am = re.search(
            title_pat + r"[\s,]+(?:edited\s+)?by\s+(.+?),?\s+is\s+reviewed\s+",
            text,
            re.IGNORECASE,
        )
        author_str = self._clean_author(am.group(1).strip().rstrip(",")) if am else ""
        a_first, a_last = self._split_name(author_str)

        # Match each "reviewer at venue_name" pair by searching for the known venue names
        reviewer_map = {}  # venue_name -> reviewer
        for i, (_, vname) in enumerate(venue_links):
            vpat = re.escape(vname)
            if i == 0:
                # First venue: "reviewed by R1 at V1"
                m = re.search(
                    r"reviewed\s+by\s+(.+?)\s+at\s+" + vpat,
                    text,
                    re.IGNORECASE,
                )
                if m:
                    reviewer_map[vname] = m.group(1).strip().rstrip(",")
            else:
                # Subsequent venues: ", and (by) R at V" — comma required
                m = re.search(
                    r",\s+and\s+(?:by\s+)?(.+?)\s+at\s+" + vpat,
                    text,
                    re.IGNORECASE,
                )
                if m:
                    reviewer_map[vname] = m.group(1).strip().rstrip(",")

        records = []
        for url, vname in venue_links:
            reviewer_str = reviewer_map.get(vname, "")
            r_first, r_last = self._split_name(reviewer_str)
            records.append(
                self._make_record(
                    title, a_first, a_last, r_first, r_last,
                    vname, url, post_date,
                )
            )
        return records

    def _parse_text(self, text, book_titles, venue_name, review_url, post_date):
        """Parse author/reviewer from the full text of a <li>.

        Handles passive, active, and multi-book formats.
        """
        records = []

        # Detect multi-book: "are (together) reviewed by"
        if re.search(r"are\s+(together\s+)?reviewed\s+by", text, re.IGNORECASE):
            records = self._parse_multi_book(
                text, book_titles, venue_name, review_url, post_date
            )
            if records:
                self.stats["multi_book_entries"] += 1
                return records

        # Detect passive format: "is reviewed by" or "is reviewed at" (no reviewer)
        if re.search(r"is\s+reviewed\s+(by|at)\b", text, re.IGNORECASE):
            records = self._parse_passive(
                text, book_titles, venue_name, review_url, post_date
            )
            if records:
                return records

        # Detect active format: "X reviews Y"
        if re.search(r"\breviews?\s+", text.lower()):
            records = self._parse_active(
                text, book_titles, venue_name, review_url, post_date
            )
            if records:
                return records

        # Detect possessive format: "Author's Title, reviewed by R in/at V"
        if re.search(r",?\s+reviewed\s+by\b", text, re.IGNORECASE):
            records = self._parse_possessive(
                text, book_titles, venue_name, review_url, post_date
            )
            if records:
                return records

        # Fallback: try basic extraction
        if book_titles:
            records = self._parse_fallback(
                text, book_titles[0], venue_name, review_url, post_date
            )
            if records:
                return records

        return []

    def _parse_passive(self, text, book_titles, venue_name, review_url, post_date):
        """Parse passive format:
        - 'Title by Author is reviewed by Reviewer at Venue'
        - 'Title, edited by Editor, is reviewed by Reviewer at Venue'
        - 'Title by Author is reviewed at Venue' (no reviewer)
        - multi-venue: '...by R1 at V1, and by R2 at V2'
        """
        title = book_titles[0] if book_titles else ""
        if not title:
            return []

        # Build a flexible pattern for the title (collapse whitespace, flexible punctuation)
        title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())

        # Extract author: text between title and "is reviewed"
        # Handle both "by Author" and ", edited by Editor,"
        a_pattern = (
            title_pat + r"[\s,]+(?:edited\s+)?by\s+(.+?),?\s+is\s+reviewed\s+"
        )
        am = re.search(a_pattern, text, re.IGNORECASE)
        if am:
            author_str = self._clean_author(am.group(1).strip().rstrip(","))
        else:
            author_str = ""
        a_first, a_last = self._split_name(author_str)

        # Extract reviewer: "is reviewed by Reviewer at/in"
        # May be absent ("is reviewed at Venue" with no reviewer)
        r_pattern = r"is\s+reviewed\s+by\s+(.+?)\s+(?:at|in)\s+"
        rm = re.search(r_pattern, text, re.IGNORECASE)
        if rm:
            reviewer_str = rm.group(1).strip().rstrip(",")
        else:
            reviewer_str = ""
        r_first, r_last = self._split_name(reviewer_str)

        return [
            self._make_record(
                title, a_first, a_last, r_first, r_last,
                venue_name, review_url, post_date,
            )
        ]

    def _parse_active(self, text, book_titles, venue_name, review_url, post_date):
        """Parse active format: 'Reviewer reviews Title, by Author at Venue'"""
        title = book_titles[0] if book_titles else ""
        if not title:
            return []

        title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())

        # Extract reviewer: text before "reviews Title"
        pattern = r"^(.+?)\s+reviews?\s+" + title_pat
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            return []
        reviewer_str = m.group(1).strip()

        # Extract author: text between "title, by " or "title by " and " at/in "
        a_pattern = title_pat + r",?\s+by\s+(.+?),?\s+(?:at|in)\s+"
        am = re.search(a_pattern, text, re.IGNORECASE)
        if not am:
            return []
        author_str = am.group(1).strip().rstrip(",")

        author_str = self._clean_author(author_str)
        a_first, a_last = self._split_name(author_str)
        r_first, r_last = self._split_name(reviewer_str)

        return [
            self._make_record(
                title, a_first, a_last, r_first, r_last,
                venue_name, review_url, post_date,
            )
        ]

    def _parse_possessive(self, text, book_titles, venue_name, review_url, post_date):
        """Parse possessive format: "Author's Title, reviewed by Reviewer in/at Venue"."""
        title = book_titles[0] if book_titles else ""
        if not title:
            return []

        title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())

        # Extract author: text before the title (possessive: "Author's Title")
        m = re.search(r"^(.+?)'s?\s+" + title_pat, text, re.IGNORECASE)
        if m:
            author_str = m.group(1).strip()
        else:
            author_str = ""

        # Extract reviewer: "reviewed by Reviewer in/at"
        rm = re.search(
            r"reviewed\s+by\s+(.+?)\s+(?:at|in)\s+", text, re.IGNORECASE
        )
        reviewer_str = rm.group(1).strip().rstrip(",") if rm else ""

        author_str = self._clean_author(author_str)
        a_first, a_last = self._split_name(author_str)
        r_first, r_last = self._split_name(reviewer_str)

        return [
            self._make_record(
                title, a_first, a_last, r_first, r_last,
                venue_name, review_url, post_date,
            )
        ]

    def _parse_multi_book(self, text, book_titles, venue_name, review_url, post_date):
        """Parse multi-book format:
        'Title1 by A1 and Title2 by A2 are (together) reviewed by R at Venue'
        """
        # Extract reviewer
        rm = re.search(
            r"are\s+(?:together\s+)?reviewed\s+by\s+(.+?)\s+at\s+",
            text,
            re.IGNORECASE,
        )
        if not rm:
            return []
        reviewer_str = rm.group(1).strip().rstrip(",")
        r_first, r_last = self._split_name(reviewer_str)

        # Text before "are (together) reviewed"
        before = text[: rm.start()].strip()

        records = []
        for title in book_titles:
            title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())
            # Find "title by author" in the before-text
            am = re.search(
                title_pat + r",?\s+by\s+(.+?)(?:\s*,\s+and\s+|\s+and\s+|$)",
                before,
                re.IGNORECASE,
            )
            if am:
                author_str = self._clean_author(am.group(1).strip().rstrip(","))
                a_first, a_last = self._split_name(author_str)
            else:
                a_first, a_last = "", ""

            records.append(
                self._make_record(
                    title, a_first, a_last, r_first, r_last,
                    venue_name, review_url, post_date,
                )
            )

        return records

    def _parse_fallback(self, text, title, venue_name, review_url, post_date):
        """Last-resort parsing: extract what we can."""
        title_pat = r"[\s,]*\s+".join(re.escape(w) for w in title.split())
        # Try to find "by Author" after the title
        am = re.search(title_pat + r",?\s+by\s+(.+?)(?:\s+at\s+|$)", text, re.IGNORECASE)
        author_str = self._clean_author(am.group(1).strip().rstrip(",")) if am else ""
        a_first, a_last = self._split_name(author_str) if author_str else ("", "")

        return [
            self._make_record(
                title, a_first, a_last, "", "",
                venue_name, review_url, post_date,
            )
        ]

    # ── Name and text helpers ──────────────────────────────────────

    def _clean_author(self, author_str):
        """Strip 'translated by X', 'edited by X', etc. from author strings."""
        author_str = re.sub(
            r",?\s+translated\s+by\s+.+$", "", author_str, flags=re.IGNORECASE
        )
        author_str = re.sub(
            r",?\s+edited\s+by\s+.+$", "", author_str, flags=re.IGNORECASE
        )
        # Strip trailing "and" fragments from multi-book splitting
        author_str = re.sub(r"\s+and\s*$", "", author_str).strip()
        return author_str

    def _split_name(self, full_name):
        """Split 'First Last' into (first, last). Multi-word: everything
        except last word goes into first_name."""
        full_name = full_name.strip().rstrip(",.")
        if not full_name:
            return ("", "")
        parts = full_name.split()
        if len(parts) == 1:
            return ("", parts[0])
        return (" ".join(parts[:-1]), parts[-1])

    # Normalize common venue name variants
    VENUE_ALIASES = {
        "Times Literary Supplement": "The Times Literary Supplement",
        "TLS": "The Times Literary Supplement",
        "The Los Angeles Review of Books": "Los Angeles Review of Books",
        "New York Review of Books": "The New York Review of Books",
        "The New York Times Book Review": "The New York Times",
        "Wall Street Journal": "The Wall Street Journal",
        "Washington Post": "The Washington Post",
        "Kirkus": "Kirkus Reviews",
        "The Kirkus Review": "Kirkus Reviews",
    }

    def _make_record(
        self, title, a_first, a_last, r_first, r_last,
        venue, review_url, post_date,
    ):
        """Create a dict matching db.insert_reviews() column names."""
        venue = venue.strip(". ")
        venue = self.VENUE_ALIASES.get(venue, venue)
        return {
            "book_title": title.strip(),
            "book_author_first_name": a_first.strip(),
            "book_author_last_name": a_last.strip(),
            "reviewer_first_name": r_first.strip(),
            "reviewer_last_name": r_last.strip(),
            "publication_source": venue,
            "publication_date": post_date,
            "review_link": review_url.strip(),
            "review_summary": "",
            "access_type": "Open",
            "doi": "",
            "entry_type": "review",
            "symposium_group": "",
        }

    # ── Processing pipeline ────────────────────────────────────────

    def process_post(self, post):
        """Extract reviews from a single WP API post dict.

        Returns a list of review record dicts.
        """
        content = post.get("content", {}).get("rendered", "")
        post_date = post.get("date", "")[:10]  # YYYY-MM-DD
        slug = post.get("slug", "")

        ol = self._extract_reviews_ol(content)
        if ol is None:
            return []

        self.stats["posts_with_reviews"] += 1
        records = []
        for li in ol.find_all("li", recursive=False):
            try:
                parsed = self._parse_li(li, post_date)
                records.extend(parsed)
            except Exception as e:
                self.stats["parse_errors"] += 1
                self.log(f"  Parse error in {slug}: {e}", "WARNING")

        return records

    def upload_to_db(self, records):
        """Deduplicate and batch-insert records into the database."""
        new_records = []
        for r in records:
            link = r.get("review_link", "").strip()
            if link and not db.review_link_exists(link):
                new_records.append(r)
            else:
                self.stats["duplicates_skipped"] += 1

        if new_records:
            db.insert_reviews(new_records)
        self.stats["uploaded"] = len(new_records)
        return len(new_records)

    def run_bulk(self, dry_run=False, limit=None):
        """Fetch and process all Weekly Update posts (initial import)."""
        posts = self.fetch_all_posts()
        if limit:
            posts = posts[:limit]
        self.stats["posts_fetched"] = len(posts)

        all_records = []
        for i, post in enumerate(posts):
            slug = post.get("slug", "")
            records = self.process_post(post)
            if records:
                all_records.extend(records)
                self.log(
                    f"  [{i + 1}/{len(posts)}] {slug}: {len(records)} reviews"
                )

        self.stats["reviews_parsed"] = len(all_records)
        self.log(
            f"Parsed {len(all_records)} reviews from "
            f"{self.stats['posts_with_reviews']}/{len(posts)} posts"
        )

        if dry_run:
            self.log("Dry run — skipping database upload")
            self._print_sample(all_records)
        else:
            uploaded = self.upload_to_db(all_records)
            self.log(f"Uploaded {uploaded} new reviews, "
                     f"skipped {self.stats['duplicates_skipped']} duplicates")

        return self.stats

    def run_incremental(self, dry_run=False):
        """Fetch and process the most recent posts (weekly update)."""
        posts = self.fetch_recent_posts(count=5)
        self.stats["posts_fetched"] = len(posts)

        all_records = []
        for post in posts:
            records = self.process_post(post)
            all_records.extend(records)

        self.stats["reviews_parsed"] = len(all_records)

        if all_records and not dry_run:
            self.upload_to_db(all_records)

        self.log(
            f"Incremental: {len(all_records)} reviews from {len(posts)} posts, "
            f"{self.stats['uploaded']} new"
        )
        return self.stats

    def _print_sample(self, records, n=10):
        """Print a sample of parsed records for dry-run review."""
        self.log(f"Sample of first {min(n, len(records))} records:")
        for r in records[:n]:
            author = f"{r['book_author_first_name']} {r['book_author_last_name']}".strip()
            reviewer = f"{r['reviewer_first_name']} {r['reviewer_last_name']}".strip()
            print(
                f"  {r['book_title']}"
                f" | by {author or '?'}"
                f" | reviewed by {reviewer or '?'}"
                f" | {r['publication_source']}"
                f" | {r['publication_date']}"
            )


def main():
    parser = argparse.ArgumentParser(description="Daily Nous book review scraper")
    parser.add_argument(
        "--recent", action="store_true", help="Only fetch the 5 most recent posts"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Parse only, don't write to database"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Process only the first N posts"
    )
    args = parser.parse_args()

    scraper = DailyNousScraper()

    if args.recent:
        stats = scraper.run_incremental(dry_run=args.dry_run)
    else:
        stats = scraper.run_bulk(dry_run=args.dry_run, limit=args.limit)

    print(f"\nStats: {stats}")


if __name__ == "__main__":
    main()
