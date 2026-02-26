#!/usr/bin/env python3
"""
Mainstream media review scraper for PhilReviews.

Searches Google Custom Search and the Guardian API for reviews of philosophy
books in major non-academic outlets (TLS, LARB, Guardian, NYT, LRB, etc.).

Targets books already well-represented in the database (3+ academic reviews),
since those are most likely to have attracted mainstream media attention.

Usage:
    python3 mainstream_review_scraper.py                    # bulk scan
    python3 mainstream_review_scraper.py --dry-run           # search but don't insert
    python3 mainstream_review_scraper.py --min-reviews 2     # lower threshold
    python3 mainstream_review_scraper.py --limit 20          # first N books (testing)
    python3 mainstream_review_scraper.py --status            # show scan progress
    python3 mainstream_review_scraper.py --reset             # reset state, start over
    python3 mainstream_review_scraper.py --guardian-only      # skip Google CSE
"""

import argparse
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from urllib.parse import urlparse, urlunparse

import requests

import db

# ── Configuration ──────────────────────────────────────────────────

STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mainstream_search_state.json"
)

USER_AGENT = (
    "PhilReviews/2.0 (academic research aggregator; mailto:mzwolinski@sandiego.edu)"
)

# Target outlets: domain → (venue name, access type)
DOMAIN_TO_VENUE = {
    "the-tls.co.uk": ("The Times Literary Supplement", "Restricted"),
    "lareviewofbooks.org": ("Los Angeles Review of Books", "Open"),
    "wsj.com": ("The Wall Street Journal", "Restricted"),
    "theguardian.com": ("The Guardian", "Open"),
    "nybooks.com": ("The New York Review of Books", "Restricted"),
    "lrb.co.uk": ("London Review of Books", "Restricted"),
    "kirkusreviews.com": ("Kirkus Reviews", "Open"),
    "bostonreview.net": ("Boston Review", "Open"),
    "nytimes.com": ("The New York Times", "Restricted"),
    "literaryreview.co.uk": ("Literary Review", "Restricted"),
    "washingtonpost.com": ("The Washington Post", "Restricted"),
    "newyorker.com": ("The New Yorker", "Restricted"),
    "telegraph.co.uk": ("The Telegraph", "Restricted"),
    "thenation.com": ("The Nation", "Open"),
    "theatlantic.com": ("The Atlantic", "Restricted"),
    "australianbookreview.com.au": ("Australian Book Review", "Open"),
}

# Venue names already in the DB (from Daily Nous) that count as "mainstream"
MAINSTREAM_VENUE_NAMES = {v[0] for v in DOMAIN_TO_VENUE.values()}

# Generic titles to skip
GENERIC_TITLES = {
    "book review", "book reviews", "review", "reviews",
    "critical notice", "critical notices", "discussion",
    "review article", "review essay", "symposium",
    "untitled", "erratum", "corrigendum", "correction",
}

# Minimum significant words for a title to be searchable
MIN_TITLE_WORDS = 3


# ── State management ──────────────────────────────────────────────

def load_state():
    """Load scan progress from state file."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {
        "last_run": None,
        "books_completed": [],
        "total_reviews_found": 0,
        "last_book_index": 0,
    }


def save_state(state):
    """Persist scan progress."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def reset_state():
    """Delete state file to start fresh."""
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print("State reset — next run will start from the beginning.")
    else:
        print("No state file found.")


def show_status():
    """Print current scan progress."""
    state = load_state()
    completed = len(state.get("books_completed", []))
    found = state.get("total_reviews_found", 0)
    last_run = state.get("last_run", "never")
    last_idx = state.get("last_book_index", 0)
    print(f"Last run:        {last_run}")
    print(f"Books completed: {completed}")
    print(f"Last book index: {last_idx}")
    print(f"Reviews found:   {found}")


# ── Candidate book selection ──────────────────────────────────────

def get_significant_words(title):
    """Return words with 3+ characters from a title."""
    return [w for w in re.findall(r"[a-zA-Z]{3,}", title)]


def get_candidate_books(min_reviews=3):
    """Query DB for books with enough academic reviews to search mainstream."""
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get books with min_reviews+ reviews
    rows = conn.execute("""
        SELECT book_title, book_author_first_name, book_author_last_name,
               COUNT(*) as cnt
        FROM reviews
        WHERE book_title IS NOT NULL AND book_title != ''
          AND book_author_last_name IS NOT NULL AND book_author_last_name != ''
        GROUP BY LOWER(book_title), LOWER(book_author_last_name)
        HAVING COUNT(*) >= ?
        ORDER BY cnt DESC
    """, (min_reviews,)).fetchall()

    # Get existing mainstream venue coverage to exclude
    existing_mainstream = set()
    for venue_name in MAINSTREAM_VENUE_NAMES:
        ms_rows = conn.execute("""
            SELECT LOWER(book_title) || '|' || LOWER(book_author_last_name) as key
            FROM reviews
            WHERE publication_source = ?
              AND book_title IS NOT NULL AND book_title != ''
              AND book_author_last_name IS NOT NULL AND book_author_last_name != ''
        """, (venue_name,)).fetchall()
        for r in ms_rows:
            existing_mainstream.add(r["key"])

    conn.close()

    candidates = []
    for row in rows:
        title = row["book_title"].strip()
        last = row["book_author_last_name"].strip()
        first = (row["book_author_first_name"] or "").strip()

        # Skip generic titles
        if title.lower() in GENERIC_TITLES:
            continue

        # Skip titles with too few significant words
        sig_words = get_significant_words(title)
        if len(sig_words) < MIN_TITLE_WORDS:
            continue

        # Skip books that already have mainstream coverage
        book_key = f"{title.lower()}|{last.lower()}"
        if book_key in existing_mainstream:
            continue

        candidates.append({
            "book_title": title,
            "book_author_first_name": first,
            "book_author_last_name": last,
            "review_count": row["cnt"],
            "key": book_key,
        })

    return candidates


# ── URL normalization ─────────────────────────────────────────────

def normalize_url(url):
    """Strip query parameters and fragments from a URL."""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def domain_from_url(url):
    """Extract the registrable domain from a URL."""
    hostname = urlparse(url).hostname or ""
    # Strip www. prefix
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


# ── Verification pipeline ─────────────────────────────────────────

def verify_result(result_title, result_snippet, result_url, book_title, author_last):
    """Check if a search result is actually a review of this book.

    Returns True if the result passes verification.
    """
    # Tier 1: Domain check
    domain = domain_from_url(result_url)
    if domain not in DOMAIN_TO_VENUE:
        return False

    combined = f"{result_title} {result_snippet}".lower()
    author_last_lower = author_last.lower()

    # Author last name must appear
    if author_last_lower not in combined:
        return False

    # Tier 2: Title word matching
    sig_words = get_significant_words(book_title)
    if not sig_words:
        return False

    matched = sum(1 for w in sig_words if w.lower() in combined)
    match_ratio = matched / len(sig_words)

    # Tier 3: Review signal detection
    review_signals = [
        "review", "reviewed", "reviews",
        f"on {book_title.lower()[:30]}",
        "university press", "oxford", "cambridge", "princeton",
        "harvard", "routledge", "mit press",
    ]
    has_review_signal = any(sig in combined for sig in review_signals)

    # Short titles need stricter matching
    is_short_title = len(sig_words) < 4

    if is_short_title:
        # Short titles: require both review signal AND good match
        return match_ratio >= 0.60 and has_review_signal
    elif has_review_signal:
        # Review signal lowers the threshold
        return match_ratio >= 0.40
    else:
        return match_ratio >= 0.60


def extract_reviewer_from_snippet(snippet):
    """Try to extract a reviewer name from a search snippet.

    Looks for patterns like "By Name" or "by Name" at the start.
    Returns (first_name, last_name) or ("", "").
    """
    if not snippet:
        return ("", "")

    # Common patterns: "By First Last ...", "By First Last |"
    m = re.match(r"^[Bb]y\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", snippet)
    if m:
        parts = m.group(1).strip().split()
        if 2 <= len(parts) <= 3:
            return (" ".join(parts[:-1]), parts[-1])

    return ("", "")


def extract_date_from_snippet(snippet):
    """Try to extract a date from a Google snippet.

    Google often prepends dates like "Jan 15, 2024 — ..." or "15 Jan 2024 —".
    Returns YYYY-MM-DD string or "".
    """
    if not snippet:
        return ""

    # Pattern: "Mon DD, YYYY"
    m = re.match(
        r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
        r"(\d{1,2}),?\s+(\d{4})",
        snippet,
    )
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Pattern: "DD Mon YYYY"
    m = re.match(
        r"^(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})",
        snippet,
    )
    if m:
        try:
            dt = datetime.strptime(
                f"{m.group(2)} {m.group(1)} {m.group(3)}", "%b %d %Y"
            )
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return ""


# ── Google Custom Search API ──────────────────────────────────────

class GoogleCSESearcher:
    """Search Google Custom Search Engine for book reviews."""

    API_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(self, api_key, cx):
        self.api_key = api_key
        self.cx = cx
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.queries_today = 0

    def search(self, book_title, author_last):
        """Search for reviews of a book. Returns list of result dicts."""
        query = f'"{book_title}" {author_last} review'
        params = {
            "key": self.api_key,
            "cx": self.cx,
            "q": query,
            "num": 10,
        }

        try:
            resp = self.session.get(self.API_URL, params=params, timeout=30)
            self.queries_today += 1

            if resp.status_code == 429:
                print("  Google CSE: rate limited (429), waiting 60s...")
                time.sleep(60)
                return []
            if resp.status_code == 403:
                print("  Google CSE: quota exceeded (403)")
                return []

            resp.raise_for_status()
            data = resp.json()
            return data.get("items", [])

        except requests.RequestException as e:
            print(f"  Google CSE error: {e}")
            return []


# ── Guardian API ──────────────────────────────────────────────────

class GuardianSearcher:
    """Download all Guardian book reviews and match against candidate books."""

    API_URL = "https://content.guardianapis.com/search"

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def search(self, book_title, author_last):
        """Per-book search (legacy). Use fetch_all_reviews + match instead."""
        clean_title = re.sub(r"[:\?\!\*]", "", book_title)
        query = f'"{clean_title}" {author_last}'
        params = {
            "api-key": self.api_key,
            "q": query,
            "section": "books",
            "show-fields": "byline,trailText",
            "page-size": 10,
        }

        try:
            resp = self.session.get(self.API_URL, params=params, timeout=30)
            if resp.status_code == 429:
                print("  Guardian: rate limited, waiting 10s...")
                time.sleep(10)
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", {}).get("results", [])

        except requests.RequestException as e:
            print(f"  Guardian API error: {e}")
            return []

    def fetch_all_reviews(self):
        """Download all Guardian book reviews (tagged tone/reviews).

        Returns a list of all review items from the API.
        """
        all_results = []
        page = 1
        total_pages = None

        while True:
            params = {
                "api-key": self.api_key,
                "section": "books",
                "tag": "tone/reviews",
                "show-fields": "byline,trailText",
                "page-size": 200,
                "page": page,
            }

            try:
                resp = self.session.get(self.API_URL, params=params, timeout=30)
                if resp.status_code == 429:
                    print("  Guardian: rate limited, waiting 30s...")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                data = resp.json()
                response = data.get("response", {})
                results = response.get("results", [])
                if total_pages is None:
                    total_pages = response.get("pages", 1)
                    total = response.get("total", 0)
                    print(f"  Guardian: {total} book reviews across {total_pages} pages")

                all_results.extend(results)

                if page % 20 == 0:
                    print(f"  Guardian: fetched page {page}/{total_pages} "
                          f"({len(all_results)} reviews so far)")

                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.25)  # Stay well within rate limits

            except requests.RequestException as e:
                print(f"  Guardian API error on page {page}: {e}")
                time.sleep(5)
                continue

        return all_results

    def match_reviews_to_books(self, reviews, candidates):
        """Match Guardian reviews against candidate books from the DB.

        Guardian review titles typically follow the pattern:
        "Book Title by Author review – subtitle"

        Returns list of (guardian_item, candidate_book) tuples.
        """
        # Index by author last name for quick filtering
        by_author = {}
        for book in candidates:
            last = book["book_author_last_name"].lower()
            by_author.setdefault(last, []).append(book)

        matches = []
        seen_urls = set()

        for item in reviews:
            g_title = item.get("webTitle", "")
            g_trail = (item.get("fields", {}).get("trailText", "") or "")
            g_url = item.get("webUrl", "")
            g_title_lower = g_title.lower()

            # Check each author that appears in this review's title
            for author_last, books in by_author.items():
                # Author last name must appear as a whole word in the TITLE
                # (not just the trail text — too many false matches)
                if not re.search(
                    r'\b' + re.escape(author_last) + r'\b',
                    g_title_lower,
                ):
                    continue

                for book in books:
                    sig_words = get_significant_words(book["book_title"])
                    if not sig_words:
                        continue

                    # Only count words 4+ chars as significant for matching
                    # to avoid false hits on "the", "and", "for", etc.
                    strong_words = [w for w in sig_words if len(w) >= 4]
                    if not strong_words:
                        continue

                    # Match against the title (primary) — this is most reliable
                    matched_title = sum(
                        1 for w in strong_words
                        if w.lower() in g_title_lower
                    )
                    title_ratio = matched_title / len(strong_words)

                    # For a match, we want most strong title words in the
                    # Guardian headline — which typically contains the book title
                    has_review = "review" in g_title_lower

                    # Require high title match (≥75%) and "review" in headline
                    if title_ratio >= 0.75 and has_review:
                        match_key = (g_url, book["key"])
                        if match_key not in seen_urls:
                            seen_urls.add(match_key)
                            matches.append((item, book))

        return matches


# ── Main scraper ──────────────────────────────────────────────────

class MainstreamReviewScraper:
    """Orchestrates searching and verification for mainstream reviews."""

    def __init__(self, google_api_key=None, google_cx=None, guardian_api_key=None):
        self.google = None
        self.guardian = None

        if google_api_key and google_cx:
            self.google = GoogleCSESearcher(google_api_key, google_cx)
        if guardian_api_key:
            self.guardian = GuardianSearcher(guardian_api_key)

        self.stats = {
            "books_searched": 0,
            "google_queries": 0,
            "guardian_queries": 0,
            "results_checked": 0,
            "results_verified": 0,
            "duplicates_skipped": 0,
            "uploaded": 0,
        }
        self.found_reviews = []

    def log(self, msg, level="INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {level}: {msg}")

    def search_book(self, book, guardian_only=False):
        """Search for mainstream reviews of a single book.

        Returns list of verified review dicts ready for DB insertion.
        """
        title = book["book_title"]
        last = book["book_author_last_name"]
        first = book["book_author_first_name"]
        verified = []

        # Google CSE search
        if self.google and not guardian_only:
            results = self.google.search(title, last)
            self.stats["google_queries"] += 1

            for item in results:
                self.stats["results_checked"] += 1
                result_title = item.get("title", "")
                snippet = item.get("snippet", "")
                url = item.get("link", "")

                if not url:
                    continue

                if not verify_result(result_title, snippet, url, title, last):
                    continue

                url = normalize_url(url)
                if db.review_link_exists(url):
                    self.stats["duplicates_skipped"] += 1
                    continue

                # Check not already found in this run
                if any(r["review_link"] == url for r in verified):
                    continue

                domain = domain_from_url(url)
                venue_name, access_type = DOMAIN_TO_VENUE[domain]
                pub_date = extract_date_from_snippet(snippet)
                r_first, r_last = extract_reviewer_from_snippet(snippet)

                verified.append({
                    "book_title": title,
                    "book_author_first_name": first,
                    "book_author_last_name": last,
                    "reviewer_first_name": r_first,
                    "reviewer_last_name": r_last,
                    "publication_source": venue_name,
                    "publication_date": pub_date,
                    "review_link": url,
                    "review_summary": "",
                    "access_type": access_type,
                    "doi": "",
                    "entry_type": "review",
                    "symposium_group": "",
                })
                self.stats["results_verified"] += 1

            # Rate limit: 1 query/second
            time.sleep(1)

        # Guardian API search
        if self.guardian:
            g_results = self.guardian.search(title, last)
            self.stats["guardian_queries"] += 1

            for item in g_results:
                self.stats["results_checked"] += 1
                g_title = item.get("webTitle", "")
                g_url = item.get("webUrl", "")
                g_trail = item.get("fields", {}).get("trailText", "")

                if not g_url:
                    continue

                # Use webTitle + trailText as the snippet for verification
                g_snippet = f"{g_title} {g_trail}"
                if not verify_result(g_title, g_snippet, g_url, title, last):
                    continue

                g_url = normalize_url(g_url)
                if db.review_link_exists(g_url):
                    self.stats["duplicates_skipped"] += 1
                    continue

                if any(r["review_link"] == g_url for r in verified):
                    continue

                # Extract reviewer from Guardian byline field
                byline = item.get("fields", {}).get("byline", "")
                r_first, r_last = ("", "")
                if byline:
                    parts = byline.strip().split()
                    if 2 <= len(parts) <= 3:
                        r_first = " ".join(parts[:-1])
                        r_last = parts[-1]

                # Guardian publication date
                pub_date = ""
                web_pub = item.get("webPublicationDate", "")
                if web_pub:
                    pub_date = web_pub[:10]  # YYYY-MM-DD

                verified.append({
                    "book_title": title,
                    "book_author_first_name": first,
                    "book_author_last_name": last,
                    "reviewer_first_name": r_first,
                    "reviewer_last_name": r_last,
                    "publication_source": "The Guardian",
                    "publication_date": pub_date,
                    "review_link": g_url,
                    "review_summary": "",
                    "access_type": "Open",
                    "doi": "",
                    "entry_type": "review",
                    "symposium_group": "",
                })
                self.stats["results_verified"] += 1

            time.sleep(0.5)

        return verified

    def run(self, min_reviews=3, limit=None, dry_run=False, guardian_only=False):
        """Run the mainstream review scan.

        Args:
            min_reviews: Minimum academic reviews a book must have.
            limit: Max number of books to search (for testing).
            dry_run: If True, don't insert into DB.
            guardian_only: If True, skip Google CSE.
        """
        if guardian_only and not self.guardian:
            self.log("Guardian API key not configured — nothing to do", "ERROR")
            return self.stats

        if not guardian_only and not self.google and not self.guardian:
            self.log("No API keys configured", "ERROR")
            return self.stats

        self.log(f"Fetching candidate books (min {min_reviews} reviews)...")
        candidates = get_candidate_books(min_reviews)
        self.log(f"Found {len(candidates)} candidate books")

        all_reviews = []

        # ── Guardian bulk approach ──────────────────────────────
        if self.guardian:
            self.log("Downloading all Guardian book reviews for bulk matching...")
            g_reviews = self.guardian.fetch_all_reviews()
            self.log(f"Downloaded {len(g_reviews)} Guardian reviews")
            self.stats["guardian_queries"] = len(g_reviews) // 200 + 1

            matches = self.guardian.match_reviews_to_books(g_reviews, candidates)
            self.log(f"Found {len(matches)} potential matches, verifying...")

            for item, book in matches:
                g_url = normalize_url(item.get("webUrl", ""))
                if not g_url:
                    continue

                self.stats["results_checked"] += 1

                if db.review_link_exists(g_url):
                    self.stats["duplicates_skipped"] += 1
                    continue

                if any(r["review_link"] == g_url for r in all_reviews):
                    continue

                # Extract reviewer from byline
                byline = item.get("fields", {}).get("byline", "")
                r_first, r_last = ("", "")
                if byline:
                    parts = byline.strip().split()
                    if 2 <= len(parts) <= 3:
                        r_first = " ".join(parts[:-1])
                        r_last = parts[-1]

                pub_date = ""
                web_pub = item.get("webPublicationDate", "")
                if web_pub:
                    pub_date = web_pub[:10]

                review = {
                    "book_title": book["book_title"],
                    "book_author_first_name": book["book_author_first_name"],
                    "book_author_last_name": book["book_author_last_name"],
                    "reviewer_first_name": r_first,
                    "reviewer_last_name": r_last,
                    "publication_source": "The Guardian",
                    "publication_date": pub_date,
                    "review_link": g_url,
                    "review_summary": "",
                    "access_type": "Open",
                    "doi": "",
                    "entry_type": "review",
                    "symposium_group": "",
                }
                all_reviews.append(review)
                self.stats["results_verified"] += 1
                self.log(
                    f"  FOUND: {item.get('webTitle', '')[:70]}"
                    f"\n         → \"{book['book_title']}\" by {book['book_author_last_name']}"
                    f"\n         {g_url}"
                )

        # ── Google CSE per-book approach ────────────────────────
        if self.google and not guardian_only:
            state = load_state()
            completed_keys = set(state.get("books_completed", []))

            remaining = [b for b in candidates if b["key"] not in completed_keys]
            self.log(f"Google CSE: {len(remaining)} books remaining")

            if limit:
                remaining = remaining[:limit]
                self.log(f"Limited to {limit} books")

            for i, book in enumerate(remaining):
                title = book["book_title"]
                author = book["book_author_last_name"]
                self.log(
                    f"[{i + 1}/{len(remaining)}] "
                    f'Searching: "{title}" by {author} '
                    f"({book['review_count']} academic reviews)"
                )

                reviews = self.search_book(book, guardian_only=False)
                self.stats["books_searched"] += 1

                if reviews:
                    all_reviews.extend(reviews)
                    for r in reviews:
                        self.log(f"  FOUND: {r['publication_source']} — {r['review_link']}")

                # Update state after each book
                completed_keys.add(book["key"])
                state["books_completed"] = list(completed_keys)
                state["last_book_index"] = i
                state["last_run"] = datetime.now().isoformat()
                state["total_reviews_found"] = (
                    state.get("total_reviews_found", 0) + len(reviews)
                )
                save_state(state)

        self.log(f"\nScan complete: {len(all_reviews)} reviews found")

        if dry_run:
            self.log("Dry run — skipping database upload")
            self._print_results(all_reviews)
        else:
            if all_reviews:
                new_count = self._upload(all_reviews)
                self.log(f"Uploaded {new_count} new reviews")
            else:
                self.log("No new reviews to upload")

        self._print_stats()
        return self.stats

    def _upload(self, reviews):
        """Deduplicate and insert reviews into the database."""
        new_reviews = []
        for r in reviews:
            if not db.review_link_exists(r["review_link"]):
                new_reviews.append(r)
            else:
                self.stats["duplicates_skipped"] += 1

        if new_reviews:
            db.insert_reviews(new_reviews)
        self.stats["uploaded"] = len(new_reviews)
        return len(new_reviews)

    def _print_results(self, reviews):
        """Print found reviews for dry-run inspection."""
        if not reviews:
            return
        self.log(f"\nFound {len(reviews)} reviews:")
        for r in reviews:
            author = f"{r['book_author_first_name']} {r['book_author_last_name']}".strip()
            reviewer = f"{r['reviewer_first_name']} {r['reviewer_last_name']}".strip()
            print(
                f"  {r['book_title']}"
                f" | by {author}"
                f" | {r['publication_source']}"
                f" | reviewer: {reviewer or '?'}"
                f" | {r['publication_date'] or 'no date'}"
            )
            print(f"    {r['review_link']}")

    def _print_stats(self):
        """Print summary statistics."""
        print(f"\n{'─' * 50}")
        print("Mainstream Review Scraper Stats:")
        print(f"  Books searched:     {self.stats['books_searched']}")
        print(f"  Google queries:     {self.stats['google_queries']}")
        print(f"  Guardian queries:   {self.stats['guardian_queries']}")
        print(f"  Results checked:    {self.stats['results_checked']}")
        print(f"  Results verified:   {self.stats['results_verified']}")
        print(f"  Duplicates skipped: {self.stats['duplicates_skipped']}")
        print(f"  Uploaded:           {self.stats['uploaded']}")


def main():
    parser = argparse.ArgumentParser(
        description="Search mainstream media for philosophy book reviews"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Search but don't insert into database",
    )
    parser.add_argument(
        "--min-reviews", type=int, default=3,
        help="Minimum academic reviews for a book to be searched (default: 3)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Search only the first N books (for testing)",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show scan progress and exit",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Reset state file and start over",
    )
    parser.add_argument(
        "--guardian-only", action="store_true",
        help="Skip Google CSE, use Guardian API only",
    )
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.reset:
        reset_state()
        return

    # Load API keys from environment / .env
    from dotenv import load_dotenv
    load_dotenv()

    google_key = os.environ.get("GOOGLE_CSE_API_KEY")
    google_cx = os.environ.get("GOOGLE_CSE_CX")
    guardian_key = os.environ.get("GUARDIAN_API_KEY")

    if not args.guardian_only and (not google_key or not google_cx):
        print("Error: GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX must be set in .env")
        print("Use --guardian-only to skip Google CSE, or add keys to .env")
        return

    if not guardian_key:
        print("Warning: GUARDIAN_API_KEY not set — Guardian search disabled")

    scraper = MainstreamReviewScraper(
        google_api_key=google_key,
        google_cx=google_cx,
        guardian_api_key=guardian_key,
    )
    scraper.run(
        min_reviews=args.min_reviews,
        limit=args.limit,
        dry_run=args.dry_run,
        guardian_only=args.guardian_only,
    )


if __name__ == "__main__":
    main()
