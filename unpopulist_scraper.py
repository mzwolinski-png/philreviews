#!/usr/bin/env python3
"""
Scraper for The Unpopulist book reviews.

Uses the Substack API to find posts tagged "book-review", then extracts
book metadata (title, author) from the article body using Haiku.

Low-volume source (~1-2 reviews/month), so LLM extraction is practical.
"""

import json
import os
import re
import time

import requests

from scraper_base import BaseScraper

ARCHIVE_URL = "https://www.theunpopulist.net/api/v1/archive"
POST_URL = "https://www.theunpopulist.net/api/v1/posts/{slug}"
SOURCE_NAME = "The Unpopulist"
BASE_URL = "https://www.theunpopulist.net/p/"


def fetch_book_review_posts():
    """Fetch recent posts tagged 'book-review' from the Substack API.

    The unauthenticated Substack API returns ~20-25 most recent posts.
    Since The Unpopulist publishes 3-5 posts/week and ~1-2 book reviews/month,
    any new book review will be within this window for weekly checks.
    """
    resp = requests.get(ARCHIVE_URL, params={
        "sort": "new", "limit": 50, "offset": 0,
    }, timeout=30)
    resp.raise_for_status()
    batch = resp.json()
    posts = []
    for p in batch:
        tags = [t.get("slug", "") for t in (p.get("postTags") or [])]
        if "book-review" in tags:
            posts.append(p)
    return posts


def fetch_post_body(slug):
    """Fetch the full body text of a single post."""
    resp = requests.get(POST_URL.format(slug=slug), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    html = data.get("body_html") or ""
    # Strip HTML tags to get plain text
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_books_with_haiku(title, reviewer, body_text):
    """Use Haiku to extract book title(s) and author(s) from the review body."""
    from anthropic import Anthropic

    client = Anthropic()
    prompt = f"""This is a book review article titled "{title}" by {reviewer}.

Extract the book(s) being reviewed. For each book, give:
- book_title: the full title of the book
- book_author: the author(s) of the book (not the reviewer)

Return ONLY valid JSON: a list of objects, e.g.:
[{{"book_title": "Example Title", "book_author": "First Last"}}]

If the review covers multiple books, include all of them.

Article text (first 2000 chars):
{body_text[:2000]}"""

    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text.strip()
    # Extract JSON from response (handle markdown code blocks)
    json_match = re.search(r"\[.*\]", text, re.DOTALL)
    if json_match:
        return json.loads(json_match.group())
    return []


class UnpopulistScraper(BaseScraper):
    name = "unpopulist"
    default_delay = 1.0

    def __init__(self):
        super().__init__()
        self.stats = {"found": 0, "new": 0, "skipped": 0}

    def run(self, dry_run=False):
        import db

        self.log.info("Fetching Unpopulist book reviews...")
        posts = fetch_book_review_posts()
        self.stats["found"] = len(posts)
        self.log.info(f"Found {len(posts)} book review posts")

        new_posts = []
        for p in posts:
            slug = p.get("slug", "")
            url = BASE_URL + slug
            if db.review_link_exists(url):
                self.stats["skipped"] += 1
                continue
            # Also check with #book2 fragment (multi-book reviews)
            if db.review_link_exists(url + "#book2"):
                pass  # URL exists but there might be a primary entry too
            new_posts.append(p)

        if not new_posts:
            self.log.info("No new Unpopulist book reviews")
            return self.stats

        self.log.info(f"{len(new_posts)} new posts to process")

        for p in new_posts:
            slug = p.get("slug", "")
            url = BASE_URL + slug
            title = p.get("title", "")
            bylines = p.get("publishedBylines") or []
            reviewer = bylines[0].get("name", "") if bylines else ""
            date = (p.get("post_date") or "")[:10]

            try:
                body = fetch_post_body(slug)
                time.sleep(1)  # Rate limit
            except Exception:
                self.log.warning(f"Failed to fetch body for {slug}")
                continue

            try:
                books = extract_books_with_haiku(title, reviewer, body)
            except Exception:
                self.log.warning(f"Failed to extract books from {slug}")
                continue

            if not books:
                self.log.warning(f"No books extracted from: {title}")
                continue

            for i, book in enumerate(books):
                book_title = book.get("book_title", "").strip()
                book_author = book.get("book_author", "").strip()
                if not book_title:
                    continue

                # Split author into first/last
                parts = book_author.rsplit(" ", 1)
                if len(parts) == 2:
                    first, last = parts
                else:
                    first, last = "", book_author

                link = url if i == 0 else f"{url}#book{i + 1}"

                if db.review_link_exists(link):
                    continue

                # Split reviewer into first/last
                rev_parts = reviewer.rsplit(" ", 1)
                rev_first = rev_parts[0] if len(rev_parts) == 2 else ""
                rev_last = rev_parts[-1]

                record = {
                    "book_title": book_title,
                    "book_author_first_name": first,
                    "book_author_last_name": last,
                    "reviewer_first_name": rev_first,
                    "reviewer_last_name": rev_last,
                    "publication_source": SOURCE_NAME,
                    "publication_date": date,
                    "review_link": link,
                    "access_type": "open",
                    "entry_type": "review",
                    "subfield_primary": "political",
                }

                if not dry_run:
                    db.insert_review(record)

                self.stats["new"] += 1
                self.log.info(f"  + {book_title[:50]} by {book_author} (rev: {reviewer})")

        return self.stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    scraper = UnpopulistScraper()
    stats = scraper.run()
    print(f"\nStats: {stats}")
