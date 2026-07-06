#!/usr/bin/env python3
"""Ingest Guardian PHILOSOPHY book reviews via the open Content API.

The Guardian Content API exposes its entire book-review archive for free
(section=books, tag=tone/reviews, ~35k items, full structured metadata).
The existing cross-reference scraper downloads this archive but keeps only
reviews of books already in the DB. This pass instead runs each candidate
through the same Haiku relevance gate used by the Atlantic scraper, so
Guardian-reviewed philosophy books NOT yet tracked are captured too.

Usage:
  python3 guardian_backfill.py --since 2025-01-01 --max-pages 1 --dry-run  # pilot
  python3 guardian_backfill.py --since 2024-01-01                          # ingest a window
  python3 guardian_backfill.py                                             # full archive
"""
import argparse
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

import db
from atlantic_scraper import relevance_gate

API_URL = "https://content.guardianapis.com/search"
UA = "PhilReviews/2.0 (mailto:mzwolinski@sandiego.edu)"
SOURCE = "The Guardian"

# Guardian genre tags that never contain academic philosophy — skip before the
# (expensive) relevance gate to avoid wasting calls on fiction/poetry/etc.
SKIP_TAGS = {
    "books/fiction", "books/crime", "books/sciencefiction-and-fantasy",
    "books/poetry", "books/thrillers", "books/romance", "books/horror",
    "books/short-stories", "books/comics-and-graphic-novels",
    "books/childrens-books-site", "books/childrensuserreviews",
    "books/crime-fiction", "books/fantasy", "books/audiobooks",
    "books/cookbooks", "books/food-and-drink", "books/gardening-books",
    "books/sportandleisure", "books/health-mind-and-body",
}


def fetch_reviews(api_key, from_date=None, to_date=None, max_pages=None):
    """Page the Content API for book reviews (newest first)."""
    out, page, total_pages, errors = [], 1, None, 0
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})
    while True:
        params = {
            "api-key": api_key, "section": "books", "tag": "tone/reviews",
            "show-fields": "byline,trailText", "show-tags": "keyword",
            "page-size": 200, "page": page, "order-by": "newest",
        }
        if from_date:
            params["from-date"] = from_date
        if to_date:
            params["to-date"] = to_date
        try:
            r = sess.get(API_URL, params=params, timeout=30)
            if r.status_code == 429:
                # Daily quota (500/day, resets midnight UTC) may be exhausted —
                # e.g. by the mainstream scan earlier in the same weekly run.
                # Cap retries: on 2026-07-05 an unbounded retry loop here stalled
                # the whole weekly update for 9.5 hours until the quota reset.
                errors += 1
                if errors > 8:
                    print(f"  Guardian API rate-limited persistently — giving up "
                          f"with {len(out)} reviews fetched")
                    break
                time.sleep(20); continue
            r.raise_for_status()
            resp = r.json().get("response", {})
            errors = 0
        except requests.RequestException as e:
            errors += 1
            if errors > 8:
                print(f"  Guardian API failing persistently ({e}) — giving up "
                      f"with {len(out)} reviews fetched")
                break
            print(f"  api error page {page}: {e}"); time.sleep(5); continue
        items = resp.get("results", [])
        if total_pages is None:
            total_pages = resp.get("pages", 1)
            print(f"  Guardian: {resp.get('total', 0)} reviews across {total_pages} pages"
                  + (f" (capping at {max_pages})" if max_pages else ""))
        out.extend(items)
        if page >= total_pages or (max_pages and page >= max_pages):
            break
        page += 1
        time.sleep(0.25)
    return out


def _split_name(full):
    """First listed person -> (first, last)."""
    full = re.split(r"\s+and\s+|;|,\s+", (full or "").strip())[0].strip()
    full = re.sub(r"^by\s+", "", full, flags=re.I).strip()
    p = full.split()
    return (" ".join(p[:-1]), p[-1]) if len(p) > 1 else ("", full)


def parse_headline(title):
    """Guardian review headline -> (book_title, author_first, author_last) or None.
    Handles 'Book Title by Author review – subtitle' and 'edited by'."""
    t = re.sub(r"\s*[–—-]\s*review\b", " review", title, flags=re.I)
    m = re.match(r"^(.*?)\s+(?:by|edited by)\s+(.+?)\s+review\b", t, re.I)
    if not m:
        return None
    book = m.group(1).strip(" '\"‘’“”")
    af, al = _split_name(m.group(2))
    if len(book) < 3 or not al or "review" in book.lower():
        return None
    return book, af, al


def run(api_key, since=None, dry_run=False, max_pages=None):
    reviews = fetch_reviews(api_key, from_date=since, max_pages=max_pages)
    st = {"fetched": len(reviews), "parsed": 0, "fiction_skipped": 0,
          "already_in_db": 0, "gate_checked": 0, "gate_pass": 0,
          "inserted": 0, "samples": []}
    to_add, seen = [], set()
    for it in reviews:
        url = it.get("webUrl", "")
        title = it.get("webTitle", "")
        fields = it.get("fields", {}) or {}
        trail = fields.get("trailText", "") or ""
        byline = fields.get("byline", "") or ""
        date = (it.get("webPublicationDate", "") or "")[:10]
        parsed = parse_headline(title)
        if not parsed or not url:
            continue
        st["parsed"] += 1
        tag_ids = {t.get("id", "") for t in (it.get("tags") or [])}
        if tag_ids & SKIP_TAGS:  # fiction/poetry/etc. — never philosophy
            st["fiction_skipped"] += 1
            continue
        book, af, al = parsed
        if url in seen or db.review_link_exists(url):
            st["already_in_db"] += 1; continue
        seen.add(url)
        author_display = (af + " " + al).strip()
        st["gate_checked"] += 1
        relevant, prim, sec = relevance_gate(book, author_display, trail)
        if not relevant:
            continue
        st["gate_pass"] += 1
        rf, rl = _split_name(byline)
        to_add.append({
            "book_title": book, "book_author_first_name": af, "book_author_last_name": al,
            "reviewer_first_name": rf, "reviewer_last_name": rl,
            "publication_source": SOURCE, "publication_date": date, "review_link": url,
            "access_type": "Open", "entry_type": "review",
            "subfield_primary": prim, "subfield_secondary": sec,
        })
        if len(st["samples"]) < 18:
            st["samples"].append(f"{book} — {author_display} (rev. {rf} {rl}) [{date}] {prim or '?'}")
        # Flush periodically so a long run is crash-safe / resumable (dedup on
        # re-run skips what's already inserted).
        if not dry_run and len(to_add) >= 25:
            db.insert_reviews(to_add)
            st["inserted"] += len(to_add)
            to_add = []
    if to_add and not dry_run:
        db.insert_reviews(to_add)
        st["inserted"] += len(to_add)
    elif dry_run:
        st["inserted"] = f"(dry-run: {len(to_add)} would insert)"
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="from-date YYYY-MM-DD")
    ap.add_argument("--max-pages", type=int, help="cap pages (200 reviews each)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    key = os.environ.get("GUARDIAN_API_KEY")
    if not key:
        print("GUARDIAN_API_KEY not set"); sys.exit(1)
    st = run(key, since=a.since, dry_run=a.dry_run, max_pages=a.max_pages)
    print("\n=== Guardian philosophy backfill ===")
    for k in ("fetched", "parsed", "fiction_skipped", "already_in_db", "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {st[k]}")
    print("samples (gate-passed):")
    for s in st["samples"]:
        print("  +", s)
