#!/usr/bin/env python3
"""Ingest NYT PHILOSOPHY book reviews via the Article Search API.

The cross-reference scraper queries NYT only for books already in the DB.
This pulls NYT's book-review stream (Article Search, section=Books,
type=Review) and runs each through the Atlantic scraper's Haiku relevance
gate, capturing philosophy books not already tracked. Book authors are left
blank where NYT doesn't expose them cleanly — the weekly OpenAlex pass
(retry_missing_book_authors) fills them in.

Usage:
  python3 nyt_backfill.py --max-pages 2 --dry-run        # pilot (newest ~20)
  python3 nyt_backfill.py --begin 20240101               # ingest a window
  python3 nyt_backfill.py --years 2026 2025 2024         # backfill by year
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

API_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
FQ = 'typeOfMaterials:Review AND section.name:Books'
UA = "PhilReviews/2.0 (mailto:mzwolinski@sandiego.edu)"
PAGE_DELAY = 7  # NYT free tier ~5-10 req/min


def fetch_reviews(api_key, begin_date=None, end_date=None, max_pages=100):
    """Page Article Search (max 100 pages = 1000 docs per query window)."""
    out, page = [], 0
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})
    while page < min(max_pages, 100):
        params = {"api-key": api_key, "fq": FQ, "sort": "newest", "page": page}
        if begin_date:
            params["begin_date"] = begin_date
        if end_date:
            params["end_date"] = end_date
        try:
            r = sess.get(API_URL, params=params, timeout=30)
            if r.status_code in (429, 403):
                time.sleep(30); continue
            r.raise_for_status()
            resp = r.json().get("response", {})
        except requests.RequestException as e:
            print(f"  nyt api error page {page}: {e}"); time.sleep(10); continue
        docs = resp.get("docs") or []
        if page == 0:
            print(f"  NYT: {resp.get('metadata', {}).get('hits', 0)} review hits in window")
        if not docs:
            break
        out.extend(docs)
        page += 1
        time.sleep(PAGE_DELAY)
    return out


def _split_name(full):
    full = re.sub(r"^by\s+", "", (full or "").strip(), flags=re.I)
    full = re.split(r"\s+and\s+|;|,\s+and\s+", full)[0].strip()
    p = full.split()
    return (" ".join(p[:-1]), p[-1]) if len(p) > 1 else ("", full)


def _book_title(doc):
    """Reviewed-book title from NYT 'Title' (creative_works) keyword.
    NYT appends ' (Book)' to creative-work values — strip it."""
    for k in doc.get("keywords", []):
        if k.get("name") in ("Title", "creative_works") and k.get("value"):
            v = re.sub(r"\s*\(Book\)\s*$", "", k["value"].strip())
            return v.strip()
    return None


def run(api_key, begin_date=None, end_date=None, dry_run=False, max_pages=100):
    reviews = fetch_reviews(api_key, begin_date, end_date, max_pages)
    st = {"fetched": len(reviews), "parsed": 0, "already_in_db": 0,
          "gate_checked": 0, "gate_pass": 0, "inserted": 0, "samples": []}
    to_add, seen = [], set()
    for doc in reviews:
        url = doc.get("web_url", "")
        title = _book_title(doc)
        if not url or not title or len(title) < 3:
            continue
        st["parsed"] += 1
        if url in seen or db.review_link_exists(url):
            st["already_in_db"] += 1; continue
        seen.add(url)
        abstract = (doc.get("abstract") or doc.get("snippet") or "")
        date = (doc.get("pub_date") or "")[:10]
        rf, rl = _split_name(doc.get("byline", {}).get("original", ""))
        desk = doc.get("news_desk", "")
        source = "The New York Times Book Review" if desk == "BookReview" else "The New York Times"
        st["gate_checked"] += 1
        relevant, prim, sec = relevance_gate(title, "", abstract)
        if not relevant:
            continue
        st["gate_pass"] += 1
        to_add.append({
            "book_title": title, "book_author_first_name": "", "book_author_last_name": "",
            "reviewer_first_name": rf, "reviewer_last_name": rl,
            "publication_source": source, "publication_date": date, "review_link": url,
            "access_type": "Restricted", "entry_type": "review",
            "subfield_primary": prim, "subfield_secondary": sec,
        })
        if len(st["samples"]) < 18:
            st["samples"].append(f"{title} (rev. {rf} {rl}) [{date}] {prim or '?'} <{desk}>")
        if not dry_run and len(to_add) >= 25:
            db.insert_reviews(to_add); st["inserted"] += len(to_add); to_add = []
    if to_add and not dry_run:
        db.insert_reviews(to_add); st["inserted"] += len(to_add)
    elif dry_run:
        st["inserted"] = f"(dry-run: {len(to_add)} would insert)"
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--begin", help="begin_date YYYYMMDD")
    ap.add_argument("--end", help="end_date YYYYMMDD")
    ap.add_argument("--years", nargs="+", type=int, help="backfill these years (one window each)")
    ap.add_argument("--max-pages", type=int, default=100)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    key = os.environ.get("NYT_API_KEY")
    if not key:
        print("NYT_API_KEY not set"); sys.exit(1)
    windows = ([(f"{y}0101", f"{y}1231") for y in a.years]
               if a.years else [(a.begin, a.end)])
    agg = {"fetched": 0, "parsed": 0, "already_in_db": 0, "gate_checked": 0,
           "gate_pass": 0, "inserted": 0, "samples": []}
    for begin, end in windows:
        st = run(key, begin_date=begin, end_date=end, dry_run=a.dry_run, max_pages=a.max_pages)
        for k in ("fetched", "parsed", "already_in_db", "gate_checked", "gate_pass"):
            agg[k] += st[k]
        if isinstance(st["inserted"], int):
            agg["inserted"] += st["inserted"]
        agg["samples"] = (agg["samples"] + st["samples"])[:18]
    print("\n=== NYT philosophy backfill ===")
    for k in ("fetched", "parsed", "already_in_db", "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {agg[k]}")
    print("samples (gate-passed):")
    for s in agg["samples"]:
        print("  +", s)
