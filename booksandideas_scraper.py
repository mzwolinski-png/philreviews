#!/usr/bin/env python3
"""Ingest Books & Ideas (English sister of La Vie des idées) philosophy reviews.

booksandideas.net is the English edition of laviedesidees.fr (same SPIP site
structure: paginated Philosophie rubric, "About: Author, Title, Publisher"
meta-description head on recensions, reviewer in meta author). ~82% of its
recensions are translations of LVI reviews already in the DB, so the policy is
PREFER ENGLISH: an overlapping review (matched by book-author + reviewer last
names against existing La Vie des idées rows) has its row upgraded in place to
the English link/source; only genuinely new recensions are gated and inserted.

Usage:
  python3 booksandideas_scraper.py --max-offset 40 --dry-run   # pilot
  python3 booksandideas_scraper.py                             # full rubric
"""
import argparse
import html as htmllib
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

BASE = "https://booksandideas.net"
RUBRIC = "/+-Philosophie-+"
SOURCE = "Books & Ideas"
LVI_SOURCE = "La Vie des idées"
UA = "PhilReviews/2.0 (+https://philreviews.org; book-review index; mailto:mzwolinski@sandiego.edu)"
DELAY = 1.0

MONTHS = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
          "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
          "december": 12}
_DATE_DM = re.compile(r'class="date">[^<0-9]*(\d{1,2})\s+(' + "|".join(MONTHS) + r")", re.I)
_ABOUT = re.compile(r"About\s*:?\s*(.+?)\s+-\s+(.*)", re.S | re.I)


def _get(session, url, tries=3):
    for i in range(tries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if i == tries - 1:
                print(f"  fetch failed {url}: {e}"); return None
            time.sleep(3 * (i + 1))
    return None


def philosophy_article_urls(session, max_offset=None):
    urls, offset = [], 0
    while True:
        html = _get(session, f"{BASE}{RUBRIC}?debut_article_flow={offset}") or ""
        found = [u for u in dict.fromkeys(
            re.findall(r'href="(https://booksandideas\.net/[A-Z][^"]+)"', html))
            if "laviedesidees" not in u]
        new = [u for u in found if u not in urls]
        urls += new
        if max_offset is not None and offset >= max_offset:
            break
        if not new and offset > 0:
            break
        offset += 40
        time.sleep(DELAY)
    return list(dict.fromkeys(urls))


def parse_recension(html, url):
    desc = re.search(r'<meta name="description" content="([^"]*)"', html)
    if not desc:
        return None
    ab = _ABOUT.search(htmllib.unescape(desc.group(1)))
    if not ab:
        return None  # essay/interview
    cite = re.sub(r"\s+", " ", ab.group(1)).strip(" .,")
    intro = re.sub(r"\s+", " ", ab.group(2)).strip()
    parts = [p.strip() for p in cite.split(",") if p.strip()]
    if len(parts) < 2:
        return None
    author, book_title = parts[0], parts[1]
    first_auth = re.split(r"\s+et\s+|\s+and\s+|\s*&\s*", author)[0].strip()
    toks = first_auth.split()
    af, al = (" ".join(toks[:-1]), toks[-1]) if len(toks) > 1 else ("", first_auth)

    rev = re.search(r'<meta name="author" content="([^"]+)"', html)
    rf, rl = "", ""
    if rev:
        rp = htmllib.unescape(rev.group(1)).split()
        rf, rl = (" ".join(rp[:-1]), rp[-1]) if len(rp) > 1 else ("", rev.group(1))

    date = ""
    dm = _DATE_DM.search(html)
    if dm:
        day, mon = dm.group(1), dm.group(2)
        ym = re.search(rf"\b{day}\s+{re.escape(mon)}\s+(\d{{4}})", html, re.I)
        if ym:
            date = f"{ym.group(1)}-{MONTHS[mon.lower()]:02d}-{int(day):02d}"

    return {"book_title": book_title, "author_first": af, "author_last": al,
            "rev_first": rf, "rev_last": rl, "date": date, "link": url,
            "summary": intro[:600]}


def run(dry_run=False, max_offset=None):
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    st = {"articles_found": 0, "recensions": 0, "already_in_db": 0,
          "upgraded_to_english": 0, "gate_checked": 0, "gate_pass": 0,
          "inserted": 0, "samples": []}

    articles = philosophy_article_urls(session, max_offset=max_offset)
    st["articles_found"] = len(articles)
    print(f"  Books & Ideas: {len(articles)} philosophy articles discovered")
    to_add = []
    for url in articles:
        if db.review_link_exists(url):
            st["already_in_db"] += 1; continue
        time.sleep(DELAY)
        html = _get(session, url)
        if not html:
            continue
        rec = parse_recension(html, url)
        if not rec:
            continue
        st["recensions"] += 1
        # Overlap: same book-author + reviewer already present as an LVI row ->
        # upgrade that row to the English version (prefer-English policy).
        with db._connect() as conn:
            row = conn.execute(
                "SELECT id FROM reviews WHERE publication_source=? AND "
                "book_author_last_name=? AND reviewer_last_name=?",
                (LVI_SOURCE, rec["author_last"], rec["rev_last"])).fetchone()
            if row:
                if not dry_run:
                    conn.execute(
                        "UPDATE reviews SET review_link=?, publication_source=? WHERE id=?",
                        (url, SOURCE, row[0]))
                st["upgraded_to_english"] += 1
                continue
        author_disp = (rec["author_first"] + " " + rec["author_last"]).strip()
        st["gate_checked"] += 1
        relevant, prim, sec = relevance_gate(rec["book_title"], author_disp, rec["summary"])
        if not relevant:
            continue
        st["gate_pass"] += 1
        to_add.append({
            "book_title": rec["book_title"],
            "book_author_first_name": rec["author_first"], "book_author_last_name": rec["author_last"],
            "reviewer_first_name": rec["rev_first"], "reviewer_last_name": rec["rev_last"],
            "publication_source": SOURCE, "publication_date": rec["date"],
            "review_link": url, "review_summary": rec["summary"],
            "access_type": "Open", "entry_type": "review",
            "subfield_primary": prim, "subfield_secondary": sec,
        })
        if len(st["samples"]) < 20:
            st["samples"].append(f"{rec['book_title'][:44]} — {author_disp} (rev. {rec['rev_first']} {rec['rev_last']}) [{rec['date']}] {prim or '?'}")
        if not dry_run and len(to_add) >= 25:
            db.insert_reviews(to_add); st["inserted"] += len(to_add); to_add = []
    if to_add and not dry_run:
        db.insert_reviews(to_add); st["inserted"] += len(to_add)
    elif dry_run:
        st["inserted"] = f"(dry-run: {len(to_add)} would insert)"
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-offset", type=int, help="cap rubric pagination offset (pilot)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    st = run(dry_run=a.dry_run, max_offset=a.max_offset)
    print("\n=== Books & Ideas philosophy backfill ===")
    for k in ("articles_found", "recensions", "already_in_db", "upgraded_to_english",
              "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {st[k]}")
    print("samples (net-new, gate-passed):")
    for s in st["samples"]:
        print("  +", s)
