#!/usr/bin/env python3
"""Ingest French PHILOSOPHY book reviews from La Vie des idées.

La Vie des idées (laviedesidees.fr) is a free, curated French intellectual
review (Collège de France) with a dedicated Philosophie section. Its
"recensions" are book reviews; each page carries the reviewer (<meta author>),
an "À propos de : Author, Title, Publisher" citation of the reviewed book, and
a French date. Denser in philosophy than OpenEdition Lectures and non-academic
(no Crossref overlap). We discover via the paginated Philosophie rubric, keep
only recensions, and gate each (the gate handles French).

Usage:
  python3 laviedesidees_scraper.py --max-offset 40 --dry-run   # pilot (one page)
  python3 laviedesidees_scraper.py                             # full Philosophie backfill
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

BASE = "https://laviedesidees.fr"
RUBRIC = "/+-Philosophie-+"
SOURCE = "La Vie des idées"
UA = "PhilReviews/2.0 (+https://philreviews.org; book-review index; mailto:mzwolinski@sandiego.edu)"
DELAY = 1.0  # robots.txt Crawl-delay: 1

MOIS = {"janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
        "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
        "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12}
_MOISALT = "|".join(MOIS)
# Publication day+month from the styled date element ("...le 29 juin"); the year
# is looked up separately so we don't grab a historical date from the body.
_DATE_DM = re.compile(r'class="date">[^<0-9]*(\d{1,2})\s+(' + _MOISALT + r")", re.I)
# The reviewed-book citation is the head of the meta description:
# "À propos de : Author, Title, Publisher - <intro>"
_APROPOS = re.compile(r"propos de\s*:?\s*(.+?)\s+-\s+(.*)", re.S | re.I)


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
    """Walk the paginated Philosophie rubric, collecting article URLs."""
    urls, offset = [], 0
    seen_pages = set()
    while True:
        html = _get(session, f"{BASE}{RUBRIC}?debut_article_flow={offset}") or ""
        found = [u for u in dict.fromkeys(
            re.findall(r'href="(https://laviedesidees\.fr/[A-Z][^"]+)"', html))
            if "booksandideas" not in u]
        new = [u for u in found if u not in urls]
        urls += new
        if max_offset is not None and offset >= max_offset:
            break
        if not new and offset > 0:  # ran off the end
            break
        offset += 40
        if offset in seen_pages:
            break
        seen_pages.add(offset)
        time.sleep(DELAY)
    return list(dict.fromkeys(urls))


def parse_recension(html, url):
    """-> dict or None (None if it's not a book review)."""
    desc = re.search(r'<meta name="description" content="([^"]*)"', html)
    if not desc:
        return None
    content = htmllib.unescape(desc.group(1))
    ap = _APROPOS.search(content)  # essais lack the "À propos de :" head
    if not ap:
        return None
    cite = re.sub(r"\s+", " ", ap.group(1)).strip(" .,")
    intro = re.sub(r"\s+", " ", ap.group(2)).strip()
    # "Author, Title, Publisher" -> author = 1st segment, title = 2nd
    parts = [p.strip() for p in cite.split(",") if p.strip()]
    if len(parts) < 2:
        return None
    author, book_title = parts[0], parts[1]
    ap_toks = author.split()
    af, al = (" ".join(ap_toks[:-1]), ap_toks[-1]) if len(ap_toks) > 1 else ("", author)

    rev = re.search(r'<meta name="author" content="([^"]+)"', html)
    rf, rl = "", ""
    if rev:
        rp = htmllib.unescape(rev.group(1)).split()
        rf, rl = (" ".join(rp[:-1]), rp[-1]) if len(rp) > 1 else ("", rev.group(1))

    # Publication date: day+month from the date element, year from the matching
    # "DD month YYYY" (avoids historical dates elsewhere in the page).
    date = ""
    dm = _DATE_DM.search(html)
    if dm:
        day, mon = dm.group(1), dm.group(2)
        ym = re.search(rf"\b{day}\s+{re.escape(mon)}\s+(\d{{4}})", html, re.I)
        if ym:
            date = f"{ym.group(1)}-{MOIS[mon.lower()]:02d}-{int(day):02d}"

    return {"book_title": book_title, "author_first": af, "author_last": al,
            "rev_first": rf, "rev_last": rl, "date": date, "link": url, "summary": intro[:600]}


def run(dry_run=False, max_offset=None):
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    st = {"articles_found": 0, "recensions": 0, "already_in_db": 0,
          "gate_checked": 0, "gate_pass": 0, "inserted": 0, "samples": []}

    articles = philosophy_article_urls(session, max_offset=max_offset)
    st["articles_found"] = len(articles)
    print(f"  La Vie des idées: {len(articles)} philosophy articles discovered")
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
            continue  # not a book review
        st["recensions"] += 1
        # Prefer-English policy: if this review was upgraded to its Books &
        # Ideas English version, its row no longer carries the LVI link — so
        # link dedup misses it and we'd re-insert the French original.
        with db._connect() as conn:
            if conn.execute(
                    "SELECT 1 FROM reviews WHERE publication_source='Books & Ideas' "
                    "AND book_author_last_name=? AND reviewer_last_name=?",
                    (rec["author_last"], rec["rev_last"])).fetchone():
                st["already_in_db"] += 1
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
    print("\n=== La Vie des idées philosophy backfill ===")
    for k in ("articles_found", "recensions", "already_in_db", "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {st[k]}")
    print("samples (gate-passed):")
    for s in st["samples"]:
        print("  +", s)
