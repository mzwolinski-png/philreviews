#!/usr/bin/env python3
"""Restore erased co-authors on multi-author books (Phase B, external evidence).

Many pipelines historically captured only the first author of multi-author
books (e.g. 'The Individualists' rows listing only Zwolinski, 'A Better Ape'
rows listing only Kumar). Phase A propagated fuller lists already present in
the DB; this pass handles books where EVERY row is under-attributed, by
checking OpenAlex.

Guards (all must hold before touching a row):
  - OpenAlex title match: normalized equality with our book_title
  - type in (book, monograph, reference-work)
  - our stored surname == the FIRST OpenAlex authorship's surname
  - OpenAlex lists 2+ authors (otherwise nothing to add)
Then all rows of that book get the full 'A, B and C' author list. Every change
is logged to scripts/coauthor_expansion_report.txt for review. Resumable via
scripts/coauthor_state.json.

Usage: python3 scripts/expand_coauthors.py [--min-reviews 3] [--limit N] [--dry-run]
"""
import argparse
import json
import os
import re
import sys
import time
import unicodedata

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import db

STATE = os.path.join(ROOT, "scripts", "coauthor_state.json")
REPORT = os.path.join(ROOT, "scripts", "coauthor_expansion_report.txt")
UA = {"User-Agent": "PhilReviews/2.0 (mailto:mzwolinski@sandiego.edu)"}
OA = "https://api.openalex.org/works"


def _norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _load_done():
    try:
        return set(json.load(open(STATE))["done"])
    except Exception:
        return set()


def _save_done(done):
    json.dump({"done": sorted(done)}, open(STATE, "w"))


def candidates(min_reviews):
    with db._connect() as conn:
        return conn.execute("""
            SELECT book_title, book_author_first_name, book_author_last_name, COUNT(*) n
            FROM reviews
            WHERE book_title != '' AND book_author_last_name != ''
              AND book_title NOT LIKE '%[%'
              AND book_author_first_name NOT LIKE '% and %'
              AND book_author_last_name NOT LIKE '% and %'
              AND book_author_first_name NOT LIKE '%,%'
            GROUP BY book_title, book_author_first_name, book_author_last_name
            HAVING COUNT(*) >= ?""", (min_reviews,)).fetchall()


def openlibrary_authors(title, surname, session):
    """Role-clean author list from Open Library, or None if no record."""
    try:
        r = session.get("https://openlibrary.org/search.json", params={
            "title": title, "author": surname, "limit": 3,
            "fields": "title,author_name"}, timeout=30)
        r.raise_for_status()
        docs = r.json().get("docs", [])
    except Exception:
        time.sleep(5)
        return None
    tnorm = _norm(title)
    for d in docs:
        dt = _norm(d.get("title", ""))
        if dt and (dt == tnorm or tnorm.startswith(dt) or dt.startswith(tnorm)):
            names = d.get("author_name") or []
            if names:
                return names
    return None


def openalex_authors(title, session):
    """-> list of display names for the best-matching book, or None."""
    try:
        r = session.get(OA, params={
            "filter": "type:book|monograph|reference-work",
            "search": title, "per-page": 5}, timeout=30)
        if r.status_code == 429:
            time.sleep(15)
            return None
        r.raise_for_status()
        results = r.json().get("results", [])
    except Exception:
        time.sleep(5)
        return None
    tnorm = _norm(title)
    for w in results:
        wt = _norm(w.get("title") or w.get("display_name") or "")
        # exact normalized match, or ours == their title-before-subtitle
        if wt == tnorm or (wt and len(wt) >= 8 and (tnorm.startswith(wt) or wt.startswith(tnorm))):
            names = [a.get("author", {}).get("display_name", "")
                     for a in w.get("authorships", [])]
            names = [n for n in names if n]
            if names:
                return names
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-reviews", type=int, default=3)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    done = _load_done()
    cands = [c for c in candidates(a.min_reviews) if f"{c[0]}|{c[2]}" not in done]
    if a.limit:
        cands = cands[:a.limit]
    print(f"candidates: {len(cands)}", flush=True)
    session = requests.Session(); session.headers.update(UA)
    rep = open(REPORT, "a")
    checked = expanded = 0
    for title, af, al, n in cands:
        key = f"{title}|{al}"
        checked += 1
        names = openalex_authors(title, session)
        done.add(key)
        if names and len(names) >= 2 and _norm(al) and _norm(names[0]).endswith(_norm(al)):
            cur = f"{af or ''} {al}".strip()
            joined = ", ".join(names[:-1]) + " and " + names[-1]
            if _norm(joined) == _norm(cur):
                continue
            # Verify against Open Library (role-clean: excludes editors, which
            # OpenAlex conflates — e.g. it lists Scheffler as OWM co-author).
            ol = openlibrary_authors(title, al, session)
            if ol is not None and len(ol) == 1:
                continue  # OL says single-author: OpenAlex extras are editors etc.
            agree = ol is not None and len(ol) >= 2 and \
                {_norm(x.split()[-1]) for x in ol} == {_norm(x.split()[-1]) for x in names}
            toks = joined.split()
            naf, nal = " ".join(toks[:-1]), toks[-1]
            if agree:
                if not a.dry_run:
                    with db._connect() as conn:
                        conn.execute(
                            "UPDATE reviews SET book_author_first_name=?, book_author_last_name=? "
                            "WHERE book_title=? AND book_author_last_name=? AND book_author_first_name=?",
                            (naf, nal, title, al, af))
                expanded += 1
                line = f"APPLIED ({n} rows): {cur!r} -> {joined!r} | {title[:60]}"
            else:
                line = f"PROPOSED ({n} rows): {cur!r} -> {joined!r} | {title[:60]} [OpenAlex only]"
            print("  " + line, flush=True)
            rep.write(line + "\n"); rep.flush()
        if checked % 50 == 0:
            _save_done(done)
            print(f"  ...{checked} checked, {expanded} expanded", flush=True)
        time.sleep(0.5)
    _save_done(done)
    rep.close()
    print(f"DONE: {checked} checked, {expanded} books expanded", flush=True)


if __name__ == "__main__":
    main()
