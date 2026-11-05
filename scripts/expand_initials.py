#!/usr/bin/env python3
"""Expand initials-only book-author first names ("P. Gomberg" -> "Paul Gomberg").

Some publishers deposit book authors by initials (Journal of Applied
Philosophy, Political Studies Review), and the scan copies them as given.
This step looks the book up in OpenAlex, then Open Library, and replaces an
author's initials only when a listed author has the same surname and given
names that start with those initials. Anything ambiguous is left alone.

Runs weekly on recently added rows. The historical archive is deliberately
out of scope: older journals printed names like "C. D. Broad", which is how
those authors are known.

Multi-author rows follow the site convention: first = "A B, C D and E",
last = "F" for authors "A B", "C D", "E F".

Usage:
  python3 scripts/expand_initials.py              # last 14 days
  python3 scripts/expand_initials.py --days 120 --dry-run
"""
import argparse
import os
import re
import sqlite3
import sys
import time
import unicodedata

import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DB = os.path.join(ROOT, 'reviews.db')
MAILTO = os.getenv('CROSSREF_EMAIL', 'mzwolinski@sandiego.edu')

_INITIALS = re.compile(r'^(?:[A-Z]\.\s?)+$')


def _fold(s):
    s = unicodedata.normalize('NFKD', s or '')
    return ''.join(ch for ch in s if not unicodedata.combining(ch)).lower().strip()


def split_authors(first, last):
    """Site convention back into [(given, surname), ...]."""
    first, last = (first or '').strip(), (last or '').strip()
    parts = re.split(r',\s*|\s+and\s+|\s*&\s*', first)
    parts = [p for p in parts if p]
    if not parts:
        return [('', last)]
    authors = []
    for p in parts[:-1]:
        bits = p.rsplit(' ', 1)
        # "L. Brunning": given "L.", surname "Brunning"
        authors.append((bits[0], bits[1]) if len(bits) == 2 else ('', bits[0]))
    authors.append((parts[-1], last))
    return authors


def join_authors(authors):
    """[(given, surname), ...] back into (first, last) by the site convention."""
    full = [f'{g} {s}'.strip() for g, s in authors[:-1]]
    g_last, s_last = authors[-1]
    if not full:
        return g_last, s_last
    if len(full) == 1:
        return f'{full[0]} and {g_last}'.strip(), s_last
    return f"{', '.join(full)} and {g_last}".strip(), s_last


def is_initials(given):
    return bool(given) and bool(_INITIALS.match(given.strip()))


def plausible_given(given):
    """A real spelled-out given name, not a lookup artifact.

    Rejects names that are still only initials ("P. A", "M.L.R"), catalogue
    debris ("P J ARISTOTLE translated with an introduction ... by") and
    OpenAlex truncations that end on a non-ASCII letter ("Bj\u00f8" for Bj\u00f8rn).
    """
    words = [w for w in re.split(r'[\s.]+', given) if w]
    if not words or len(words) > 4:
        return False
    if any(w[0].islower() or (len(w) > 1 and w.isupper()) for w in words):
        return False
    spelled = [w for w in words if len(w) > 1]
    if not spelled:
        return False
    return not any(not w[-1].isascii() and len(w) <= 4 for w in spelled)


def initials_fit(initials, given):
    """'P.' fits 'Paul'; 'K. P.' fits 'K. Paige'."""
    want = [c for c in initials if c.isalpha()]
    have = [w[0].upper() for w in re.split(r'[\s.\-]+', given) if w]
    return have[:len(want)] == want and plausible_given(given)


def _candidates(title, surnames):
    """Full author names listed for this book by OpenAlex, then Open Library."""
    names = []
    short = re.split(r'[:?]', title)[0].strip()
    try:
        r = requests.get('https://api.openalex.org/works', params={
            'search': short, 'filter': 'type:book', 'per-page': 5,
            'mailto': MAILTO}, timeout=20)
        for w in r.json().get('results', []):
            if _fold(short)[:40] not in _fold(w.get('display_name', '')):
                continue
            names += [a['author']['display_name'] for a in w.get('authorships', [])
                      if a.get('author', {}).get('display_name')]
    except Exception:
        pass
    if not names:
        try:
            r = requests.get('https://openlibrary.org/search.json', params={
                'title': short, 'author': surnames[0], 'limit': 5,
                'fields': 'title,author_name'}, timeout=20)
            for d in r.json().get('docs', []):
                if _fold(short)[:40] in _fold(d.get('title', '')):
                    names += d.get('author_name', [])
        except Exception:
            pass
    return names


def _known_by_initials(conn, initials, surname, given, source):
    """Other journals in the index mostly write this author as initials.

    J. P. Moreland and A. A. Long publish under their initials; expanding them
    is accurate but reads wrong. Our own index is the evidence for usage.
    """
    if conn is None:
        return False
    norm = re.sub(r'[\s.]+', '', initials)
    full = inits = 0
    for first, in conn.execute(
            "SELECT book_author_first_name FROM reviews WHERE book_author_last_name = ? "
            "AND publication_source != ?", (surname, source)):
        last_given = re.split(r',\s*|\s+and\s+', first or '')[-1].strip()
        if not last_given:
            continue
        if re.sub(r'[\s.]+', '', last_given) == norm:
            inits += 1
        elif last_given.split()[0] == given.split()[0]:
            full += 1
    return inits > full


def expand_row(title, first, last, lookup=_candidates, conn=None, source=''):
    """Return (new_first, new_last) or None when nothing can be safely expanded."""
    authors = split_authors(first, last)
    todo = [i for i, (g, s) in enumerate(authors) if is_initials(g) and s]
    if not todo:
        return None
    names = lookup(title, [authors[i][1] for i in todo])
    changed = False
    for i in todo:
        g, s = authors[i]
        fits = set()
        for n in names:
            bits = n.strip().rsplit(' ', 1)
            if len(bits) == 2 and _fold(bits[1]) == _fold(s) and initials_fit(g, bits[0]):
                fits.add(bits[0].strip())
        if len(fits) == 1:
            full = fits.pop()
            if _known_by_initials(conn, g, s, full, source):
                continue
            authors[i] = (full, s)
            changed = True
    return join_authors(authors) if changed else None


def expand_recent(days=14, dry_run=False):
    conn = sqlite3.connect(DB, timeout=30)
    rows = conn.execute(
        "SELECT id, book_title, book_author_first_name, book_author_last_name, "
        "publication_source FROM reviews "
        "WHERE created_at >= datetime('now', ?) AND book_author_first_name GLOB '*[A-Z].*' "
        # Older reviews name authors as they were then known (A. A. Long,
        # R. C. Zaehner); expanding those reads wrong even when accurate.
        "AND publication_date >= '2015'",
        (f'-{days} days',)).fetchall()
    checked, details = 0, []
    for rid, title, first, last, src in rows:
        if not any(is_initials(g) for g, _ in split_authors(first, last)):
            continue
        checked += 1
        new = expand_row(title or '', first, last, conn=conn, source=src)
        if new and new != (first, last):
            details.append(f"[{rid}] {src}: {first} {last} -> {new[0]} {new[1]} ({title[:50]})")
            if not dry_run:
                conn.execute("UPDATE reviews SET book_author_first_name = ?, "
                             "book_author_last_name = ? WHERE id = ?", (new[0], new[1], rid))
        time.sleep(0.2)
    if not dry_run:
        conn.commit()
    conn.close()
    return {'checked': checked, 'updated': len(details), 'details': details}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=14)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    res = expand_recent(a.days, a.dry_run)
    print(f"checked {res['checked']}, expanded {res['updated']}")
    for d in res['details']:
        print(' ', d)
