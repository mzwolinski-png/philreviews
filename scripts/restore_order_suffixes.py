#!/usr/bin/env python3
"""Restore religious-order suffixes (O.P., S.J., ...) stripped 2026-06-21.

Convention (user decision 2026-07-06): order suffixes are kept, canonically
"Lastname, O.P.". The June integrity pass stripped ~180 of them from author/
reviewer fields; the row list wasn't preserved, so this re-fetches Crossref
metadata for DOI rows in the order-heavy journals and re-appends a suffix
wherever the raw metadata shows one adjacent to the stored last name.

Idempotent + resumable (skips rows whose names already end in a suffix; state
cursor in scripts/order_suffix_state.json). Run: python3 scripts/restore_order_suffixes.py
"""
import json
import os
import re
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import db

JOURNALS = (
    "The Thomist: A Speculative Quarterly Review",
    "The Heythrop Journal",
    "New Blackfriars",
    "American Catholic Philosophical Quarterly",
)
SUFFIXES = ("O.P.", "S.J.", "O.S.B.", "O.F.M.", "O.F.M. Cap.", "O.Carm.",
            "O.S.A.", "C.S.C.", "S.T.D.", "O.M.I.", "C.SS.R.", "S.D.B.")
_SUF_RE = "|".join(re.escape(s) for s in sorted(SUFFIXES, key=len, reverse=True))
STATE = os.path.join(ROOT, "scripts", "order_suffix_state.json")
UA = {"User-Agent": "PhilReviews/2.0 (mailto:mzwolinski@sandiego.edu)"}


def _load():
    try:
        return json.load(open(STATE))["done_ids"]
    except Exception:
        return []


def _save(done):
    json.dump({"done_ids": done}, open(STATE, "w"))


def _find_suffix_after(name_last, blob):
    """Suffix token that follows `name_last` in raw metadata text, or None."""
    if not name_last:
        return None
    m = re.search(re.escape(name_last) + r"\s*,?\s*(" + _SUF_RE + r")", blob)
    return m.group(1) if m else None


def main():
    done = set(_load())
    with db._connect() as conn:
        conn.row_factory = None
        rows = conn.execute(
            f"SELECT id, doi, book_author_last_name, reviewer_last_name FROM reviews "
            f"WHERE publication_source IN ({','.join('?'*len(JOURNALS))}) AND doi!=''",
            JOURNALS).fetchall()
    todo = [r for r in rows if r[0] not in done and (
        not any((r[2] or "").endswith(s) for s in SUFFIXES)
        or not any((r[3] or "").endswith(s) for s in SUFFIXES))]
    print(f"rows to check: {len(todo)} (of {len(rows)})", flush=True)
    sess = requests.Session(); sess.headers.update(UA)
    restored, checked, batch = 0, 0, []
    for rid, doi, al, rl in todo:
        if rid in done:
            continue
        checked += 1
        try:
            r = sess.get(f"https://api.crossref.org/works/{doi}", timeout=30)
            if r.status_code == 404:
                done.add(rid); continue
            r.raise_for_status()
            blob = json.dumps(r.json()["message"], ensure_ascii=False)
        except Exception:
            time.sleep(5); continue  # retried next run via state
        updates = {}
        s_a = _find_suffix_after(al, blob)
        if s_a and not (al or "").endswith(s_a):
            updates["book_author_last_name"] = f"{al}, {s_a}"
        s_r = _find_suffix_after(rl, blob)
        if s_r and not (rl or "").endswith(s_r):
            updates["reviewer_last_name"] = f"{rl}, {s_r}"
        if updates:
            with db._connect() as conn:
                sets = ", ".join(f"{k}=?" for k in updates)
                conn.execute(f"UPDATE reviews SET {sets} WHERE id=?",
                             (*updates.values(), rid))
            restored += 1
            print(f"  RESTORED {rid}: {updates}", flush=True)
        done.add(rid)
        if checked % 50 == 0:
            _save(sorted(done))
            print(f"  ...{checked} checked, {restored} restored", flush=True)
        time.sleep(0.4)
    _save(sorted(done))
    print(f"DONE: {checked} checked, {restored} suffixes restored", flush=True)


if __name__ == "__main__":
    main()
