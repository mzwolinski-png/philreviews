#!/usr/bin/env python3
"""
Backfill `access_type` against Unpaywall's authoritative open-access status.

Why: `crossref_scraper` used to set `access_type = 'Open' if item.get('license')`.
Crossref's `license` field is NOT an OA signal — subscription publishers attach
their standard terms-of-use URL there (Cambridge /core/terms, Wiley
termsAndConditions, Springer text-and-data-mining). That mismarked tens of
thousands of paywalled reviews as free. The scraper is fixed
(`_access_type_from_license`); this script repairs the back catalogue.

Design:
- **Resolve phase** queries Unpaywall for each DOI and appends to a jsonl cache,
  so the run is kill-safe and resumable — re-running skips everything cached.
- **Apply phase** is separate and gated behind `--apply`; without it you get a
  dry-run summary only.
- A DOI Unpaywall doesn't know (404) is left ALONE, never flipped — absence of
  evidence isn't evidence of paywalling.
- Casing is normalized to 'Open' / 'Restricted' ('Paywalled'/'Subscription' →
  'Restricted') for every row it touches.

Usage:
    python3 scripts/backfill_access_type.py            # resolve + dry-run report
    python3 scripts/backfill_access_type.py --apply    # resolve + write changes
    python3 scripts/backfill_access_type.py --scope all  # also verify Restricted rows
"""

import json
import os
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db

EMAIL = "mzwolinski@gmail.com"
CACHE = os.path.join(ROOT, "reports", "unpaywall_cache.jsonl")
WORKERS = 5

_lock = threading.Lock()
_done = 0


def _load_cache():
    seen = {}
    if os.path.exists(CACHE):
        with open(CACHE) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                    seen[r["doi"]] = r["is_oa"]
                except Exception:
                    continue
    return seen


def _lookup(doi):
    """Returns True/False for OA status, or None if unknown (404 / error)."""
    url = (f"https://api.unpaywall.org/v2/{urllib.parse.quote(doi)}"
           f"?email={EMAIL}")
    for attempt in (1, 2, 3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PhilReviews/1.0"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                return bool(json.load(resp).get("is_oa"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None          # Unpaywall has no record — leave the row alone
            if e.code in (429, 500, 502, 503):
                time.sleep(2 * attempt)
                continue
            return None
        except Exception:
            time.sleep(1.5 * attempt)
    return None


def resolve(dois):
    """Fill the cache for any DOI not already in it."""
    global _done
    cache = _load_cache()
    todo = [d for d in dois if d not in cache]
    print(f"resolve: {len(dois)} dois, {len(dois) - len(todo)} cached, {len(todo)} to fetch")
    if not todo:
        return cache
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    fh = open(CACHE, "a")

    def work(doi):
        global _done
        is_oa = _lookup(doi)
        with _lock:
            fh.write(json.dumps({"doi": doi, "is_oa": is_oa}) + "\n")
            _done += 1
            if _done % 500 == 0:
                fh.flush()
                print(f"  … {_done}/{len(todo)}", flush=True)
        return doi, is_oa

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for doi, is_oa in ex.map(work, todo):
            cache[doi] = is_oa
    fh.close()
    return cache


CANON = {"open": "Open", "restricted": "Restricted",
         "paywalled": "Restricted", "subscription": "Restricted"}


def main():
    apply_changes = "--apply" in sys.argv
    scope_all = "--scope" in sys.argv and "all" in sys.argv

    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    where = ("doi IS NOT NULL AND doi != ''"
             if scope_all else
             "lower(access_type)='open' AND doi IS NOT NULL AND doi != ''")
    rows = conn.execute(
        f"SELECT id, doi, access_type FROM reviews WHERE {where}").fetchall()
    print(f"candidate rows: {len(rows)}")

    cache = resolve(sorted({r["doi"] for r in rows}))

    to_restricted, to_open, casing_only, unknown = [], [], [], 0
    for r in rows:
        is_oa = cache.get(r["doi"])
        cur = (r["access_type"] or "").strip()
        if is_oa is None:
            unknown += 1
            continue
        want = "Open" if is_oa else "Restricted"
        if CANON.get(cur.lower(), cur) != want:
            (to_open if want == "Open" else to_restricted).append((r["id"], want))
        elif cur != want:
            casing_only.append((r["id"], want))

    print(f"\n  -> Restricted (was open, not actually OA): {len(to_restricted)}")
    print(f"  -> Open (was restricted, actually OA):     {len(to_open)}")
    print(f"  casing normalization only:                 {len(casing_only)}")
    print(f"  unknown to Unpaywall (left untouched):     {unknown}")

    if not apply_changes:
        print("\n(dry run — pass --apply to write)")
        return

    updates = to_restricted + to_open + casing_only
    conn.executemany("UPDATE reviews SET access_type=? WHERE id=?",
                     [(w, i) for i, w in updates])
    conn.commit()
    print(f"\nAPPLIED {len(updates)} updates")


if __name__ == "__main__":
    main()
