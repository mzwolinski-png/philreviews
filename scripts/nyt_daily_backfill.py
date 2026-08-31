#!/usr/bin/env python3
"""Daily NYT philosophy backfill — walks the archive backward one year per run.

Driven by a LaunchAgent (com.philreviews.nyt-backfill, daily 2am). Each run
takes the next year from the state file, gates that year's NYT book reviews for
philosophy, and decrements. NYT Article Search caps at 1000 docs/query, so each
year is split into two half-year windows. Stays under NYT's ~500-request/day
limit. Does NOT sync to Fly — the Sunday update.py sync pushes the accumulated
rows. Secrets come from .env (none embedded in the plist).
"""
import json
import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

import nyt_backfill

STATE = os.path.join(ROOT, "scripts", "nyt_backfill_state.json")
START_YEAR = 2023   # 2024-2026 already backfilled manually
# NYT only began attaching the structured book-title keyword (creative_works /
# "Title (Book)") around 2013. Before that, Article Search still returns the
# reviews (~1000/yr) but with no clean book-title field — nyt_backfill._book_title
# finds nothing, so ~93% are dropped and philosophy yield is ~0. Diagnosed
# 2026-07-17: the walk was already at 2006 (< floor), so this halts it gracefully.
FLOOR_YEAR = 2013

logging.basicConfig(
    filename=os.path.join(ROOT, "scripts", "nyt_backfill.log"),
    level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("nyt_daily")


def _load():
    try:
        with open(STATE) as f:
            return int(json.load(f).get("next_year", START_YEAR))
    except Exception:
        return START_YEAR


def _save(year):
    with open(STATE, "w") as f:
        json.dump({"next_year": year}, f)


def main():
    key = os.environ.get("NYT_API_KEY")
    if not key:
        log.error("NYT_API_KEY not set — aborting")
        return
    year = _load()
    if year < FLOOR_YEAR:
        log.info(f"NYT backfill complete (reached floor {FLOOR_YEAR}); nothing to do")
        return
    total = 0
    for begin, end in ((f"{year}0101", f"{year}0630"), (f"{year}0701", f"{year}1231")):
        try:
            st = nyt_backfill.run(key, begin_date=begin, end_date=end)
            n = st["inserted"] if isinstance(st["inserted"], int) else 0
            total += n
            log.info(f"{year} {begin}-{end}: gated {st['gate_checked']} "
                     f"pass {st['gate_pass']} inserted {n}")
        except Exception:
            log.exception(f"window {begin}-{end} failed")
    _save(year - 1)
    log.info(f"YEAR {year} done: {total} inserted total; next_year set to {year - 1}")


if __name__ == "__main__":
    main()
