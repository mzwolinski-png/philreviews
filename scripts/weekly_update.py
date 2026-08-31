#!/usr/bin/env python3
"""
PhilReviews Weekly Update — Check all journals for new Crossref content.

Runs weekly via LaunchAgent. For each journal in the database, queries Crossref
for articles published since the last check, filters to book reviews, and inserts
new entries. Triggers a site rebuild if any new reviews are added.

Usage:
    python3 scripts/weekly_update.py          # normal run
    python3 scripts/weekly_update.py --dry-run  # preview without inserting
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta

# Ensure project root is on the path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db
from crossref_scraper import CrossrefReviewScraper, _to_db_fields
from scraper_base import setup_logging
from notify import send_run_summary

STATE_FILE = os.path.join(ROOT, "scripts", "weekly_state.json")

log = setup_logging("weekly_update", os.path.join(ROOT, "scripts", "weekly_update.log"))


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_from_date(state: dict) -> str:
    """Return the date string to use as 'from-indexed-date' filter.

    Uses the last successful run date minus 2 days of overlap (to catch
    items indexed after publication). Falls back to 14 days ago.
    """
    last_run = state.get("last_run")
    if last_run:
        dt = datetime.fromisoformat(last_run) - timedelta(days=2)
    else:
        dt = datetime.now() - timedelta(days=14)
    return dt.strftime("%Y-%m-%d")


def check_journal(scraper: CrossrefReviewScraper, journal_name: str,
                  from_date: str, dry_run: bool = False,
                  added_reviews: list = None) -> int:
    """Check one journal for new reviews since from_date. Returns count of new inserts."""
    journal_cfg = scraper.JOURNALS.get(journal_name, {})
    detection_mode = journal_cfg.get("detection_mode", "all")
    is_all_reviews = journal_cfg.get("all_reviews", False)

    log.info(f"Checking {journal_name} (since {from_date})...")

    try:
        import requests
        params = {
            "filter": f"container-title:{journal_name},from-index-date:{from_date}",
            "rows": 100,
            "cursor": "*",
            "mailto": "mzwolinski@sandiego.edu",
        }
        items = []
        while True:
            resp = requests.get(
                "https://api.crossref.org/works", params=params, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("message", {}).get("items", [])
            if not batch:
                break
            items.extend(batch)
            next_cursor = data.get("message", {}).get("next-cursor", "")
            if not next_cursor:
                break
            params["cursor"] = next_cursor
            time.sleep(0.5)
    except Exception as e:
        log.error(f"  Error fetching {journal_name}: {e}")
        return 0

    # Filter to book reviews
    from crossref_scraper import is_book_review
    if is_all_reviews:
        reviews = items
    else:
        reviews = [item for item in items if is_book_review(item, detection_mode)]

    if not reviews:
        log.info(f"  {journal_name}: {len(items)} items, 0 reviews")
        return 0

    log.info(f"  {journal_name}: {len(items)} items, {len(reviews)} reviews found")

    # Extract and insert
    new_count = 0
    for item in reviews:
        extracted = scraper.extract_review(item)
        if not extracted:
            continue

        doi = extracted.get("DOI", "")
        link = extracted.get("Review Link", "")

        # Skip if already in DB
        if doi and db.doi_exists(doi):
            continue
        if link and db.review_link_exists(link):
            continue

        # Convert Airtable-style keys to DB column names
        db_record = _to_db_fields(extracted)
        title = db_record.get("book_title", "?")

        if dry_run:
            log.info(f"    [DRY RUN] Would add: {title}")
            new_count += 1
            continue

        db.insert_review(db_record)
        new_count += 1
        log.info(f"    Added: {title}")
        if added_reviews is not None:
            author = " ".join(filter(None, [
                db_record.get("book_author_first_name", ""),
                db_record.get("book_author_last_name", ""),
            ]))
            reviewer = " ".join(filter(None, [
                db_record.get("reviewer_first_name", ""),
                db_record.get("reviewer_last_name", ""),
            ]))
            added_reviews.append({
                "book_title": title,
                "book_author": author,
                "journal": journal_name,
                "reviewer": reviewer,
            })

    return new_count


from deploy import sync_to_fly


def _get_subscriber_count():
    """Get verified subscriber count from Fly.io production DB."""
    try:
        import subprocess
        result = subprocess.run(
            ["fly", "ssh", "console", "-a", "philreviews", "-C",
             "python3 -c \"import sqlite3; c=sqlite3.connect('/data/reviews.db'); "
             "print(c.execute('SELECT COUNT(*) FROM subscribers WHERE verified=1').fetchone()[0])\""],
            capture_output=True, text=True, timeout=30,
        )
        return int(result.stdout.strip())
    except Exception:
        return "?"


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        log.info("=== DRY RUN MODE ===")

    start_time = datetime.now()
    log.info("=" * 60)
    log.info(f"PhilReviews weekly update — {start_time.isoformat()}")
    log.info("=" * 60)

    state = load_state()
    from_date = get_from_date(state)
    log.info(f"Checking for content indexed since {from_date}")

    scraper = CrossrefReviewScraper()
    total_new = 0
    added_reviews = []

    for journal_name in sorted(scraper.JOURNALS.keys()):
        try:
            n = check_journal(scraper, journal_name, from_date, dry_run=dry_run,
                              added_reviews=added_reviews)
            total_new += n
        except Exception as e:
            log.error(f"Error processing {journal_name}: {e}")
        time.sleep(1)  # Be polite to Crossref

    log.info(f"\nTotal new reviews added: {total_new}")

    if total_new > 0 and not dry_run:
        # Classify new entries into subfields
        try:
            from classify_subfields import classify_new_reviews
            classified = classify_new_reviews()
            log.info(f"Subfield classification: {classified} entries classified")
        except Exception:
            log.exception("Subfield classification failed")

        # Data integrity check on new entries
        try:
            from integrity_check import run_integrity_check
            ic = run_integrity_check(since=from_date)
            log.info(f"Integrity check: {ic['checked']} checked, {ic['fixed']} fixed, {ic['deleted']} deleted")
            for d in ic['details']:
                log.info(f"  {d}")
        except Exception:
            log.exception("Integrity check failed")

        sync_to_fly()

    if not dry_run:
        state["last_run"] = datetime.now().isoformat()
        state["last_new_count"] = total_new
        save_state(state)

    log.info("Weekly update complete.")

    # Send email summary
    if not dry_run:
        total_reviews = len(db.get_all_reviews())
        try:
            send_run_summary("weekly_update.py (Crossref)", {
                "before": total_reviews - total_new,
                "after": total_reviews,
                "details": f"Crossref delta check across {len(scraper.JOURNALS)} journals\nNew reviews found: {total_new}\nSubscribers: {_get_subscriber_count()}",
                "errors": [],
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "added_reviews": added_reviews,
            })
        except Exception:
            log.exception("Failed to send email notification")

    # Send weekly digest to subscribers
    if not dry_run:
        try:
            from scripts.send_digest import main as send_digest
            send_digest()
        except Exception:
            # Try alternative import path (when run from project root)
            try:
                from send_digest import main as send_digest
                send_digest()
            except Exception:
                log.exception("Failed to send subscriber digest")


if __name__ == "__main__":
    main()
