#!/usr/bin/env python3
"""
PhilReviews weekly update script.

Runs all scrapers to pull in new reviews, then logs a summary.
Designed to be run unattended via cron or launchd.

Usage:
    python3 update.py              # full update (all scrapers)
    python3 update.py --crossref   # Crossref journals only
    python3 update.py --ndpr       # NDPR only
    python3 update.py --daily-nous # Daily Nous only
    python3 update.py --dry-run    # don't write to database
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

# Ensure imports work regardless of cwd
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import db

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"update_{datetime.now():%Y-%m-%d_%H%M%S}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("update")


def count_reviews():
    """Return the current total review count."""
    return len(db.get_all_reviews())


def run_crossref(dry_run=False):
    """Run the multi-journal Crossref scraper."""
    log.info("Starting Crossref multi-journal scraper...")
    try:
        from crossref_scraper import CrossrefReviewScraper

        scraper = CrossrefReviewScraper()
        scraper.run(dry_run=dry_run)
        log.info(f"Crossref scraper stats: {scraper.stats}")
        return scraper.stats
    except Exception:
        log.exception("Crossref scraper failed")
        return None


def run_ndpr(dry_run=False):
    """Run the NDPR recent-reviews scraper."""
    log.info("Starting NDPR scraper...")
    try:
        from ndpr_scraper import NDPRScraper

        scraper = NDPRScraper()
        reviews = scraper.get_recent_reviews(limit=30)

        if not reviews:
            log.info("NDPR: no reviews found")
            return {"found": 0, "new": 0}

        new_reviews = scraper.check_for_duplicates(reviews)
        new_count = len(new_reviews)

        if new_reviews and not dry_run:
            scraper.add_to_db(new_reviews)

        log.info(f"NDPR: {len(reviews)} found, {new_count} new")
        return {"found": len(reviews), "new": new_count}
    except Exception:
        log.exception("NDPR scraper failed")
        return None


def run_daily_nous(dry_run=False):
    """Run the Daily Nous weekly update scraper."""
    log.info("Starting Daily Nous scraper...")
    try:
        from daily_nous_scraper import DailyNousScraper

        scraper = DailyNousScraper()
        stats = scraper.run_incremental(dry_run=dry_run)
        log.info(f"Daily Nous: {stats.get('reviews_parsed', 0)} parsed, "
                 f"{stats.get('uploaded', 0)} new")
        return stats
    except Exception:
        log.exception("Daily Nous scraper failed")
        return None


def run_mainstream(dry_run=False):
    """Run the mainstream media review scraper."""
    log.info("Starting mainstream media scraper...")
    try:
        from dotenv import load_dotenv
        load_dotenv()

        from mainstream_review_scraper import MainstreamReviewScraper

        google_key = os.environ.get("GOOGLE_CSE_API_KEY")
        google_cx = os.environ.get("GOOGLE_CSE_CX")
        guardian_key = os.environ.get("GUARDIAN_API_KEY")

        scraper = MainstreamReviewScraper(
            google_api_key=google_key,
            google_cx=google_cx,
            guardian_api_key=guardian_key,
        )
        stats = scraper.run(dry_run=dry_run)
        log.info(f"Mainstream: {stats.get('results_verified', 0)} verified, "
                 f"{stats.get('uploaded', 0)} uploaded")
        return stats
    except Exception:
        log.exception("Mainstream scraper failed")
        return None


def rebuild_and_deploy():
    """Rebuild the static site and push to GitHub if content changed."""
    log.info("Rebuilding static site...")
    try:
        from build import build
        build()
    except Exception:
        log.exception("Static site build failed")
        return

    # Only commit and push if docs/ actually changed
    try:
        result = subprocess.run(
            ["git", "diff", "--quiet", "docs/"],
            cwd=ROOT,
            capture_output=True,
        )
        if result.returncode == 0:
            # Also check for untracked files in docs/
            untracked = subprocess.run(
                ["git", "ls-files", "--others", "--exclude-standard", "docs/"],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            if not untracked.stdout.strip():
                log.info("No changes in docs/ — skipping deploy")
                return

        log.info("Deploying updated site to GitHub Pages...")
        subprocess.run(["git", "add", "docs/"], cwd=ROOT, check=True)
        subprocess.run(
            ["git", "commit", "-m", "Update static site"],
            cwd=ROOT,
            check=True,
        )
        subprocess.run(["git", "push"], cwd=ROOT, check=True)
        log.info("Deploy complete")
    except Exception:
        log.exception("Git deploy failed")


def main():
    parser = argparse.ArgumentParser(description="PhilReviews weekly update")
    parser.add_argument("--crossref", action="store_true", help="Run Crossref only")
    parser.add_argument("--ndpr", action="store_true", help="Run NDPR only")
    parser.add_argument("--daily-nous", action="store_true", help="Run Daily Nous only")
    parser.add_argument("--mainstream", action="store_true",
                        help="Run mainstream media scraper (uses API quota)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    args = parser.parse_args()

    # Default: run everything (except mainstream, which uses API quota)
    run_all = not args.crossref and not args.ndpr and not args.daily_nous and not args.mainstream

    log.info("=" * 60)
    log.info("PhilReviews update started")
    log.info(f"Log file: {log_file}")

    before = count_reviews()
    log.info(f"Reviews before update: {before}")

    crossref_stats = None
    ndpr_stats = None
    dn_stats = None

    if run_all or args.crossref:
        crossref_stats = run_crossref(dry_run=args.dry_run)

    if run_all or args.ndpr:
        ndpr_stats = run_ndpr(dry_run=args.dry_run)

    if run_all or args.daily_nous:
        dn_stats = run_daily_nous(dry_run=args.dry_run)

    mainstream_stats = None
    if args.mainstream:
        mainstream_stats = run_mainstream(dry_run=args.dry_run)

    after = count_reviews()
    net_new = after - before

    log.info("-" * 40)
    log.info(f"Reviews after update: {after}")
    log.info(f"Net new reviews added: {net_new}")

    if crossref_stats:
        log.info(f"Crossref uploaded: {crossref_stats.get('uploaded', 0)}")
    if ndpr_stats:
        log.info(f"NDPR new: {ndpr_stats.get('new', 0)}")
    if dn_stats:
        log.info(f"Daily Nous new: {dn_stats.get('uploaded', 0)}")
    if mainstream_stats:
        log.info(f"Mainstream new: {mainstream_stats.get('uploaded', 0)}")

    # Rebuild static site and deploy to GitHub Pages
    if not args.dry_run:
        rebuild_and_deploy()

    log.info("Update finished")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
