#!/usr/bin/env python3
"""
PhilReviews weekly update — single unified script.

Default run: NDPR + Daily Nous + Unpopulist + Crossref delta check.
Then: classify → integrity check → sync to Fly.io → admin email → subscriber digest.

Usage:
    python3 update.py              # full weekly update (all scrapers)
    python3 update.py --crossref   # Crossref delta check only
    python3 update.py --ndpr       # NDPR only
    python3 update.py --daily-nous # Daily Nous only
    python3 update.py --mainstream # mainstream media scraper (uses API quota)
    python3 update.py --dry-run    # don't write to database
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

# Ensure imports work regardless of cwd
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import db
from deploy import sync_to_fly
from notify import send_run_summary
from scraper_base import setup_logging

LOG_FILE = os.path.join(ROOT, "scripts", "weekly_update.log")
STATE_FILE = os.path.join(ROOT, "scripts", "weekly_state.json")

log = setup_logging("update", LOG_FILE)


def count_reviews():
    return len(db.get_all_reviews())


# --- State management (for Crossref delta) ---

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_from_date(state: dict) -> str:
    last_run = state.get("last_run")
    if last_run:
        dt = datetime.fromisoformat(last_run) - timedelta(days=2)
    else:
        dt = datetime.now() - timedelta(days=14)
    return dt.strftime("%Y-%m-%d")


# --- Scrapers ---

def run_crossref_delta(from_date, dry_run=False):
    """Crossref delta check across all journals since from_date."""
    log.info(f"Starting Crossref delta check (since {from_date})...")
    import requests
    from crossref_scraper import CrossrefReviewScraper, is_book_review, _to_db_fields

    scraper = CrossrefReviewScraper()
    total_new = 0
    added_reviews = []

    for journal_name in sorted(scraper.JOURNALS.keys()):
        journal_cfg = scraper.JOURNALS.get(journal_name, {})
        detection_mode = journal_cfg.get("detection_mode", "all")
        is_all_reviews = journal_cfg.get("all_reviews", False)

        log.info(f"Checking {journal_name} (since {from_date})...")
        try:
            params = {
                "filter": f"container-title:{journal_name},from-index-date:{from_date}",
                "rows": 100, "cursor": "*",
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
            time.sleep(1)
            continue

        if is_all_reviews:
            reviews = items
        else:
            reviews = [item for item in items if is_book_review(item, detection_mode)]

        if not reviews:
            log.info(f"  {journal_name}: {len(items)} items, 0 reviews")
            time.sleep(1)
            continue

        log.info(f"  {journal_name}: {len(items)} items, {len(reviews)} reviews found")

        new_count = 0
        for item in reviews:
            extracted = scraper.extract_review(item)
            if not extracted:
                continue
            doi = extracted.get("DOI", "")
            link = extracted.get("Review Link", "")
            if doi and db.doi_exists(doi):
                continue
            if link and db.review_link_exists(link):
                continue

            db_record = _to_db_fields(extracted)
            title = db_record.get("book_title", "?")

            if dry_run:
                log.info(f"    [DRY RUN] Would add: {title}")
                new_count += 1
                continue

            db.insert_review(db_record)
            new_count += 1
            log.info(f"    Added: {title}")
            added_reviews.append({
                "book_title": title,
                "book_author": " ".join(filter(None, [
                    db_record.get("book_author_first_name", ""),
                    db_record.get("book_author_last_name", ""),
                ])),
                "journal": journal_name,
                "reviewer": " ".join(filter(None, [
                    db_record.get("reviewer_first_name", ""),
                    db_record.get("reviewer_last_name", ""),
                ])),
            })

        total_new += new_count
        time.sleep(1)

    log.info(f"Crossref delta: {total_new} new reviews from {len(scraper.JOURNALS)} journals")
    return {"new": total_new, "journals": len(scraper.JOURNALS), "added_reviews": added_reviews}


def run_ndpr(dry_run=False):
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
    log.info("Starting Daily Nous scraper...")
    try:
        from daily_nous_scraper import DailyNousScraper
        scraper = DailyNousScraper()
        stats = scraper.run_incremental(dry_run=dry_run)
        log.info(f"Daily Nous: {stats.get('reviews_parsed', 0)} parsed, {stats.get('uploaded', 0)} new")
        return stats
    except Exception:
        log.exception("Daily Nous scraper failed")
        return None


def run_philosophy_now(dry_run=False):
    log.info("Starting Philosophy Now scraper...")
    try:
        from philosophy_now_scraper import PhilosophyNowScraper
        scraper = PhilosophyNowScraper()
        stats = scraper.run(dry_run=dry_run)  # incremental by default
        return stats
    except Exception:
        log.exception("Philosophy Now scraper failed")
        return None


def run_lawliberty(dry_run=False):
    log.info("Starting Law & Liberty scraper...")
    try:
        from lawliberty_scraper import LawLibertyScraper
        scraper = LawLibertyScraper()
        stats = scraper.run(dry_run=dry_run)  # incremental via RSS
        return stats
    except Exception:
        log.exception("Law & Liberty scraper failed")
        return None


def run_public_discourse(dry_run=False):
    log.info("Starting Public Discourse scraper...")
    try:
        from public_discourse_scraper import PublicDiscourseScraper
        scraper = PublicDiscourseScraper()
        stats = scraper.run(dry_run=dry_run)  # 2 feed pages by default
        return stats
    except Exception:
        log.exception("Public Discourse scraper failed")
        return None


def run_bjps(dry_run=False):
    log.info("Starting BJPS Review of Books scraper...")
    try:
        from bjps_scraper import BJPSScraper
        scraper = BJPSScraper()
        stats = scraper.run(dry_run=dry_run)
        return stats
    except Exception:
        log.exception("BJPS scraper failed")
        return None


def run_syndicate(dry_run=False):
    log.info("Starting Syndicate scraper...")
    try:
        from syndicate_scraper import SyndicateScraper
        scraper = SyndicateScraper()
        stats = scraper.run(dry_run=dry_run)
        return stats
    except Exception:
        log.exception("Syndicate scraper failed")
        return None


def run_crb(dry_run=False):
    log.info("Starting Claremont Review of Books scraper...")
    try:
        from crb_scraper import CRBScraper
        scraper = CRBScraper()
        stats = scraper.run(dry_run=dry_run)  # RSS feed incremental
        return stats
    except Exception:
        log.exception("CRB scraper failed")
        return None


def run_tpm(dry_run=False):
    """TPM Archive is historical (1998-2021), no new reviews expected."""
    # Skipping in weekly runs since the archive site has stopped publishing
    return None


def run_quillette(dry_run=False):
    log.info("Starting Quillette scraper...")
    try:
        from quillette_scraper import QuilletteScraper
        scraper = QuilletteScraper()
        stats = scraper.run(dry_run=dry_run)  # first 2 pages
        return stats
    except Exception:
        log.exception("Quillette scraper failed")
        return None


def run_apa_blog(dry_run=False):
    log.info("Starting APA Blog scraper...")
    try:
        from apa_blog_scraper import APABlogScraper
        scraper = APABlogScraper()
        stats = scraper.run(dry_run=dry_run)  # first 2 pages
        return stats
    except Exception:
        log.exception("APA Blog scraper failed")
        return None


def run_tir(dry_run=False):
    log.info("Starting The Independent Review scraper...")
    try:
        from tir_scraper import TIRScraper
        scraper = TIRScraper()
        stats = scraper.run(dry_run=dry_run)  # last 4 issues
        return stats
    except Exception:
        log.exception("TIR scraper failed")
        return None


def run_radical_philosophy(dry_run=False):
    log.info("Starting Radical Philosophy scraper...")
    try:
        from radical_philosophy_scraper import RadicalPhilosophyScraper
        scraper = RadicalPhilosophyScraper()
        stats = scraper.run(dry_run=dry_run, max_pages=2)  # recent 200 posts
        return stats
    except Exception:
        log.exception("Radical Philosophy scraper failed")
        return None


def run_mm(dry_run=False):
    log.info("Starting Markets & Morality scraper...")
    try:
        from mm_scraper import MMScraper
        scraper = MMScraper()
        stats = scraper.run(dry_run=dry_run)  # RSS feed (incremental)
        return stats
    except Exception:
        log.exception("Markets & Morality scraper failed")
        return None


def run_atlantic(dry_run=False):
    log.info("Starting The Atlantic scraper...")
    try:
        from atlantic_scraper import AtlanticScraper
        scraper = AtlanticScraper()
        stats = scraper.run(dry_run=dry_run)  # books/culture feed (incremental)
        return stats
    except Exception:
        log.exception("The Atlantic scraper failed")
        return None


def run_unpopulist(dry_run=False):
    log.info("Starting Unpopulist scraper...")
    try:
        from unpopulist_scraper import UnpopulistScraper
        scraper = UnpopulistScraper()
        stats = scraper.run(dry_run=dry_run)
        log.info(f"Unpopulist: {stats.get('found', 0)} found, {stats.get('new', 0)} new")
        return stats
    except Exception:
        log.exception("Unpopulist scraper failed")
        return None


def run_mainstream(dry_run=False, active_since_days=None):
    log.info("Starting mainstream media scraper...")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        from mainstream_review_scraper import MainstreamReviewScraper
        scraper = MainstreamReviewScraper(
            google_api_key=os.environ.get("GOOGLE_CSE_API_KEY"),
            google_cx=os.environ.get("GOOGLE_CSE_CX"),
            guardian_api_key=os.environ.get("GUARDIAN_API_KEY"),
            nyt_api_key=os.environ.get("NYT_API_KEY"),
            brave_api_key=os.environ.get("BRAVE_API_KEY"),
        )
        stats = scraper.run(dry_run=dry_run, active_since_days=active_since_days)
        log.info(f"Mainstream: {stats.get('results_verified', 0)} verified, {stats.get('uploaded', 0)} uploaded")
        return stats
    except Exception:
        log.exception("Mainstream scraper failed")
        return None


def _get_subscriber_count():
    # Primary: the /health endpoint over HTTPS (reliable; reads the production
    # subscribers DB locally on the server). Falls back to the SSH tunnel, which
    # is flaky when the WireGuard tunnel is cold (the old cause of "?").
    try:
        import requests
        r = requests.get("https://philreviews.org/health", timeout=20)
        subs = r.json().get("subscribers")
        if isinstance(subs, int):
            return subs
    except Exception:
        pass
    try:
        result = subprocess.run(
            ["fly", "ssh", "console", "-a", "philreviews", "-C",
             "python3 -c \"import sqlite3; c=sqlite3.connect('/data/subscribers.db'); "
             "print(c.execute('SELECT COUNT(*) FROM subscribers WHERE verified=1').fetchone()[0])\""],
            capture_output=True, text=True, timeout=90,
        )
        return int(result.stdout.strip())
    except Exception:
        return "?"


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description="PhilReviews weekly update")
    parser.add_argument("--crossref", action="store_true", help="Crossref delta only")
    parser.add_argument("--ndpr", action="store_true", help="NDPR only")
    parser.add_argument("--daily-nous", action="store_true", help="Daily Nous only")
    parser.add_argument("--mainstream", action="store_true", help="Mainstream media (uses API quota)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    args = parser.parse_args()

    selective = args.crossref or args.ndpr or args.daily_nous or args.mainstream
    run_all = not selective

    if args.dry_run:
        log.info("=== DRY RUN MODE ===")

    start_time = datetime.now()
    # created_at is stored in UTC (SQLite CURRENT_TIMESTAMP), so created_at-based
    # queries (email new-reviews list, HTML report) must compare against a UTC
    # marker — not local time, which would miss everything by the TZ offset.
    start_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info(f"PhilReviews weekly update — {start_time.isoformat()}")
    log.info("=" * 60)

    state = load_state()
    from_date = get_from_date(state)
    before = count_reviews()
    log.info(f"Reviews before update: {before}")
    errors = []

    # --- Run scrapers ---
    ndpr_stats = None
    dn_stats = None
    unpop_stats = None
    crossref_stats = None
    mainstream_stats = None
    phil_now_stats = None
    ll_stats = None
    pd_stats = None
    bjps_stats = None
    syndicate_stats = None
    crb_stats = None
    quillette_stats = None
    apa_stats = None
    tir_stats = None
    rp_stats = None
    mm_stats = None
    atlantic_stats = None
    crossref_added = []

    if run_all or args.ndpr:
        ndpr_stats = run_ndpr(dry_run=args.dry_run)

    if run_all or args.daily_nous:
        dn_stats = run_daily_nous(dry_run=args.dry_run)

    if run_all:
        unpop_stats = run_unpopulist(dry_run=args.dry_run)
        phil_now_stats = run_philosophy_now(dry_run=args.dry_run)
        ll_stats = run_lawliberty(dry_run=args.dry_run)
        pd_stats = run_public_discourse(dry_run=args.dry_run)
        bjps_stats = run_bjps(dry_run=args.dry_run)
        syndicate_stats = run_syndicate(dry_run=args.dry_run)
        crb_stats = run_crb(dry_run=args.dry_run)
        quillette_stats = run_quillette(dry_run=args.dry_run)
        apa_stats = run_apa_blog(dry_run=args.dry_run)
        tir_stats = run_tir(dry_run=args.dry_run)
        rp_stats = run_radical_philosophy(dry_run=args.dry_run)
        mm_stats = run_mm(dry_run=args.dry_run)
        atlantic_stats = run_atlantic(dry_run=args.dry_run)

    if run_all or args.crossref:
        crossref_stats = run_crossref_delta(from_date, dry_run=args.dry_run)
        crossref_added = crossref_stats.get("added_reviews", []) if crossref_stats else []

    if args.mainstream:
        # Explicit flag: full scan, no time filter
        mainstream_stats = run_mainstream(dry_run=args.dry_run)
    elif run_all:
        # Weekly run: scan only books with review activity in the last 10 days
        # (keeps API quota low while catching newly-reviewed books)
        mainstream_stats = run_mainstream(dry_run=args.dry_run, active_since_days=10)

    # Surface any scrapers that crashed (a runner returns None only on an
    # exception). Without this, a broken scraper silently vanishes from the
    # report — no count, no error — so it could stay dead for weeks unnoticed.
    attempted = []
    if run_all or args.ndpr:
        attempted.append(("NDPR", ndpr_stats))
    if run_all or args.daily_nous:
        attempted.append(("Daily Nous", dn_stats))
    if run_all:
        attempted += [
            ("Unpopulist", unpop_stats), ("Philosophy Now", phil_now_stats),
            ("Law & Liberty", ll_stats), ("Public Discourse", pd_stats),
            ("BJPS", bjps_stats), ("Syndicate", syndicate_stats), ("CRB", crb_stats),
            ("Quillette", quillette_stats), ("APA Blog", apa_stats), ("TIR", tir_stats),
            ("Radical Philosophy", rp_stats), ("Markets & Morality", mm_stats),
            ("Atlantic", atlantic_stats),
        ]
    if run_all or args.crossref:
        attempted.append(("Crossref", crossref_stats))
    if args.mainstream or run_all:
        attempted.append(("Mainstream", mainstream_stats))
    for _name, _st in attempted:
        if _st is None:
            errors.append(f"{_name} scraper FAILED (returned no result — see log for traceback)")

    # Philosophia book-symposium detection. Philosophia's italic_only mode skips the
    # "Précis of X" and "Replies to ..." pieces (no italic book title), so symposia
    # are never assembled. This rolling-window pass reconstructs each symposium
    # (book + author + all pieces, labeled) and groups it. Additive/idempotent: it
    # only fills ungrouped/missing rows and never overrides an existing group.
    philosophia_symp_stats = None
    if (run_all or args.crossref) and not args.dry_run:
        try:
            import philosophia_symposia
            philosophia_symp_stats = philosophia_symposia.run(window_months=24)
            log.info(
                f"Philosophia symposia: {philosophia_symp_stats['inserted']} pieces added, "
                f"{philosophia_symp_stats['updated']} grouped "
                f"({philosophia_symp_stats['symposia']} symposia scanned)")
        except Exception:
            log.exception("Philosophia symposia detection failed")

    after = count_reviews()
    net_new = after - before
    log.info("-" * 40)
    log.info(f"Reviews after update: {after}")
    log.info(f"Net new reviews added: {net_new}")
    tier1_removed = 0
    reconcile_report_path = None

    # --- Post-processing ---
    if not args.dry_run and net_new > 0:
        # Classify
        try:
            from classify_subfields import classify_new_reviews
            classified = classify_new_reviews()
            log.info(f"Subfield classification: {classified} entries classified")
        except Exception:
            log.exception("Subfield classification failed")

        # Integrity check
        try:
            from integrity_check import run_integrity_check
            ic = run_integrity_check(since=from_date)
            log.info(f"Integrity check: {ic['checked']} checked, {ic['fixed']} fixed, {ic['deleted']} deleted")
            for d in ic['details']:
                log.info(f"  {d}")
        except Exception:
            log.exception("Integrity check failed")

        # Retry Crossref for entries missing reviewer names (Project MUSE lag)
        try:
            from integrity_check import retry_missing_reviewers
            rr = retry_missing_reviewers()
            if rr['updated']:
                log.info(f"Reviewer retry: {rr['updated']}/{rr['checked']} filled in")
                for d in rr['details']:
                    log.info(f"  {d}")
        except Exception:
            log.exception("Reviewer retry failed")

        # Look up missing book authors via OpenAlex
        try:
            from integrity_check import retry_missing_book_authors
            ba = retry_missing_book_authors()
            if ba['updated']:
                log.info(f"Book author retry: {ba['updated']}/{ba['checked']} filled in")
                for d in ba['details']:
                    log.info(f"  {d}")
        except Exception:
            log.exception("Book author retry failed")

        # Tier 1 filter: drop popular/fringe-source reviews of books we don't
        # have evidence are philosophy (no other DB review by the same author
        # of a book sharing 2+ significant title words).
        try:
            from tier1_filter import cleanup_tier1
            t1 = cleanup_tier1()
            tier1_removed = t1.get('deleted', 0)
            if tier1_removed:
                log.info(f"Tier 1 filter: removed {tier1_removed}/{t1['checked']} reviews")
        except Exception:
            log.exception("Tier 1 filter failed")

        # Title reconciliation: auto-apply Category A (pure case/punctuation
        # variants — cannot merge distinct works), and surface Category B
        # (subtitle add/remove) as a review-only report for manual approval.
        try:
            from reconcile_titles import run_weekly, write_catb_report
            rec = run_weekly(db_path=db.DB_PATH)
            if rec["catA_applied"]:
                log.info(f"Title reconcile: auto-applied {rec['catA_applied']} "
                         f"case/punctuation fixes across {rec['catA_books']} books")
            if rec["catB"]:
                rep = os.path.join(ROOT, "reports",
                                   f"title_merges_{start_time:%Y-%m-%d}.html")
                write_catb_report(rec["catB"], rep)
                reconcile_report_path = rep
                log.info(f"Title reconcile: {rec['catB_books']} subtitle-merge "
                         f"proposals for review -> {rep}")
        except Exception:
            log.exception("Title reconciliation failed")

        # Recompute net_new now that post-processing (integrity check + Tier 1
        # filter) has removed false positives. The admin email should report
        # the post-cut numbers, not the raw scrape count.
        after = count_reviews()
        net_new = after - before
        log.info(f"Net new after post-processing: {net_new} (Tier 1 removed {tier1_removed})")

    # Sync
    sync_ok = True
    if not args.dry_run:
        sync_ok = sync_to_fly()
        if not sync_ok:
            errors.append(
                "Fly.io sync FAILED after retries — production was NOT updated this run. "
                "The local DB has the new entries; the next run re-scrapes the same window "
                "and retries the sync.")

    # Save state — only stamp last_run on a SUCCESSFUL sync. If the sync failed,
    # leaving last_run stale (a) lets the Monday watchdog flag the stale production
    # and (b) makes the next run re-scrape the same window and retry the upload.
    if not args.dry_run:
        state["last_new_count"] = net_new
        if sync_ok:
            state["last_run"] = datetime.now().isoformat()
        save_state(state)

    log.info("Update complete.")
    log.info("=" * 60)

    # --- Admin email ---
    if not args.dry_run:
        detail_lines = []
        if ndpr_stats:
            detail_lines.append(f"NDPR: {ndpr_stats.get('new', 0)} new")
        if dn_stats:
            detail_lines.append(f"Daily Nous: {dn_stats.get('uploaded', 0)} new")
        if unpop_stats:
            detail_lines.append(f"Unpopulist: {unpop_stats.get('new', 0)} new")
        if phil_now_stats:
            detail_lines.append(f"Philosophy Now: {phil_now_stats.get('uploaded', 0)} new")
        if ll_stats:
            detail_lines.append(f"Law & Liberty: {ll_stats.get('uploaded', 0)} new")
        if pd_stats:
            detail_lines.append(f"Public Discourse: {pd_stats.get('uploaded', 0)} new")
        if bjps_stats:
            detail_lines.append(f"BJPS Review: {bjps_stats.get('uploaded', 0)} new")
        if syndicate_stats:
            detail_lines.append(f"Syndicate: {syndicate_stats.get('uploaded', 0)} new")
        if crb_stats:
            detail_lines.append(f"CRB: {crb_stats.get('uploaded', 0)} new")
        if quillette_stats:
            detail_lines.append(f"Quillette: {quillette_stats.get('uploaded', 0)} new")
        if apa_stats:
            detail_lines.append(f"APA Blog: {apa_stats.get('uploaded', 0)} new")
        if tir_stats:
            detail_lines.append(f"The Independent Review: {tir_stats.get('uploaded', 0)} new")
        if rp_stats:
            detail_lines.append(f"Radical Philosophy: {rp_stats.get('uploaded', 0)} new")
        if mm_stats:
            detail_lines.append(f"Markets & Morality: {mm_stats.get('uploaded', 0)} new")
        if atlantic_stats:
            detail_lines.append(
                f"The Atlantic: {atlantic_stats.get('uploaded', 0)} new "
                f"({atlantic_stats.get('off_topic', 0)} off-topic skipped)")
        if crossref_stats:
            detail_lines.append(f"Crossref: {crossref_stats.get('new', 0)} new (across {crossref_stats.get('journals', 0)} journals)")
        if philosophia_symp_stats and (philosophia_symp_stats['inserted'] or philosophia_symp_stats['updated']):
            detail_lines.append(
                f"Philosophia symposia: {philosophia_symp_stats['inserted']} pieces added, "
                f"{philosophia_symp_stats['updated']} grouped")
        if mainstream_stats:
            detail_lines.append(f"Mainstream: {mainstream_stats.get('uploaded', 0)} new")
        if tier1_removed:
            detail_lines.append(f"Tier 1 filter removed: {tier1_removed} (non-philosophy)")
        if reconcile_report_path:
            detail_lines.append(
                f"Title merges to review: file://{reconcile_report_path}")
        detail_lines.append(f"Subscribers: {_get_subscriber_count()}")

        # Safety net: flag any newly added entries whose author/title fields still
        # look garbled or non-review after the corrective integrity pass, so they
        # can be spot-checked before they linger in production.
        try:
            from integrity_check import flag_suspect_recent
            flagged = flag_suspect_recent(since=start_utc)
            if flagged['count']:
                detail_lines.append(
                    f"⚠ {flagged['count']} suspect entr"
                    f"{'y' if flagged['count'] == 1 else 'ies'} needing review:")
                for s in flagged['suspects']:
                    detail_lines.append(
                        f"   [{s['id']}] {s['source']}: \"{s['title'][:55]}\" "
                        f"/ \"{s['author']}\"  ({', '.join(s['reasons'])})")
                    log.warning(f"SUSPECT [{s['id']}] {s['source']}: "
                                f"{s['title'][:60]!r} / {s['author']!r} <- {s['reasons']}")
        except Exception:
            log.exception("Suspect-entry flagging failed")

        # Interactive review queue: new entries are left at reviewed=0 for
        # Keep/Flag/Reject triage in the review app.
        try:
            import db as _db
            with _db._connect() as _c:
                pending = _c.execute(
                    "SELECT COUNT(*) FROM reviews WHERE reviewed = 0").fetchone()[0]
            if pending:
                detail_lines.append(
                    f"📋 Review queue: {pending} entr"
                    f"{'y' if pending == 1 else 'ies'} awaiting review — "
                    f"run `python3 review_app.py` then open http://localhost:8770")
            # Surface any still-open flags from previous reviews
            with _db._connect() as _c:
                open_flags = _c.execute(
                    "SELECT COUNT(*) FROM review_flags WHERE resolved = 0").fetchone()[0]
            if open_flags:
                detail_lines.append(f"🚩 {open_flags} flagged entr"
                                    f"{'y' if open_flags == 1 else 'ies'} awaiting Claude's attention")
        except Exception:
            log.exception("Review-queue summary failed")

        # Collect full list of added reviews (Crossref already tracked;
        # for other scrapers, query by created_at)
        added_reviews = list(crossref_added)
        if net_new > len(added_reviews):
            conn = sqlite3.connect(db.DB_PATH)
            conn.row_factory = sqlite3.Row
            extra_rows = conn.execute(
                """SELECT book_title, book_author_first_name, book_author_last_name,
                          reviewer_first_name, reviewer_last_name, publication_source
                   FROM reviews WHERE created_at >= ? ORDER BY publication_source, book_title""",
                (start_utc,),
            ).fetchall()
            conn.close()
            existing_titles = {r["book_title"] for r in added_reviews}
            for row in extra_rows:
                if row["book_title"] not in existing_titles:
                    added_reviews.append({
                        "book_title": row["book_title"] or "?",
                        "book_author": " ".join(filter(None, [
                            row["book_author_first_name"] or "",
                            row["book_author_last_name"] or "",
                        ])),
                        "journal": row["publication_source"] or "",
                        "reviewer": " ".join(filter(None, [
                            row["reviewer_first_name"] or "",
                            row["reviewer_last_name"] or "",
                        ])),
                    })

        # Generate the HTML spot-check report for entries added this run
        report_path = None
        try:
            from generate_weekly_report import generate_report
            report_path = generate_report(since=start_utc)
            log.info(f"Weekly report written: {report_path}")
        except Exception:
            log.exception("Failed to generate weekly report")

        try:
            send_run_summary("weekly update", {
                "before": before,
                "after": after,
                "details": "\n".join(detail_lines) or "(no scrapers produced output)",
                "errors": errors,
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "added_reviews": added_reviews,
                "report_path": report_path,
            })
        except Exception:
            log.exception("Failed to send email notification")

    # Note: subscriber digest runs separately on Monday 8 AM via LaunchAgent


if __name__ == "__main__":
    main()
