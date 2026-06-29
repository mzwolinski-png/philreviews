#!/usr/bin/env python3
"""
Send weekly digest emails to verified subscribers.

Queries reviews with publication_date in the last 14 days, groups by
subscriber preferences (subfield filters), and sends personalized digests.

Usage:
    python3 scripts/send_digest.py          # normal run
    python3 scripts/send_digest.py --dry-run  # preview without sending
"""

import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db
from notify import send_email, FROM_PUBLIC, BASE_URL
from scraper_base import setup_logging

log = setup_logging("send_digest", os.path.join(ROOT, "scripts", "send_digest.log"))

SUBFIELD_NAMES = {
    "ethics": "Ethics & Moral Philosophy",
    "applied-ethics": "Applied & Professional Ethics",
    "political": "Political & Social Philosophy",
    "legal": "Philosophy of Law",
    "epistemology": "Epistemology & Philosophy of Mind",
    "metaphysics": "Metaphysics & Logic",
    "science": "Philosophy of Science",
    "aesthetics": "Aesthetics & Philosophy of Art",
    "religion": "Philosophy of Religion & Theology",
    "history": "History of Philosophy",
    "ancient": "Ancient & Medieval Philosophy",
    "modern": "Early Modern Philosophy",
    "continental": "Continental & Phenomenological",
    "feminist": "Feminist Philosophy",
    "non-western": "Non-Western & Comparative",
}


def _get_subscribers_from_fly():
    """Fetch verified subscribers from the Fly.io subscribers DB."""
    import subprocess, json
    try:
        script = (
            "import sqlite3, json; "
            "c=sqlite3.connect('/data/subscribers.db'); "
            "c.row_factory=sqlite3.Row; "
            "rows=c.execute('SELECT id,email,token,subfields FROM subscribers WHERE verified=1').fetchall(); "
            "print(json.dumps([dict(r) for r in rows]))"
        )
        result = subprocess.run(
            ["fly", "ssh", "console", "-a", "philreviews", "-C",
             f'python3 -c "{script}"'],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout.strip())
    except Exception as e:
        log.warning(f"Failed to fetch subscribers from Fly.io: {e}")
    return []


def get_recent_reviews(days=14):
    """Get reviews with publication_date in the last N days."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    import sqlite3
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT book_title, book_author_first_name, book_author_last_name,
                  reviewer_first_name, reviewer_last_name,
                  publication_source, review_link, doi,
                  subfield_primary, subfield_secondary, publication_date
           FROM reviews
           WHERE publication_date >= ?
           ORDER BY publication_date DESC, id DESC""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def filter_for_subscriber(reviews, subfields_str):
    """Filter reviews to match subscriber's subfield preferences."""
    if subfields_str == "all":
        return reviews
    wanted = set(subfields_str.split(","))
    return [
        r for r in reviews
        if (r.get("subfield_primary") or "") in wanted
        or (r.get("subfield_secondary") or "") in wanted
    ]


def format_digest(reviews, token):
    """Format a plain-text digest email."""
    lines = [
        "PhilReviews Weekly Digest",
        f"{datetime.now():%B %d, %Y}",
        "",
        f"{len(reviews)} new review{'s' if len(reviews) != 1 else ''} published this week:",
        "",
    ]

    # Group by journal
    by_journal = {}
    for r in reviews:
        j = r.get("publication_source", "Unknown")
        by_journal.setdefault(j, []).append(r)

    for journal in sorted(by_journal.keys()):
        lines.append(f"--- {journal} ---")
        for r in by_journal[journal]:
            title = r.get("book_title", "?")
            author = " ".join(filter(None, [
                r.get("book_author_first_name", ""),
                r.get("book_author_last_name", ""),
            ]))
            reviewer = " ".join(filter(None, [
                r.get("reviewer_first_name", ""),
                r.get("reviewer_last_name", ""),
            ]))
            link = r.get("review_link") or ""
            if not link and r.get("doi"):
                link = f"https://doi.org/{r['doi']}"

            entry = f"  {title}"
            if author:
                entry += f" by {author}"
            if reviewer:
                entry += f" (rev. {reviewer})"
            lines.append(entry)
            if link:
                lines.append(f"    {link}")
        lines.append("")

    lines.extend([
        "---",
        f"Browse all reviews: {BASE_URL}",
        f"Unsubscribe: {BASE_URL}/unsubscribe?token={token}",
    ])

    return "\n".join(lines)


def main():
    dry_run = "--dry-run" in sys.argv

    log.info("=" * 50)
    log.info(f"Weekly digest — {datetime.now():%Y-%m-%d %H:%M}")

    reviews = get_recent_reviews(days=14)
    log.info(f"Reviews published in last 14 days: {len(reviews)}")

    if not reviews:
        log.info("No recent reviews — skipping digest.")
        return

    subscribers = _get_subscribers_from_fly()
    log.info(f"Verified subscribers: {len(subscribers)}")

    if not subscribers:
        log.info("No subscribers — skipping.")
        return

    sent = 0

    for sub in subscribers:
        filtered = filter_for_subscriber(reviews, sub["subfields"])
        if not filtered:
            log.info(f"  {sub['email']}: 0 matching reviews, skipping")
            continue

        body = format_digest(filtered, sub["token"])
        subfield_label = sub["subfields"] if sub["subfields"] != "all" else "all subfields"
        subject = f"[PhilReviews] {len(filtered)} new review{'s' if len(filtered) != 1 else ''} this week"

        if dry_run:
            log.info(f"  [DRY RUN] {sub['email']}: {len(filtered)} reviews ({subfield_label})")
        else:
            ok = send_email(subject, body, to=sub["email"], from_addr=FROM_PUBLIC)
            if ok:
                sent += 1
                log.info(f"  Sent to {sub['email']}: {len(filtered)} reviews ({subfield_label})")
            else:
                log.warning(f"  Failed to send to {sub['email']}")

    log.info(f"Digest complete: {sent} emails sent")


if __name__ == "__main__":
    main()
