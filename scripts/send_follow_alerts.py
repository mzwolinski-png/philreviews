#!/usr/bin/env python3
"""
Send per-book / per-author alert emails to followers.

Runs in the weekly pipeline after the DB sync: fetches verified follows from
the Fly.io subscribers DB (same fly-ssh path as send_digest), matches them
against reviews added locally since the run's start marker, and sends one
email per follower listing all their hits. Quiet weeks send nothing.

Matching (uses db.norm_follow_value — diacritic-stripped lowercase alnum):
- author follow: the normalized follow value must appear as a token substring
  of the normalized "first last" author string (so joined multi-author
  strings like "Stephen Macedo and Frances Lee" match a "Frances Lee" follow).
- book follow: normalized titles must match exactly, or one must extend the
  other at a word boundary (handles subtitle-present/absent variants).
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db
from notify import send_follow_alert

log = logging.getLogger("follow_alerts")


def _get_follows_from_fly():
    try:
        script = (
            "import sqlite3, json; "
            "c=sqlite3.connect('/data/subscribers.db'); "
            "c.row_factory=sqlite3.Row; "
            "rows=c.execute('SELECT email,follow_type,follow_value,norm_value,token "
            "FROM follows WHERE verified=1').fetchall(); "
            "print(json.dumps([dict(r) for r in rows]))"
        )
        result = subprocess.run(
            ["fly", "ssh", "console", "-a", "philreviews", "-C",
             f'python3 -c "{script}"'],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            # last line guards against ssh banner noise
            return json.loads(result.stdout.strip().splitlines()[-1])
    except Exception as e:
        log.warning(f"Failed to fetch follows from Fly.io: {e}")
    return []


def _title_matches(norm_follow: str, norm_title: str) -> bool:
    if not norm_follow or not norm_title:
        return False
    if norm_follow == norm_title:
        return True
    longer, shorter = ((norm_title, norm_follow)
                       if len(norm_title) >= len(norm_follow)
                       else (norm_follow, norm_title))
    return len(shorter) >= 10 and longer.startswith(shorter + " ")


def _author_matches(norm_follow: str, norm_author: str) -> bool:
    if not norm_follow or not norm_author:
        return False
    return f" {norm_follow} " in f" {norm_author} "


def match_follows(follows, reviews):
    """Group matches per email: {email: [{follow_value, follow_type, token,
    reviews: [...]}]}."""
    per_email = {}
    for f in follows:
        hits = []
        for r in reviews:
            author = " ".join(filter(None, [r.get("book_author_first_name"),
                                            r.get("book_author_last_name")]))
            if f["follow_type"] == "book":
                ok = _title_matches(f["norm_value"],
                                    db.norm_follow_value(r.get("book_title")))
            else:
                ok = _author_matches(f["norm_value"], db.norm_follow_value(author))
            if ok:
                link = r.get("review_link") or (
                    f"https://doi.org/{r['doi']}" if r.get("doi") else
                    "https://philreviews.org")
                hits.append({
                    "book_title": r.get("book_title", ""),
                    "book_author": author,
                    "reviewer": " ".join(filter(None, [r.get("reviewer_first_name"),
                                                       r.get("reviewer_last_name")])),
                    "journal": r.get("publication_source", ""),
                    "link": link,
                })
        if hits:
            per_email.setdefault(f["email"], []).append({
                "follow_value": f["follow_value"],
                "follow_type": f["follow_type"],
                "token": f["token"],
                "reviews": hits,
            })
    return per_email


def send_follow_alerts(since_utc: str, dry_run: bool = False) -> dict:
    """Match follows against reviews created since `since_utc`; send alerts.
    Returns {'follows': n, 'emails_sent': n, 'matches': n}."""
    follows = _get_follows_from_fly()
    if not follows:
        log.info("No verified follows — skipping alerts.")
        return {"follows": 0, "emails_sent": 0, "matches": 0}

    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    reviews = [dict(r) for r in conn.execute(
        """SELECT book_title, book_author_first_name, book_author_last_name,
                  reviewer_first_name, reviewer_last_name, publication_source,
                  review_link, doi
           FROM reviews WHERE created_at >= ? AND entry_type != 'symposium'""",
        (since_utc,)).fetchall()]
    conn.close()

    per_email = match_follows(follows, reviews)
    sent = 0
    total = sum(len(m["reviews"]) for ms in per_email.values() for m in ms)
    for email, matches in per_email.items():
        if dry_run:
            log.info(f"[dry-run] would alert {email}: "
                     f"{sum(len(m['reviews']) for m in matches)} match(es)")
            continue
        if send_follow_alert(email, matches):
            sent += 1
    log.info(f"Follow alerts: {len(follows)} follows, {total} matches, "
             f"{sent} emails sent{' (dry run)' if dry_run else ''}")
    return {"follows": len(follows), "emails_sent": sent, "matches": total}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    since = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") \
        else "1970-01-01"
    print(send_follow_alerts(since, dry_run="--apply" not in sys.argv))
