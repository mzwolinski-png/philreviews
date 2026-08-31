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
    """Fetch verified subscribers from the Fly.io subscribers DB.

    Returns a list on success, or None if the fetch FAILED. The distinction
    matters: on 2026-08-03 the `fly ssh console` call timed out at 30s, this
    returned [], and main() read that as "no subscribers" and exited 0 — the
    weekly digest silently never went out and nothing alerted. `fly ssh` has
    to bring up a session against a possibly-cold machine, so 30s was simply
    too tight; it now retries with a generous timeout and reports failure.
    """
    import subprocess, json, time
    script = (
        "import sqlite3, json; "
        "c=sqlite3.connect('/data/subscribers.db'); "
        "c.row_factory=sqlite3.Row; "
        "rows=c.execute('SELECT id,email,token,subfields FROM subscribers WHERE verified=1').fetchall(); "
        "print(json.dumps([dict(r) for r in rows]))"
    )
    last_err = None
    for attempt in (1, 2, 3):
        try:
            result = subprocess.run(
                ["fly", "ssh", "console", "-a", "philreviews", "-C",
                 f'python3 -c "{script}"'],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0 and result.stdout.strip():
                # tolerate SSH banner noise before the JSON payload
                for line in reversed(result.stdout.strip().splitlines()):
                    line = line.strip()
                    if line.startswith("["):
                        return json.loads(line)
            last_err = (result.stderr or result.stdout or "empty response").strip()[:200]
        except Exception as e:
            last_err = str(e)[:200]
        log.warning(f"Subscriber fetch attempt {attempt}/3 failed: {last_err}")
        if attempt < 3:
            time.sleep(15)
    log.error(f"Could not fetch subscribers from Fly.io after 3 attempts: {last_err}")
    return None


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


def _h(s):
    """HTML-escape."""
    import html as _html
    return _html.escape(str(s or ""))


def format_digest_html(reviews, token):
    """Styled HTML version of the digest (sent alongside the plain-text
    fallback). Email-client constraints: inline styles only, no web fonts
    (Georgia/serif approximates the site's Libre Baskerville), single-column
    layout that survives Gmail/Outlook/Apple Mail."""
    navy = "#1a3a5c"
    rule = "#d7e1ea"
    grey = "#4a5a68"

    by_journal = {}
    for r in reviews:
        by_journal.setdefault(r.get("publication_source", "Unknown"), []).append(r)

    parts = [f"""\
<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#f4f7fa;">
<div style="max-width:640px;margin:0 auto;padding:24px 16px;
     font-family:Georgia,'Times New Roman',serif;color:#22303a;">
  <div style="text-align:center;padding:18px 0 6px;">
    <a href="{BASE_URL}" style="text-decoration:none;">
      <span style="font-size:26px;font-weight:bold;color:{navy};
            letter-spacing:0.5px;">Phil<span style="color:#7a93ab;">Reviews</span></span>
    </a>
    <div style="font-family:Helvetica,Arial,sans-serif;font-size:12px;color:{grey};
         text-transform:uppercase;letter-spacing:2px;margin-top:6px;">
      Weekly Digest &middot; {datetime.now():%B %-d, %Y}</div>
  </div>
  <div style="border-top:2px solid {navy};margin:14px 0 18px;"></div>
  <p style="font-size:15px;margin:0 0 20px;color:{grey};text-align:center;">
    {len(reviews)} new review{'s' if len(reviews) != 1 else ''} of philosophy books this week</p>
"""]

    for journal in sorted(by_journal.keys()):
        parts.append(f"""\
  <div style="font-family:Helvetica,Arial,sans-serif;font-size:12px;color:{navy};
       text-transform:uppercase;letter-spacing:1.5px;font-weight:bold;
       border-bottom:1px solid {rule};padding-bottom:4px;margin:22px 0 10px;">
    {_h(journal)}</div>
""")
        for r in by_journal[journal]:
            title = _h(r.get("book_title", "?"))
            author = " ".join(filter(None, [r.get("book_author_first_name", ""),
                                            r.get("book_author_last_name", "")]))
            reviewer = " ".join(filter(None, [r.get("reviewer_first_name", ""),
                                              r.get("reviewer_last_name", "")]))
            link = r.get("review_link") or (
                f"https://doi.org/{r['doi']}" if r.get("doi") else "")
            title_html = (f'<a href="{_h(link)}" style="color:{navy};'
                          f'text-decoration:none;border-bottom:1px solid {rule};">'
                          f'{title}</a>' if link else title)
            byline = f' <span style="color:{grey};">by {_h(author)}</span>' if author else ""
            rev_line = (f'<div style="font-family:Helvetica,Arial,sans-serif;'
                        f'font-size:12.5px;color:{grey};margin-top:2px;">'
                        f'Reviewed{" by " + _h(reviewer) if reviewer else ""}</div>'
                        if reviewer else "")
            parts.append(f"""\
  <div style="margin:0 0 12px;">
    <div style="font-size:15.5px;line-height:1.45;">{title_html}{byline}</div>
    {rev_line}
  </div>
""")

    parts.append(f"""\
  <div style="border-top:1px solid {rule};margin:26px 0 14px;"></div>
  <div style="text-align:center;font-family:Helvetica,Arial,sans-serif;
       font-size:12.5px;color:{grey};line-height:2;">
    <a href="{BASE_URL}" style="color:{navy};">Browse all reviews</a>
    &nbsp;&middot;&nbsp;
    <a href="{BASE_URL}/unsubscribe?token={token}" style="color:{grey};">Unsubscribe</a><br>
    You're receiving this because you subscribed at philreviews.org.
  </div>
</div>
</body></html>""")
    return "".join(parts)


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
    if subscribers is None:
        # Infrastructure failure, NOT an empty list — alert instead of exiting 0
        msg = ("The weekly subscriber digest did NOT go out: the subscriber list "
               "could not be fetched from Fly.io after 3 attempts.\n\n"
               f"Reviews ready to send: {len(reviews)}\n"
               "Log: scripts/send_digest.log\n\n"
               "Re-run manually once Fly is reachable:\n"
               "  cd ~/PhilReview && python3 scripts/send_digest.py\n")
        log.error("Aborting: subscriber fetch failed.")
        try:
            send_email("[PhilReviews] ALERT: weekly digest did not send", msg)
        except Exception:
            log.exception("Could not send failure alert")
        sys.exit(1)

    log.info(f"Verified subscribers: {len(subscribers)}")
    if not subscribers:
        log.info("No verified subscribers — nothing to send.")
        return

    sent = 0

    for sub in subscribers:
        filtered = filter_for_subscriber(reviews, sub["subfields"])
        if not filtered:
            log.info(f"  {sub['email']}: 0 matching reviews, skipping")
            continue

        body = format_digest(filtered, sub["token"])
        html = format_digest_html(filtered, sub["token"])
        subfield_label = sub["subfields"] if sub["subfields"] != "all" else "all subfields"
        subject = f"[PhilReviews] {len(filtered)} new review{'s' if len(filtered) != 1 else ''} this week"

        if dry_run:
            log.info(f"  [DRY RUN] {sub['email']}: {len(filtered)} reviews ({subfield_label})")
        else:
            ok = send_email(subject, body, to=sub["email"], from_addr=FROM_PUBLIC,
                            html_body=html)
            if ok:
                sent += 1
                log.info(f"  Sent to {sub['email']}: {len(filtered)} reviews ({subfield_label})")
            else:
                log.warning(f"  Failed to send to {sub['email']}")

    log.info(f"Digest complete: {sent} emails sent")
    # A run that reaches here having sent nothing to a non-empty subscriber
    # list is also a failure worth surfacing.
    if not dry_run and subscribers and sent == 0:
        try:
            send_email("[PhilReviews] ALERT: weekly digest sent 0 emails",
                       f"{len(subscribers)} verified subscribers and {len(reviews)} "
                       f"reviews, but 0 emails were sent. See scripts/send_digest.log.")
        except Exception:
            log.exception("Could not send zero-send alert")


if __name__ == "__main__":
    main()
