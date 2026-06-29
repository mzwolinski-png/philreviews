#!/usr/bin/env python3
"""Email notification for PhilReviews automated runs."""

import logging
import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("notify")

GMAIL_USER = "mzwolinski@gmail.com"  # SMTP login
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
NOTIFY_TO = os.getenv("NOTIFY_TO", "mzwolinski@gmail.com")
FROM_INTERNAL = GMAIL_USER  # admin notifications
FROM_PUBLIC = "PhilReviews <updates@philreviews.org>"  # subscriber-facing emails


def send_email(subject: str, body: str, to: str = None, from_addr: str = None,
               content_type: str = "plain"):
    """Send an email via Gmail SMTP. Fails silently with a log warning."""
    to = to or NOTIFY_TO
    if not GMAIL_APP_PASSWORD:
        log.warning("GMAIL_APP_PASSWORD not set — skipping email notification")
        return False

    msg = MIMEText(body, content_type)
    msg["Subject"] = subject
    msg["From"] = from_addr or GMAIL_USER
    msg["To"] = to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info(f"Email sent: {subject}")
        return True
    except Exception as e:
        log.warning(f"Email failed: {e}")
        return False


BASE_URL = "https://philreviews.org"


def send_verification_email(email: str, token: str):
    """Send a double opt-in verification email to a new subscriber."""
    verify_url = f"{BASE_URL}/verify?token={token}"
    subject = "Confirm your PhilReviews subscription"
    body = (
        f"Thanks for subscribing to PhilReviews weekly digest!\n\n"
        f"Please confirm your email by clicking the link below:\n\n"
        f"  {verify_url}\n\n"
        f"If you didn't sign up, you can safely ignore this email.\n\n"
        f"-- PhilReviews\n"
        f"https://philreviews.org\n"
    )
    return send_email(subject, body, to=email, from_addr=FROM_PUBLIC)


def send_run_summary(script_name: str, stats: dict):
    """Format and send a summary email for a weekly run.

    stats should contain:
        before (int): review count before run
        after (int): review count after run
        details (str): per-scraper breakdown
        errors (list[str]): any errors encountered
        duration_s (float): runtime in seconds
        added_reviews (list[dict]): new reviews added, each with
            book_title, book_author, journal, reviewer (all optional)
    """
    net_new = stats.get("after", 0) - stats.get("before", 0)
    errors = stats.get("errors", [])
    duration = stats.get("duration_s", 0)
    mins = int(duration // 60)
    secs = int(duration % 60)

    status = "OK" if not errors else f"{len(errors)} error(s)"
    subject = f"[PhilReviews] {script_name}: +{net_new} reviews ({status})"

    lines = [
        f"PhilReviews {script_name} — {datetime.now():%Y-%m-%d %H:%M}",
        f"",
        f"Reviews before: {stats.get('before', '?'):,}",
        f"Reviews after:  {stats.get('after', '?'):,}",
        f"Net new:        {net_new:,}",
        f"Runtime:        {mins}m {secs}s",
        f"",
        f"--- Details ---",
        stats.get("details", "(none)"),
    ]

    if errors:
        lines += ["", "--- Errors ---"] + errors

    # Link to the local HTML report (for spot-checking accuracy of new
    # entries). The detailed list of added reviews is now in the report
    # rather than the email body.
    report_path = stats.get("report_path")
    if report_path:
        lines += [
            "",
            f"--- New reviews report ---",
            f"file://{report_path}",
            f"(Open in browser. Each entry links to the live site and the original review.)",
        ]
    else:
        # Fallback for runs that didn't generate a report
        added = stats.get("added_reviews", [])
        if added:
            lines += ["", f"--- New reviews ({len(added)}) ---"]
            for r in added:
                title = r.get("book_title", "?")
                author = r.get("book_author", "")
                journal = r.get("journal", "")
                reviewer = r.get("reviewer", "")
                entry = f"  • {title}"
                if author:
                    entry += f" by {author}"
                if reviewer:
                    entry += f" (rev. {reviewer})"
                if journal:
                    entry += f"  [{journal}]"
                lines.append(entry)

    send_email(subject, "\n".join(lines))


def send_monthly_pi_reminder():
    """Send a semi-annual reminder to download new data from Philosopher's Index."""
    subject = f"[PhilReviews] Semi-annual reminder: download Philosopher's Index data"
    body = (
        f"Semi-annual PI download reminder — {datetime.now():%Y-%m-%d}\n\n"
        f"Time to check EBSCO for new Philosopher's Index exports.\n\n"
        f"Steps:\n"
        f"  1. Log in to EBSCO / Philosopher's Index\n"
        f"  2. Export new reviews as CSV\n"
        f"  3. Run: cd ~/PhilReview && python3 pi_import.py <csv_files>\n"
        f"  4. Check logs for match/insert counts\n\n"
        f"This fills in reviewer names and other metadata that Crossref lacks\n"
        f"(e.g. Review of Metaphysics, PDCNET journals).\n"
    )
    return send_email(subject, body)


def send_missed_run_alert(script_name: str, last_run: str):
    """Alert that a scheduled weekly run appears to have been missed."""
    subject = f"[PhilReviews] MISSED RUN: {script_name}"
    body = (
        f"The scheduled {script_name} run appears to have been missed.\n"
        f"Last successful run: {last_run}\n"
        f"Current time: {datetime.now():%Y-%m-%d %H:%M}\n\n"
        f"The machine may have been asleep during the scheduled window.\n"
        f"Run manually with:\n"
        f"  cd ~/PhilReview && python3 update.py\n"
        f"  cd ~/PhilReview && python3 scripts/weekly_update.py\n"
    )
    send_email(subject, body)
