#!/usr/bin/env python3
"""Email notification for PhilReviews automated runs."""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("notify")

GMAIL_USER = "mzwolinski@gmail.com"  # SMTP login
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
NOTIFY_TO = os.getenv("NOTIFY_TO", "mzwolinski@gmail.com")
FROM_INTERNAL = GMAIL_USER  # admin notifications
FROM_PUBLIC = "PhilReviews <updates@philreviews.org>"  # subscriber-facing emails


def send_email(subject: str, body: str, to: str = None, from_addr: str = None,
               content_type: str = "plain", html_body: str = None):
    """Send an email. Fails silently with a log warning.

    When html_body is given, sends multipart/alternative (plain `body` as
    fallback + the HTML part) — clients that render HTML show the styled
    version, everything else gets the text.

    Subscriber-facing mail (From: updates@philreviews.org) goes via Resend,
    which DKIM-signs as philreviews.org — required since Yahoo/AT&T (and
    increasingly others) block unaligned mail. Gmail send-as signs d=gmail.com,
    which fails DMARC alignment for our From-domain, and our own DMARC
    p=quarantine then instructs receivers to junk it. Admin mail stays on
    Gmail. Falls back to Gmail if RESEND_API_KEY is unset so nothing breaks.
    """
    to = to or NOTIFY_TO
    from_addr = from_addr or GMAIL_USER
    use_resend = bool(RESEND_API_KEY) and "philreviews.org" in from_addr

    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
    else:
        msg = MIMEText(body, content_type)
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to

    try:
        if use_resend:
            with smtplib.SMTP_SSL("smtp.resend.com", 465, timeout=30) as smtp:
                smtp.login("resend", RESEND_API_KEY)
                smtp.send_message(msg)
        else:
            if not GMAIL_APP_PASSWORD:
                log.warning("GMAIL_APP_PASSWORD not set — skipping email notification")
                return False
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
                smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
                smtp.send_message(msg)
        log.info(f"Email sent{' via Resend' if use_resend else ''}: {subject}")
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
        f"Please confirm your email by opening the link below and clicking the\n"
        f"confirm button on that page:\n\n"
        f"  {verify_url}\n\n"
        f"If you didn't sign up, you can safely ignore this email.\n\n"
        f"-- PhilReviews\n"
        f"https://philreviews.org\n"
    )
    return send_email(subject, body, to=email, from_addr=FROM_PUBLIC)


def send_follow_verification_email(email: str, token: str, what: str):
    """Double opt-in for a new-review alert from an address we don't know yet."""
    verify_url = f"{BASE_URL}/follow/verify?token={token}"
    subject = "Confirm your PhilReviews alert"
    body = (
        f"You asked to be emailed when PhilReviews indexes {what}.\n\n"
        f"Please confirm by opening the link below and clicking the confirm\n"
        f"button on that page:\n\n"
        f"  {verify_url}\n\n"
        f"If you didn't request this, you can safely ignore this email.\n\n"
        f"-- PhilReviews\n"
        f"https://philreviews.org\n"
    )
    return send_email(subject, body, to=email, from_addr=FROM_PUBLIC)


def send_follow_alert(email: str, matches: list):
    """Alert one follower about this week's matching reviews.

    matches: list of dicts with keys follow_value, follow_type, token, and
    reviews (list of dicts: book_title, book_author, reviewer, journal, link).
    """
    n = sum(len(m["reviews"]) for m in matches)
    if n == 1:
        r = matches[0]["reviews"][0]
        subject = f"New review of {r['book_title']}"
    else:
        subject = f"{n} new reviews of books you follow"
    lines = ["New reviews matching your PhilReviews alerts:", ""]
    for m in matches:
        for r in m["reviews"]:
            entry = f"• {r['book_title']}"
            if r.get("book_author"):
                entry += f" by {r['book_author']}"
            lines.append(entry)
            detail = "   reviewed"
            if r.get("reviewer"):
                detail += f" by {r['reviewer']}"
            if r.get("journal"):
                detail += f" in {r['journal']}"
            lines.append(detail)
            if r.get("link"):
                lines.append(f"   {r['link']}")
            lines.append("")
    lines.append("—")
    for m in matches:
        what = (f"“{m['follow_value']}”" if m["follow_type"] == "book"
                else f"books by {m['follow_value']}")
        lines.append(f"Stop alerts for {what}: {BASE_URL}/unfollow?token={m['token']}")
    lines += ["", "PhilReviews — https://philreviews.org"]

    import html as _html
    _e = lambda s: _html.escape(str(s or ""))
    navy, rule, grey = "#1a3a5c", "#d7e1ea", "#4a5a68"
    items = []
    for m in matches:
        for r in m["reviews"]:
            link = _e(r.get("link", ""))
            t = (f'<a href="{link}" style="color:{navy};text-decoration:none;'
                 f'border-bottom:1px solid {rule};">{_e(r["book_title"])}</a>'
                 if link else _e(r["book_title"]))
            by = f' <span style="color:{grey};">by {_e(r["book_author"])}</span>' \
                 if r.get("book_author") else ""
            det = " · ".join(filter(None, [
                f"Reviewed by {_e(r['reviewer'])}" if r.get("reviewer") else "",
                _e(r.get("journal", ""))]))
            items.append(
                f'<div style="margin:0 0 14px;"><div style="font-size:15.5px;'
                f'line-height:1.45;">{t}{by}</div>'
                f'<div style="font-family:Helvetica,Arial,sans-serif;font-size:12.5px;'
                f'color:{grey};margin-top:2px;">{det}</div></div>')
    unfollows = " &nbsp;&middot;&nbsp; ".join(
        f'<a href="{BASE_URL}/unfollow?token={_e(m["token"])}" style="color:{grey};">'
        f'Stop alerts for {_e(m["follow_value"])}</a>' for m in matches)
    html_body = (
        f'<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f4f7fa;">'
        f'<div style="max-width:640px;margin:0 auto;padding:24px 16px;'
        f"font-family:Georgia,'Times New Roman',serif;color:#22303a;\">"
        f'<div style="text-align:center;padding:18px 0 6px;">'
        f'<a href="{BASE_URL}" style="text-decoration:none;">'
        f'<span style="font-size:26px;font-weight:bold;color:{navy};letter-spacing:0.5px;">'
        f'Phil<span style="color:#7a93ab;">Reviews</span></span></a>'
        f'<div style="font-family:Helvetica,Arial,sans-serif;font-size:12px;color:{grey};'
        f'text-transform:uppercase;letter-spacing:2px;margin-top:6px;">Review Alert</div></div>'
        f'<div style="border-top:2px solid {navy};margin:14px 0 18px;"></div>'
        f'<p style="font-size:15px;margin:0 0 20px;color:{grey};text-align:center;">'
        f'New review{"s" if n != 1 else ""} matching your alerts</p>'
        + "".join(items) +
        f'<div style="border-top:1px solid {rule};margin:26px 0 14px;"></div>'
        f'<div style="text-align:center;font-family:Helvetica,Arial,sans-serif;'
        f'font-size:12.5px;color:{grey};line-height:2;">{unfollows}</div>'
        f'</div></body></html>')
    return send_email(subject, "\n".join(lines), to=email, from_addr=FROM_PUBLIC,
                      html_body=html_body)


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
    """Send a semi-annual reminder to refresh the two manual academic-DB pulls:
    Philosopher's Index (EBSCO) and JSTOR metadata (DfR). Batched on the same
    Jan 1 / Jul 1 cadence."""
    subject = "[PhilReviews] Semi-annual reminder: download Philosopher's Index + JSTOR data"
    body = (
        f"Semi-annual data-refresh reminder — {datetime.now():%Y-%m-%d}\n\n"
        f"Two manual academic-DB pulls to do together:\n\n"
        f"=== 1. Philosopher's Index (EBSCO) ===\n"
        f"  1. Log in to EBSCO / Philosopher's Index\n"
        f"  2. Export new reviews as CSV\n"
        f"  3. Run: cd ~/PhilReview && python3 pi_import.py <csv_files>\n"
        f"  4. Check logs for match/insert counts\n\n"
        f"=== 2. JSTOR metadata (same DfR-style export as the Feb 2026 import) ===\n"
        f"  1. Download a fresh JSTOR metadata archive for the philosophy journals\n"
        f"     (metadata only, no full text). Scope it to fill gaps (e.g. Journal of\n"
        f"     Aesthetic Education, currently 0) and recent years — the moving wall\n"
        f"     advances each cycle, releasing newly-available issues.\n"
        f"  2. Hand the file to Claude to (re)build/run the JSTOR importer — the\n"
        f"     original one-off script was never saved to the repo; Claude parses the\n"
        f"     archive, dedups against existing rows (by DOI/link), classifies, loads.\n\n"
        f"Both fill reviewer names + metadata Crossref lacks (Review of Metaphysics,\n"
        f"PDCNET/JSTOR journals) and extend coverage as the JSTOR moving wall advances.\n"
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
