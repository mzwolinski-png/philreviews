#!/usr/bin/env python3
"""Generate an HTML report of reviews added in the most recent weekly run.

Saved to ~/PhilReview/reports/weekly_report_YYYY-MM-DD.html.
Mimics the live site's review listing layout so entries can be spot-checked
for accuracy before they go out in the subscriber digest.
"""

import html
import os
import sqlite3
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import db

REPORTS_DIR = os.path.join(ROOT, "reports")


def _fmt_name(first, last):
    return " ".join(filter(None, [(first or "").strip(), (last or "").strip()]))


def _h(s):
    return html.escape(s or "")


def generate_report(since: str, until: str = None) -> str:
    """Generate an HTML report of reviews created between `since` and `until`.

    Args:
        since: ISO timestamp lower bound (e.g. '2026-05-17 05:00:00').
        until: ISO timestamp upper bound (defaults to now).

    Returns:
        Absolute path of the generated HTML file.
    """
    # created_at is stored in UTC (SQLite CURRENT_TIMESTAMP); compare in UTC.
    if until is None:
        until = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, book_title, book_author_first_name, book_author_last_name,
               reviewer_first_name, reviewer_last_name, publication_source,
               publication_date, review_link, doi, subfield_primary,
               subfield_secondary, entry_type
        FROM reviews
        WHERE created_at >= ? AND created_at <= ?
        ORDER BY publication_source, publication_date DESC, book_title
        """,
        (since, until),
    ).fetchall()
    conn.close()

    by_source = {}
    for r in rows:
        by_source.setdefault(r["publication_source"] or "(unknown source)", []).append(r)

    today = datetime.now().strftime("%Y-%m-%d")
    title = f"PhilReviews weekly report — {today}"
    body_parts = [
        f"<h1>{_h(title)}</h1>",
        f'<p class="meta">{len(rows)} reviews added between '
        f"{_h(since)} and {_h(until)}. "
        f'Click book titles to view on philreviews.org; '
        f'click "source ↗" to open the original review.</p>',
    ]

    for source in sorted(by_source, key=lambda s: (-len(by_source[s]), s)):
        entries = by_source[source]
        body_parts.append(
            f'<h2>{_h(source)} '
            f'<span class="count">({len(entries)})</span></h2>'
        )
        body_parts.append('<ul class="reviews">')
        for r in entries:
            ba = _fmt_name(r["book_author_first_name"], r["book_author_last_name"])
            rv = _fmt_name(r["reviewer_first_name"], r["reviewer_last_name"])
            title_str = r["book_title"] or "(no title)"
            link = r["review_link"] or ""
            pub_date = r["publication_date"] or ""
            entry_type = r["entry_type"] or ""

            # Link the book title to a site search by title (helps spot-check)
            search_url = (
                "https://philreviews.org/?search="
                + html.escape(title_str, quote=True).replace(" ", "+")
            )

            row_parts = [f'<li class="review" id="r{r["id"]}">']
            row_parts.append(
                f'<a class="book-title" href="{_h(search_url)}" target="_blank">'
                f'{_h(title_str)}</a>'
            )
            if entry_type and entry_type != "review":
                row_parts.append(
                    f' <span class="entry-type">[{_h(entry_type)}]</span>'
                )
            row_parts.append("<div class=\"detail\">")
            if ba:
                row_parts.append(f'by <span class="book-author">{_h(ba)}</span>')
            else:
                row_parts.append('<span class="missing">by ??</span>')
            if rv:
                row_parts.append(
                    f' &middot; reviewed by <span class="reviewer">{_h(rv)}</span>'
                )
            else:
                row_parts.append(' &middot; <span class="missing">reviewer ??</span>')
            row_parts.append(
                f' &middot; <span class="journal">{_h(source)}</span>'
            )
            if pub_date:
                row_parts.append(
                    f' &middot; <span class="pub-date">{_h(pub_date)}</span>'
                )
            if link:
                row_parts.append(
                    f' &middot; <a class="source-link" href="{_h(link)}" '
                    f'target="_blank">source ↗</a>'
                )
            row_parts.append('</div></li>')
            body_parts.append("".join(row_parts))
        body_parts.append("</ul>")

    css = """
    body { font-family: 'Source Sans 3', Helvetica, Arial, sans-serif; max-width: 900px;
           margin: 40px auto; padding: 0 20px; color: #222; line-height: 1.45; }
    h1 { font-family: 'Libre Baskerville', Georgia, serif; font-size: 28px;
         margin-bottom: 4px; }
    p.meta { color: #666; margin: 0 0 24px 0; font-size: 14px; }
    h2 { font-family: 'Libre Baskerville', Georgia, serif; font-size: 19px;
         margin-top: 32px; margin-bottom: 8px; padding-bottom: 4px;
         border-bottom: 1px solid #ddd; }
    h2 .count { color: #888; font-weight: normal; font-size: 14px; }
    ul.reviews { list-style: none; padding: 0; }
    li.review { margin-bottom: 14px; padding: 8px 10px; border-left: 3px solid #e5e5e5; }
    li.review:hover { background: #fafafa; border-left-color: #c33; }
    a.book-title { color: #1a3b6e; font-weight: 600; text-decoration: none; font-size: 16px; }
    a.book-title:hover { text-decoration: underline; }
    .detail { color: #555; font-size: 14px; margin-top: 2px; }
    .book-author { color: #333; }
    .reviewer { color: #333; }
    .missing { color: #c33; font-style: italic; }
    .pub-date { color: #888; }
    .journal { color: #1a3b6e; font-style: italic; }
    a.source-link { color: #1a3b6e; text-decoration: none; font-size: 13px; }
    a.source-link:hover { text-decoration: underline; }
    .entry-type { color: #888; font-size: 13px; font-style: italic; }
    """

    full_html = (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        f'<title>{_h(title)}</title>'
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:wght@400;700&family=Source+Sans+3:wght@400;600&display=swap" rel="stylesheet">'
        f"<style>{css}</style>"
        "</head><body>"
        + "".join(body_parts)
        + "</body></html>"
    )

    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, f"weekly_report_{today}.html")
    with open(path, "w") as f:
        f.write(full_html)
    return path


if __name__ == "__main__":
    # Default: report on reviews added in the last 7 days
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default=None,
                        help="ISO timestamp; defaults to 7 days ago")
    args = parser.parse_args()
    if args.since:
        since = args.since
    else:
        # created_at is UTC; use a UTC-based 7-day window.
        since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    path = generate_report(since=since)
    print(f"Report written: {path}")
