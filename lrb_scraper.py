#!/usr/bin/env python3
"""Ingest PHILOSOPHY book reviews from the London Review of Books (metadata only).

The LRB paywalls review *bodies*, but everything an index needs is public:
per-issue TOCs (/the-paper/vNN/nNN, back to v01/n01 = Oct 1979), and on each
article page a structured "reviewed items" header (one div per book: title,
subtitle, author) plus the reviewer in og:title ("Reviewer · Article Title").
robots.txt permits generic crawlers (only /Media/ etc. disallowed); the named
blocks target AI-training bots, which this is not — it's an indexing crawler
that links every entry back to lrb.co.uk.

Walks issues newest-first with a resumable (volume, issue) cursor. Essays are
skipped free (no reviewed-items block = no gate call); each reviewed book is
gated separately (multi-book reviews yield one row per book, fragment-suffixed
links for uniqueness). access_type = "Paywalled".

Usage:
  python3 lrb_scraper.py --max-issues 1 --dry-run    # pilot (newest issue)
  python3 lrb_scraper.py --max-issues 24             # ~1 year
  python3 lrb_scraper.py                             # full archive (resumable)
"""
import argparse
import html as htmllib
import json
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

import db
from atlantic_scraper import relevance_gate

BASE = "https://www.lrb.co.uk"
SOURCE = "London Review of Books"
UA = "PhilReviews/2.0 (+https://philreviews.org; book-review index; mailto:mzwolinski@sandiego.edu)"
DELAY = 1.5
STATE = os.path.join(ROOT, "scripts", "lrb_state.json")
NEWEST_VOLUME = 48   # bumped by discover_newest() at runtime
MAX_ISSUE = 24       # issues per volume never exceed this

MONTHS = {m: i + 1 for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"])}

_REVIEWED_ITEM = re.compile(
    r'<div class="article-reviewed-item[^"]*">(.*?)</div>', re.S)
_ITEM_TITLE = re.compile(r'<span class="article-reviewed-item-title">(.*?)</span>', re.S)
_ITEM_SUBTITLE = re.compile(r'<span class="article-reviewed-item-subtitle">(.*?)</span>', re.S)
_ITEM_BY = re.compile(r"<span class='by'>.*?<a[^>]*>(.*?)</a>", re.S)


class NetworkDown(RuntimeError):
    """Raised when fetches fail for network (not HTTP-404) reasons.

    Must abort the run rather than return None: a None is treated as a genuine
    gap in issue numbering and skipped forever, so returning it during a DNS/
    network outage would silently drop whole issues past the cursor."""


def _get(session, url, tries=4):
    for i in range(tries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 403):
                time.sleep(15 * (i + 1)); continue
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if i == tries - 1:
                raise NetworkDown(f"{url}: {e}")
            time.sleep(10 * (i + 1))  # 10,20,30s — ride out short outages
    raise NetworkDown(f"{url}: retries exhausted")


def _txt(s):
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", s))).strip("  ")


def _norm_title(t):
    return re.sub(r"[^a-z0-9]", "", (t or "").lower())[:24]


def _already_indexed(book):
    """True if this review is already in the DB from an earlier import.
    Old imports used different URL formats (no www, no /the-paper, different
    slugs), so link dedup misses them. Match instead on the same LRB
    reviewer + book author + normalized-title prefix (author+reviewer alone is
    too coarse: e.g. Nagel reviewed several Scanlon books in the LRB)."""
    with db._connect() as conn:
        rows = conn.execute(
            "SELECT book_title FROM reviews WHERE publication_source=? AND "
            "book_author_last_name=? AND reviewer_last_name=?",
            (SOURCE, book["author_last"], book["rev_last"])).fetchall()
    mine = _norm_title(book["book_title"])
    return bool(mine) and any(
        _norm_title(r[0]) and (_norm_title(r[0]).startswith(mine) or
                               mine.startswith(_norm_title(r[0])))
        for r in rows)


def _load_state():
    try:
        with open(STATE) as f:
            d = json.load(f)
            return d["volume"], d["issue"]
    except Exception:
        return None, None


def _save_state(vol, issue):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump({"volume": vol, "issue": issue}, f)


def discover_newest(session):
    """Newest (volume, issue) from the archive page."""
    html = _get(session, f"{BASE}/archive") or ""
    pairs = [(int(v), int(n)) for v, n in
             re.findall(r'href="/the-paper/v(\d+)/n(\d+)"', html)]
    return max(pairs) if pairs else (NEWEST_VOLUME, MAX_ISSUE)


def issue_walk(newest, state=None):
    """Yield (volume, issue) newest-first, resuming after `state` if given."""
    vol, num = newest
    started = state is None or state == (None, None)
    while vol >= 1:
        while num >= 1:
            if started:
                yield vol, num
            elif (vol, num) == state:
                started = True  # resume from the saved position (re-check it)
                yield vol, num
            num -= 1
        vol -= 1
        num = MAX_ISSUE


def parse_issue(session, vol, num):
    """-> (date, [article urls]) or None if the issue doesn't exist."""
    html = _get(session, f"{BASE}/the-paper/v{vol:02d}/n{num:02d}")
    if html is None:
        return None
    date = ""
    tm = re.search(r"<title>[^<]*?(\d{1,2})\s+(" + "|".join(MONTHS) + r")\s+(\d{4})", html)
    if tm:
        date = f"{tm.group(3)}-{MONTHS[tm.group(2)]:02d}-{int(tm.group(1)):02d}"
    urls = list(dict.fromkeys(
        f"{BASE}{u}" for u in
        re.findall(rf'href="(/the-paper/v{vol:02d}/n{num:02d}/[a-z0-9.-]+/[a-z0-9-]+)"', html)))
    return date, urls


def parse_article(html, url, date):
    """-> list of per-book dicts (empty for essays)."""
    ot = re.search(r'<meta property="og:title" content="([^"]*)"', html)
    rf = rl = ""
    if ot and "·" in htmllib.unescape(ot.group(1)):
        reviewer = htmllib.unescape(ot.group(1)).split("·")[0].strip()
        rp = reviewer.split()
        rf, rl = (" ".join(rp[:-1]), rp[-1]) if len(rp) > 1 else ("", reviewer)
    out = []
    for i, item in enumerate(_REVIEWED_ITEM.findall(html)):
        t = _ITEM_TITLE.search(item)
        if not t:
            continue
        title = _txt(t.group(1)).rstrip(":").strip()
        sub = _ITEM_SUBTITLE.search(item)
        if sub:
            subtitle = _txt(sub.group(1))
            if subtitle:
                title = f"{title}: {subtitle}"
        by = _ITEM_BY.search(item)
        af = al = ""
        if by:
            first_auth = re.split(r",| and |&", _txt(by.group(1)))[0].strip()
            ap = first_auth.split()
            af, al = (" ".join(ap[:-1]), ap[-1]) if len(ap) > 1 else ("", first_auth)
        link = url if i == 0 else f"{url}#book-{i + 1}"
        out.append({"book_title": title, "author_first": af, "author_last": al,
                    "rev_first": rf, "rev_last": rl, "date": date, "link": link})
    return out


def run(dry_run=False, max_issues=None, delay=DELAY, resume=True, save_cursor=True):
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    st = {"issues": 0, "articles": 0, "reviews": 0, "books_seen": 0,
          "already_in_db": 0, "gate_checked": 0, "gate_pass": 0,
          "inserted": 0, "samples": []}
    newest = discover_newest(session)
    state = _load_state() if resume else (None, None)
    print(f"  LRB: newest issue v{newest[0]}/n{newest[1]}"
          + (f", resuming at v{state[0]}/n{state[1]}" if state and state[0] else ""))
    to_add = []
    for vol, num in issue_walk(newest, state):
        if max_issues and st["issues"] >= max_issues:
            break
        time.sleep(delay)
        parsed = parse_issue(session, vol, num)
        if parsed is None:
            continue  # gap in numbering
        st["issues"] += 1
        if save_cursor:  # weekly newest-issue scans must not clobber the backfill cursor
            _save_state(vol, num)
        date, urls = parsed
        for url in urls:
            if db.review_link_exists(url):
                st["already_in_db"] += 1; continue
            time.sleep(delay)
            html = _get(session, url)
            if not html:
                continue
            st["articles"] += 1
            books = parse_article(html, url, date)
            if not books:
                continue  # essay — no gate cost
            st["reviews"] += 1
            for b in books:
                st["books_seen"] += 1
                if db.review_link_exists(b["link"]) or _already_indexed(b):
                    st["already_in_db"] += 1; continue
                author_disp = (b["author_first"] + " " + b["author_last"]).strip()
                st["gate_checked"] += 1
                relevant, prim, sec = relevance_gate(b["book_title"], author_disp, "")
                if not relevant:
                    continue
                st["gate_pass"] += 1
                to_add.append({
                    "book_title": b["book_title"],
                    "book_author_first_name": b["author_first"],
                    "book_author_last_name": b["author_last"],
                    "reviewer_first_name": b["rev_first"], "reviewer_last_name": b["rev_last"],
                    "publication_source": SOURCE, "publication_date": b["date"],
                    "review_link": b["link"],
                    "access_type": "Paywalled", "entry_type": "review",
                    "subfield_primary": prim, "subfield_secondary": sec,
                })
                if len(st["samples"]) < 20:
                    st["samples"].append(
                        f"{b['book_title'][:46]} — {author_disp} (rev. {b['rev_first']} {b['rev_last']}) [{b['date']}] {prim or '?'}")
                if not dry_run and len(to_add) >= 25:
                    db.insert_reviews(to_add); st["inserted"] += len(to_add); to_add = []
    if to_add and not dry_run:
        db.insert_reviews(to_add); st["inserted"] += len(to_add)
    elif dry_run:
        st["inserted"] = f"(dry-run: {len(to_add)} would insert)"
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-issues", type=int, help="cap issues this run")
    ap.add_argument("--no-resume", action="store_true", help="start from the newest issue")
    ap.add_argument("--delay", type=float, default=DELAY)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    st = run(dry_run=a.dry_run, max_issues=a.max_issues, delay=a.delay,
             resume=not a.no_resume)
    print("\n=== LRB philosophy backfill ===")
    for k in ("issues", "articles", "reviews", "books_seen", "already_in_db",
              "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {st[k]}")
    print("samples (gate-passed):")
    for s in st["samples"]:
        print("  +", s)
