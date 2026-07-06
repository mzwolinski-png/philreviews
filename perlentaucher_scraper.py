#!/usr/bin/env python3
"""Ingest German-press PHILOSOPHY book reviews via Perlentaucher.

Perlentaucher (perlentaucher.de) aggregates ~112k "Rezensionsnotizen" — short
free German-language digests of book reviews that appeared in the major German
feuilletons (FAZ, Süddeutsche Zeitung, Die Zeit, NZZ, Frankfurter Rundschau,
taz, …). Each book page lists one note per reviewing paper. No English index
covers this, and almost every one of these outlets is otherwise paywalled.

Discovery uses Perlentaucher's own human-curated philosophy topic taxonomy
(/buchKSL/philosophie-*.html, …) as the seed list, so we don't have to gate all
112k notes. Each candidate book is confirmed + subfield-classified once by the
Atlantic Haiku relevance gate (it handles German), then every note on that book
becomes a row attributed to its paper.

Usage:
  python3 perlentaucher_scraper.py --max-topics 2 --dry-run   # pilot
  python3 perlentaucher_scraper.py                            # full philosophy sweep
"""
import argparse
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

BASE = "https://www.perlentaucher.de"
TOPIC_SITEMAP = "http://www.perlentaucher.de/cdata/sitemap/buecher-themengebiete.xml"
ROUNDUP_SITEMAP = "http://www.perlentaucher.de/cdata/sitemap/buecherschauen.xml"
SACHBUCH_SITEMAP = "http://www.perlentaucher.de/cdata/sitemap/sachbuch.xml"
DEEP_STATE = os.path.join(ROOT, "scripts", "perlentaucher_deep_state.json")
UA = "PhilReviews/2.0 (+https://philreviews.org; book-review index; mailto:mzwolinski@sandiego.edu)"
DELAY = 2.0  # polite crawl delay for a small independent site

# Topic slugs (under /buchKSL/) whose books are in philosophy's orbit. We still
# gate each book, so a loose filter only costs a few wasted gate calls.
PHIL_TOPIC_RE = re.compile(
    r"philosoph|ethik|moral|aesthetik|ästhetik|erkenntnis|metaphysik|"
    r"politische-theorie|staatstheorie|gesellschaftstheorie|geistesgeschichte|"
    r"freiheit-aufklaerung|beethoven-hegel-hoelderlin|"
    r"stichwort-(aesthetik|ethik|moral|geist|bewusstsein|metaphysik|aufklaerung|"
    r"nietzsche|ludwig-wittgenstein|hegel|kant|heidegger)",
    re.I,
)


def _get(session, url, tries=3):
    for i in range(tries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                time.sleep(10 * (i + 1)); continue
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if i == tries - 1:
                print(f"  fetch failed {url}: {e}")
                return None
            time.sleep(3 * (i + 1))
    return None


def philosophy_topic_urls(session):
    """Philosophy-domain topic pages from the topic sitemap."""
    xml = _get(session, TOPIC_SITEMAP) or ""
    slugs = re.findall(r'(buchKSL/[^<]+\.html)', xml)
    return [f"{BASE}/{s}" for s in slugs if PHIL_TOPIC_RE.search(s)]


def book_urls_on_topic(session, topic_url):
    html = _get(session, topic_url) or ""
    return [f"{BASE}{m}" for m in
            dict.fromkeys(re.findall(r'href="(/buch/[^"]+\.html)"', html))]


def recent_book_urls(session, days):
    """Book URLs from the most recent `days` daily roundups (weekly mode).
    Roundups list every genre, so the gate does the philosophy filtering."""
    xml = _get(session, ROUNDUP_SITEMAP) or ""
    dates = re.findall(r'(buecherschau/\d{4}-\d{2}-\d{2}\.html)', xml)  # newest first
    urls = []
    for d in dates[:days]:
        urls += book_urls_on_topic(session, f"{BASE}/{d}")
    return list(dict.fromkeys(urls))


def sachbuch_book_urls(session):
    """Every non-fiction (/buch/) page from the Sachbuch sitemap (~32k). The
    deep philosophy seam: includes politics/history/science/philosophy, gate
    filters to philosophy."""
    xml = _get(session, SACHBUCH_SITEMAP) or ""
    # Force https so links match the topic/recent modes' links (else a book seen
    # in both modes would dedup-miss and double-insert).
    return [re.sub(r"^http://", "https://", m)
            for m in re.findall(r'<loc>\s*(https?://[^<]+/buch/[^<]+\.html)\s*</loc>', xml)]


def _load_deep_index():
    try:
        import json
        with open(DEEP_STATE) as f:
            return int(json.load(f).get("index", 0))
    except Exception:
        return 0


def _save_deep_index(i):
    import json
    os.makedirs(os.path.dirname(DEEP_STATE), exist_ok=True)
    with open(DEEP_STATE, "w") as f:
        json.dump({"index": i}, f)


# Paper name stops at the first tag ([^<]+) so it can't bleed across notes; the
# date sits in either <a>…</a> (linked to that day's roundup) or <span>…</span>.
_PAPER_DATE = re.compile(
    r'<h3 class="newspaper">\s*Rezensionsnotiz zu\s*([^<]+?),\s*'
    r'<(?:a|span)[^>]*>(\d{2}\.\d{2}\.\d{4})</(?:a|span)>\s*</h3>\s*'
    r'<div class="paragraph">(.*?)</div>',
    re.S)


def _strip(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


_CAP = r"[A-ZÄÖÜ][\wäöüßé.-]+"
_PART = r"(?:von|van|de|del|della|di|du|der|zu|ter)"
_NAME = rf"({_CAP}(?:\s+{_PART})?\s+{_CAP})"  # First (particle) Last — 2-3 tokens


def _reviewer(note_text):
    """Best-effort reviewer name from a German note. Blank if unsure.
    German papers decline the role word (Rezensent/-in/-en/-ten), so match
    the stem; otherwise fall back to a name that opens the note before a comma."""
    m = re.search(rf"Rezensent\w*\s+{_NAME}", note_text)
    if not m:
        m = re.match(rf"{_NAME},\s", note_text)
    if not m:
        return "", ""
    parts = re.sub(r"[.,]+$", "", m.group(1)).split()
    return " ".join(parts[:-1]), parts[-1]


def parse_book_page(html, book_url):
    """-> dict(title, author_first, author_last, blurb, notes[]) or None."""
    tm = re.search(r"<title>(.*?)\s*-\s*Perlentaucher\s*</title>", html, re.S)
    if not tm:
        return None
    head = re.sub(r"\s+", " ", tm.group(1)).strip()
    if ":" not in head:
        return None
    author_str, title = head.split(":", 1)
    title = title.strip().rstrip(".")
    # Full co-author list: strip editor markers ((Hg.), (Hrsg.)), normalize
    # '/' and 'und' separators to 'A, B and C' (keeping only the first author
    # used to strand co-editors, and raw '(Hg.) / ' strings polluted fields).
    author_str = re.sub(r"\s*\((?:Hg|Hrsg|Hgg|Bearb)\.?\)\s*", " ", author_str)
    parts = [p.strip() for p in re.split(r"\s*/\s*|,|\bund\b|;", author_str) if p.strip()]
    joined = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1] if parts else ""
    ap = joined.split()
    af, al = (" ".join(ap[:-1]), ap[-1]) if len(ap) > 1 else ("", joined)

    blurb = ""
    bm = re.search(r"Klappentext\s*</[^>]+>(.*?)(?:BuchLink|Rezensionsnotiz|<div class=\"box)", html, re.S)
    if bm:
        blurb = _strip(bm.group(1))[:600]

    notes = []
    for paper, date, body in _PAPER_DATE.findall(html):
        d, m_, y = date.split(".")
        rf, rl = _reviewer(_strip(body))
        notes.append({
            "paper": re.sub(r"\s+", " ", paper).strip(),
            "date": f"{y}-{m_}-{d}",
            "summary": _strip(body),
            "rev_first": rf, "rev_last": rl,
        })
    if not notes:
        return None
    return {"title": title, "author_first": af, "author_last": al,
            "blurb": blurb, "notes": notes}


def run(dry_run=False, max_topics=None, recent_days=None, deep=False, delay=DELAY):
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    st = {"topics": 0, "books_found": 0, "books_gated": 0, "books_relevant": 0,
          "already_in_db": 0, "notes_inserted": 0, "samples": []}

    deep_base = 0
    if deep:         # deep mode: full Sachbuch (non-fiction) list, resumable
        full = sachbuch_book_urls(session)
        deep_base = _load_deep_index()
        book_list = full[deep_base:]
        print(f"  Perlentaucher DEEP: {len(full)} non-fiction books, "
              f"resuming at {deep_base} ({len(book_list)} to go)")
    elif recent_days:  # weekly mode: discover via recent daily roundups
        book_list = recent_book_urls(session, recent_days)
        print(f"  Perlentaucher: {len(book_list)} books in last {recent_days} roundups")
    else:            # backfill mode: sweep the philosophy topic taxonomy
        topics = philosophy_topic_urls(session)
        if max_topics:
            topics = topics[:max_topics]
        st["topics"] = len(topics)
        book_list = []
        for turl in topics:
            book_list += book_urls_on_topic(session, turl)
        book_list = list(dict.fromkeys(book_list))
        print(f"  Perlentaucher: {len(topics)} philosophy topics -> {len(book_list)} books")

    for _i, burl in enumerate(book_list):
            if deep and _i % 50 == 0:
                _save_deep_index(deep_base + _i)
            st["books_found"] += 1
            time.sleep(delay)
            html = _get(session, burl)
            if not html:
                continue
            book = parse_book_page(html, burl)
            if not book:
                continue
            # Pre-compute note links; if none are new, skip the (paid) gate so
            # weekly re-runs only spend gate calls on genuinely new books.
            for n in book["notes"]:
                n["link"] = f"{burl}#{n['paper'].replace(' ', '')[:12]}-{n['date']}"
            new_notes = [n for n in book["notes"] if not db.review_link_exists(n["link"])]
            st["already_in_db"] += len(book["notes"]) - len(new_notes)
            if not new_notes:
                continue
            st["books_gated"] += 1
            author_disp = (book["author_first"] + " " + book["author_last"]).strip()
            relevant, prim, sec = relevance_gate(book["title"], author_disp, book["blurb"])
            if not relevant:
                continue
            st["books_relevant"] += 1
            to_add = []
            for n in new_notes:
                link = n["link"]
                to_add.append({
                    "book_title": book["title"],
                    "book_author_first_name": book["author_first"],
                    "book_author_last_name": book["author_last"],
                    "reviewer_first_name": n["rev_first"],
                    "reviewer_last_name": n["rev_last"],
                    "publication_source": n["paper"],
                    "publication_date": n["date"],
                    "review_link": link,
                    "review_summary": n["summary"],
                    "access_type": "Open",
                    "entry_type": "review",
                    "subfield_primary": prim,
                    "subfield_secondary": sec,
                })
            if to_add and not dry_run:
                db.insert_reviews(to_add)
            st["notes_inserted"] += len(to_add)
            if to_add and len(st["samples"]) < 20:
                st["samples"].append(
                    f"{book['title'][:42]} — {author_disp} [{len(to_add)} notes: "
                    f"{', '.join(sorted({n['paper'] for n in book['notes']}))[:60]}] {prim or '?'}")
    if deep:
        _save_deep_index(deep_base + len(book_list))  # mark cursor complete
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-topics", type=int, help="cap topic pages (pilot)")
    ap.add_argument("--recent", type=int, metavar="DAYS",
                    help="weekly mode: scan the last DAYS daily roundups instead of topics")
    ap.add_argument("--deep", action="store_true",
                    help="deep mode: crawl the full ~32k Sachbuch list (resumable via state file)")
    ap.add_argument("--delay", type=float, default=DELAY)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    st = run(dry_run=a.dry_run, max_topics=a.max_topics,
             recent_days=a.recent, deep=a.deep, delay=a.delay)
    print("\n=== Perlentaucher philosophy backfill ===")
    for k in ("topics", "books_found", "books_gated", "books_relevant",
              "already_in_db", "notes_inserted"):
        print(f"  {k}: {st[k]}")
    print("samples:")
    for s in st["samples"]:
        print("  +", s)
