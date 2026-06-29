#!/usr/bin/env python3
"""Philosophia book-symposium detector.

Philosophia runs an ongoing "Book Symposium" series. Each symposium consists of:
  - a "Précis of <Book>" by the book's author,
  - several commentary pieces by other philosophers, and
  - an author's reply ("Replies to <names>" / "Reply to Critics/Commentators" /
    "<Book>: Replies to ...").

These are typed as journal articles in Crossref, not reviews, so the journal's
``italic_only`` detection mode skips the précis and reply entirely and groups
nothing. This module fetches a rolling window of Philosophia items, reconstructs
each symposium (book + author + all pieces, with [Précis]/[Author's Reply]
labels), and upserts them into the DB grouped under a stable ``symposium_group``.

Design notes / conservatism:
  - A symposium is anchored on a clear "Précis of X" OR a reply whose title
    embeds the book ("<Book>: Replies to ..."). Nothing else anchors a group.
  - Commentaries are admitted only when high-precision: the commentator's surname
    is named in the reply title, OR the commentary title mentions the book
    author's surname / a distinctive book-title word, within the time window.
  - Idempotent: dedup by DOI; existing rows are re-grouped, missing ones inserted.

Run weekly from update.py via ``run(window_months=24)``; standalone self-test
validates against a cached metadata file.
"""

import os
import re
import sys
import json
import html
import time
import sqlite3
import unicodedata
import datetime
import urllib.parse
import urllib.request
from collections import defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import db  # noqa: E402

PHILOSOPHIA_ISSN = "0048-3893"
SOURCE = "Philosophia"

_PRECIS_RE = re.compile(r"^\s*(?:pr[ée]cis)\s+(?:of\s+)?(.+)$", re.I)
_REPLY_RE = re.compile(r"\b(?:repl(?:y|ies)|response[s]?)\b", re.I)
_REPLY_TO_RE = re.compile(r"\b(?:to|on)\b\s+(.+)$", re.I)
_CORR_RE = re.compile(r"^\s*(?:correction|publisher correction|erratum|retraction)\b", re.I)
# reply title that embeds the book, e.g. "The Hope and Horror of Physicalism: Replies to ..."
# or "Reflections on Choosing Well: Reply to Commentators"
_REPLY_BOOK_COLON_RE = re.compile(r"^(.*?):\s*(?:repl(?:y|ies)|response[s]?)\b", re.I)
_REPLY_BOOK_ON_RE = re.compile(r"\bon\s+(.+?):\s*(?:repl(?:y|ies)|response[s]?)\b", re.I)


def _norm(s):
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def _clean_title(raw):
    """Strip HTML/italic tags and collapse whitespace, preserving display text."""
    t = re.sub(r"<[^>]+>", "", raw or "")
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def _split_name(full):
    p = full.split()
    return (" ".join(p[:-1]), p[-1]) if p else ("", "")


def _surname(full):
    return _split_name(full)[1].lower()


def _reviewer_fields(authors):
    """Dual-author convention: 'First1 Last1 and First2' / 'Last2'."""
    if len(authors) >= 2:
        f1, l1 = _split_name(authors[0])
        f2, _ = _split_name(authors[1])
        return f"{f1} {l1} and {f2}".strip(), _split_name(authors[1])[1]
    if authors:
        return _split_name(authors[0])
    return "", ""


def _slug(book):
    s = _norm(book)
    return "-".join(s.split()[:5]) or "symposium"


def _strip_book_noise(book, author_names):
    """Remove a leading author-name prefix and trailing publisher/year parenthetical
    from an extracted book title ("Précis of <author> <book> (Publisher, Year)")."""
    b = _clean_title(book)
    b = re.sub(r"\s*\([^)]*\b(?:19|20)\d\d[^)]*\)\s*$", "", b)  # (Springer, 2022)
    b = re.sub(r"\s*\([^)]*\bpress\b[^)]*\)\s*$", "", b, flags=re.I)
    b = re.sub(r"^the book[:,]?\s+", "", b, flags=re.I)  # "Précis of the Book: <Title>"
    for name in author_names:
        first, last = _split_name(name)
        for pref in (f"{first} {last}", name, last):
            if pref and b.lower().startswith(pref.lower()):
                b = b[len(pref):].lstrip(" ,:-")
                break
    return b.strip(" ,.:-")


def _parse_reply_commentators(reply_title):
    """Extract commentator surnames named in a reply title.
    'Replies to Fassio, Schleifer McCormick, Finlay, and Schmidt' -> surnames."""
    m = _REPLY_TO_RE.search(reply_title)
    if not m:
        return []
    tail = m.group(1)
    tail = re.split(r"[:(]", tail)[0]
    parts = re.split(r",|\band\b|&", tail)
    surnames = []
    for p in parts:
        p = p.strip().strip(".")
        if not p:
            continue
        # last token of each name fragment is the surname
        surnames.append(p.split()[-1].lower())
    return [s for s in surnames if len(s) > 2]


# ---- fetch -----------------------------------------------------------------

def fetch_items(window_months=24, mailto="mzwolinski@sandiego.edu"):
    since = (datetime.date.today() - datetime.timedelta(days=int(window_months * 30.5))).isoformat()
    out = []
    cursor = "*"
    base = f"https://api.crossref.org/journals/{PHILOSOPHIA_ISSN}/works"
    while True:
        params = {
            "filter": f"from-pub-date:{since}",
            "select": "DOI,title,author,published,page",
            "rows": "1000", "cursor": cursor, "mailto": mailto,
        }
        url = base + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "philreviews/1.0"})
        data = json.load(urllib.request.urlopen(req, timeout=90))["message"]
        items = data["items"]
        if not items:
            break
        for m in items:
            out.append({
                "doi": m.get("DOI"),
                "title": _clean_title((m.get("title") or [""])[0]),
                "authors": [(a.get("given", "") + " " + a.get("family", "")).strip()
                            for a in m.get("author", [])],
                "date": m.get("published", {}).get("date-parts", [[None]])[0],
                "page": m.get("page"),
            })
        cursor = data.get("next-cursor")
        if len(items) < 1000:
            break
        time.sleep(0.2)
    return out


def _date_tuple(d):
    if not d or d[0] is None:
        return None
    return (d[0], d[1] if len(d) > 1 else 1, d[2] if len(d) > 2 else 1)


def _months_apart(a, b):
    ta, tb = _date_tuple(a), _date_tuple(b)
    if not ta or not tb:
        return 999
    return abs((ta[0] - tb[0]) * 12 + (ta[1] - tb[1]))


# ---- detection -------------------------------------------------------------

def detect_symposia(items, window_months_cluster=15):
    """Return a list of symposium dicts:
    {book, book_author_first, book_author_last, slug, year, pieces:[...]}.
    Each piece: {doi, role, reviewer_first, reviewer_last, date, authors}."""
    items = [it for it in items if it.get("doi") and not _CORR_RE.match(it["title"])]
    by_doi = {it["doi"]: it for it in items}

    precis_anchors = []   # (item, book)
    reply_book_anchors = []  # (item, book) for reply-embedded-book symposia
    for it in items:
        title = it["title"]
        pm = _PRECIS_RE.match(title)
        if pm and it["authors"]:
            book = _strip_book_noise(pm.group(1), it["authors"])
            if len(book) >= 4:
                precis_anchors.append((it, book))
            continue
        if _REPLY_RE.search(title):
            # Only the "...on <Book>: Reply..." form is reliable ("Reflections on
            # Choosing Well: Reply to Commentators"). The bare "<X>: Replies to ..."
            # form mis-fires on reply titles that aren't the book ("Our Normative
            # Nature: Replies to ..."), so it's deliberately excluded.
            mb = _REPLY_BOOK_ON_RE.search(title)
            if mb and it["authors"]:
                book = _strip_book_noise(mb.group(1), it["authors"])
                if len(book) >= 6 and "repl" not in book.lower():
                    reply_book_anchors.append((it, book))

    symposia = []
    used_dois = set()

    def build(anchor_item, book, anchor_is_precis):
        ba_first, ba_last = _split_name(anchor_item["authors"][0])
        adate = anchor_item["date"]
        # find the reply by the same author (book author)
        reply = None
        for it in items:
            if it["doi"] == anchor_item["doi"]:
                continue
            if _surname(it["authors"][0] if it["authors"] else "") == ba_last.lower() \
               and _REPLY_RE.search(it["title"]) \
               and _months_apart(it["date"], adate) <= window_months_cluster:
                reply = it
                break
        named = _parse_reply_commentators(reply["title"]) if reply else []
        bl = ba_last.lower()

        pieces = []
        # anchor
        if anchor_is_precis:
            pieces.append((anchor_item, "precis"))
        else:
            pieces.append((anchor_item, "reply"))  # reply-embedded-book: anchor IS the reply
        if reply and reply["doi"] != anchor_item["doi"]:
            pieces.append((reply, "reply"))

        # Count how many window pieces each first-author surname has, to detect
        # ambiguous surnames (two different authors sharing a surname, where the
        # reply only names the surname — e.g. Eva Schmidt vs Elke Schmidt).
        surname_counts = defaultdict(int)
        for it in items:
            if it["authors"]:
                surname_counts[_surname(it["authors"][0])] += 1
        book_words = {w for w in _norm(book).split() if len(w) >= 6}

        # commentaries
        for it in items:
            d = it["doi"]
            if d in (anchor_item["doi"], reply["doi"] if reply else None):
                continue
            if _months_apart(it["date"], adate) > window_months_cluster:
                continue
            asurn = _surname(it["authors"][0]) if it["authors"] else ""
            if asurn == bl:
                continue  # same author as book author -> not a commentary
            twords = set(_norm(it["title"]).split())
            # High-precision only: the commentator is named in the reply, OR the
            # commentary title contains the book author's surname ("On Doris's
            # Character Trouble"). Book-title-word overlap is too noisy (common
            # philosophy words match unrelated articles) and is intentionally unused.
            author_named = bool(bl) and bl in twords
            named_hit = asurn in named
            # Disambiguate shared surnames: if the named surname belongs to >1
            # author in the window, only accept the piece that actually engages
            # the book (its title names the book author or a distinctive book word).
            if named_hit and surname_counts.get(asurn, 0) > 1:
                if not (author_named or (book_words & twords)):
                    named_hit = False
            if named_hit or author_named:
                pieces.append((it, "commentary"))

        # dedupe pieces by doi, keep first role
        seen = {}
        for it, role in pieces:
            if it["doi"] not in seen:
                seen[it["doi"]] = (it, role)
        final = list(seen.values())
        if len(final) < 3:
            return None  # not a genuine multi-piece symposium
        year = (adate[0] if adate and adate[0] else 0)
        out_pieces = []
        for it, role in final:
            rf, rl = _reviewer_fields(it["authors"])
            out_pieces.append({
                "doi": it["doi"], "role": role,
                "reviewer_first": rf, "reviewer_last": rl,
                "date": _date_tuple(it["date"]), "authors": it["authors"],
            })
        return {
            "book": book, "book_author_first": ba_first, "book_author_last": ba_last,
            "slug": _slug(book), "year": year, "pieces": out_pieces,
        }

    for it, book in precis_anchors:
        s = build(it, book, True)
        if s and not any(p["doi"] in used_dois for p in s["pieces"]):
            symposia.append(s)
            used_dois.update(p["doi"] for p in s["pieces"])

    for it, book in reply_book_anchors:
        if it["doi"] in used_dois:
            continue
        s = build(it, book, False)
        if s and not any(p["doi"] in used_dois for p in s["pieces"]):
            symposia.append(s)
            used_dois.update(p["doi"] for p in s["pieces"])

    return symposia


# ---- upsert ----------------------------------------------------------------

_LABEL = {"precis": " [Précis]", "reply": " [Author's Reply]", "commentary": ""}


def upsert(symposia, conn=None, dry_run=False):
    own = conn is None
    if own:
        conn = db._connect()
    conn.row_factory = sqlite3.Row
    inserted = updated = 0
    ins_sql = """INSERT OR IGNORE INTO reviews
        (book_title,book_author_first_name,book_author_last_name,
         reviewer_first_name,reviewer_last_name,publication_source,publication_date,
         review_link,doi,entry_type,symposium_group,reviewed)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,1)"""
    for s in symposia:
        # Reuse an existing group if any piece is already grouped (protects manually
        # curated symposia: we only ADD to them, never re-slug or override them).
        existing_groups = []
        present = {}
        for p in s["pieces"]:
            row = conn.execute(
                "SELECT id, symposium_group, entry_type FROM reviews WHERE lower(doi)=?",
                (p["doi"].lower(),)).fetchone()
            present[p["doi"]] = row
            if row and row["symposium_group"]:
                existing_groups.append(row["symposium_group"])
        canonical = existing_groups[0] if existing_groups else f"{SOURCE}|{s['year']}|{s['slug']}"
        for p in s["pieces"]:
            title = s["book"] + _LABEL[p["role"]]
            d = p["date"]
            date = f"{d[0]:04d}-{d[1]:02d}-{d[2]:02d}" if d else None
            row = present[p["doi"]]
            if row:
                # Never override an existing group; only fill in ungrouped rows.
                if row["symposium_group"]:
                    continue
                if not dry_run:
                    conn.execute(
                        """UPDATE reviews SET book_title=?,book_author_first_name=?,
                           book_author_last_name=?,reviewer_first_name=?,reviewer_last_name=?,
                           entry_type='symposium',symposium_group=? WHERE id=?""",
                        (title, s["book_author_first"], s["book_author_last"],
                         p["reviewer_first"], p["reviewer_last"], canonical, row["id"]))
                updated += 1
            else:
                if not dry_run:
                    conn.execute(ins_sql, (
                        title, s["book_author_first"], s["book_author_last"],
                        p["reviewer_first"], p["reviewer_last"], SOURCE, date,
                        f"https://doi.org/{p['doi']}", p["doi"], "symposium", canonical))
                inserted += 1
    if not dry_run:
        conn.commit()
    if own:
        conn.close()
    return {"inserted": inserted, "updated": updated, "symposia": len(symposia)}


def run(window_months=24, dry_run=False):
    items = fetch_items(window_months=window_months)
    symposia = detect_symposia(items)
    res = upsert(symposia, dry_run=dry_run)
    res["books"] = sorted(s["book"][:50] for s in symposia)
    return res


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        # validate against a cached metadata file
        path = sys.argv[sys.argv.index("--selftest") + 1]
        raw = json.load(open(path))
        items = [{"doi": d, "title": _clean_title(m["title"]), "authors": m["authors"],
                  "date": m["date"], "page": m.get("page")} for d, m in raw.items()]
        symp = detect_symposia(items)
        print(f"Detected {len(symp)} symposia:")
        for s in sorted(symp, key=lambda x: -x["year"]):
            roles = defaultdict(int)
            for p in s["pieces"]:
                roles[p["role"]] += 1
            print(f"  [{s['year']}] {s['book'][:48]:50} by {s['book_author_last']:14} "
                  f"{len(s['pieces'])} pieces ({dict(roles)})")
    else:
        res = run(window_months=int(sys.argv[1]) if len(sys.argv) > 1 else 24,
                  dry_run="--dry-run" in sys.argv)
        print(res)
