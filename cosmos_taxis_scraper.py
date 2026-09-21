#!/usr/bin/env python3
"""Cosmos + Taxis: reviews, critical notices and book symposia.

C+T deposits nothing with Crossref (checked: zero works under any spelling of
the container title), so the journal's own issue pages are the only route.

Each issue page is a flat run of PDF links. An anchor's text is the piece's
title and its parent's text is "Title AUTHOR", under ALL-CAPS or Title-Case
section headings:

    C+T 13:5+6                          <- volume and issue
    Symposium on
    Hayekian Systems: ...               <- the book under discussion
    Introduction
    SCOTT SCHEALL
    ...
    REVIEWS
    Should Werner Sombart Get Some Respect?
    CHRISTOPHER ADAIR-TOTEFF

Two format eras matter. Issues up to 2024 give contributor names in title case
and often name the book's author in the title ("... by Sanford Ikeda"); from
2025 names are set in caps and the book author is usually absent, which is why
older rows in the database have a book author and newer ones do not.

This replaces scrape_cosmos_taxis.py, which walked the page text rather than
the anchors. That lost the PDF URL every row uses as its review_link, took the
nav bar's "Reviews" tab as a section heading, and swept up the page trailer
("Authors Index", "Back Issues") and the In Memoriam notices as books.
"""

import logging
import re
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

import db

log = logging.getLogger(__name__)

BASE_URL = "https://cosmosandtaxis.org"
SOURCE = "Cosmos + Taxis"
FIRST_YEAR = 2013

# Sections whose contents are about a book. ARTICLES, EDITORIAL and IN MEMORIAM
# are the ones this exists to exclude.
REVIEW_SECTIONS = {
    "reviews", "book reviews", "review", "book review",
    "critical notices", "critical notice",
    "review essays", "review essay",
}
SKIP_SECTIONS = {
    "articles", "article", "editorial", "editorials", "in memoriam",
    "notes", "announcements", "errata", "corrigendum", "interview",
    "interviews", "poetry", "comment", "comments",
}
# Lines that end the body of an issue page.
TRAILER = re.compile(
    r"^(authors?\s+index|back\s+issues|full\s+issue|header\s+image|editorial\s+information|x)\b",
    re.I,
)
SYMPOSIUM_RE = re.compile(r"^symposi(um|a)\b\s*(on\b|:|$)", re.I)
GUEST_EDITOR_RE = re.compile(r"^guest\s+editors?\b", re.I)
REPLY_RE = re.compile(
    r"^(responses?|replies|reply|rejoinder|author'?s?\s+(responses?|reply)|"
    r"replies?\s+to\s+(my\s+)?critics)\s*$", re.I)
PRECIS_RE = re.compile(r"^(symposium\s+)?(pr[ée]cis|summary|prologue)\s*$", re.I)
INTRO_RE = re.compile(
    r"^((symposium|editorial|editor'?s?|guest\s+editor'?s?)\s+)?introduction\s*$", re.I)

# Surname particles stay lowercase when a caps name is folded back to normal.
PARTICLES = {"van", "de", "der", "den", "von", "la", "le", "di", "du",
             "del", "della", "da", "dos", "ter", "al", "bin", "ibn"}

_WS = re.compile(r"\s+")


def _clean(text):
    return _WS.sub(" ", (text or "")).strip()


#: splits a name on hyphens and apostrophes while keeping the separators, so
#: each part can be capitalised in one pass. Doing it separator-by-separator
#: lower-cases everything after the first letter on the second pass.
_NAME_PART = re.compile(r"([-'’])")
#: "GEORGE STEIRIS and GEORGE POLITIS" is not str.isupper(), so caps detection
#: ignores the connectors that join two contributors.
_CONNECTOR = re.compile(r"\b(and|&|with)\b", re.I)


def _proper_name(name):
    """Fold an ALL-CAPS contributor name back to normal case.

    Names that are already mixed case pass through untouched, so the title-case
    spellings in pre-2025 issues are never re-guessed. That matters: "diZerega"
    and "van de Haar" are correct as printed and any re-capitalisation is a
    regression.
    """
    name = _clean(name)
    if not name:
        return name
    # ignore connectors when deciding whether this is a caps name
    if not _CONNECTOR.sub("", name).isupper():
        return name

    def cap_word(word, first):
        if _CONNECTOR.fullmatch(word):
            return word.lower()
        if word.lower() in PARTICLES and not first:
            return word.lower()
        if re.fullmatch(r"[A-Z]\.?", word):      # an initial: J. / J
            return word
        parts = _NAME_PART.split(word)           # ADAIR-TOTEFF, O'BRIEN
        out = "".join(p if _NAME_PART.fullmatch(p) else p[:1].upper() + p[1:].lower()
                      for p in parts)
        # Mc is the one prefix common enough to be worth restoring; Mac is left
        # alone because Macdonald and MacDonald are both real spellings.
        return re.sub(r"^Mc([a-z])", lambda m: "Mc" + m.group(1).upper(), out)

    words = name.split()
    return " ".join(cap_word(w, i == 0) for i, w in enumerate(words))


def _split_name(full):
    """Split a contributor name into (first, last) the way db expects."""
    full = _clean(full)
    if not full:
        return "", ""
    parts = full.split()
    if len(parts) == 1:
        return "", parts[0]
    # Keep a trailing particle with the surname: "Edwin van de Haar".
    i = len(parts) - 1
    while i > 1 and parts[i - 1].lower() in PARTICLES:
        i -= 1
    return " ".join(parts[:i]), " ".join(parts[i:])


def _book_author_from_title(title):
    """Pull the book's author out of a review title, where the issue gives one.

    Pre-2025 issues write "Title by Author" or "Title edited by A, B and C".
    Returns (title_without_author, author_string).
    """
    m = re.search(r"[,\s]\s*(?:eds?\.|edited\s+by|by)\s+(.+)$", title, re.I)
    if not m:
        return title, ""
    author = _clean(m.group(1)).rstrip(".,;")
    # "by" appears inside plenty of real titles; only believe it when what
    # follows looks like one to three names rather than a clause.
    if len(author) > 90 or len(author.split()) > 12:
        return title, ""
    if not re.match(r"^[A-ZÀ-ɏ]", author):
        return title, ""
    return _clean(title[:m.start()]).rstrip(".,;"), author


def _author_fields(author_string):
    """Map a book-author string onto the project's multi-author convention."""
    author_string = _clean(author_string)
    if not author_string:
        return "", ""
    names = re.split(r"\s*(?:,|\band\b|&)\s*", author_string)
    names = [n for n in (x.strip() for x in names) if n]
    if not names:
        return "", ""
    if len(names) == 1:
        return _split_name(names[0])
    # first = everything but the final surname, last = the final surname
    last_first, last_last = _split_name(names[-1])
    head = ", ".join(names[:-1])
    return _clean(f"{head} and {last_first}".strip()), last_last


class CosmosTaxisScraper:
    def __init__(self, session=None, delay=0.4):
        self.delay = delay
        self.session = session or requests.Session()
        self.session.headers.update(
            {"User-Agent": "PhilReviews/1.0 (academic research; philreviews.org)"})

    # -- fetching -----------------------------------------------------------
    def _get(self, url):
        # a small journal on shared hosting; a backfill is ~85 page fetches
        time.sleep(self.delay)
        r = self.session.get(url, timeout=25)
        r.raise_for_status()
        return r.text

    def issue_urls(self, year):
        """Issue page URLs for a year, in page order."""
        try:
            html = self._get(f"{BASE_URL}/{year}-2/")
        except Exception as exc:
            log.warning("C+T: no year page for %s (%s)", year, exc)
            return []
        soup = BeautifulSoup(html, "html.parser")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/ct-" in href and href.startswith(BASE_URL) and href not in urls:
                urls.append(href)
        return urls

    # -- parsing ------------------------------------------------------------
    def parse_issue(self, url, year, html=None):
        """Return review/symposium records for one issue page."""
        html = html if html is not None else self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        # title -> PDF url, so a record can carry the piece's own link
        links = {}
        for a in soup.find_all("a", href=True):
            if a["href"].lower().split("?")[0].endswith(".pdf"):
                key = _clean(a.get_text(" ", strip=True))
                if key and key not in links:
                    links[key] = a["href"]
        if not links:
            return []

        lines = [_clean(l) for l in soup.get_text("\n").split("\n")]
        lines = [l for l in lines if l]

        # The nav menu repeats "Reviews", "Symposia" and the year list, so the
        # body is anchored on the "C+T 13:5+6" masthead line rather than the
        # first heading-looking thing on the page.
        start, volume, issue = None, "", ""
        for i, line in enumerate(lines):
            m = re.match(r"^C\+T\s+(\d+)\s*:\s*(\S+)", line)
            if m and i > 5:
                start, volume, issue = i, m.group(1), m.group(2)
                break
        if start is None:
            log.warning("C+T: no masthead line on %s", url)
            return []

        records = []
        section = None           # None until a heading is seen
        symposium_book = None
        symposium_authors = ""
        i = start + 1
        while i < len(lines):
            line = lines[i]
            if TRAILER.match(line):
                break

            low = line.lower().rstrip(":")
            head = low.split(":", 1)[0].strip()      # "in memoriam: <name>"
            # A line that is itself a PDF link is a contribution, never a
            # heading: "Symposium Prologue" is a piece, "Symposium on X" is not.
            is_item = line in links
            if not is_item and SYMPOSIUM_RE.match(line):
                section = "symposium"
                symposium_book, symposium_authors, i = self._read_symposium_header(lines, i)
                continue
            if not is_item and (low in REVIEW_SECTIONS or head in REVIEW_SECTIONS):
                section, symposium_book = "review", None
                i += 1
                continue
            if not is_item and (low in SKIP_SECTIONS or head in SKIP_SECTIONS):
                section, symposium_book = "skip", None
                i += 1
                continue

            if section in ("review", "symposium") and line in links:
                title = line
                author = lines[i + 1] if i + 1 < len(lines) else ""
                if not self._looks_like_person(author):
                    log.debug("C+T: no contributor after %r on %s", title[:50], url)
                    i += 1
                    continue
                rec = self._build(section, title, author, links[title], year,
                                  volume, issue, symposium_book, symposium_authors)
                if rec:
                    records.append(rec)
                i += 2
                continue
            i += 1

        return records

    def _read_symposium_header(self, lines, i):
        """Consume a symposium heading, returning (book_title, authors, next_i).

        Two shapes occur: "Symposium on" with the book on the next line, and
        "Symposium on <Author>'s" with the book on the next line. A "Guest
        editor, X" line may follow either.
        """
        head = lines[i]
        authors = ""
        rest = re.sub(r"^symposi(um|a)\s*(on|:)?\s*", "", head, flags=re.I).strip()
        m = re.match(r"^(.*?)[’']s$", rest)      # "Jobst Landgrebe and Barry Smith's"
        if m:
            authors, rest = m.group(1).strip(), ""
        i += 1
        if not rest and i < len(lines):
            rest = lines[i]
            i += 1
        while i < len(lines) and GUEST_EDITOR_RE.match(lines[i]):
            i += 1
        return (_clean(rest) or None), authors, i

    @staticmethod
    def _looks_like_person(line):
        """Is this line a contributor name rather than another title?"""
        if not line or len(line) > 80:
            return False
        if TRAILER.match(line) or re.search(r"\d", line):
            return False
        if line.rstrip().endswith((".", "?", "!", ":", ";")):
            return False
        return 1 <= len(line.split()) <= 9

    @staticmethod
    def _symposium_label(title, contributor_last, symposium_authors):
        """Which kind of symposium piece this is, in the project's vocabulary.

        A reply is usually titled "Response" or "Reply", but not always: C+T
        lets the author give their reply a real title, in which case the only
        signal is that the contributor is one of the book's authors.
        """
        if REPLY_RE.match(title):
            return "[Author's Reply]"
        if PRECIS_RE.match(title):
            return "[Précis]"
        if INTRO_RE.match(title):
            return "[Symposium Introduction]"
        if contributor_last and symposium_authors:
            surnames = {w.lower().strip(".,") for w in symposium_authors.split()}
            if contributor_last.lower() in surnames:
                return "[Author's Reply]"
        return ""

    def _build(self, section, title, author, link, year, volume, issue,
               symposium_book, symposium_authors):
        reviewer_first, reviewer_last = _split_name(_proper_name(author))
        if not reviewer_last:
            return None

        rec = {
            "publication_source": SOURCE,
            "publication_date": f"{year}-01-01",   # C+T dates by volume year
            "review_link": link,
            "access_type": "Open",
            "reviewer_first_name": reviewer_first,
            "reviewer_last_name": reviewer_last,
            "entry_type": "review",
        }

        if section == "symposium":
            if not symposium_book:
                return None
            first, last = _author_fields(symposium_authors)
            label = self._symposium_label(title, reviewer_last, symposium_authors)
            book_title = f"{symposium_book} {label}".strip() if label else symposium_book
            rec.update({
                "book_title": book_title,
                "book_author_first_name": first,
                "book_author_last_name": last,
                "entry_type": "symposium",
                "symposium_group": f"{SOURCE}|{year}|{volume}|{issue}",
            })
            return rec

        book_title, author_string = _book_author_from_title(title)
        first, last = _author_fields(author_string)
        rec.update({
            "book_title": book_title,
            "book_author_first_name": first,
            "book_author_last_name": last,
        })
        return rec

    # -- driver -------------------------------------------------------------
    def run(self, dry_run=False, years=None, backfill=False):
        """Scrape and insert. Defaults to this year and last year.

        The journal publishes a handful of double issues a year and back
        issues never change, so a weekly run has no reason to walk 2013.
        """
        if years is None:
            this_year = date.today().year
            years = range(FIRST_YEAR, this_year + 1) if backfill else \
                (this_year, this_year - 1)

        found, records = 0, []
        for year in years:
            urls = self.issue_urls(year)
            log.info("C+T %s: %d issue pages", year, len(urls))
            for url in urls:
                try:
                    recs = self.parse_issue(url, year)
                except Exception:
                    log.exception("C+T: failed to parse %s", url)
                    continue
                found += len(recs)
                records.extend(recs)

        # dedupe within the run: a piece can be listed on more than one page
        seen, unique = set(), []
        for r in records:
            if r["review_link"] in seen:
                continue
            seen.add(r["review_link"])
            unique.append(r)

        new = [r for r in unique if not db.review_link_exists(r["review_link"])]
        stats = {
            "found": found,
            "unique": len(unique),
            "already_indexed": len(unique) - len(new),
            "new": len(new),
            "inserted": 0,
        }
        if new and not dry_run:
            db.insert_reviews(new)
            stats["inserted"] = len(new)
        log.info("C+T: %(found)d found, %(new)d new, %(inserted)d inserted", stats)
        return stats


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Scrape Cosmos + Taxis reviews and symposia")
    ap.add_argument("--dry-run", action="store_true", help="parse but do not insert")
    ap.add_argument("--backfill", action="store_true",
                    help="walk every year from %d, not just the current two" % FIRST_YEAR)
    ap.add_argument("--year", type=int, action="append", help="specific year (repeatable)")
    args = ap.parse_args()

    stats = CosmosTaxisScraper().run(
        dry_run=args.dry_run, years=args.year, backfill=args.backfill)
    print(stats)
