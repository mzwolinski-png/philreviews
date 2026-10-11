"""Find a book's author or editors from publishers' own Crossref records.

Several journals deposit a review under the book's title alone (HOPOS, American
Political Thought, Ethics), so the row arrives with no book author. The weekly
retry asked OpenAlex, which filled about one in five: it skipped books before
2010, took only the first author, and never returned editors.

Crossref's book records come from the publishers. The match must be exact -
the full title, subtitle included - because a near match on a short title
("Philosophy of Education") is some other book.
"""
import re
import unicodedata

import requests

UA = {"User-Agent": "PhilReviews/1.0 (mailto:mzwolinski@gmail.com)"}
BOOK_TYPES = "type:book,type:edited-book,type:monograph,type:reference-book"


def _key(text):
    t = unicodedata.normalize("NFKD", text or "")
    t = "".join(ch for ch in t if not unicodedata.combining(ch)).lower()
    t = re.sub(r"<[^>]+>", "", t).replace("&amp;", "&").replace("&", " and ")
    return re.sub(r"[^a-z0-9]", "", t)


def _name_case(name):
    """ALEX ZAKARAS -> Alex Zakaras; names already in mixed case are left alone."""
    if not name.isupper():
        return name
    return " ".join(w if re.fullmatch(r"[A-Z]\.", w) else "-".join(p.capitalize() for p in w.split("-"))
                    for w in name.split())


def crossref_book_people(title, reviewer_last="", review_year=None, session=None, timeout=30):
    """Return (names, role) for an exact title match, or ([], None).

    names is a list of "Given Family"; role is "author" or "editor".
    """
    want = _key(title)
    if len(want) < 12:          # too short to be sure of: "Emotion", "Legality"
        return [], None
    get = (session or requests).get
    try:
        r = get("https://api.crossref.org/works", headers=UA, timeout=timeout,
                params={"query.bibliographic": re.sub(r"<[^>]+>", "", title)[:200],
                        "filter": BOOK_TYPES, "rows": 6})
        items = r.json()["message"]["items"]
    except Exception:
        return [], None
    for it in items:
        t = (it.get("title") or [""])[0]
        sub = (it.get("subtitle") or [""])[0]
        if want not in (_key(t), _key(f"{t} {sub}")):
            continue
        # A review follows its book by a few years at most. Without this a
        # generic title ("Philosophy of Education") matches some other book.
        parts = ((it.get("published") or it.get("issued") or {}).get("date-parts") or [[None]])[0]
        book_year = parts[0] if parts else None
        if review_year and book_year:
            if not (review_year - 8 <= book_year <= review_year + 1):
                continue
        elif len(want) < 30:
            continue            # no year to check against: only trust a long, specific title
        role = "author" if it.get("author") else "editor"
        people = it.get("author") or it.get("editor") or []
        names = [_name_case(f"{p.get('given', '')} {p.get('family', '')}".strip())
                 for p in people if p.get("family")]
        if not names:
            continue
        if reviewer_last and any(reviewer_last.lower() == n.split()[-1].lower() for n in names):
            continue            # the "book" is the review itself, or the reviewer's own
        return names, role
    return [], None


def author_fields(names):
    """Project convention: first = everything but the final surname."""
    if not names:
        return "", ""
    if len(names) == 1:
        parts = names[0].split()
        return " ".join(parts[:-1]), parts[-1]
    last_parts = names[-1].split()
    head = ", ".join(names[:-1])
    return f"{head} and {' '.join(last_parts[:-1])}".strip(), last_parts[-1]
