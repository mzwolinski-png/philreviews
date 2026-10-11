"""The reviewed book, from a Springer article page's subtitle line.

Metascience gives each review an essayistic headline ("The last Viennese
polymath") and prints the book under it:

    <h1 class="c-article-title">Reply to my Critics: ...</h1>
    <p class="c-article-title__sub">Marc Lange: Because without cause: Non-causal
       explanation in science and mathematics. Oxford: Oxford University Press,
       2017, xxii+489pp, $74.00 HB</p>

Neither Crossref nor the Springer Meta API carries that line, so these reviews
were stored under the headline with no book author (or, worse, with the
reviewer as author). The line is on the page whether or not the article is
paywalled.
"""
import re

import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (compatible; PhilReviews/1.0; +https://philreviews.org)"}
_PUBINFO = re.compile(r"\b(1[89]|20)\d\d\b|\d\s?pp\b|\bpp\.?\s?\d|\bISBN\b", re.I)
_PUBTAIL = re.compile(
    r"[,.:]\s+[A-Z][\w .&/-]{1,40}:\s+[^:]{0,60}\b(?:Press|University|Springer|Routledge|Books|Verlag|"
    r"Publishing|Publishers|Macmillan|Wiley|Brill|Elsevier|Polity|Blackwell|Bloomsbury|Gruyter|Palgrave|"
    r"Harvard|Norton|Penguin|Sage|Lexington|Rowman)\b[^:]{0,60}$")
_EDS = re.compile(r"\s*[\(\[]?\beds?\.?[\)\]]?\s*$", re.I)


def fetch_subtitle_lines(doi, session=None, timeout=30):
    get = (session or requests).get
    r = get(f"https://link.springer.com/article/{doi}", headers=UA, timeout=timeout)
    if r.status_code != 200:
        return []
    el = BeautifulSoup(r.text, "html.parser").select_one("p.c-article-title__sub")
    if not el:
        return []
    return [re.sub(r"\s+", " ", x).strip() for x in el.get_text("\n").split("\n") if x.strip()]


_PARTICLES = {"van", "von", "de", "der", "den", "del", "della", "di", "du", "da", "la", "le", "ten", "ter"}


def _is_name(part):
    toks = part.split()
    if not (2 <= len(toks) <= 6):
        return False
    for t in toks[:-1]:
        if re.fullmatch(r"(?:[A-Z]\.)+|[A-Z]\.?", t) or t.lower() in _PARTICLES:
            continue                                    # initials, particles
        if not re.fullmatch(r"[A-Z\u00C0-\u024F][\w'\u2019-]*", t):
            return False
    # a surname is a whole word: not an initial, no trailing full stop
    return bool(re.fullmatch(r"(?:[dDlL]['\u2019])?[A-Z\u00C0-\u024F][\w'\u2019-]+", toks[-1]))


def _names(text):
    text = re.sub(r"\s+", " ", text).strip(" ,;")
    # "Wuppuluri, Shyam and A.C. Grayling" / "Guay, Alexandre, and Thomas Pradeu"
    m = re.match(r"^([^\s,]+(?: [^\s,]+)?),\s+([^,]+?),?\s+(?:and|&)\s+(.+)$", text)
    if m and len(m.group(2).split()) <= 3 and _is_name(f"{m.group(2)} {m.group(1)}"):
        text = f"{m.group(2)} {m.group(1)} and {m.group(3)}"
    elif re.match(r"^[^\s,]+,\s+[^,]+$", text) and len(text.split()) <= 4:
        last, first = [x.strip() for x in text.split(",", 1)]
        if _is_name(f"{first} {last}"):
            text = f"{first} {last}"
    parts = [p.strip() for p in re.split(r"\s*,\s*(?:and\s+)?|\s+and\s+|\s*&\s*", text) if p.strip()]
    return parts if parts and all(_is_name(p) for p in parts) else None


_PUBWORD = (r"(?:Press|Springer|Books|Academic|Publishing|Publishers|Verlag|Society|Routledge|Macmillan|"
            r"Wiley|Brill|Elsevier|Polity|Blackwell|Bloomsbury|Gruyter|Palgrave|Ashgate|Norton|Penguin)")
#: how the text after a title begins when it is no longer the title
_PUBSTART = re.compile(
    r"^(?:[A-Z\u00C0-\u024F][\w ,&/()\u00C0-\u024F-]{1,48}:\s(?=[A-Z])"   # "Oxford and New York: Oxford ..."
    r"|(?:(?:[A-Z][\w&.'-]*|of|for|and|the|de)\s+){0,6}" + _PUBWORD + r"\b"     # "The University of Chicago Press"
    r"|\((?=[^)]*\b(?:Studies|Series|Library|History of)\b)"                 # "(History of ... series)"
    r"|(?:1[89]|20)\d\d\b"                                                  # "2020. New York: ..."
    r"|Boston [Ss]tudies|Royal Institute|The Western Ontario|Synthese Library|Vol(?:ume)?\.?\s*\d)")
_HEAD_NOTE = re.compile(r"\s*\([^)]*\)")         # "(eds.)", "(Editors)", "(trans. & ed.)", "(ill.)"
_EDITED = re.compile(r"\b(eds?|editors?|ed\.)\b", re.I)


def join_lines(lines):
    """A citation is sometimes broken after the names: join it back up."""
    lines = [l.strip() for l in lines if l.strip()]
    if len(lines) > 1 and not lines[0].endswith(":") and _names(_EDS.sub("", lines[0])):
        lines = [lines[0] + ":"] + lines[1:]
    return re.sub(r"\s+", " ", " ".join(lines)).strip()


def _split_head(line):
    """(names, edited, rest) - the authors or editors, then everything after."""
    for sep in (": ", "; ", ", ", ". "):        # the colon is usual; the others occur
        if sep not in line:
            continue
        head, rest = line.split(sep, 1)
        edited = bool(_EDITED.search(head))
        clean = _EDS.sub("", _HEAD_NOTE.sub("", head))
        names = _names(clean)
        if names:
            return names, edited, rest
    return None, False, None


def _cut_title(rest):
    """The title is everything up to the sentence that starts the imprint."""
    for m in re.finditer(r"(?<=[.?!])\s+|[,:]\s+", rest):
        title, tail = rest[:m.start()], rest[m.end():]
        if m.group(0)[0] in ",:":
            # a comma or colon only ends the title directly before the imprint
            if not re.match(r"^(?:[A-Z][\w &/-]{1,30}:\s+)?(?:[A-Z][\w&'-]*\s+){0,4}" + _PUBWORD + r"\b", tail):
                continue
        if re.search(r"(?:^|[\s.])[A-Z]\.?$", title):
            continue                            # "N. R." / "W. E. B.": an initial, not a sentence end
        if _PUBSTART.match(tail):
            return title
    return None


def parse_citation(line):
    """'Authors[ (eds)]: Title. City: Publisher, year, pp, price' ->
    (title, [names], edited) or None."""
    line = re.sub(r"\s+", " ", line or "").strip()
    names, edited, rest = _split_head(line)
    if not names or not _PUBINFO.search(rest) and not re.search(_PUBWORD, rest):
        return None
    title = _cut_title(rest)
    if title is None:
        return None
    title = title.strip(" ,;")
    title = re.sub(r"\s*\((?:[^)]*\b(?:Studies|Series|Library)\b[^)]*)\)?$", "", title)   # a series in brackets
    title = re.sub(r",?\s+\(?\d+(?:st|nd|rd|th)\s+edn?\.?\)?$", "", title, flags=re.I)        # ", 2nd edn"
    title = title.rstrip(".").strip() if not re.search(r"\b[A-Z]\.$", title) else title.strip()
    if len(title) < 4:
        return None
    return title, names, edited
