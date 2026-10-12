#!/usr/bin/env python3
"""
Scraper for Bryn Mawr Classical Review, kept to ancient philosophy.

BMCR reviews all of classics (about 13,500 reviews since 1990) and deposits no
DOIs. Its listings (https://bmcr.brynmawr.edu/2026/10/, twenty to a page) give
everything we index:

    <a href=".../2026/2026.01.05/" class="ref-wrapper">
      <small class="ref-id">BMCR 2026.01.05</small>
      <p class="ref-title">Sextus Empiricus Against the Arithmeticians</p>
      <p class="ref-details"> Lorenzo Corti <span class="slash">/</span>Brill, 2023 </p>
      <p class="ref-author">Reviewed by Sebastiano Molinelli<span class="comma">,</span></p>

The site carries no subject tags, so a review is kept only when its title names
a philosopher, school or philosophical topic (KEYWORDS), or when the same book
is already in the index from another journal. Epigraphy, archaeology, poetry
and military history fall away.

The weekly run reads the current and previous month; --backfill walks every
year. Some listing pages answer 500 while still carrying their entries, so a
page is parsed whatever its status and only a 404 ends a walk.
"""

import html as htmllib
import re
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date

import requests

import db
from marx_philosophy_scraper import author_fields

JOURNAL_NAME = 'Bryn Mawr Classical Review'
BASE_URL = 'https://bmcr.brynmawr.edu/'

_ENTRY = re.compile(r'<a href="([^"]+)" class="ref-wrapper">(.*?)</a>', re.S)
_TAG = re.compile(r'<[^>]+>')
_SLASH = re.compile(r'<span class="slash">.*?</span>', re.S)
_COMMA = re.compile(r'<span class="comma">.*?</span>', re.S)

KEYWORDS = re.compile(r"""\b(
    philosoph\w*|filosof\w*|
    plato\w*|aristot\w*|socrat\w*|sokrat\w*|presocratic\w*|pre-socratic\w*|vorsokrat\w*|
    stoic\w*|stoa|stoï\w*|stoiker|epicur\w*|epikur\w*|[ée]picur\w*|
    sceptic\w*|skeptic\w*|skepsis|pyrrhon\w*|plotin\w*|neoplaton\w*|neuplaton\w*|néoplaton\w*|
    porphyr(?:y|ios|ius|e)|proclus|proklos|iamblich\w*|jamblich\w*|damascius|simplicius|philoponus|
    alexander\ of\ aphrodisias|olympiodorus|
    heraclit\w*|heraklit\w*|parmenid\w*|empedocl\w*|empedokl\w*|democrit\w*|demokrit\w*|anaxagor\w*|
    anaximander|pythagor\w*|protagoras|xenophanes|chrysipp\w*|cleanthes|epictet\w*|epiktet\w*|
    marcus\ aurelius|marc\ aur[eè]l\w*|lucreti\w*|lucr[eè]ce|lukrez|sextus\ empiricus|cynics|cynicism|kyniker|
    theophrast\w*|peripatet\w*|boethius|
    metaphysi\w*|ethics|ethik|éthique|epistemolog\w*|ontolog\w*|dialecti\w*|dialektik|
    eudaimon\w*|cosmolog\w*|kosmolog\w*|teleolog\w*|
    timaeus|tim[ée]e|timaios|phaedo|phaidon|phaedrus|phaidros|theaetetus|nicomachean|eudemian|
    de\ anima|organon|enneads?|ennéades?|
    de\ officiis|de\ finibus|tusculan\w*|de\ natura\ deorum|de\ fato|epistulae\ morales|
    political\ thought|political\ theory|moral\ psychology|theory\ of\ knowledge
)\b""", re.I | re.X)

_TITLE_STOP = frozenset("""a an the and of in on to for with from by is as at or de la le les des du der die das
und von zur zum im il lo gli dei di del della el los las y e et en studies study essays introduction
commentary translation edition volume vol new""".split())


# A book these sources reviewed is not thereby philosophy: newspapers and
# general reviews cover ancient history too, and the theology and history of
# science journals review patristics and ancient medicine.
GENERAL_SOURCES = frozenset([
    'The New York Review of Books', 'London Review of Books', 'The Times Literary Supplement',
    'The New York Times Book Review', 'The New York Times', 'The Guardian', 'The Times',
    'The Wall Street Journal', 'The Washington Post', 'The Financial Times', 'The Atlantic',
    'The New Yorker', 'The New Republic', 'Boston Review', 'Los Angeles Review of Books',
    'Claremont Review of Books', 'Frankfurter Allgemeine Zeitung', 'Süddeutsche Zeitung',
    'Neue Zürcher Zeitung', 'Die Zeit', 'Frankfurter Rundschau', 'Die Tageszeitung', 'Die Welt',
    'Deutschlandfunk', 'Deutschlandfunk Kultur', 'The Heythrop Journal', 'New Blackfriars',
    'British Journal for the History of Science',
    'Medical History: An International Journal for the History of Medicine and Related Sciences',
])


def _text(fragment: str) -> str:
    return re.sub(r'\s+', ' ', htmllib.unescape(_TAG.sub(' ', fragment))).strip()


_EPITHET = re.compile(r'(?i)^((?:emperor |archbishop |bishop |patriarch |king )?of [^\W\d]+|saint|pope)\s+(.+)$')
_INSTITUTION = re.compile(r'(?i)\b(universit\w*|institut\w*|scuola|soci[eé]t[eé]|centre|center|maison|museum|'
                          r'colloqu\w*|congress\w*|academy|acad[eé]mie|accademia|association|fondation|'
                          r'foundation|school|college|library|symposium)\b')


def clean_authors(raw: str) -> list:
    """'Louis Robert,  G. W. (Glen Warren) Bowersock' -> ['Louis Robert', 'G. W. Bowersock'].

    The listing takes names from a library catalogue, so an ancient author comes
    as 'of Cilicia Simplicius' or 'Epicurus.' ahead of the editors. Such a name
    in first place is the author and stands alone; elsewhere it is the subject
    of the book and is dropped.
    """
    out, ancient = [], set()
    for name in re.sub(r'\s*\([^)]*\)', '', raw).split(','):
        name = name.strip()
        is_ancient = bool(re.search(r'[^\W\d]{4,}\.$', name)) and not re.search(r'\b(Jr|Sr)\.$', name)
        if re.fullmatch(r'\s*(Jr|Sr|II|III|IV)\.?', name) and out:
            out[-1] = f'{out[-1]} {name.strip(" .")}'  # 'Francis A. Grabowski, III'
            continue
        name = name.strip(' .')
        m = _EPITHET.match(name)
        if m:
            epithet, name = m.group(1), m.group(2).strip(' .')
            # 'of Aphrodisias Alexander' is Alexander of Aphrodisias; a title is not part of the name
            if epithet.lower().startswith('of ') and ' ' not in name:
                name = f'{name} {epithet}'
            is_ancient = True
        if not name or re.match(r'(?i)(edited|translated|additional|with|introduction)\b', name):
            continue
        if re.search(r'\d|\)$', name) or _INSTITUTION.search(name) or not re.search(r'[A-Za-z]', name):
            continue
        out.append(name)
        if is_ancient:
            ancient.add(name)
    if out and out[0] in ancient:
        return out[:1]
    return [n for n in out if n not in ancient] or out[:1]


_LEAD = re.compile(r'(?i)^(also seen:\s*|\(?edd?s?\.\)?,\s*|trans\.(/comm\.)?,\s*|et al\.,\s*|\d{2,4}\.\d{1,2}\.\d{1,3},?\s*)')
_ROLE_WORDS = frozenset(['and', 'ed', 'edd', 'eds', 'et', 'al', 'trans', 'comm'])


def clean_title(title: str, authors: list) -> str:
    """Drop the '1995.01.03, Goodman, ' prefix that early entries carry."""
    surnames = {a.split()[-1].lower().replace('’', "'") for a in authors}
    for _ in range(4):
        before = title
        title = _LEAD.sub('', title).strip()
        head, sep, rest = title.partition(', ')
        if sep and rest and surnames:
            words = [w.lower().replace('’', "'") for w in re.findall(r"[^\W\d]+(?:['’-][^\W\d]+)*", head)]
            words = [w for w in words if w not in _ROLE_WORDS and len(w) > 1]
            if words and all(w in surnames for w in words):
                title = rest.strip()
        if title == before:
            break
    title = re.sub(r',\s*edd?s?\.\s.*$', '', title)
    return re.sub(r'(?i)[.,]\s*(translated|edited|notes)\s+by\s.*$', '', title)


_SERIES = re.compile(r"""\d|\b(studies|series|supplements?|suppl|monographs?|texts|library|collection|companions|
    commentar\w*|presences|antiquity|classical|classics|bd|vol|biblioth\w*|biblioteca|beiträge|untersuchungen|
    hypomnemata|mnemosyne|studi|studien|études|cahiers|latomus|oxford|cambridge|brill\W?s?|routledge|blackwell|
    loeb|clarendon|world|reihe|schriften|forschungen|collana|quaderni|testi|recherches|historia|zetemata)\b""",
                     re.I | re.X)


def strip_series(title: str) -> str:
    """'Sabina Augusta: An Imperial Journey. Women in antiquity' -> the title alone."""
    for _ in range(2):
        main, sep, tail = title.rpartition('. ')
        if not sep or len(main.split()) < 2 or len(tail.split()) > 12 or not _SERIES.search(tail):
            break
        title = main
    return title.rstrip(' .')


def parse_date(ref_id: str) -> str:
    """'BMCR 2026.01.05' -> '2026-01-01' (the number is year.month.sequence)."""
    m = re.search(r'(\d{4})\.(\d{1,2})\.', ref_id)
    if not m or not 1 <= int(m.group(2)) <= 12:
        return m.group(1) + '-01-01' if m else ''
    return f'{m.group(1)}-{int(m.group(2)):02d}-01'


def build_record(url: str, ref_id: str, title: str, authors_text: str, reviewers: list):
    """One listing entry as a record for db.insert_reviews, or None if unusable."""
    authors = clean_authors(authors_text)
    # Early titles are prefixed with the editors' surnames, so match on every name listed.
    listed = [n.strip(' .') for n in re.sub(r'\s*\([^)]*\)', '', authors_text).split(',') if n.strip(' .')]
    book_title = strip_series(clean_title(title, listed))
    if not book_title or not reviewers:
        return None
    if len(authors) == 1 and ' of ' in authors[0]:
        a_first, a_last = '', authors[0]  # 'Alexander of Aphrodisias' has no surname to split off
    else:
        a_first, a_last = author_fields(authors if len(authors) <= 4 else authors[:1])
    r_first, r_last = author_fields(reviewers)
    return {
        'book_title': book_title,
        'book_author_first_name': a_first,
        'book_author_last_name': a_last,
        'reviewer_first_name': r_first,
        'reviewer_last_name': r_last,
        'publication_source': JOURNAL_NAME,
        'publication_date': parse_date(ref_id),
        'review_link': url,
        'review_summary': '',
        'access_type': 'Open',
        'doi': None,
        'entry_type': 'review',
        '_authors': authors,
    }


def parse_listing(page_html: str) -> list:
    """Every review on one listing page, unfiltered."""
    out = []
    for m in _ENTRY.finditer(page_html):
        url, body = m.groups()
        ref = re.search(r'ref-id">([^<]*)', body)
        title = re.search(r'ref-title">(.*?)</p>', body, re.S)
        details = re.search(r'ref-details">(.*?)</p>', body, re.S)
        reviewer = re.search(r'ref-author">\s*Reviewed by(.*?)</p>', body, re.S)
        # No details means a "Books received" list or an editorial notice.
        if not (ref and title and details and reviewer):
            continue
        # Several books in one review are listed one after another; index the first.
        rec = build_record(url, ref.group(1), _text(title.group(1)),
                           _text(_SLASH.split(details.group(1))[0]),
                           [r for r in (_text(x) for x in _COMMA.split(reviewer.group(1))) if r])
        if rec:
            out.append(rec)
    return out


def _title_words(title: str) -> frozenset:
    # A series name often trails the title ("... . Cambridge classical studies"),
    # so compare on the part before the first full stop or colon.
    main = re.split(r'[.:]\s', title, maxsplit=1)[0]
    return frozenset(w for w in re.findall(r'[^\W\d_]{3,}', main.lower()) if w not in _TITLE_STOP)


def build_index(conn) -> dict:
    """surname -> title word sets, for books the other journals have reviewed."""
    idx = defaultdict(list)
    for last, title, source in conn.execute(
            "SELECT book_author_last_name, book_title, publication_source FROM reviews "
            "WHERE publication_source != ? AND book_author_last_name != ''", (JOURNAL_NAME,)):
        words = _title_words(title or '')
        if words and source not in GENERAL_SOURCES:
            idx[last.split()[-1].lower()].append(words)
    return idx


def already_indexed(rec: dict, idx: dict) -> bool:
    """The same book, by the same author, is in the index from another journal."""
    words = _title_words(rec['book_title'])
    if len(words) < 2:
        return False
    for author in rec.get('_authors') or [rec['book_author_last_name']]:
        for ref in idx.get(author.split()[-1].lower(), ()) if author else ():
            shared = len(words & ref)
            if shared >= 2 and shared >= 0.75 * min(len(words), len(ref)):
                return True
    return False


def is_philosophy(rec: dict, idx: dict) -> bool:
    return bool(KEYWORDS.search(rec['book_title'])) or already_indexed(rec, idx)


class BMCRScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'PhilReviews/1.0 (mailto:mzwolinski@sandiego.edu)'

    def walk(self, path: str, max_pages=60):
        """Yield the records of every listing page under path ('2026/' or '2026/10/')."""
        for n in range(1, max_pages + 1):
            url = BASE_URL + path + (f'page/{n}/' if n > 1 else '')
            resp = self.session.get(url, timeout=40)
            if resp.status_code == 404:
                return
            yield from parse_listing(resp.text)
            time.sleep(0.5)

    def run(self, dry_run=False, backfill=False):
        today = date.today()
        if backfill:
            paths = [f'{y}/' for y in range(1990, today.year + 1)]
        else:
            prev = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
            paths = [f'{prev[0]}/{prev[1]:02d}/', f'{today.year}/{today.month:02d}/']
        found, seen = [], set()
        for path in paths:
            try:
                for rec in self.walk(path):
                    if rec['review_link'] not in seen:
                        seen.add(rec['review_link'])
                        found.append(rec)
            except Exception as e:
                print(f'  BMCR: {path} failed: {e}')
        conn = sqlite3.connect(db.DB_PATH)
        idx = build_index(conn)
        conn.close()
        new = [r for r in found if is_philosophy(r, idx) and not db.review_link_exists(r['review_link'])]
        for r in new:
            r.pop('_authors', None)
        if new and not dry_run:
            db.insert_reviews(new)
        print(f'BMCR: {len(found)} listed, {len(new)} new in ancient philosophy')
        return {'found': len(found), 'new': len(new), 'uploaded': 0 if dry_run else len(new)}


def main():
    BMCRScraper().run(dry_run='--dry-run' in sys.argv, backfill='--backfill' in sys.argv)


if __name__ == '__main__':
    main()
