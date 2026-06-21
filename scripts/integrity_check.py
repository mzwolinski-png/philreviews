#!/usr/bin/env python3
"""
Data integrity check for newly added reviews.

Detects and auto-fixes common issues:
- Publisher/bib info in book titles
- HTML tags in titles
- Garbled author fields (title fragments, publisher names)
- "Review of" / "Review:" prefixes
- Swapped title/author fields
- Junk titles (prices, page counts, very short)
- Non-reviews that slipped through (symposium comments, responses)

Returns a dict with counts of fixes and deletions.
"""

import os
import re
import sqlite3
import sys
import logging

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db

log = logging.getLogger("integrity_check")

# Journals whose Crossref review records put the author at the FRONT of the title
# ("First Last: Title" or Springer "Last, First: Title"). Author-from-title rules
# are gated to this set so ordinary subtitled titles ("Justice, Justice: ...") on
# other journals are never mistaken for an author.
AUTHOR_COLON_TITLE_JOURNALS = {
    'Journal of the History of Biology',
    'History and Philosophy of the Life Sciences',
    'Studies in East European Thought',
    'Theoretical Medicine and Bioethics',
    'Human Studies',
    'Phenomenology and the Cognitive Sciences',
    'Phronesis',
    'Erkenntnis',
    'Journal of Indian Philosophy',
    'Philosophia',
    'The Journal of Value Inquiry',
    'Res Publica',
    'Ethical Theory and Moral Practice',
    'International Journal for Philosophy of Religion',
    'Journal of Agricultural and Environmental Ethics',
}

# Lowercase words that signal a phrase is a TITLE, not a person's name. Used to
# stop author-extraction rules from mistaking a title that merely contains a
# possessive or comma ("The Legacy of Parmenides, ..." / "Aristotle's Ethics")
# for an author.
_NAME_STOPWORDS = {
    'the', 'a', 'an', 'of', 'in', 'on', 'and', 'or', 'to', 'for', 'with', 'at',
    'by', 'from', 'into', 'about', 'as', 'behind', 'between', 'after', 'before',
    'against', 'through', 'over', 'under', 'within', 'without', 'upon', 'toward',
    'towards', 'per', 'via', 'vs', 'versus', 'beyond', 'among', 'amid', 'amongst',
    'is', 'are', 'how', 'why', 'what', 'when', 'where', 'not', 'no',
}
# Lowercase nobiliary/name particles that ARE allowed inside a surname.
_NAME_PARTICLES = {
    'de', 'van', 'von', 'der', 'den', 'di', 'da', 'del', 'della', 'du', 'la',
    'le', 'dos', 'das', 'ten', 'ter', 'el', 'al', 'bin', 'ibn', 'y', 'st',
}


def _is_person_name(s):
    """True if `s` plausibly is a personal name (1-4 name words, each
    capitalized or an allowed particle/initial), and not a title fragment.
    Rejects anything with title punctuation or English function words."""
    s = (s or '').strip()
    if not s or any(c in s for c in ':;,'):
        return False
    parts = s.split()
    if not (2 <= len(parts) <= 4):
        return False
    for p in parts:
        low = p.lower().strip('.')
        if low in _NAME_PARTICLES:
            continue
        if low in _NAME_STOPWORDS:
            return False
        if not p[:1].isupper():
            return False
    return True


# Religious-order suffixes (Dominican O.P., Jesuit S.J., etc.) that the scraper
# sometimes mis-parses into the author name (as a first name, or appended to the
# surname). Dropped only when a name token EXACTLY equals one of these — never as
# a substring — and the ambiguous-with-initials forms (C.M./C.P.) are excluded.
_ORDER_SUFFIXES = {
    "O.P.", "O.P", "S.J.", "S.J", "O.S.B.", "O.S.B", "O.F.M.", "O.F.M",
    "O.F.M.Cap.", "O.S.A.", "O.S.A", "C.S.C.", "C.S.C", "C.Ss.R.", "C.Ss.R",
    "O.Carm.", "O.Carm", "O.Cist.", "O.Cist", "O.Praem.", "O.Praem",
    "O.C.D.", "O.C.D", "S.D.B.", "S.D.B", "F.S.C.", "F.S.C", "O.M.I.", "O.M.I",
    "S.S.S.", "Cap.", "OP", "SJ", "OSB", "OFM", "OSA", "OCD",
}
_ORDER_PARTICLES = {"van", "von", "de", "del", "della", "di", "da", "du",
                    "la", "le", "dos", "das", "den", "ter", "ten", "el"}


def _strip_order_suffix(af, al):
    """If an order suffix is mis-parsed into the author fields, drop it and
    re-split the remaining name. Returns (first, last) or None if no change."""
    toks = [t for t in re.split(r"[\s,]+", f"{af or ''} {al or ''}".strip()) if t]
    if not any(t.strip(",") in _ORDER_SUFFIXES for t in toks):
        return None
    kept = [t for t in toks if t.strip(",") not in _ORDER_SUFFIXES]
    if not kept:
        return None
    if len(kept) == 1:
        nf, nl = "", kept[0]
    elif kept[-2].lower() in _ORDER_PARTICLES:
        nf, nl = " ".join(kept[:-2]), kept[-2] + " " + kept[-1]
    else:
        nf, nl = " ".join(kept[:-1]), kept[-1]
    return None if (nf, nl) == ((af or "").strip(), (al or "").strip()) else (nf, nl)


def run_integrity_check(since=None, dry_run=False):
    """Run integrity checks on recent entries.

    Args:
        since: only check entries with created_at >= this string (e.g. '2026-03-29')
               If None, checks all entries.
        dry_run: if True, report issues but don't fix them.

    Returns:
        dict with 'fixed', 'deleted', 'issues' counts and 'details' list.
    """
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where = "WHERE created_at >= ?" if since else ""
    params = (since,) if since else ()
    rows = conn.execute(
        f"SELECT id, book_title, book_author_first_name, book_author_last_name, "
        f"reviewer_first_name, reviewer_last_name, publication_source, doi "
        f"FROM reviews {where}", params
    ).fetchall()

    fixed = 0
    deleted = 0
    details = []

    for r in rows:
        rid = r['id']
        t = r['book_title'] or ''
        af = r['book_author_first_name'] or ''
        al = r['book_author_last_name'] or ''
        author = f"{af} {al}".strip()

        # --- Delete non-reviews ---
        if re.search(r'^(Response|Reply|Comment)\s*(to|on)\s', t, re.I) or \
           re.search(r"Comments on .+['\u2019]s$", author):
            if not dry_run:
                cur.execute("DELETE FROM reviews WHERE id = ?", (rid,))
            deleted += 1
            details.append(f"DELETED {rid}: non-review '{t[:50]}'")
            continue

        # --- Delete junk titles (prices, page counts only) ---
        if re.match(r'^[\d\s£$€xvi+,.]+$', t) or len(t) < 3:
            if not dry_run:
                cur.execute("DELETE FROM reviews WHERE id = ?", (rid,))
            deleted += 1
            details.append(f"DELETED {rid}: junk title '{t}'")
            continue

        changes = {}

        # --- Strip HTML tags from title ---
        if re.search(r'<[a-z/]', t, re.I):
            new_title = re.sub(r'<[^>]+>', '', t).strip()
            new_title = re.sub(r'\s+', ' ', new_title)
            if new_title != t:
                changes['book_title'] = new_title
                t = new_title

        # --- Publisher/city/bib patterns (shared across strip and swap logic) ---
        _CITIES = (r'Oxford|Cambridge|Princeton|New York|London|Edinburgh|Berlin|Boston|'
                   r'Chicago|Freiburg|Stuttgart|Hamburg|Indianapolis|Albany|Stanford|Ithaca|'
                   r'Baltimore|Minneapolis|Notre Dame|Dordrecht|Leiden|Paris|Toronto|'
                   r'Grand Rapids|Westminster|Louisville|Washington|Dublin|Collegeville|'
                   r'Maryknoll|Mahwah|Milwaukee|Turnhout|Philadelphia|San Francisco|'
                   r'Kalamazoo|Harrisburg|Huntingdon|Liguori|Louvain|South Bend|Tacoma|'
                   r'Norwich|Suffolk|Leominster|Nottingham|Fribourg')
        _PUBLISHERS = (r'Oxford University Press|Cambridge University Press|Princeton University Press|'
                       r'Harvard University Press|Yale University Press|University Press|'
                       r'Catholic University of America Press|University of Notre Dame Press|'
                       r'University of Scranton Press|'
                       r'Routledge|De Gruyter|Bloomsbury|Palgrave|MIT Press|Clarendon Press|'
                       r'Edinburgh University Press|SUNY Press|Springer|Brill|'
                       r'SCM Press|SCM-Canterbury|S\.C\.M\. Press|SPCK|Eerdmans|Baker Academic|'
                       r'Continuum|Gracewing|Westminster John Knox|'
                       r'Darton|Geoffrey Chapman|New City Press|Alba House|Sapientia Press|'
                       r'Cistercian|Sheed|Burns|Gill|Cassell|Faber|Chapman|'
                       r'Paulist|Herder|Orbis|T&T Clark|Hendrickson|Ignatius|'
                       r'Ave Maria|Harper|Crossroad|Fordham|Brepols|Ashgate|'
                       r'Belknap Press|Boydell Press|Bristol Press|Angelico Press|'
                       r'Canterbury Press|Academic Press|Collins|Teneo Press|'
                       r'Anchor|Penguin|Vintage|Lutterworth|Dominican|Peeters|Hodder|'
                       r'Allen & Unwin|George Allen|ICS Publications|Tan Books|'
                       r'Liturgical Press|Nomos|Suhrkamp|Gallimard|Vrin|'
                       r'Karl Alber|Klostermann|Duncker|Mohr Siebeck|Quodlibet|Olms|Felix Meiner')

        def _strip_bib(text):
            """Strip publisher info, cities, page counts, prices from end of text."""
            for pat in [
                rf'(?:,\s*|\.\s+)\(?(?:{_CITIES})[,:\s].*$',
                rf'(?:,\s*|\.\s+)\(?(?:{_PUBLISHERS}).*$',
                r'\s*ISBN.*$',
                r'[,.\s]*\bPp\.?\s.*$',
                r'[,.\s]*\bpp\.\s.*$',
                r'[,.\s]*\d+\s*pp\.?.*$',
                r'[,.\s]*£[\d.]+.*$',
                r'[,.\s]*\$[\d.]+.*$',
                r'[,.\s]*\d+\s*Seiten.*$',
                # Trailing publication year — require a real separator (comma/
                # period/space) before it so we don't chop the second half of a
                # date range in the title (e.g. "...Modernity, 1723–2023").
                r'[,.\s]+(?:19|20)\d{2}\s*$',
            ]:
                text = re.sub(pat, '', text, flags=re.I).strip().rstrip(' .,;:/(')
            return text

        def _name_to_first_last(name_str):
            """Best-effort: turn a raw author string (with possible translator/
            publisher tails, honorifics, run-together CamelCase, or multiple
            authors) into (first, last). Returns (None, None) if it doesn't look
            like a personal name — callers must check before applying."""
            s = _strip_bib(name_str or '')
            # primary author only (drop co-authors after "and"/"&")
            s = re.split(r'\s+(?:and|&)\s+', s)[0]
            # drop translator/editor clauses
            s = re.sub(r'[,;]?\s*\b(?:trans|tr|ed|eds|edited|translated)\b\.?.*$',
                       '', s, flags=re.I).strip()
            # drop honorifics / religious-order markers
            s = re.sub(r'\b(?:S\.?J\.?|O\.?P\.?|O\.?S\.?B\.?|Rev\.?|Sister|Soeur|'
                       r'Sœur|Fr\.?|Mons\.?|Most\s+Rev\.?)\b', '', s).strip()
            # separate run-together initials/names: "C.Chronister" -> "C. Chronister",
            # "AnnHartle" -> "Ann Hartle"
            s = re.sub(r'([A-Z])\.([A-Z][a-z])', r'\1. \2', s)
            s = re.sub(r'([a-zÀ-ſ])([A-Z])', r'\1 \2', s)
            s = re.sub(r'\s+', ' ', s).strip().strip('.,;:')
            parts = s.split()
            if any(ch.isdigit() for ch in s):
                return (None, None)
            if re.search(r'\b(?:University|Press|Verlag|Editions?)\b', s):
                return (None, None)
            # final guard: must read as a person name, not a title fragment
            # (rejects e.g. "Systems in Medicine", "Theory Alone").
            if not _is_person_name(s):
                return (None, None)
            return (' '.join(parts[:-1]), parts[-1])

        # Keep the HTML-stripped-but-bib-intact title so citation-marker rules
        # (e.g. "Title by Author, Publisher, Year") can use the tail as evidence
        # before it gets stripped below.
        t_full = t

        # --- Strip publisher info from title ---
        new_title = _strip_bib(t)
        if new_title != t and len(new_title) >= 5:
            changes['book_title'] = new_title
            t = new_title

        # --- Swapped fields: title has bib info, author has real title ---
        title_is_bib = bool(re.match(rf'^(?:{_PUBLISHERS}|{_CITIES})', t, re.I)) or \
                       (len(t) < 15 and re.search(r'Press|£|\$|pp\b|Pp\b|University', t, re.I))
        if title_is_bib and len(author) > 15 and not re.search(r'University|Press', author, re.I):
            new_title = _strip_bib(author)
            if len(new_title) >= 10:
                changes['book_title'] = new_title
                changes['book_author_first_name'] = None
                changes['book_author_last_name'] = None

        # --- "Author's Title" possessive format → extract author ---
        if not author or len(author) < 2:
            m = re.match(r"^(.+?)['\u2019]s\s+(?:\d{4}\.\s*)?(.+)", t)
            if m:
                auth_str = m.group(1).strip()
                title_str = m.group(2).strip()
                parts = auth_str.split()
                # Only split when the pre-possessive part is itself a person name
                # ("Aristotle's Ethics" / "Europe's First Great Queen" must NOT split).
                if _is_person_name(auth_str):
                    changes['book_title'] = title_str
                    changes['book_author_first_name'] = ' '.join(parts[:-1])
                    changes['book_author_last_name'] = parts[-1]

        # --- "Author, Title" format (Political Studies Review et al.) ---
        if not author or len(author) < 2:
            m = re.match(r'^([A-Z][a-zA-Z.\s]+?),\s+([A-Z].{15,})', t)
            if m:
                auth_str = m.group(1).strip()
                title_str = m.group(2).strip()
                parts = auth_str.split()
                # A real author's name never recurs inside its own title; if a
                # 4+ char author word reappears in the title, the comma was a
                # title separator ("Global Governance, Global Government...").
                _aw = {w.lower().strip('.,;:') for w in parts if len(w) > 3}
                _tw = {w.lower().strip('.,;:') for w in title_str.split()}
                # Gated by _is_person_name so titles with an early comma
                # ("The Legacy of Parmenides, ...") aren't read as "Author, Title".
                if _is_person_name(auth_str) and not (_aw & _tw):
                    changes['book_title'] = title_str
                    changes['book_author_first_name'] = ' '.join(parts[:-1])
                    changes['book_author_last_name'] = parts[-1]

        # --- "Review of <Author>, <Title>" (author-first form, e.g. HoEI) ---
        if not author or len(author) < 2:
            m = re.match(r'^Review of\s+(.+?),\s+([A-ZÀ-ſ].{10,})$', t, re.I)
            if m:
                first, last = _name_to_first_last(m.group(1))
                if last:
                    changes['book_title'] = m.group(2).strip()
                    changes['book_author_first_name'] = first
                    changes['book_author_last_name'] = last
                    t = changes['book_title']

        # --- "Review of " prefix (title-only fallback) ---
        if re.match(r'^Review of\s+', t, re.I):
            changes['book_title'] = re.sub(r'^Review of\s+', '', t, flags=re.I)
            t = changes['book_title']

        # --- Springer "Last, First: Title" (e.g. Journal of Value Inquiry) ---
        # Gated to known author-first journals so titles like "Justice, Justice: ..."
        # aren't misread as an author.
        if ((not author or len(author) < 2)
                and r['publication_source'] in AUTHOR_COLON_TITLE_JOURNALS):
            m = re.match(r"^([A-Z][\w.À-ſ'-]+),\s+"
                         r"([A-Z][\w.À-ſ'-]+(?:\s+[A-Z][\w.À-ſ'-]+)?):\s+"
                         r"([A-ZÀ-ſ].{10,})$", t)
            if m and 1 <= len(m.group(2).split()) <= 2:
                changes['book_title'] = m.group(3).strip()
                changes['book_author_first_name'] = m.group(2).strip()
                changes['book_author_last_name'] = m.group(1).strip()
                t = changes['book_title']

        # --- Edited volume "Author(s) (eds.), Title" ---
        if not author or len(author) < 2:
            m = re.match(r'^(.+?)\s+\(eds?\.?\),?\s+([A-ZÀ-ſ].{6,})$', t)
            if m:
                first, last = _name_to_first_last(m.group(1))
                if last:
                    changes['book_title'] = m.group(2).strip()
                    changes['book_author_first_name'] = first
                    changes['book_author_last_name'] = last
                    t = changes['book_title']

        # --- "Title By/by/Par Author[, Publisher...]" ---
        # Matched against the bib-intact title (t_full). Capital "By"/"Par" is a
        # reliable citation marker; lowercase "by" only when a publisher/page/year
        # tail follows the author (so real titles like "Inspired by Nature" or
        # "Death by Black Hole" aren't split).
        if not author or len(author) < 2:
            m = re.match(r'^(.{6,}?)[.,]?\s+(?:By|Par)\s+([A-ZÀ-ſ].+)$', t_full)
            if not m:
                m = re.match(r'^(.{6,}?)\s+by\s+([A-ZÀ-ſ].+?'
                             r'(?:Press|University|Routledge|Springer|Verlag|'
                             r'\d{4}|\bpp\b|ISBN|£|\$).*)$', t_full)
            if m:
                first, last = _name_to_first_last(m.group(2))
                title_part = _strip_bib(m.group(1).strip().rstrip(' .,;:'))
                if last and len(title_part) >= 6:
                    changes['book_title'] = title_part
                    changes['book_author_first_name'] = first
                    changes['book_author_last_name'] = last
                    t = title_part

        # --- Duplicated text in title ("Title Title") ---
        dup_m = re.match(r'^(.{15,}?)\s+\1', t)
        if dup_m:
            changes['book_title'] = dup_m.group(1).strip()

        # --- Heythrop "ByAuthorName" with no space ---
        by_m = re.search(r'\.\s*By([A-Z][a-z])', t)
        if by_m:
            full_m = re.match(r'^(.+?)\.\s*By([A-Z].+?)(?:\.\s*(?:Pp|Oxford|Cambridge|London|Edinburgh).*)?$', t)
            if full_m:
                changes['book_title'] = full_m.group(1).strip()
                author_raw = re.sub(r'([a-z])([A-Z])', r'\1 \2', full_m.group(2)).strip()
                parts = author_raw.split()
                if len(parts) >= 2:
                    changes['book_author_first_name'] = ' '.join(parts[:-1])
                    changes['book_author_last_name'] = parts[-1]

        # --- Old Political Theory "Books in Review : ..." Crossref format ---
        # E.g. "Books in Review : THE FOUNDATIONS OF MODERN POLITICAL THOUGHT
        # by Quentin Skinner. Volume I: THE RENAISSANCE..."
        if r['publication_source'] == 'Political Theory' and re.match(r'^Books?\s+in\s+Review\s*:', t, re.I) and r['doi']:
            try:
                import requests
                cr = requests.get(
                    f"https://api.crossref.org/works/{r['doi']}",
                    params={'mailto': 'mzwolinski@sandiego.edu'}, timeout=10,
                ).json().get('message', {})
                cr_title = (cr.get('title') or [''])[0]
                cr_clean = re.sub(r'<[^>]+>', '', cr_title).strip()
                cr_clean = re.sub(r'\s+', ' ', cr_clean)
                bir_m = re.match(r'^Books?\s+in\s+Review\s*:?\s*(.+)$', cr_clean, re.I)
                if bir_m:
                    body = bir_m.group(1).strip()
                    # Match "TITLE by/edited by Author" with full last name support
                    pat = re.compile(
                        r'(?:edited\s+by|tr\.?\s+by|trans\.\s+by|by)\s+'
                        r'([A-Z][a-zA-Z\u00C0-\u017F\-\']+(?:\s+[A-Z]\.)*\s+'
                        r"(?:d[\'\u2019]|de\s|van\s|von\s)?"
                        r"[A-Z][a-zA-Z\u00C0-\u017F\-\']+"
                        r'(?:\s*,?\s*(?:Jr\.?|Sr\.?|II|III|IV))?)',
                        re.IGNORECASE,
                    )
                    am = pat.search(body)
                    if am:
                        new_title = body[:am.start()].strip().rstrip(' .,;:')
                        author = am.group(1).strip().rstrip(' .,;:')
                        # Title-case if mostly all-caps
                        if (sum(c.isupper() for c in new_title if c.isalpha()) /
                            max(sum(c.isalpha() for c in new_title), 1) > 0.7):
                            new_title = new_title.title()
                            for word in ['And','Of','The','In','On','To','For',
                                         'A','An','Or','But','As','At','By','With']:
                                new_title = re.sub(rf'\b{word}\b(?!\s*$)', word.lower(), new_title)
                            if new_title:
                                new_title = new_title[0].upper() + new_title[1:]
                            new_title = re.sub(r':\s+(\w)', lambda m: ': ' + m.group(1).upper(), new_title)
                        parts = author.split()
                        if parts and parts[-1].rstrip('.') in ('Jr','Sr','II','III','IV'):
                            suffix = parts[-1]
                            parts = parts[:-1]
                            if parts:
                                last = parts[-1] + ' ' + suffix
                                first = ' '.join(parts[:-1])
                            else:
                                first, last = '', suffix
                        elif len(parts) >= 2:
                            first, last = ' '.join(parts[:-1]), parts[-1]
                        else:
                            first, last = '', parts[0] if parts else ''
                        if new_title and len(new_title) > 5:
                            changes['book_title'] = new_title
                            changes['book_author_first_name'] = first
                            changes['book_author_last_name'] = last
                            t = new_title
            except Exception as exc:
                log.debug(f"PT Books-in-Review retry failed for {r['doi']}: {exc}")

        # --- "By [ConcatName]" title with author fields holding the real title ---
        # Wiley/Heythrop/Phil-Investigations Crossref records sometimes mis-parse:
        # the title field becomes just "By JohnCottingham" while the actual book
        # title fragments end up in book_author_first/last. Re-fetch Crossref to
        # recover the real title and split out the author from the "By" portion.
        by_only = re.match(r'^By\s+([A-Z][a-z]+(?:[A-Z][a-z]+)+)\s*$', t)
        if by_only and r['doi']:
            try:
                import requests
                cr = requests.get(
                    f"https://api.crossref.org/works/{r['doi']}",
                    params={'mailto': 'mzwolinski@sandiego.edu'}, timeout=10,
                ).json().get('message', {})
                cr_title = (cr.get('title') or [''])[0]
                # Strip HTML and pull the part before ". By"
                cr_clean = re.sub(r'<[^>]+>', '', cr_title).strip()
                title_m = re.match(r'^(.+?)\.\s*By\s*([A-Z].+?)(?:[,.]\s*(?:Pp|Oxford|Cambridge|London|New York|Edinburgh|Berlin|Routledge|Springer|Wiley|Bloomsbury|\d{4}\.).*)?$', cr_clean)
                if title_m:
                    changes['book_title'] = title_m.group(1).strip()
                    author_raw = re.sub(r'([a-z])([A-Z])', r'\1 \2', title_m.group(2)).strip()
                    # Strip any trailing publisher/year/page bits
                    author_raw = re.sub(r',?\s*(?:Oxford|Cambridge|London|New York|Routledge|Springer|Wiley|Bloomsbury).*$', '', author_raw).strip()
                    parts = author_raw.split()
                    if len(parts) >= 2:
                        changes['book_author_first_name'] = ' '.join(parts[:-1])
                        changes['book_author_last_name'] = parts[-1]
                    elif len(parts) == 1:
                        changes['book_author_last_name'] = parts[0]
                        changes['book_author_first_name'] = None
                    t = changes['book_title']
            except Exception as exc:
                log.debug(f"Crossref retry failed for {r['doi']}: {exc}")

        # --- Publisher name in author field ---
        if re.search(r'(?:Cambridge|Oxford|Princeton|University|Press)\s*$', author) and \
           not re.search(r'Blackwell|Brill|Gerald.*Press', author):
            # Likely garbled - clear it
            changes['book_author_first_name'] = None
            changes['book_author_last_name'] = None

        # --- Mangled author with publisher/city info embedded ---
        # E.g. "TerwielAnna, Minneapolis: University of Minnesota Press" / "Anna Terwiel Prison Abolition..."
        if (re.search(r'(?:Minneapolis|Berlin|London|New York|Cambridge|Oxford|Boston):', af) or
            re.search(r'(?:Minneapolis|Berlin|London|New York|Cambridge|Oxford|Boston):', al) or
            re.search(r'University.{0,40}(?:Press|of|the)\s', af) or
            re.search(r'University.{0,40}(?:Press|of|the)\s', al)):
            changes['book_author_first_name'] = None
            changes['book_author_last_name'] = None

        # --- "by" / "By" / "Tr." / "Ed." prefix in author fields ---
        # Strip an editorial prefix WITHOUT corrupting real names that merely
        # start with these letters ("Byung-Chul", "Edward", "Tristan").
        # Matches: "by Foo"/"By Foo" (space-separated), "byFoo" (lowercase "by"
        # concatenated to a Capitalized name — a Crossref artifact), and the
        # period-bounded abbreviations "Tr."/"Ed."/"trans."/"Trans.".
        def _strip_author_prefix(s):
            for pat in (r'^(?:by|By|BY)\s+(.+)$',          # "By Jared Michelson"
                        r'^by([A-Z].+)$',                    # "byRainer" -> "Rainer"
                        r'^(?:Tr\.|tr\.|Ed\.|ed\.|trans\.|Trans\.)\s*(.+)$'):
                m = re.match(pat, s)
                if m:
                    return m.group(1).strip()
            return s
        new_af = _strip_author_prefix(af)
        if new_af != af:
            changes['book_author_first_name'] = new_af
            af = new_af
        new_al = _strip_author_prefix(al)
        if new_al != al:
            changes['book_author_last_name'] = new_al
            al = new_al

        # --- author first-name field is exactly a stray "By"/"by" ---
        # ". By Jared Michelson" parses to first="By", last="Jared Michelson".
        # Move the real name out of the last-name field and re-split.
        if af in ('By', 'by', 'BY') and al and ' ' in al:
            parts = al.split()
            changes['book_author_first_name'] = ' '.join(parts[:-1])
            changes['book_author_last_name'] = parts[-1]
            af = changes['book_author_first_name']
            al = changes['book_author_last_name']

        # --- Concatenated CamelCase names (no space) ---
        # E.g. "MarkHopwood" → "Mark Hopwood", "byRainer" → "Rainer"
        # Pattern: lowercase letter immediately followed by uppercase, but skip
        # surname prefixes like Mc/Mac/De/La/Le/Du/Di/Da (e.g. "McDougall").
        # Skip splits where the first part is < 3 chars (likely a typo, not concat).
        def _split_camel(val):
            # Insert space at lowercase→uppercase boundary
            spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', val)
            if spaced == val:
                return val
            # Re-join surname prefixes that we shouldn't have split:
            # "Mc Dougall" → "McDougall", "Mac Donald" → "MacDonald", etc.
            spaced = re.sub(
                r'\b(Mc|Mac|De|La|Le|Du|Di|Da|Van|Von|Del|Della|Bin|Al)\s+([A-Z])',
                r'\1\2', spaced
            )
            # Reject the split if any resulting "name" word is < 3 chars (likely
            # a typo like "AlIison" producing "Al Iison" rather than concat).
            words = spaced.split()
            if any(len(w) < 3 and w not in ('J.', 'M.', 'A.', 'S.', 'D.',
                                              'P.', 'R.', 'L.', 'C.', 'B.',
                                              'K.', 'T.', 'W.', 'H.', 'G.',
                                              'F.', 'E.', 'N.', 'O.') for w in words):
                return val
            return spaced

        for field in ('book_author_first_name', 'book_author_last_name'):
            val = changes.get(field, r[field]) or ''
            if re.search(r'[a-z][A-Z]', val):
                fixed_val = _split_camel(val)
                if fixed_val == val:
                    continue
                # If splitting last name produces "First Last" and first name is empty,
                # split into first/last fields
                if field == 'book_author_last_name' and not (af and len(af) > 1):
                    parts = fixed_val.split()
                    if len(parts) >= 2:
                        changes['book_author_first_name'] = ' '.join(parts[:-1])
                        changes['book_author_last_name'] = parts[-1]
                        af = changes['book_author_first_name']
                        al = changes['book_author_last_name']
                        continue
                changes[field] = fixed_val
                if field == 'book_author_last_name':
                    al = fixed_val
                else:
                    af = fixed_val

        # --- Strip mis-parsed religious-order suffixes (O.P., S.J., ...) ---
        _stripped = _strip_order_suffix(af, al)
        if _stripped:
            changes['book_author_first_name'], changes['book_author_last_name'] = _stripped
            af, al = _stripped

        # --- "Author: Title" in title field when book_author empty ---
        # E.g. "Matthew Wale: Making Entomologists..." or
        #      "Joachim H. Mowitz and Arno L. Goudsmit: Movements of Form"
        # Only applied to journals where Crossref records use this format
        # (AUTHOR_COLON_TITLE_JOURNALS, defined at module level) — avoids false
        # positives on titles like "Divined Explanations: ...".
        if not af and not al and r['publication_source'] in AUTHOR_COLON_TITLE_JOURNALS:
            # Split at first colon — title may contain its own colons (subtitles)
            m = re.match(
                r'^([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+)?'  # First [Middle] [Last]
                r'(?:\s+(?:and|&)\s+[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+)?)?'  # optional 2nd author
                r'):\s+([A-Z].{15,})$',
                t,
            )
            if m:
                name_part = m.group(1).strip()
                title_part = m.group(2).strip()
                # Take only the first author if multiple
                first_author = re.split(r'\s+(?:and|&)\s+', name_part)[0].strip()
                name_words = first_author.split()
                # Last word must be at least 4 letters and look like a surname
                if (2 <= len(name_words) <= 4 and len(name_words[-1]) >= 4 and
                    not re.search(r'(?:tion|ness|ment|ity|ing|ed|ly)$', name_words[-1].lower())):
                    skip = {'Book', 'Critical', 'Review', 'Volume', 'Special',
                            'Symposium', 'Note', 'Special Issue'}
                    if name_words[0] not in skip:
                        changes['book_title'] = title_part
                        changes['book_author_first_name'] = ' '.join(name_words[:-1])
                        changes['book_author_last_name'] = name_words[-1]
                        t = title_part
                        af = changes['book_author_first_name']
                        al = changes['book_author_last_name']

        # --- Apply changes ---
        if changes:
            if not dry_run:
                sets = ', '.join(f"{k} = ?" for k in changes)
                vals = list(changes.values()) + [rid]
                cur.execute(f"UPDATE reviews SET {sets} WHERE id = ?", vals)
            fixed += 1
            details.append(f"FIXED {rid}: {list(changes.keys())} in '{r['book_title'][:40]}'")

    if not dry_run:
        conn.commit()
    conn.close()

    return {
        'fixed': fixed,
        'deleted': deleted,
        'issues': fixed + deleted,
        'checked': len(rows),
        'details': details,
    }


def retry_missing_reviewers(since_days=180, dry_run=False):
    """Retry Crossref lookup for entries with missing reviewer names.

    Some journals (Project MUSE titles like JHOP, The Thomist) deposit
    entries to Crossref before the author field is populated. This retry
    fills in reviewer names once Crossref has the data.

    Args:
        since_days: only check entries added within this many days (default 180).
        dry_run: if True, report but don't update.

    Returns:
        dict with 'updated', 'checked', 'details'.
    """
    import requests

    conn = sqlite3.connect(db.DB_PATH)
    cur = conn.cursor()

    # Find entries with DOIs but no reviewer name, from the last N days
    cur.execute(f"""
        SELECT id, doi, publication_source
        FROM reviews
        WHERE doi IS NOT NULL AND doi != ''
        AND (reviewer_first_name IS NULL OR trim(reviewer_first_name) = '')
        AND (reviewer_last_name IS NULL OR trim(reviewer_last_name) = '')
        AND created_at >= datetime('now', '-{int(since_days)} days')
    """)
    rows = cur.fetchall()

    updated = 0
    details = []

    for row_id, doi, source in rows:
        first = ''
        last = ''
        try:
            r = requests.get(f'https://api.crossref.org/works/{doi}', timeout=15)
            if r.status_code == 200:
                authors = r.json()['message'].get('author', [])
                if authors:
                    first = authors[0].get('given', '').strip()
                    last = authors[0].get('family', '').strip()
        except Exception as e:
            log.debug(f"Crossref error for {doi}: {e}")

        # MUSE fallback: 10.1353/* DOIs deposit reviewer names slowly to Crossref
        # but expose `citation_author` meta tag immediately on the article page.
        if not last and doi.startswith('10.1353/'):
            try:
                resp = requests.get(
                    f'https://doi.org/{doi}', timeout=15, allow_redirects=True,
                    headers={'User-Agent': 'Mozilla/5.0'},
                )
                if resp.status_code == 200:
                    m = re.search(r'name="citation_author"\s+content="([^"]+)"', resp.text)
                    if m:
                        name = m.group(1).strip()
                        parts = name.split()
                        if len(parts) >= 2:
                            first = ' '.join(parts[:-1])
                            last = parts[-1]
                        elif parts:
                            last = parts[0]
            except Exception as e:
                log.debug(f"MUSE error for {doi}: {e}")

        if not last:
            continue
        if not dry_run:
            cur.execute(
                "UPDATE reviews SET reviewer_first_name = ?, reviewer_last_name = ? WHERE id = ?",
                (first, last, row_id)
            )
        updated += 1
        details.append(f"FILLED {row_id} ({source}): {first} {last}")

    if not dry_run:
        conn.commit()
    conn.close()

    return {
        'updated': updated,
        'checked': len(rows),
        'details': details,
    }


def retry_missing_book_authors(since_days=14, dry_run=False):
    """Look up book authors via OpenAlex for entries where book_author is empty.

    Targets entries that have a non-empty title but no book author — common
    for journals where Crossref records provide only the book title (e.g.
    HOPOS, American Political Thought, some Hastings entries).

    Args:
        since_days: only check entries added within this many days (default 14).
        dry_run: if True, report but don't update.

    Returns:
        dict with 'updated', 'checked', 'details'.
    """
    import requests
    import time

    conn = sqlite3.connect(db.DB_PATH)
    cur = conn.cursor()

    updated = 0
    details = []

    # Pre-fetch the reviewer name for each row so we can avoid matching it as
    # the book author (a common OpenAlex failure mode for symposium articles).
    cur.execute(f"""
        SELECT id, book_title, publication_source,
               reviewer_first_name, reviewer_last_name
        FROM reviews
        WHERE (book_author_first_name IS NULL OR trim(book_author_first_name) = '')
        AND (book_author_last_name IS NULL OR trim(book_author_last_name) = '')
        AND book_title IS NOT NULL AND length(trim(book_title)) > 10
        AND created_at >= datetime('now', '-{int(since_days)} days')
    """)
    rows = cur.fetchall()

    def _normalize(s):
        return re.sub(r'[^a-z0-9 ]', '', (s or '').lower()).strip()

    for row_id, title, source, rev_first, rev_last in rows:
        try:
            clean = re.sub(r'<[^>]+>', '', title or '').strip()
            if not clean:
                continue
            resp = requests.get(
                'https://api.openalex.org/works',
                params={'search': clean[:200], 'per_page': 10,
                        'mailto': 'mzwolinski@sandiego.edu'},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            results = resp.json().get('results', [])
            target_norm = _normalize(clean)

            # Strict matching only:
            # 1. type must be book or monograph (not article/chapter/etc.)
            # 2. normalized title must match exactly OR target must be a prefix of result
            # 3. publication year must be 2015+ (most book reviews are of recent books)
            # 4. author cannot be the reviewer
            best = None
            for w in results:
                if w.get('type') not in ('book', 'monograph'):
                    continue
                w_title = _normalize(w.get('title') or '')
                if not w_title:
                    continue
                # Exact match or our title is a strong prefix of theirs
                if w_title != target_norm and not w_title.startswith(target_norm):
                    if not target_norm.startswith(w_title) or len(target_norm) - len(w_title) > 30:
                        continue
                # Year sanity check
                year = w.get('publication_year') or 0
                if year and year < 2010:
                    continue
                # Don't match if the OpenAlex author is our reviewer
                authors = w.get('authorships', [])
                if not authors:
                    continue
                name = authors[0].get('author', {}).get('display_name', '').strip()
                if not name:
                    continue
                if rev_last and rev_last.lower() in name.lower():
                    continue
                best = w
                break

            if not best:
                continue
            name = best['authorships'][0]['author']['display_name'].strip()
            parts = name.split()
            if len(parts) < 2:
                continue
            # Skip names with weird formatting (commas indicate "Last, First" or descriptors)
            if ',' in name or any(len(p) > 25 for p in parts):
                continue
            first = ' '.join(parts[:-1])
            last = parts[-1]
            if not dry_run:
                cur.execute(
                    "UPDATE reviews SET book_author_first_name = ?, book_author_last_name = ? WHERE id = ?",
                    (first, last, row_id),
                )
            updated += 1
            details.append(f"OPENALEX {row_id} ({source[:25]}): {first} {last} for {clean[:40]}")
            time.sleep(0.5)  # be polite to OpenAlex
        except Exception as e:
            log.debug(f"OpenAlex error for {(title or '')[:40]}: {e}")

    if not dry_run:
        conn.commit()
    conn.close()

    return {'updated': updated, 'checked': len(rows), 'details': details}


# Tokens that should never appear in a clean author field — their presence means
# publisher/citation/translator text leaked in during parsing.
_AUTHOR_JUNK_PATTERNS = [
    (r'\d{4}', 'year'),
    (r'[£$]', 'price'),
    (r'\bISBN\b', 'isbn'),
    (r'\b[Pp]p\b\.?', 'page-count'),
    (r'\b\d+\s*pp\b', 'page-count'),
    (r'translat', 'translator'),
    (r'&amp', 'html-amp'),
    (r'\(eds?\b', 'editor-paren'),
    (r'\bedited by\b', 'editor'),
    (r'\s+by\s+', 'by-clause'),
    (r'\b(University|Routledge|Springer|Verlag|Editions?|Macmillan|'
     r'Blackwell|Clarendon|Palgrave|Wiley|Brill|Continuum|Eerdmans|'
     r'Publishing|Publishers)\b', 'publisher'),
    (r'<[^>]+>', 'html-tag'),
    (r'^(By|by|Par|On|Review of|Trans|Ed)\b', 'citation-prefix'),
]


def _author_junk_reasons(s):
    """Return a list of reasons an author-field string looks like leaked junk."""
    s = (s or '').strip()
    if not s:
        return []
    return [why for pat, why in _AUTHOR_JUNK_PATTERNS if re.search(pat, s)]


def flag_suspect_recent(since):
    """Read-only safety net: flag entries added since `since` whose author/title
    fields still look garbled or non-review after the corrective integrity pass.

    This does NOT modify data — it surfaces suspects so they can be reviewed
    before (or shortly after) they reach production. Catches whatever the
    format-specific auto-fixers miss, regardless of source journal.

    Returns {'count': int, 'suspects': [ {id, source, title, author, reasons} ]}.
    """
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, publication_source, book_title,
                  book_author_first_name, book_author_last_name,
                  entry_type
           FROM reviews WHERE created_at >= ?
           ORDER BY publication_source, id""",
        (since,),
    ).fetchall()
    conn.close()

    suspects = []
    for r in rows:
        af = (r['book_author_first_name'] or '').strip()
        al = (r['book_author_last_name'] or '').strip()
        title = (r['book_title'] or '').strip()
        reasons = []

        # 1) Junk leaked into author fields
        reasons += [f'author:{why}' for why in _author_junk_reasons(af)]
        reasons += [f'author:{why}' for why in _author_junk_reasons(al)]

        # 2) Author missing while the title still carries an author cue
        #    (real garble signature: "Review of X, Title" / "Title by X" / "Last, First:")
        if not af and not al and (r['entry_type'] or 'review') != 'symposium':
            if (re.search(r'\s+by\s+[A-Z]', title)
                    or re.match(r'^Review of\b', title)
                    or re.match(r'^[A-Z][a-z]+,\s+[A-Z][a-z]+:', title)
                    or re.match(r'^[A-Z][a-z]+:\s+[A-Z]', title)):
                reasons.append('missing-author-with-cue')

        # 3) Title residue: HTML, leading "Last, First:" author prefix, or
        #    a single concatenated CamelCase token (e.g. "AnnHartle")
        if '<' in title:
            reasons.append('title:html-tag')
        if re.match(r'^[A-Z][a-z]+,\s+[A-Z][a-z]+:\s', title):
            reasons.append('title:author-prefix')
        if ' ' not in title and re.search(r'[a-z][A-Z]', title):
            reasons.append('title:concatenated')

        if reasons:
            suspects.append({
                'id': r['id'],
                'source': r['publication_source'],
                'title': title,
                'author': (af + ' ' + al).strip(),
                'reasons': sorted(set(reasons)),
            })

    return {'count': len(suspects), 'suspects': suspects}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    if "--flag-suspect" in sys.argv:
        since = None
        for arg in sys.argv[1:]:
            if arg.startswith("--since="):
                since = arg.split("=", 1)[1]
        res = flag_suspect_recent(since or "1970-01-01")
        print(f"Suspect entries: {res['count']}")
        for s in res['suspects']:
            print(f"  [{s['id']}] {s['source']}: {s['title'][:50]!r} / {s['author']!r}  <- {', '.join(s['reasons'])}")
        sys.exit(0)
    dry_run = "--dry-run" in sys.argv
    since = None
    for arg in sys.argv[1:]:
        if arg.startswith("--since="):
            since = arg.split("=", 1)[1]

    result = run_integrity_check(since=since, dry_run=dry_run)
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"{prefix}Checked: {result['checked']}, Fixed: {result['fixed']}, Deleted: {result['deleted']}")
    for d in result['details']:
        print(f"  {d}")
