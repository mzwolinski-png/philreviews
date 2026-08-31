#!/usr/bin/env python3
"""
PhilReviews Springer Nature API Scanner — Daily incremental scan.

Scans Springer Nature journals for book reviews using the Meta API's genre
field. The free tier allows ~500 API calls/day (10 records per call), so this
script processes a batch each day and picks up where it left off.

Progress is tracked in springer_state.json. Once all journals are scanned,
the script switches to checking only recent content (last 30 days).

Usage:
    python3 scripts/springer_scan.py              # normal daily batch
    python3 scripts/springer_scan.py --dry-run     # preview without inserting
    python3 scripts/springer_scan.py --reset       # reset progress, start over
    python3 scripts/springer_scan.py --status      # show current progress
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests

# Ensure project root is on the path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db
from scraper_base import setup_logging

STATE_FILE = os.path.join(ROOT, "scripts", "springer_state.json")

META_API_KEY = "37c3af4d34efb70c709ab050579d4f8c"
META_API_URL = "https://api.springernature.com/meta/v2/json"

# Max API calls per daily run (free tier safety margin)
DAILY_CALL_LIMIT = 450

# Springer-published philosophy journals and their ISSNs
SPRINGER_JOURNALS = {
    "Acta Analytica": "0353-5150",
    "Biology and Philosophy": "0169-3867",
    "Continental Philosophy Review": "1387-2842",
    "Dao": "1540-3009",
    "Erkenntnis": "0165-0106",
    "Ethical Theory and Moral Practice": "1386-2820",
    "Ethics and Information Technology": "1388-1957",
    "Foundations of Science": "1233-1821",
    "Grazer Philosophische Studien": "0165-9227",
    "Journal of Business Ethics": "0167-4544",
    "Journal of Indian Philosophy": "0022-1791",
    "Law and Philosophy": "0167-5249",
    "Medicine, Health Care and Philosophy": "1386-7423",
    "Minds and Machines": "0924-6495",
    "Neuroethics": "1874-5490",
    "Phenomenology and the Cognitive Sciences": "1568-7759",
    "Philosophia": "0048-3893",
    "Philosophical Studies": "0031-8116",
    "Ratio": "0034-0006",
    "Res Publica": "1356-4765",
    "Studia Logica": "0039-3215",
    "Synthese": "0039-7857",
    "Theoria": "0040-5825",
    # Added Apr 2026 — poor Crossref detection, Springer genre field is more reliable
    "Human Studies": "0163-8548",
    "Theoretical Medicine and Bioethics": "1386-7415",
    "Human Rights Review": "1524-8879",
    "Philosophy of Management": "1740-3812",
    "Monash Bioethics Review": "1321-2753",
    "History and Philosophy of the Life Sciences": "0391-9714",
    "Studies in East European Thought": "0925-9392",
    "Journal of the History of Biology": "0022-5010",
}

log = setup_logging("springer_scan", os.path.join(ROOT, "scripts", "springer_scan.log"))


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"journals": {}, "phase": "initial_scan", "total_api_calls": 0}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_journal_total(issn: str) -> int:
    """Get total number of articles for a journal from Springer API."""
    try:
        resp = requests.get(META_API_URL, params={
            "q": f"issn:{issn}", "s": 1, "p": 1, "api_key": META_API_KEY,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        total_str = data.get("result", [{}])[0].get("total", "0")
        return int(total_str)
    except Exception as e:
        log.error(f"Error getting total for ISSN {issn}: {e}")
        return 0


# --- "Review of: Author, Title, <publisher, year, pages, ISBN, price>" parser ---
# Some Springer journals (notably Studies in East European Thought) give the
# review article's title as the full bibliographic citation of the reviewed
# book. Parse the reviewed book + author out of that string at ingest, instead
# of storing the whole citation as book_title. Fails safe (returns author '' or
# None) rather than mis-splitting when the format is unfamiliar.
_PUB_KW = re.compile(r'\b(Press|Publ\b|Publishers?|Publishing|Verlag|Books|University|Macmillan|'
    r'Palgrave|Routledge|Springer|Brill|Bloomsbury|Continuum|Eerdmans|Rialp|Anzos|Ediciones|'
    r'Izdatel|Vodoley|THEOS|Academic Studies|Peter Lang|Harvard|Yale|Princeton|Cornell|Chicago|'
    r'Columbia|Oxford|Cambridge|Edinburgh|Fordham|Wiley|Blackwell|Sage|Duke|Stanford|MIT|'
    r'de Gruyter|Mohr|Suhrkamp|ibidem|Center for Humanitarian|'
    r'Uniwersytet|Universytet|Universit(?:ät|at|é|y|à|et)|Wydawnictwo|[ÉE]ditions|Editrice|'
    r'Editore|Nauka|Aletheia|Vandenhoeck|Ruprecht|Sch[öo]ningh|Reclam)\b', re.I)
_LOC_KW = re.compile(r'\b(New York|London|Cham|Moscow|Saint Petersburg|St\.? Petersburg|'
    r'Sankt[- ]Peterburg|Madrid|Boston|Berlin|Paris|Hannover|Stuttgart|Grand Rapids|Oxford|'
    r'Cambridge|Edinburgh|Washington|Chicago|Toronto|Leiden|Notre Dame|Collegeville|Ann Arbor|'
    r'Bloomington|Las Vegas|Zurich|Zug|Dublin|Bern|Vienna|Wien|Amsterdam|Milano?|Rome|Roma|'
    r'Warsaw|Warszawa|Kyiv|Kiev|Minneapolis|Baltimore|Ithaca|New Haven|Zielona G[óo]ra|'
    r'Krak[óo]w|Pozna[ńn]|Lublin|G[öo]ttingen|T[üu]bingen|Freiburg|M[üu]nchen|Munich|Frankfurt)\b',
    re.I)
_CITE_YEAR = r'(?:1[5-9]\d{2}|20\d{2})'
_CITE_CONNECTORS = {'and', 'y', '&', 'de', 'van', 'der', 'von', 'del', 'la', 'di', 'dos', 'e'}


def _cite_split(s):
    """Split on top-level commas, respecting () [] {} and quotes."""
    parts, buf, depth, q = [], '', 0, False
    for ch in s:
        if ch in '«([{':
            depth += 1; buf += ch
        elif ch in '»)]}':
            depth = max(0, depth - 1); buf += ch
        elif ch in '"“”':
            q = not q; buf += ch
        elif ch == ',' and depth == 0 and not q:
            parts.append(buf.strip()); buf = ''
        else:
            buf += ch
    if buf.strip():
        parts.append(buf.strip())
    return parts


def _cite_is_meta(seg):
    """True if a trailing comma-segment is publication metadata, not title text."""
    s = seg.strip()
    if not s:
        return True
    if re.search(r'\bISBN\b', s, re.I):
        return True
    if re.search(r'[€£$]\s?\d', s):
        return True
    if re.search(r'\b\d+\s*(?:pages?|pp\.?|p\.)(?:\b|$)', s, re.I):
        return True
    if re.search(r'\b(hardcover|paperback|hardback|hardbound|hbk|pbk|cloth|e-?book|ebook|'
                 r'softcover|softback)\b', s, re.I):
        return True
    if re.search(r'\b(?:ed\.?\s+by|edited by|translated\b.*\bby|trans\.?\b.*\bby)\b', s, re.I):
        return True
    if re.match(r'^\(?\s*in\s+[A-Z][a-z]+\s*\)?[.,]?$', s, re.I):   # (In Russian)
        return True
    if re.match(r'^no price\.?$', s, re.I):
        return True
    if _PUB_KW.search(s):
        return True
    if len(s.split()) <= 3 and _LOC_KW.search(s):
        return True
    if re.match(r'^\(?\s*' + _CITE_YEAR + r'(?:\s*[-–]\s*' + _CITE_YEAR + r')?\s*\)?[.,]?$', s):
        return True
    if re.fullmatch(r'\d{1,6}', s):     # stray leaked page/article id
        return True
    return False


def _cite_strip_tail(text):
    segs = _cite_split(text)
    while len(segs) > 1 and _cite_is_meta(segs[-1]):
        segs.pop()
    return ', '.join(segs).strip().rstrip(' .,;')


def _cite_is_person_name(s):
    s = s.strip().rstrip('.')
    if not s or any(ch.isdigit() for ch in s):
        return False
    toks = s.split()
    if not (2 <= len(toks) <= 7):
        return False
    real = 0
    for tok in toks:
        if tok.lower().strip('.') in _CITE_CONNECTORS:
            continue
        if not re.match(r"^[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÿ.'’\-]*$", tok):
            return False
        real += 1
    return real >= 2


def _cite_split_name(full):
    toks = full.strip().split()
    if len(toks) == 1:
        return '', toks[0]
    return ' '.join(toks[:-1]), toks[-1]


def parse_review_citation(title):
    """'Review of: Author, Title, <pub info>' -> (book_title, author_first, author_last),
    or None if `title` is not a 'Review of' citation. author_* are '' when no author
    is present in the string (e.g. an unattributed edited volume)."""
    t = re.sub(r'\s+', ' ', (title or '')).strip()
    m = re.match(r'^Review\s*(?:of)?\s*:?\s+(.*)$', t, re.I)
    if not m:
        return None
    body = m.group(1).strip().rstrip(' .,;')
    segs = _cite_split(body)
    while len(segs) > 1 and _cite_is_meta(segs[-1]):
        segs.pop()
    if not segs:
        return None
    core = ', '.join(segs).strip()

    ed = re.match(r'^(.*?\((?:eds?\.?|editors?|compilers?|editors and compilers)[^)]*\)),?\s*(.+)$',
                  core, re.I)
    if ed:
        author = re.sub(r'\s*\((?:eds?\.?|editors?|compilers?)[^)]*\)', '', ed.group(1),
                        flags=re.I).strip().rstrip(',')
        af, al = _cite_split_name(author)
        return _cite_strip_tail(ed.group(2)), af, al

    if len(segs) >= 2 and _cite_is_person_name(segs[0]):
        af, al = _cite_split_name(segs[0].strip())
        return _cite_strip_tail(', '.join(segs[1:])), af, al

    return _cite_strip_tail(core), '', ''


def parse_springer_record(rec: dict, journal_name: str):
    """Parse a Springer Meta API record into a review dict for DB insertion."""
    genre = rec.get("genre", [])
    if not any("Book Review" in g for g in genre):
        return None

    title = rec.get("title", "").strip()
    if not title or title.lower() in ("book review", "book reviews"):
        # Bare title — still insert but mark it
        pass

    # Some journals store the reviewed book as a full "Review of: ..." citation
    # in the article title; extract the book title + author when so.
    book_title = title
    book_author_first = book_author_last = ""
    parsed_cite = parse_review_citation(title)
    if parsed_cite is not None:
        book_title, book_author_first, book_author_last = parsed_cite
    else:
        # Essay-titled reviews: "<essay title>: on <Author>'s <Book>"
        # (HPLS pattern, e.g. "Beyond anthropocentrism in comparative
        # thanatology: on Susana Monsó's Playing Possum"). The possessive
        # name gives the book author; the remainder is the book title
        # (often the short form — enrichment may extend it later).
        m_on = re.match(
            r"^.{5,120}?[:—-]\s*(?:on|about|reflections on)\s+"
            r"([A-ZÀ-Þ][\w.À-ɏ]+(?:\s+[A-ZÀ-Þ][\w.À-ɏ]+){0,3})"
            r"[’']s\s+(.{4,})$", title)
        if m_on:
            author = m_on.group(1).strip()
            cand_title = m_on.group(2).strip().rstrip(' .,;')
            parts = author.split()
            if len(parts) >= 2 and not re.search(r'\d', author):
                book_title = cand_title
                book_author_first = ' '.join(parts[:-1])
                book_author_last = parts[-1]
        elif book_author_last == "":
            # Fall back to the shared Crossref title parser, which knows the
            # "Author, Title" and publisher-citation shapes Springer journals
            # also use ("Geertje Dekkers, Myriad, Microscopic and Marvelous:
            # …, Trans. Andy Brown" was landing whole in book_title with no
            # author at all — found 2026-08-16).
            try:
                from crossref_parsing import parse_review_title
                p2 = parse_review_title(title)
            except Exception:
                p2 = None
            if p2 and p2.get("book_author_last") and p2.get("book_title"):
                book_title = p2["book_title"]
                book_author_first = p2.get("book_author_first", "")
                book_author_last = p2["book_author_last"]

    # Apparatus tails belong to neither field
    book_title = re.split(
        r',\s+(?:Trans(?:\.|lated)|Ed(?:\.|ited))\b', book_title, flags=re.I)[0].strip().rstrip('.,;: ')

    doi = rec.get("doi", "")
    url = ""
    urls = rec.get("url", [])
    for u in urls:
        if u.get("format") == "html":
            url = u.get("value", "")
            break
    if not url and urls:
        url = urls[0].get("value", "")
    if not url and doi:
        url = f"https://doi.org/{doi}"

    # Publication date
    pub_date = rec.get("publicationDate", "")

    # Authors (Springer lists the reviewer, not the book author)
    creators = rec.get("creators", [])
    reviewer_first = ""
    reviewer_last = ""
    if creators:
        creator = creators[0].get("creator", "")
        # Format: "LastName, FirstName" or just "LastName"
        if ", " in creator:
            parts = creator.split(", ", 1)
            reviewer_last = parts[0].strip()
            reviewer_first = parts[1].strip()
        else:
            reviewer_last = creator.strip()

    # Access type
    open_access = rec.get("openaccess", "false") == "true"

    return {
        "book_title": book_title,
        "book_author_first_name": book_author_first,
        "book_author_last_name": book_author_last,
        "reviewer_first_name": reviewer_first,
        "reviewer_last_name": reviewer_last,
        "publication_source": journal_name,
        "publication_date": pub_date,
        "review_link": url,
        "review_summary": "",
        "access_type": "open" if open_access else "restricted",
        "doi": doi,
        "entry_type": "review",
        "symposium_group": "",
    }


def scan_journal_batch(journal_name: str, issn: str, start: int,
                       calls_remaining: int, dry_run: bool = False):
    """Scan a batch of articles from one journal.

    Returns (new_reviews, calls_used, next_start).
    next_start = -1 means journal is fully scanned.
    """
    new_reviews = 0
    calls_used = 0

    while calls_used < calls_remaining:
        try:
            resp = requests.get(META_API_URL, params={
                "q": f"issn:{issn}", "s": start, "p": 10, "api_key": META_API_KEY,
            }, timeout=30)
            calls_used += 1

            if resp.status_code == 429:
                log.warning(f"Rate limited at call {calls_used}. Stopping.")
                return new_reviews, calls_used, start

            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])

            if not records:
                return new_reviews, calls_used, -1  # Journal done

            total_str = data.get("result", [{}])[0].get("total", "0")
            total = int(total_str)

            for rec in records:
                parsed = parse_springer_record(rec, journal_name)
                if not parsed:
                    continue

                doi = parsed["doi"]
                link = parsed["review_link"]

                if doi and db.doi_exists(doi):
                    continue
                if link and db.review_link_exists(link):
                    continue

                if dry_run:
                    log.info(f"  [DRY RUN] Would add: {parsed['book_title'][:60]}")
                else:
                    db.insert_review(parsed)
                    log.info(f"  Added: {parsed['book_title'][:60]}")
                new_reviews += 1

            start += 10
            if start > total:
                return new_reviews, calls_used, -1  # Journal done

            time.sleep(1.2)  # Rate limiting

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                log.warning("Rate limited. Stopping for today.")
                return new_reviews, calls_used, start
            log.error(f"HTTP error scanning {journal_name}: {e}")
            return new_reviews, calls_used, start
        except Exception as e:
            log.error(f"Error scanning {journal_name}: {e}")
            return new_reviews, calls_used, start

    return new_reviews, calls_used, start


def run_initial_scan(state: dict, dry_run: bool = False):
    """Run the initial full scan of all Springer journals."""
    calls_remaining = DAILY_CALL_LIMIT
    total_new = 0

    for journal_name, issn in sorted(SPRINGER_JOURNALS.items()):
        if calls_remaining <= 0:
            break

        journal_state = state["journals"].get(journal_name, {})
        if journal_state.get("done"):
            continue

        start = journal_state.get("next_start", 1)
        log.info(f"Scanning {journal_name} (ISSN {issn}) from position {start}...")

        new, used, next_start = scan_journal_batch(
            journal_name, issn, start, calls_remaining, dry_run
        )
        calls_remaining -= used
        total_new += new

        state["journals"][journal_name] = {
            "issn": issn,
            "next_start": next_start,
            "done": next_start == -1,
            "last_scanned": datetime.now().isoformat(),
            "reviews_found": journal_state.get("reviews_found", 0) + new,
        }
        state["total_api_calls"] = state.get("total_api_calls", 0) + used

        log.info(f"  {journal_name}: {new} new reviews, {used} API calls used")

        if calls_remaining <= 0:
            log.info("Daily API call limit reached.")
            break

    # Check if all journals are done
    all_done = all(
        state["journals"].get(j, {}).get("done", False)
        for j in SPRINGER_JOURNALS
    )
    if all_done:
        state["phase"] = "incremental"
        log.info("Initial scan complete! Switching to incremental mode.")

    return total_new


def run_incremental_scan(state: dict, dry_run: bool = False):
    """Check for new content in last 30 days across all journals."""
    calls_remaining = DAILY_CALL_LIMIT
    total_new = 0
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    for journal_name, issn in sorted(SPRINGER_JOURNALS.items()):
        if calls_remaining <= 10:
            break

        log.info(f"Checking {journal_name} for new content since {from_date}...")

        try:
            resp = requests.get(META_API_URL, params={
                "q": f"issn:{issn} onlinedatefrom:{from_date}",
                "s": 1, "p": 10, "api_key": META_API_KEY,
            }, timeout=30)
            calls_remaining -= 1
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])
            total_str = data.get("result", [{}])[0].get("total", "0")
            total = int(total_str)

            for rec in records:
                parsed = parse_springer_record(rec, journal_name)
                if not parsed:
                    continue
                doi = parsed["doi"]
                link = parsed["review_link"]
                if doi and db.doi_exists(doi):
                    continue
                if link and db.review_link_exists(link):
                    continue
                if dry_run:
                    log.info(f"  [DRY RUN] Would add: {parsed['book_title'][:60]}")
                else:
                    db.insert_review(parsed)
                    log.info(f"  Added: {parsed['book_title'][:60]}")
                total_new += 1

            # If more than 10 results, paginate
            start = 11
            while start <= total and calls_remaining > 0:
                resp = requests.get(META_API_URL, params={
                    "q": f"issn:{issn} onlinedatefrom:{from_date}",
                    "s": start, "p": 10, "api_key": META_API_KEY,
                }, timeout=30)
                calls_remaining -= 1
                resp.raise_for_status()
                records = resp.json().get("records", [])
                if not records:
                    break
                for rec in records:
                    parsed = parse_springer_record(rec, journal_name)
                    if not parsed:
                        continue
                    doi = parsed["doi"]
                    link = parsed["review_link"]
                    if doi and db.doi_exists(doi):
                        continue
                    if link and db.review_link_exists(link):
                        continue
                    if dry_run:
                        log.info(f"  [DRY RUN] Would add: {parsed['book_title'][:60]}")
                    else:
                        db.insert_review(parsed)
                    total_new += 1
                start += 10
                time.sleep(1.2)

            time.sleep(1.2)
        except Exception as e:
            log.error(f"Error checking {journal_name}: {e}")

    return total_new


def show_status(state: dict):
    """Print current scan progress."""
    phase = state.get("phase", "initial_scan")
    total_calls = state.get("total_api_calls", 0)
    print(f"Phase: {phase}")
    print(f"Total API calls used: {total_calls:,}")
    print()

    done_count = 0
    for journal_name in sorted(SPRINGER_JOURNALS.keys()):
        js = state.get("journals", {}).get(journal_name, {})
        done = js.get("done", False)
        if done:
            done_count += 1
        status = "DONE" if done else f"at position {js.get('next_start', 1)}"
        reviews = js.get("reviews_found", 0)
        last = js.get("last_scanned", "never")[:10]
        print(f"  {journal_name}: {status} ({reviews} reviews found, last: {last})")

    print(f"\n{done_count}/{len(SPRINGER_JOURNALS)} journals complete")


from deploy import sync_to_fly


def main():
    if "--reset" in sys.argv:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        print("State reset. Will start fresh on next run.")
        return

    state = load_state()

    if "--status" in sys.argv:
        show_status(state)
        return

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        log.info("=== DRY RUN MODE ===")

    log.info("=" * 60)
    log.info(f"PhilReviews Springer scan — {datetime.now().isoformat()}")
    log.info(f"Phase: {state.get('phase', 'initial_scan')}")
    log.info("=" * 60)

    phase = state.get("phase", "initial_scan")

    if phase == "initial_scan":
        total_new = run_initial_scan(state, dry_run)
    else:
        total_new = run_incremental_scan(state, dry_run)

    log.info(f"New reviews found: {total_new}")

    if total_new > 0 and not dry_run:
        sync_to_fly()

    if not dry_run:
        state["last_run"] = datetime.now().isoformat()
        save_state(state)

    log.info("Springer scan complete.")

    # Watchdog: check if weekly runs happened this week
    if not dry_run:
        _check_weekly_runs()


def _check_weekly_runs():
    """Alert if Sunday's weekly update didn't happen (checked on Monday)."""
    now = datetime.now()
    if now.isoweekday() != 1:  # Only check on Monday
        return
    try:
        from notify import send_missed_run_alert

        # Check update.py — look at the state file it writes
        state_file = os.path.join(ROOT, "scripts", "weekly_state.json")
        yesterday = now - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        if os.path.exists(state_file):
            with open(state_file) as f:
                ws = json.load(f)
            last_run = ws.get("last_run", "")
            if last_run and not last_run.startswith(yesterday_str):
                send_missed_run_alert("update.py (Sunday 5 AM)", f"Last run: {last_run}")
        else:
            send_missed_run_alert("update.py (Sunday 5 AM)", "No state file found")
    except Exception as e:
        log.warning(f"Weekly run watchdog check failed: {e}")


if __name__ == "__main__":
    main()
