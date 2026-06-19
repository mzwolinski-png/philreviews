#!/usr/bin/env python3
"""The Atlantic books scraper.

Discovers book reviews from The Atlantic's Books Atom feed by reading the
embedded JSON-LD ``@type: Review`` schema on each article — which both
identifies an item as a genuine book review (newsletters / essays / poems /
fiction lack it) and exposes structured metadata: the reviewer, the reviewed
book's title and author and ISBN, and the date. No body parsing or paywall
access required.

Relevance policy (broader-with-classifier): reviews of books already in our
database are added directly; reviews of NEW books pass a philosophy-relevance
gate (Haiku) and are added only if judged philosophically relevant, with the
returned subfield(s). Fiction, poetry, memoir, and general-interest titles are
skipped.
"""

import html as html_mod
import json
import logging
import os
import re
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import db

log = logging.getLogger("atlantic_scraper")

SOURCE = "The Atlantic"
ACCESS = "Restricted"  # The Atlantic is paywalled
FEEDS = [
    "https://www.theatlantic.com/feed/channel/books/",
    "https://www.theatlantic.com/feed/channel/culture/",
]
STATE_FILE = os.path.join(ROOT, "atlantic_state.json")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
CRAWL_DELAY = 2.0

VALID_SUBFIELDS = {
    "ethics", "applied-ethics", "political", "legal", "epistemology",
    "metaphysics", "science", "aesthetics", "religion", "history",
    "ancient", "modern", "continental", "feminist", "non-western",
}


# ── State ────────────────────────────────────────────────────────

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return set(json.load(f).get("seen", []))
        except Exception:
            pass
    return set()


def save_state(seen):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"seen": sorted(seen)}, f)
    except Exception:
        log.warning("Could not write Atlantic state file")


# ── Parsing helpers ──────────────────────────────────────────────

def _clean(s):
    return re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", "", s or ""))).strip()


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def feed_entry_links(feed_xml):
    """Return article URLs from an Atom feed (deduped, query-stripped)."""
    links = []
    for entry in re.findall(r"<entry>(.*?)</entry>", feed_xml, re.S):
        m = re.search(r'<link[^>]*href="([^"]+)"', entry)
        if m:
            url = m.group(1).split("?")[0]
            links.append(url)
    seen, out = set(), []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _iter_ld_nodes(obj):
    """Yield every dict node in a parsed JSON-LD value, descending @graph."""
    if isinstance(obj, list):
        for x in obj:
            yield from _iter_ld_nodes(x)
    elif isinstance(obj, dict):
        if "@graph" in obj:
            yield from _iter_ld_nodes(obj["@graph"])
        yield obj


def _person_names(field):
    """Normalize a JSON-LD author/person field to a list of name strings."""
    out = []
    if isinstance(field, dict):
        if field.get("name"):
            out.append(field["name"])
    elif isinstance(field, list):
        for x in field:
            out.extend(_person_names(x))
    elif isinstance(field, str):
        out.append(field)
    return [n for n in (s.strip() for s in out) if n]


def _split_one(name):
    """'Pare-Poupart, Simon' -> ('Simon', 'Pare-Poupart'); 'Lily Meyer' -> ('Lily','Meyer')."""
    name = _clean(name)
    if "," in name:
        last, first = name.split(",", 1)
        return first.strip(), last.strip()
    parts = name.split()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


def book_author_fields(names):
    """Apply the DB's multi-author convention to a list of 'Last, First' names."""
    pairs = [_split_one(n) for n in names]
    if not pairs:
        return "", ""
    if len(pairs) == 1:
        return pairs[0]
    # "First1 Last1, First2 Last2, and First3" / "Last3"
    full = [f"{f} {l}".strip() for f, l in pairs]
    last_first, last_last = pairs[-1]
    head = full[:-1]
    joiner = ", and " if len(head) > 1 else " and "
    first_field = ", ".join(head[:-1] + [head[-1]]) if len(head) > 1 else head[0]
    first_field = first_field + joiner + last_first
    return first_field.strip(), last_last


def extract_review(article_html):
    """Return review dict from a Review JSON-LD node, or None if not a review."""
    for blk in re.findall(r'application/ld\+json[^>]*>(.*?)</script>', article_html, re.S):
        try:
            data = json.loads(blk)
        except Exception:
            continue
        for node in _iter_ld_nodes(data):
            if node.get("@type") != "Review":
                continue
            item = node.get("itemReviewed") or {}
            if not isinstance(item, dict):
                continue
            title = _clean(item.get("name", ""))
            if not title:
                continue
            book_names = _person_names(item.get("author"))
            af, al = book_author_fields(book_names)
            reviewer = _person_names(node.get("author"))
            rf, rl = _split_one(reviewer[0]) if reviewer else ("", "")
            date = (node.get("datePublished") or "")[:10]
            return {
                "book_title": title,
                "book_author_first_name": af,
                "book_author_last_name": al,
                "reviewer_first_name": rf,
                "reviewer_last_name": rl,
                "publication_date": date,
                "isbn": _clean(item.get("isbn", "")),
                "description": _clean(node.get("description", "")),
            }
    return None


# ── DB match + relevance ─────────────────────────────────────────

def book_lookup(conn, title, author_last):
    """If the reviewed book already exists in the DB, return its subfields."""
    nt = _norm(title)
    if not nt:
        return None
    rows = conn.execute(
        "SELECT book_title, subfield_primary, subfield_secondary FROM reviews "
        "WHERE book_author_last_name = ? COLLATE NOCASE",
        (author_last,),
    ).fetchall()
    for bt, sp, ss in rows:
        nb = _norm(bt)
        if nb and (nb == nt or nt in nb or nb in nt):
            return {"primary": sp or "", "secondary": ss or ""}
    return None


RELEVANCE_SYSTEM = (
    "You decide whether a book belongs in an index of PHILOSOPHY book reviews. "
    "Philosophy-relevant includes: ethics, political and social philosophy, "
    "philosophy of law, epistemology, philosophy of mind, metaphysics, logic, "
    "philosophy of science, aesthetics, philosophy of religion/theology, history "
    "of philosophy, ancient/medieval, early modern, continental/phenomenology, "
    "feminist philosophy, non-western/comparative philosophy, and serious "
    "intellectual works of political theory or moral/social thought. "
    "NOT relevant: fiction, poetry, memoir, biography (unless of a philosopher's "
    "thought), genre nonfiction, journalism, self-help, popular science without "
    "philosophical argument. "
    "Reply with ONLY a JSON object: {\"relevant\": true|false, \"primary\": "
    "\"<subfield code or null>\", \"secondary\": \"<subfield code or null>\"}. "
    "Subfield codes: ethics, applied-ethics, political, legal, epistemology, "
    "metaphysics, science, aesthetics, religion, history, ancient, modern, "
    "continental, feminist, non-western."
)


def relevance_gate(title, author_display, description):
    """Haiku gate for NEW books. Returns (relevant, primary, secondary)."""
    try:
        import anthropic
    except ImportError:
        return False, "", ""
    msg = f"Book: {title}\nAuthor: {author_display}\nReview blurb: {description or '(none)'}"
    try:
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            system=RELEVANCE_SYSTEM,
            messages=[{"role": "user", "content": msg}],
        )
        text = resp.content[0].text.strip()
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            return False, "", ""
        d = json.loads(m.group(0))
        if not d.get("relevant"):
            return False, "", ""
        p = d.get("primary") if d.get("primary") in VALID_SUBFIELDS else ""
        s = d.get("secondary") if d.get("secondary") in VALID_SUBFIELDS else ""
        return True, p or "", s or ""
    except Exception as e:
        log.warning(f"Relevance gate failed for '{title}': {e}")
        return False, "", ""


# ── Orchestration ────────────────────────────────────────────────

class AtlanticScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA, "Accept-Encoding": "gzip"})

    def _get(self, url):
        try:
            r = self.session.get(url, timeout=25, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            log.debug(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            log.debug(f"fetch error {url}: {e}")
        return None

    def run(self, dry_run=False, full=False):
        seen = set() if full else load_state()
        candidate_urls = []
        for feed in FEEDS:
            xml = self._get(feed)
            if xml:
                candidate_urls += feed_entry_links(xml)
        # dedupe across feeds, preserve order
        candidate_urls = list(dict.fromkeys(candidate_urls))
        new_urls = [u for u in candidate_urls if u not in seen]
        log.info(f"Atlantic: {len(candidate_urls)} feed items, {len(new_urls)} new to check")

        conn = db._connect()
        to_add, skipped_nonreview, skipped_irrelevant = [], 0, 0
        for url in new_urls:
            seen.add(url)
            html = self._get(url)
            time.sleep(CRAWL_DELAY)
            if not html:
                continue
            rev = extract_review(html)
            if not rev:
                skipped_nonreview += 1
                continue
            existing = book_lookup(conn, rev["book_title"], rev["book_author_last_name"])
            if existing is not None:
                primary, secondary = existing["primary"], existing["secondary"]
            else:
                author_display = f"{rev['book_author_first_name']} {rev['book_author_last_name']}".strip()
                relevant, primary, secondary = relevance_gate(
                    rev["book_title"], author_display, rev["description"])
                if not relevant:
                    skipped_irrelevant += 1
                    log.info(f"  skip (not philosophy): {rev['book_title']} — {author_display}")
                    continue
            record = {
                "book_title": rev["book_title"],
                "book_author_first_name": rev["book_author_first_name"],
                "book_author_last_name": rev["book_author_last_name"],
                "reviewer_first_name": rev["reviewer_first_name"],
                "reviewer_last_name": rev["reviewer_last_name"],
                "publication_source": SOURCE,
                "publication_date": rev["publication_date"],
                "review_link": url,
                "review_summary": "",
                "access_type": ACCESS,
                "doi": "",
                "entry_type": "review",
                "symposium_group": "",
                "subfield_primary": primary,
                "subfield_secondary": secondary,
            }
            to_add.append(record)
            log.info(f"  + {rev['book_title']} — review by "
                     f"{rev['reviewer_first_name']} {rev['reviewer_last_name']} "
                     f"[{'in-db' if existing else 'new/' + (primary or '?')}]")
        conn.close()

        if to_add and not dry_run:
            db.insert_reviews(to_add)
        if not dry_run:
            save_state(seen)
        log.info(f"Atlantic: {len(to_add)} reviews "
                 f"({'would add' if dry_run else 'added'}), "
                 f"{skipped_nonreview} non-reviews, {skipped_irrelevant} off-topic")
        return {"uploaded": 0 if dry_run else len(to_add),
                "found": len(to_add), "checked": len(new_urls),
                "non_reviews": skipped_nonreview, "off_topic": skipped_irrelevant,
                "records": to_add}


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--full", action="store_true", help="Ignore state; re-check all feed items")
    args = ap.parse_args()
    stats = AtlanticScraper().run(dry_run=args.dry_run, full=args.full)
    print(f"\nAtlantic: found {stats['found']}, "
          f"{'would add' if args.dry_run else 'added'} {stats['found']}; "
          f"non-reviews {stats['non_reviews']}, off-topic {stats['off_topic']}")


if __name__ == "__main__":
    main()
