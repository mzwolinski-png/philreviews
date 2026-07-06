#!/usr/bin/env python3
"""Ingest French PHILOSOPHY book reviews from OpenEdition's "Lectures".

Lectures (journals.openedition.org/lectures) is OpenEdition's academic
book-review platform — thousands of free French-language *comptes rendus*
across the humanities. It exposes a clean OAI-PMH feed (structured Dublin
Core), so we harvest that rather than scrape HTML. Each record is explicitly
typed `review`; we parse book author/title from dc:title ("Author, Title"),
reviewer from dc:creator, and run the Atlantic Haiku gate (it handles French)
to keep only philosophy.

Usage:
  python3 openedition_scraper.py --from 2026-01-01 --max-pages 3 --dry-run  # pilot
  python3 openedition_scraper.py --from 2020-01-01                          # window
  python3 openedition_scraper.py                                            # full backfill
"""
import argparse
import os
import re
import sys
import time
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

import db
from atlantic_scraper import relevance_gate

OAI_URL = "https://oai.openedition.org/"
SET = "journals:lectures"
SOURCE = "Lectures (OpenEdition)"
UA = "PhilReviews/2.0 (+https://philreviews.org; book-review index; mailto:mzwolinski@sandiego.edu)"
NS = {"oai": "http://www.openarchives.org/OAI/2.0/",
      "dc": "http://purl.org/dc/elements/1.1/",
      "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"}
DELAY = 1.0


def fetch_records(from_date=None, max_pages=None):
    """Harvest OAI records for the Lectures set, following resumptionToken."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})
    params = {"verb": "ListRecords", "metadataPrefix": "oai_dc", "set": SET}
    if from_date:
        params["from"] = from_date
    out, page, token = [], 0, None
    while True:
        q = {"verb": "ListRecords", "resumptionToken": token} if token else params
        try:
            r = sess.get(OAI_URL, params=q, timeout=45)
            if r.status_code == 503:  # OAI "retry later"
                time.sleep(10); continue
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  OAI error page {page}: {e}"); time.sleep(5); continue
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            print(f"  XML parse error: {e}"); break
        err = root.find("oai:error", NS)
        if err is not None:
            print(f"  OAI error: {err.get('code')} {err.text}"); break
        recs = root.findall(".//oai:record", NS)
        out.extend(recs)
        page += 1
        rt = root.find(".//oai:resumptionToken", NS)
        if page == 1:
            total = rt.get("completeListSize") if rt is not None else "?"
            print(f"  Lectures: ~{total} records total"
                  + (f", capping at {max_pages} pages" if max_pages else ""))
        token = rt.text if (rt is not None and rt.text) else None
        if not token or (max_pages and page >= max_pages):
            break
        time.sleep(DELAY)
    return out


def _dc(md, tag):
    return [e.text.strip() for e in md.findall(f"dc:{tag}", NS) if e.text and e.text.strip()]


def _split_reviewer(name):
    """dc:creator is 'Last, First' -> (first, last)."""
    if "," in name:
        last, first = name.split(",", 1)
        return first.strip(), last.strip()
    p = name.split()
    return (" ".join(p[:-1]), p[-1]) if len(p) > 1 else ("", name)


_ED_MARK = re.compile(r"\s*\((?:dir|éd|ed|eds)\.?\)\s*", re.I)
_NAME_TOK = r"[A-ZÉÈÀÎÔ][\w'’.-]+(?:\s+(?:de|du|von|van|le|la)?\s*[A-ZÉÈÀÎÔa-z][\w'’.-]+){1,3}"


def _split_book(title_raw):
    """dc:title is 'Author[, Author...][ (dir.)], Book Title' -> (first, last, title).
    Captures the full leading name-list (joined 'A, B and C'), drops (dir.)-style
    editor markers, keeps all co-authors (single-author capture used to strand
    co-directors in the title). Blank author if the head doesn't look like names."""
    m = re.match(rf"^({_NAME_TOK}(?:\s*,\s*{_NAME_TOK})*)\s*(?:\((?:dir|éd|ed|eds)\.?\))?\s*,\s+(.{{6,}})$",
                 title_raw.strip())
    if not m:
        return "", "", _ED_MARK.sub(" ", title_raw).strip()
    parts = [p.strip() for p in m.group(1).split(",")
             if p.strip() and not any(ch.isdigit() for ch in p)]
    book = _ED_MARK.sub(" ", m.group(2)).strip(" ,")
    if not parts or any(len(p.split()) > 4 for p in parts):
        return "", "", title_raw.strip()
    joined = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]
    toks = joined.split()
    return " ".join(toks[:-1]), toks[-1], book


def parse_record(rec):
    header = rec.find("oai:header", NS)
    if header is not None and header.get("status") == "deleted":
        return None
    md = rec.find("oai:metadata/oai_dc:dc", NS)
    if md is None:
        return None
    types = _dc(md, "type")
    if types and not any("review" in t.lower() for t in types):
        return None
    titles, creators, dates, ids, descs = (_dc(md, k) for k in
                                           ("title", "creator", "date", "identifier", "description"))
    if not titles:
        return None
    link = next((i for i in ids if i.startswith("http") and "openedition" in i), None)
    if not link:
        return None
    af, al, book = _split_book(titles[0])
    rf, rl = _split_reviewer(creators[0]) if creators else ("", "")
    return {"book_title": book, "author_first": af, "author_last": al,
            "rev_first": rf, "rev_last": rl,
            "date": (dates[0][:10] if dates else ""),
            "link": link, "summary": (descs[0][:800] if descs else "")}


def run(from_date=None, dry_run=False, max_pages=None):
    recs = fetch_records(from_date, max_pages)
    st = {"fetched": len(recs), "parsed": 0, "already_in_db": 0,
          "gate_checked": 0, "gate_pass": 0, "inserted": 0, "samples": []}
    to_add = []
    for rec in recs:
        p = parse_record(rec)
        if not p:
            continue
        st["parsed"] += 1
        if db.review_link_exists(p["link"]):
            st["already_in_db"] += 1; continue
        author_disp = (p["author_first"] + " " + p["author_last"]).strip()
        st["gate_checked"] += 1
        relevant, prim, sec = relevance_gate(p["book_title"], author_disp, p["summary"])
        if not relevant:
            continue
        st["gate_pass"] += 1
        to_add.append({
            "book_title": p["book_title"],
            "book_author_first_name": p["author_first"], "book_author_last_name": p["author_last"],
            "reviewer_first_name": p["rev_first"], "reviewer_last_name": p["rev_last"],
            "publication_source": SOURCE, "publication_date": p["date"],
            "review_link": p["link"], "review_summary": p["summary"],
            "access_type": "Open", "entry_type": "review",
            "subfield_primary": prim, "subfield_secondary": sec,
        })
        if len(st["samples"]) < 20:
            st["samples"].append(f"{p['book_title'][:46]} — {author_disp} (rev. {p['rev_first']} {p['rev_last']}) [{p['date']}] {prim or '?'}")
        if not dry_run and len(to_add) >= 25:
            db.insert_reviews(to_add); st["inserted"] += len(to_add); to_add = []
    if to_add and not dry_run:
        db.insert_reviews(to_add); st["inserted"] += len(to_add)
    elif dry_run:
        st["inserted"] = f"(dry-run: {len(to_add)} would insert)"
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_date", help="OAI from-date YYYY-MM-DD")
    ap.add_argument("--max-pages", type=int, help="cap OAI pages (pilot)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    st = run(from_date=a.from_date, dry_run=a.dry_run, max_pages=a.max_pages)
    print("\n=== OpenEdition Lectures philosophy backfill ===")
    for k in ("fetched", "parsed", "already_in_db", "gate_checked", "gate_pass", "inserted"):
        print(f"  {k}: {st[k]}")
    print("samples (gate-passed):")
    for s in st["samples"]:
        print("  +", s)
