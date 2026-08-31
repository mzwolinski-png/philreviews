#!/usr/bin/env python3
"""
LLM-assisted auto-fix for suspect new entries.

flag_suspect_recent() (integrity_check.py) detects newly added entries whose
author/title fields still look garbled after the deterministic corrective
pass. Historically those were only *flagged* in the Sunday admin email for
manual repair. In practice nearly all of them have an obvious fix (author
name embedded in the title, publisher residue, "Review of X by Y" phrasing),
so this module asks Claude to repair them automatically before the sync and
report, and only surfaces the genuinely ambiguous ones for review.

Safety properties:
- Only touches entries already flagged as suspect (never rewrites clean rows).
- Only applies fixes the model marks high-confidence; everything else stays
  flagged exactly as before.
- Never deletes: entries the model thinks aren't book reviews at all are
  surfaced as "needing review", not removed.
- Every applied fix is reported before -> after in the admin email for
  spot-checking.
- Any failure (no API key, network, bad JSON) degrades gracefully to the old
  flag-only behavior.

Usage:
    from auto_fix_suspects import auto_fix_recent
    result = auto_fix_recent(since=start_utc)          # apply
    result = auto_fix_recent(since=..., dry_run=True)  # propose only
"""

import json
import logging
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

log = logging.getLogger("auto_fix")

MODEL = "claude-sonnet-5"


def _crossref_meta(doi):
    """Best-effort Crossref lookup for independent evidence of the true title."""
    try:
        url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "PhilReviews/1.0 (mailto:mzwolinski@gmail.com)"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            msg = json.load(resp).get("message", {})
        return {
            "title": (msg.get("title") or [None])[0],
            "subtitle": (msg.get("subtitle") or [None])[0],
        }
    except Exception:
        return None


PROMPT = """You are cleaning garbled entries in a philosophy book-review database.
Each entry describes ONE published review of ONE book. The fields
book_title / book_author_first_name / book_author_last_name should hold the
REVIEWED BOOK's title and its author — but scraping sometimes leaves the
author's name inside the title, publisher/price/page residue, "Review of ..."
phrasing, HTML tags, etc.

For each entry below, decide the correct fields. Evidence you may use:
the raw fields themselves, the journal name, the reviewer, and when present:
- crossref_title/crossref_subtitle: fetched independently from the DOI
- review_excerpt: the review's opening text (often names the book's author)
- openalex_authors / openlibrary_authors: authors of a closely title-matched
  work in the OpenAlex / Open Library databases (strong evidence for a
  missing book author, especially when the match includes the full subtitle
  or a plausible publication year)

Rules:
- NEVER invent an author. Fill a missing author only from the evidence given
  (title-embedded "by X"/"ed. X", review_excerpt, openalex_authors) or when
  the book is famous enough that you are certain of its author. Otherwise
  leave the author fields as-is (or empty) and mark "unsure".
- A title wrapped in quotation marks is often the review essay's own title,
  not the book's. Extract the real book only if it is evident; else "unsure".
- Keep religious-order suffixes on names (e.g. last name "Lastname, O.P.").
- Multi-author books: put all authors except the final surname in
  book_author_first_name and the final surname in book_author_last_name
  (e.g. "Amia Srinivasan and David" / "Chalmers" for two authors —
  first-name field carries 'A and B'-style joined text).
- Edited volumes: the editors count as the authors; do not append "(ed.)".
- Always fill BOTH name fields when the full name is known: "ed. Chenyang Li"
  -> first_name "Chenyang", last_name "Li" (never last name alone).
- Strip publisher, place, price, page-count, ISBN, year residue from titles.
- Keep genuine subtitles (after a colon) — do not truncate real titles.
- If the entry does not look like a review of a book at all (it's an article,
  a "books received" list, an editorial), use action "not_review".
- confidence "high" ONLY when you are essentially certain of every field you
  change. Anything debatable -> "low".

Respond with ONLY a JSON array, one object per entry:
{"id": <id>, "action": "fix"|"unsure"|"not_review",
 "book_title": "...", "book_author_first_name": "...",
 "book_author_last_name": "...", "confidence": "high"|"low",
 "note": "<one short line: what was wrong / why unsure>"}
For "unsure"/"not_review" the field values are ignored.

Entries:
"""


def _propose_fixes(entries):
    """One API call proposing fixes for all suspect entries. Returns list of
    dicts (parsed model output) or None on any failure."""
    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed — skipping auto-fix")
        return None
    client = anthropic.Anthropic()
    payload = json.dumps(entries, ensure_ascii=False, indent=1)
    for attempt in (1, 2):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=8000,
                messages=[{"role": "user", "content": PROMPT + payload}])
            text = "".join(b.text for b in resp.content
                           if getattr(b, "type", "") == "text").strip()
            text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text)
            out = json.loads(text)
            if isinstance(out, list):
                return out
            log.warning(f"Auto-fix: non-list model output (attempt {attempt})")
        except Exception:
            log.exception(f"Auto-fix model call failed (attempt {attempt})")
    return None


def _norm_title(s):
    return re.sub(r'[^a-z0-9 ]', '', (s or '').lower()).strip()


def _openalex_authors(title):
    """Best-effort OpenAlex title search. Returns a list of author display
    names ONLY when a result's title matches ours closely — independent
    evidence for filling missing book authors, never a guess."""
    main = (title or '').split(':')[0].strip().strip('"“”')
    if len(main) < 8:
        return None
    try:
        url = ("https://api.openalex.org/works?search="
               + urllib.parse.quote(main)
               + "&select=title,authorships,publication_year&per_page=5"
               + "&mailto=mzwolinski@gmail.com")
        with urllib.request.urlopen(
                urllib.request.Request(url, headers={
                    "User-Agent": "PhilReviews/1.0"}), timeout=15) as resp:
            results = json.load(resp).get("results", [])
        want = _norm_title(main)
        for r in results:
            got = _norm_title(r.get("title") or "")
            if got == want or got.startswith(want) or want.startswith(got):
                names = [a.get("author", {}).get("display_name")
                         for a in r.get("authorships", [])]
                names = [n for n in names if n]
                if 0 < len(names) <= 6:
                    return names
        return None
    except Exception:
        return None


def _openlibrary_authors(title):
    """Open Library title search — same close-match discipline as OpenAlex."""
    main = (title or '').split(':')[0].strip().strip('"“”')
    if len(main) < 8:
        return None
    try:
        url = ("https://openlibrary.org/search.json?title="
               + urllib.parse.quote(main)
               + "&limit=5&fields=title,author_name,first_publish_year")
        with urllib.request.urlopen(
                urllib.request.Request(url, headers={
                    "User-Agent": "PhilReviews/1.0"}), timeout=20) as resp:
            docs = json.load(resp).get("docs", [])
        want = _norm_title(main)
        for d in docs:
            if _norm_title(d.get("title") or "") == want:
                names = d.get("author_name") or []
                if 0 < len(names) <= 6:
                    return names
        return None
    except Exception:
        return None


def _queue_suspects():
    """Suspects from the interactive review queue (reviewed=0), using the
    review app's broader heuristics (e.g. any missing author, not just
    missing-author-with-cue). Mirrors flag_suspect_recent's item shape."""
    try:
        from review_app import _suspect_reasons
    except ImportError:
        return []
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, publication_source, book_title,
                  book_author_first_name, book_author_last_name, entry_type
           FROM reviews WHERE reviewed = 0
           ORDER BY publication_source, id""").fetchall()
    conn.close()
    out = []
    for r in rows:
        af = (r["book_author_first_name"] or "").strip()
        al = (r["book_author_last_name"] or "").strip()
        reasons = _suspect_reasons(af, al, r["book_title"], r["entry_type"])
        if reasons:
            out.append({
                "id": r["id"], "source": r["publication_source"],
                "title": (r["book_title"] or "").strip(),
                "author": (af + " " + al).strip(),
                "reasons": reasons,
            })
    return out


BATCH = 12


def auto_fix_recent(since=None, dry_run=False, include_queue=True):
    """Repair suspect entries: those added since `since` (integrity-check
    heuristics) plus, when include_queue, everything still awaiting triage
    in the review queue (review-app heuristics, e.g. missing authors).

    Returns {'checked': int, 'fixed': [...], 'remaining': [...]} where each
    fixed item has id/source/old_title/new_title/old_author/new_author/note
    and each remaining item mirrors flag_suspect_recent's suspects (plus an
    optional model note).
    """
    from integrity_check import flag_suspect_recent

    suspects, seen = [], set()
    if since:
        for s in flag_suspect_recent(since=since)["suspects"]:
            if s["id"] not in seen:
                seen.add(s["id"])
                suspects.append(s)
    if include_queue:
        for s in _queue_suspects():
            if s["id"] not in seen:
                seen.add(s["id"])
                suspects.append(s)
    result = {"checked": len(suspects), "fixed": [], "remaining": []}
    if not suspects:
        return result

    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    entries = []
    for s in suspects:
        r = conn.execute(
            """SELECT id, publication_source, book_title,
                      book_author_first_name, book_author_last_name,
                      reviewer_first_name, reviewer_last_name, doi,
                      review_link, review_summary
               FROM reviews WHERE id = ?""", (s["id"],)).fetchone()
        if not r:
            continue
        e = {
            "id": r["id"],
            "journal": r["publication_source"],
            "book_title": r["book_title"],
            "book_author_first_name": r["book_author_first_name"],
            "book_author_last_name": r["book_author_last_name"],
            "reviewer": " ".join(filter(None, [r["reviewer_first_name"],
                                               r["reviewer_last_name"]])),
            "flag_reasons": s["reasons"],
        }
        if r["review_summary"]:
            e["review_excerpt"] = r["review_summary"][:600]
        if r["doi"]:
            meta = _crossref_meta(r["doi"])
            if meta and meta.get("title"):
                e["crossref_title"] = meta["title"]
                if meta.get("subtitle"):
                    e["crossref_subtitle"] = meta["subtitle"]
        # Missing author entirely -> try independent title matches
        if not e["book_author_first_name"] and not e["book_author_last_name"]:
            names = _openalex_authors(r["book_title"])
            if names:
                e["openalex_authors"] = names
            ol = _openlibrary_authors(r["book_title"])
            if ol:
                e["openlibrary_authors"] = ol
        entries.append(e)

    by_id = {}
    for i in range(0, len(entries), BATCH):
        chunk = entries[i:i + BATCH]
        proposals = _propose_fixes(chunk)
        if proposals:
            by_id.update({p.get("id"): p for p in proposals})

    for s, e in zip(suspects, entries):
        p = by_id.get(e["id"])
        if (p and p.get("action") == "fix" and p.get("confidence") == "high"
                and (p.get("book_title") or "").strip()):
            new_t = p["book_title"].strip()
            new_af = (p.get("book_author_first_name") or "").strip()
            new_al = (p.get("book_author_last_name") or "").strip()
            if not dry_run:
                conn.execute(
                    """UPDATE reviews SET book_title = ?,
                              book_author_first_name = ?,
                              book_author_last_name = ? WHERE id = ?""",
                    (new_t, new_af, new_al, e["id"]))
            result["fixed"].append({
                "id": e["id"], "source": e["journal"],
                "old_title": e["book_title"], "new_title": new_t,
                "old_author": s["author"],
                "new_author": (new_af + " " + new_al).strip(),
                "note": p.get("note", ""),
            })
        else:
            item = dict(s)
            if p:
                item["note"] = f"[{p.get('action')}/{p.get('confidence')}] " \
                               f"{p.get('note', '')}"
            result["remaining"].append(item)

    if not dry_run and result["fixed"]:
        conn.commit()
    conn.close()
    log.info(f"Auto-fix: {len(result['fixed'])} fixed, "
             f"{len(result['remaining'])} left for review"
             f"{' (dry run)' if dry_run else ''}")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if args else None  # no date -> queue-only sweep
    dry = "--apply" not in sys.argv
    res = auto_fix_recent(since, dry_run=dry)
    print(f"\nchecked={res['checked']} fixed={len(res['fixed'])} "
          f"remaining={len(res['remaining'])} {'(DRY RUN)' if dry else ''}\n")
    for f in res["fixed"]:
        print(f"FIX {f['id']} [{f['source']}]")
        print(f"    title:  {f['old_title']!r}\n        ->  {f['new_title']!r}")
        print(f"    author: {f['old_author']!r} -> {f['new_author']!r}"
              f"   ({f['note']})")
    for r in res["remaining"]:
        print(f"REVIEW {r['id']} [{r['source']}] {r['title'][:60]!r} "
              f"/ {r['author']!r} {r.get('note', '')}")
