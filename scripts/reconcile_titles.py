#!/usr/bin/env python3
"""Reconcile fragmented book_title variants so all reviews of the same book
share one canonical title. CONSERVATIVE.

Categories:
  A) Case/punctuation/whitespace variants (normalized-identical titles).
  B) Subtitle add/remove (one normalized title is a word-boundary prefix of
     another).

Excluded to avoid merging distinct works:
  - Multi-volume / multi-part sets (Vol, Volume, Band, Teil, Reihe, Werke,
    standalone trailing numerals/roman numerals).
  - Edition variants (Revised Edition, Anniversary Edition, Nth ed) — kept
    separate; they are legitimately different editions.
  - Symposium-label rows ([Précis], [Author's Reply], [Symposium...]).

Canonical selection: from cleaned candidates (trailing author-name junk
stripped), prefer the most frequent form that has a subtitle (contains ':');
otherwise the most frequent form. Ties broken by length.
"""
import sqlite3, re, sys
from collections import defaultdict, Counter

DB = "reviews.db"

VOLUME_MARKER = re.compile(
    r'(\bvol\.?\b|\bvolume\b|\bvolumes\b|\bband\b|\bbd\.\b|\bteil|\breihe\b|\bwerke\b'
    r'|\bvolume\s+(one|two|three|four|i{1,3}v?|\d+)\b'
    r'|,\s*(i{1,3}v?|\d+)\s*$)',
    re.IGNORECASE)
EDITION_MARKER = re.compile(
    r'(revised edition|anniversary edition|\b\d+(st|nd|rd|th)?\s*ed(ition)?\b'
    r'|second edition|third edition|new edition|expanded edition)',
    re.IGNORECASE)
SYMPOSIUM_LABEL = re.compile(r'\[(pr[ée]cis|author.?s reply|symposium[^\]]*)\]', re.IGNORECASE)
# Trailing author-name junk: "...realtext FirstName LastName" or ALLCAPS surname, or "University of ..."
AUTHOR_JUNK = re.compile(
    r'\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+\s+(?:[A-Z][a-z]+|[A-Z]{2,})$'
    r'|\s+[A-Z]{3,}$'
    r'|\s+University of .*$')

def norm(t):
    t = re.sub(r'<[^>]+>', '', t)
    t = t.replace('’', "'").replace('‘', "'").replace('‐','-').replace('–','-').replace('—','-')
    t = t.lower()
    t = re.sub(r'[^a-z0-9 ]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

AUTHOR_JUNK_DETECT = re.compile(
    r'\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+\s+(?:[A-Z][a-z]+|[A-Z]{2,})$|\s+[A-Z]{3,}$|\s+University of .*$')

def pick_canonical(raw_counts):
    """Most frequent form wins (junk/rare-subtitle variants are always rare).
    Ties: prefer a form without trailing author-name junk, then longer.
    Never truncates — only chooses among existing raw strings."""
    def junky(r):
        m = AUTHOR_JUNK_DETECT.search(r)
        return bool(m and m.start() > 20)
    return max(raw_counts, key=lambda r: (raw_counts[r], not junky(r), len(r)))

def find_clusters(db_path=None):
    """Detect title-variant clusters. Returns (catA, catB) where each is a list
    of (canonical_title, [ids_to_update], {raw_variants}). Read-only."""
    conn = sqlite3.connect(db_path or DB)
    rows = conn.execute("""
        SELECT id, book_title, book_author_last_name
        FROM reviews
        WHERE book_title IS NOT NULL AND book_title != ''
          AND book_author_last_name IS NOT NULL AND book_author_last_name != ''
    """).fetchall()
    conn.close()
    by_author = defaultdict(list)
    for rid, title, bal in rows:
        by_author[bal.lower().strip()].append((rid, title, norm(title)))

    catA, catB = [], []
    for author, items in by_author.items():
        items = [(rid, t, n) for (rid, t, n) in items if not SYMPOSIUM_LABEL.search(t)]
        # Category A
        ng = defaultdict(list)
        for rid, t, n in items:
            if len(n) >= 6: ng[n].append((rid, t))
        for n, members in ng.items():
            raws = {t for _, t in members}
            if len(raws) > 1:
                rc = Counter(t for _, t in members)
                canon = pick_canonical(rc)
                ids = [rid for rid, t in members if t != canon]
                if ids:
                    catA.append((canon, ids, raws))
        # Category B: strict subtitle add/remove via COLON separator only.
        # "X" merges with "X: subtitle" (norm of pre-colon part == norm(X)).
        # This excludes space-joined extensions ("Companion to Kant" vs
        # "Companion to Kant and Modern Philosophy") and comma/volume forms.
        norm_to_raws = defaultdict(list)
        for rid, t, n in items:
            norm_to_raws[n].append((rid, t))
        # map: bare normalized title -> list of (rid, raw) for colon-subtitle forms
        bare_norms = set(norm_to_raws.keys())
        for n, members in list(norm_to_raws.items()):
            # find colon-subtitle variants whose pre-colon norm == this bare norm
            cluster_raw_ids = defaultdict(list)
            for rid, t, nn in items:
                if ':' not in t:
                    continue
                pre = norm(t.split(':', 1)[0])
                if pre == n and nn != n:
                    cluster_raw_ids[t].append(rid)
            if not cluster_raw_ids:
                continue
            # add the bare members
            for rid, t in members:
                cluster_raw_ids[t].append(rid)
            # exclude volume / edition variants
            if any(VOLUME_MARKER.search(t) or EDITION_MARKER.search(t)
                   for t in cluster_raw_ids):
                continue
            if len(cluster_raw_ids) < 2:
                continue
            rc = Counter({t: len(ids) for t, ids in cluster_raw_ids.items()})
            canon = pick_canonical(rc)
            ids = [rid for t, idl in cluster_raw_ids.items() for rid in idl if t != canon]
            if ids:
                catB.append((canon, ids, set(cluster_raw_ids)))
    return catA, catB


def apply_clusters(clusters, db_path=None):
    """Apply a list of (canonical, [ids], variants) clusters. Returns rows updated."""
    conn = sqlite3.connect(db_path or DB)
    n = 0
    for canon, ids, _ in clusters:
        for rid in ids:
            conn.execute("UPDATE reviews SET book_title=? WHERE id=?", (canon, rid))
            n += 1
    conn.commit()
    conn.close()
    return n


def run_weekly(db_path=None):
    """Weekly maintenance entry point.

    Auto-applies Category A (pure case/punctuation — cannot merge distinct
    works) and returns Category B proposals (subtitle add/remove) for review
    WITHOUT applying them. Returns dict with counts and the catB proposals.
    """
    catA, catB = find_clusters(db_path)
    applied = apply_clusters(catA, db_path) if catA else 0
    return {
        "catA_books": len(catA),
        "catA_applied": applied,
        "catB_books": len(catB),
        "catB_rows": sum(len(ids) for _, ids, _ in catB),
        "catB": catB,
    }


def write_catb_report(catb, path):
    """Write an HTML report of Category B (subtitle) merge proposals for review."""
    import html as _html
    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<title>Title reconciliation — proposed subtitle merges</title>"
        "<style>body{font-family:'Source Sans 3',Helvetica,Arial,sans-serif;max-width:900px;"
        "margin:40px auto;padding:0 20px;color:#222;line-height:1.45}"
        "h1{font-family:'Libre Baskerville',Georgia,serif;font-size:24px}"
        "p.meta{color:#666;font-size:14px}"
        "li.cluster{margin-bottom:14px;padding:8px 10px;border-left:3px solid #e5e5e5}"
        ".canon{color:#1a3b6e;font-weight:600}.from{color:#777;font-size:13px;margin-left:18px}"
        "code{background:#f4f4f4;padding:1px 4px;border-radius:3px}</style></head><body>",
        "<h1>Proposed subtitle merges (Category B) — review before applying</h1>",
        f"<p class='meta'>{len(catb)} books, "
        f"{sum(len(ids) for _,ids,_ in catb)} rows. These are NOT yet applied. "
        f"To apply after review: <code>python3 scripts/reconcile_titles.py --apply B</code></p>",
        "<ul>",
    ]
    for canon, ids, raws in sorted(catb, key=lambda c: -len(c[1])):
        parts.append(f"<li class='cluster'><span class='canon'>{_html.escape(canon)}</span> "
                     f"<span style='color:#999'>({len(ids)} rows)</span>")
        for r in sorted(raws):
            if r != canon:
                parts.append(f"<div class='from'>← {_html.escape(r)}</div>")
        parts.append("</li>")
    parts.append("</ul></body></html>")
    with open(path, "w") as f:
        f.write("".join(parts))
    return path


def main(apply=False, category=None):
    catA, catB = find_clusters()
    def tot(c): return sum(len(ids) for _, ids, _ in c)
    print(f"Category A (case/punct): {len(catA)} books, {tot(catA)} rows to update")
    print(f"Category B (subtitle):   {len(catB)} books, {tot(catB)} rows to update")
    print("\n=== Sample B (canonical -> variants merged in) ===")
    for canon, ids, raws in sorted(catB, key=lambda c: -len(c[1]))[:14]:
        print(f"  -> {canon[:65]!r} ({len(ids)} rows)")
        for r in sorted(raws):
            if r != canon:
                print(f"       {r[:65]!r}")
    if apply:
        targets = (catA if category in (None, 'A') else []) + (catB if category in (None, 'B') else [])
        n = apply_clusters(targets)
        print(f"\nAPPLIED ({category or 'A+B'}): {n} rows updated.")


if __name__ == "__main__":
    cat = None
    for a in sys.argv:
        if a in ('A', 'B', '--A', '--B'): cat = a.strip('-')
    main(apply="--apply" in sys.argv, category=cat)
