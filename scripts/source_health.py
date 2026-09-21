"""Track which sources are supposed to be live, and alarm when one goes quiet.

The weekly run cannot currently tell "this journal closed its review section"
from "our detection rule broke". The Australasian Journal of Philosophy stopped
matching in mid-2025 and nothing noticed for a year, because a scraper that
finds nothing reports success (audit 2026-09-21).

A bare "no reviews lately" alarm is useless here: 831 of 1,504 sources are
historical imports that will never produce again. So each source carries a
status, and only ACTIVE ones can raise an alarm.

    active   configured scraper, produced within the last 12 months
    unknown  configured, but quiet since 2020-2025 — needs one human decision
    ceased   the journal really has stopped reviewing; never alarm
    archive  no scraper (past bulk import); never alarm

Seeding is automatic except for `unknown`, which is deliberately small.
"""
import html
import os
import sqlite3
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db

ACTIVE_WINDOW_DAYS = 365      # produced inside this -> active
ARCHIVE_BEFORE = "2020-01-01"  # nothing since this -> archive
STALE_DAYS = 90                # an active source silent this long -> alarm

_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_status (
    source        TEXT PRIMARY KEY,
    status        TEXT NOT NULL,
    last_reviewed TEXT,
    configured    INTEGER NOT NULL DEFAULT 0,
    note          TEXT,
    decided_at    TEXT
);
"""


def _conn():
    c = sqlite3.connect(db.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.executescript(_SCHEMA)
    return c


def _configured_sources():
    """Journal names the Crossref scraper is configured for, entity-decoded.

    Crossref registers some titles HTML-encoded ("Mind &amp; Language") and the
    scraper stores them decoded, so comparing raw keys silently misses them.
    """
    try:
        from journals import JOURNALS
    except Exception:
        return set()
    return {html.unescape(j) for j in JOURNALS}


def seed(dry_run=False):
    """Classify every source. Existing human decisions are never overwritten."""
    conn = _conn()
    configured = _configured_sources()
    cutoff = (datetime.now() - timedelta(days=ACTIVE_WINDOW_DAYS)).strftime("%Y-%m-%d")
    decided = {r["source"] for r in conn.execute(
        "SELECT source FROM source_status WHERE decided_at IS NOT NULL")}

    counts = {"active": 0, "unknown": 0, "ceased": 0, "archive": 0, "kept": 0}
    rows = conn.execute("""SELECT publication_source AS s, MAX(publication_date) AS last,
                                  MAX(created_at) AS seen
                           FROM reviews GROUP BY publication_source""").fetchall()
    for r in rows:
        src, last, seen = r["s"], (r["last"] or ""), (r["seen"] or "")[:10]
        if src in decided:
            counts["kept"] += 1
            continue
        is_cfg = src in configured
        if not is_cfg:
            status = "archive"
        elif last >= cutoff:
            status = "active"          # current issues are arriving
        elif seen >= cutoff:
            # Something was ingested recently but every issue we hold is old.
            # That is either a journal that closed years ago and got a late
            # deposit (Philosophical Books, shut in 2008) or a live journal
            # whose detection rule broke (the AJP failure). The two are
            # indistinguishable from the data, so ask rather than guess.
            status = "unknown"
        elif last < ARCHIVE_BEFORE:
            status = "archive"
        else:
            status = "unknown"
        counts[status] += 1
        if not dry_run:
            conn.execute("""INSERT INTO source_status (source, status, last_reviewed, configured)
                            VALUES (?,?,?,?)
                            ON CONFLICT(source) DO UPDATE SET
                              status = excluded.status,
                              last_reviewed = excluded.last_reviewed,
                              configured = excluded.configured""",
                         (src, status, last, 1 if is_cfg else 0))
    if not dry_run:
        conn.commit()
    conn.close()
    return counts


def set_status(source, status, note=""):
    """Record a human decision. Seeding will not override it afterwards."""
    if status not in ("active", "unknown", "ceased", "archive"):
        raise ValueError(f"unknown status: {status}")
    conn = _conn()
    conn.execute("""INSERT INTO source_status (source, status, note, decided_at)
                    VALUES (?,?,?,?)
                    ON CONFLICT(source) DO UPDATE SET
                      status = excluded.status, note = excluded.note,
                      decided_at = excluded.decided_at""",
                 (source, status, note, datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()


def _typical_gap_days(conn, source, sample=40):
    """Median gap between consecutive issue dates for a source.

    Journals date issues very differently — some continuously, some as a single
    "2026-01-01" for the whole year. A fixed threshold therefore flags every
    annually-dated journal each autumn. Measuring each source against its own
    cadence removes that noise.
    """
    dates = [r[0][:10] for r in conn.execute(
        """SELECT DISTINCT publication_date FROM reviews
           WHERE publication_source = ? AND publication_date GLOB '[12][0-9][0-9][0-9]-*'
           ORDER BY publication_date DESC LIMIT ?""", (source, sample))]
    if len(dates) < 4:
        return None
    gaps = []
    for a, b in zip(dates, dates[1:]):
        try:
            gaps.append((datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")).days)
        except ValueError:
            continue
    gaps = [g for g in gaps if g > 0]
    if not gaps:
        return None
    gaps.sort()
    return gaps[len(gaps) // 2]


def check_stale(stale_days=STALE_DAYS):
    """Active sources that look like they have stopped reaching us.

    Two independent signals, because either alone gives a wrong answer:

      * nothing INGESTED for `stale_days` — the scrape itself has stopped;
      * the newest issue we hold has fallen far behind the source's own
        cadence, which catches the case where odd late deposits keep
        trickling in while current issues are silently missed. That is exactly
        how the Australasian Journal of Philosophy failed.

    Only `active` sources qualify, which keeps the list short enough to read.
    """
    conn = _conn()
    now = datetime.now()
    ingest_cutoff = (now - timedelta(days=stale_days)).strftime("%Y-%m-%d")
    out = []
    for r in conn.execute("SELECT * FROM source_status WHERE status = 'active'"):
        src = r["source"]
        row = conn.execute(
            """SELECT MAX(publication_date) p, MAX(created_at) c
               FROM reviews WHERE publication_source = ?""", (src,)).fetchone()
        last_pub, last_seen = (row["p"] or "")[:10], (row["c"] or "")[:10]
        reasons = []
        if last_seen and last_seen < ingest_cutoff:
            reasons.append("nothing ingested")
        gap = _typical_gap_days(conn, src)
        try:
            behind = (now - datetime.strptime(last_pub, "%Y-%m-%d")).days if last_pub else None
        except ValueError:
            behind = None
        # 4x its own median gap, never firing below the floor
        if behind is not None and gap is not None:
            threshold = max(stale_days, gap * 4)
            if behind > threshold:
                reasons.append(f"newest issue {behind}d old vs {gap}d typical")
        if reasons:
            out.append({
                "source": src, "last_reviewed": last_pub, "last_ingested": last_seen,
                "days_behind": behind, "typical_gap": gap, "why": "; ".join(reasons),
            })
    conn.close()
    return sorted(out, key=lambda x: -(x["days_behind"] or 0))


def needs_decision():
    """Configured sources still awaiting a human call."""
    conn = _conn()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM source_status WHERE status = 'unknown' ORDER BY last_reviewed DESC")]
    conn.close()
    return rows


def summary():
    conn = _conn()
    rows = {r["status"]: r["n"] for r in conn.execute(
        "SELECT status, COUNT(*) n FROM source_status GROUP BY status")}
    conn.close()
    return rows


if __name__ == "__main__":
    if "--seed" in sys.argv:
        print("seeded:", seed(dry_run="--dry-run" in sys.argv))
    print("counts:", summary())
    stale = check_stale()
    print(f"\nstale active sources ({len(stale)}):")
    for s in stale:
        print(f"   {str(s['days_behind']):>5}d behind  last {s['last_reviewed']}  "
              f"{s['source'][:44]:46} {s['why']}")
    pend = needs_decision()
    print(f"\nawaiting a decision ({len(pend)}):")
    for p in pend[:20]:
        print(f"   last {p['last_reviewed']}  {p['source']}")
