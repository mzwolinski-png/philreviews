"""Source classification and the staleness alarm.

The alarm exists because a scraper that finds nothing reports success. It is
only useful if it stays short and specific, so these tests pin the two things
that make it noisy: journals that date issues annually, and journals that
closed years ago but received a late Crossref deposit.
"""
import os, sqlite3, sys, tempfile, unittest
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))


class SourceHealth(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "t.db")
        import db as real_db
        self._orig = real_db.DB_PATH
        real_db.DB_PATH = self.path
        import source_health
        self.sh = source_health
        conn = sqlite3.connect(self.path)
        conn.execute("""CREATE TABLE reviews (id INTEGER PRIMARY KEY,
            publication_source TEXT, publication_date TEXT, created_at TEXT)""")
        conn.commit(); conn.close()
        self.sh._configured_sources = lambda: {
            "Live Journal", "Annual Journal", "Broken Journal", "Closed Journal"}

    def tearDown(self):
        import db as real_db
        real_db.DB_PATH = self._orig

    def _add(self, source, dates, ingested):
        conn = sqlite3.connect(self.path)
        for d in dates:
            conn.execute("INSERT INTO reviews (publication_source, publication_date, created_at)"
                         " VALUES (?,?,?)", (source, d, ingested))
        conn.commit(); conn.close()

    @staticmethod
    def _ago(days):
        return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    def test_live_journal_is_active_and_not_stale(self):
        self._add("Live Journal", [self._ago(d) for d in (5, 35, 65, 95, 125)], self._ago(5))
        self.sh.seed()
        self.assertNotIn("Live Journal", [s["source"] for s in self.sh.check_stale()])

    def test_annually_dated_journal_does_not_trip_the_alarm(self):
        # issues dated Jan 1 each year; ingested recently. A fixed threshold
        # would flag this every autumn.
        self._add("Annual Journal", ["2026-01-01", "2025-01-01", "2024-01-01", "2023-01-01"],
                  self._ago(3))
        self.sh.seed()
        self.assertNotIn("Annual Journal", [s["source"] for s in self.sh.check_stale()])

    def test_broken_detection_is_caught_once_marked_active(self):
        # frequent issues that stop dead, while ingestion continues — the AJP shape
        dates = [self._ago(d) for d in (400, 430, 460, 490, 520)]
        self._add("Broken Journal", dates, self._ago(2))
        self.sh.seed()
        self.sh.set_status("Broken Journal", "active", note="confirmed still publishing")
        hits = [s["source"] for s in self.sh.check_stale()]
        self.assertIn("Broken Journal", hits)

    def test_long_closed_journal_is_not_called_active(self):
        # last issue years ago, one late deposit — must ask, not assume
        self._add("Closed Journal", ["2010-01-01", "2009-10-01", "2009-07-01", "2009-04-01"],
                  self._ago(2))
        self.sh.seed()
        status = {r["source"]: r["status"] for r in self.sh.needs_decision()}
        self.assertIn("Closed Journal", status)
        self.assertNotIn("Closed Journal", [s["source"] for s in self.sh.check_stale()])

    def test_human_decision_survives_reseeding(self):
        self._add("Closed Journal", ["2010-01-01", "2009-10-01", "2009-07-01"], self._ago(2))
        self.sh.seed()
        self.sh.set_status("Closed Journal", "ceased", note="review section closed 2010")
        self.sh.seed()
        self.assertEqual(self.sh.summary().get("ceased"), 1)
        self.assertNotIn("Closed Journal", [r["source"] for r in self.sh.needs_decision()])

    def test_unconfigured_sources_never_alarm(self):
        self._add("Some Old Import", ["1998-01-01"], self._ago(2))
        self.sh.seed()
        self.assertNotIn("Some Old Import", [s["source"] for s in self.sh.check_stale()])
