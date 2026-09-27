"""The production swap must take the old database's -wal/-shm files with it.

2026-09-27: a deploy's schema migration left an uncheckpointed write in
/data/reviews.db-wal; sync_to_fly swapped the database file but kept the WAL,
SQLite replayed it into the new file on open, and production answered some
searches with "database disk image is malformed".
"""
import inspect
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import deploy  # noqa: E402


class SwapClearsWal(unittest.TestCase):
    def test_swap_removes_the_old_wal_and_shm(self):
        src = inspect.getsource(deploy)
        swap = src[src.index("mv /data/reviews.db /data/reviews_old.db"):]
        swap = swap[:swap.index("rm -f /data/reviews_old.db")]
        self.assertIn("rm -f /data/reviews.db-wal /data/reviews.db-shm", swap)
        # and it happens before the new file is moved into place
        self.assertLess(swap.index("reviews.db-wal"), swap.index("mv /data/reviews_new.db"))


if __name__ == "__main__":
    unittest.main()
