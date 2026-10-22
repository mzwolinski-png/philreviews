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
        swap = src[src.index("rm -f /data/reviews.db-wal /data/reviews.db-shm"):]
        # and it happens before the new file is moved into place
        self.assertIn("mv -f /data/reviews_new.db /data/reviews.db", swap)


class SyncDoesNotTakeTheSiteDown(unittest.TestCase):
    """2026-10-22: `fly apps restart` after each sync refused connections for
    ~13 s, and Search Console counted the crawls that landed then as 5xx."""
    def test_no_restart_unless_the_live_check_fails(self):
        src = inspect.getsource(deploy._sync_once)
        swap = src.index("mv -f /data/reviews_new.db")
        check = src.index("if _site_serves():", swap)
        self.assertLess(check, src.index("_RELOAD_SCRIPT", swap))
        self.assertLess(src.index("_RELOAD_SCRIPT", swap), src.index('"apps", "restart"', swap))
        self.assertIn("os.kill(master, signal.SIGHUP)", deploy._RELOAD_PY)

    def test_reload_script_is_valid_python(self):
        compile(deploy._RELOAD_PY, "reload", "exec")


class UploadStaysOffTheVolume(unittest.TestCase):
    """2026-10-11: the compressed upload on /data left no room to unpack it."""
    def test_compressed_upload_goes_to_tmp(self):
        src = inspect.getsource(deploy)
        self.assertIn("/tmp/reviews_new.db.gz", src)
        self.assertNotIn("put {gz_path} /data/", src)

    def test_unpack_result_is_checked(self):
        src = inspect.getsource(deploy)
        self.assertIn("UNPACK_OK", src)
        self.assertIn("Could not unpack", src)


if __name__ == "__main__":
    unittest.main()
