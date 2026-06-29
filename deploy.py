#!/usr/bin/env python3
"""Shared deployment utilities for syncing the database to Fly.io."""

import gzip
import logging
import os
import shutil
import sqlite3
import subprocess
import time

import db
from scraper_base import notify_failure

log = logging.getLogger(__name__)

FLY_APP = os.environ.get("FLY_APP", "philreviews")
ROOT = os.path.dirname(os.path.abspath(__file__))


def _local_review_count():
    try:
        conn = sqlite3.connect(db.DB_PATH)
        n = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


def sync_to_fly(retries=3):
    """Upload the database to Fly.io and restart the app, retrying on failure.

    Returns True if production was successfully updated, False otherwise.
    Each attempt is self-contained (uploads to a temp path and atomically
    swaps the old DB out only after the new one is in place), so retrying is
    safe. Transient Fly/network errors were causing production to silently go
    stale for a week; the retry plus the boolean return let update.py surface
    a real failure in the admin email instead of always reporting "(OK)".
    """
    for attempt in range(1, retries + 1):
        if _sync_once():
            return True
        if attempt < retries:
            backoff = 30 * attempt  # 30s, then 60s
            log.warning(
                f"Fly.io sync attempt {attempt}/{retries} failed; retrying in {backoff}s")
            time.sleep(backoff)
    msg = f"Fly.io sync failed after {retries} attempts — production not updated"
    log.error(msg)
    notify_failure("sync_to_fly", msg)
    return False


def _sync_once():
    """One sync attempt. Returns True on success, False on failure. Logs
    failures but does not alert (the caller alerts once retries are exhausted).

    Creates a clean copy (no WAL), compresses with gzip (~65 MB vs ~200 MB),
    uploads via sftp, then decompresses on the remote machine.
    """
    log.info("Syncing database to Fly.io...")
    clean_path = db.DB_PATH + ".upload"
    gz_path = clean_path + ".gz"
    try:
        # Clean up any leftovers from a prior failed attempt
        for p in (clean_path, gz_path, clean_path + "-wal", clean_path + "-shm"):
            if os.path.exists(p):
                os.remove(p)

        # Create a clean copy with DELETE journal mode (no WAL dependency)
        shutil.copy2(db.DB_PATH, clean_path)
        for ext in ("-wal", "-shm"):
            src = db.DB_PATH + ext
            if os.path.exists(src):
                shutil.copy2(src, clean_path + ext)
        conn = sqlite3.connect(clean_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.close()
        for ext in ("-wal", "-shm"):
            p = clean_path + ext
            if os.path.exists(p):
                os.remove(p)

        # Compress for faster/more reliable upload
        with open(clean_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(clean_path)
        log.info(f"Compressed DB: {os.path.getsize(gz_path) / 1024 / 1024:.1f} MB")

        # Upload to a temp path (atomic swap: never delete old DB before new is verified)
        subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "rm -f /data/reviews_new.db.gz /data/reviews_new.db"],
            cwd=ROOT, capture_output=True, text=True, timeout=30,
        )

        result = subprocess.run(
            ["fly", "ssh", "sftp", "shell", "-a", FLY_APP],
            input=f"put {gz_path} /data/reviews_new.db.gz\n",
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        os.remove(gz_path)

        if result.returncode != 0:
            log.error(f"DB upload failed: {result.stderr}")
            return False

        # Decompress on the remote (subscribers.db is separate, never touched)
        subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "gunzip /data/reviews_new.db.gz"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )

        # Verify the uploaded DB BEFORE swapping it into place: a truncated or
        # corrupt upload that still exited 0 would otherwise overwrite good
        # production data. Run integrity_check + a row-count sanity check on the
        # remote (the image has python3 but not the sqlite3 CLI).
        check = subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "python3 -c \"import sqlite3;"
             "c=sqlite3.connect('/data/reviews_new.db');"
             "print(c.execute('PRAGMA integrity_check').fetchone()[0]);"
             "print(c.execute('SELECT COUNT(*) FROM reviews').fetchone()[0])\""],
            cwd=ROOT, capture_output=True, text=True, timeout=120,
        )
        out = [ln.strip() for ln in (check.stdout or "").replace("\r", "").splitlines() if ln.strip()]
        local_n = _local_review_count()
        remote_n = int(out[-1]) if out and out[-1].isdigit() else -1
        if check.returncode != 0 or not out or out[0] != "ok" or remote_n < local_n * 0.95:
            log.error(
                f"Uploaded DB failed verification (integrity={out[0] if out else '?'}, "
                f"remote_rows={remote_n}, local_rows={local_n}); aborting swap, "
                "production left untouched")
            return False

        result = subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "sh -c 'mv /data/reviews.db /data/reviews_old.db 2>/dev/null;"
             " mv /data/reviews_new.db /data/reviews.db"
             " && rm -f /data/reviews_old.db'"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.error(f"Remote swap failed: {result.stderr}")
            return False

        log.info("Database uploaded. Restarting app...")
        subprocess.run(
            ["fly", "apps", "restart", FLY_APP],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        log.info("Fly.io deploy complete")
        return True
    except Exception:
        log.exception("Fly.io sync attempt failed")
        for p in (clean_path, gz_path):  # best-effort local temp cleanup
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass
        return False
