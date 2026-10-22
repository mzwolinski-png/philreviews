#!/usr/bin/env python3
"""Shared deployment utilities for syncing the database to Fly.io."""

import base64
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


# Runs on the Fly machine (no procps there): HUP the gunicorn master, then wait
# for a worker that did not exist before -- it is the one serving the new file.
_RELOAD_PY = """
import os, signal, time

def gunicorns():
    procs = {}
    for p in os.listdir('/proc'):
        try:
            if p.isdigit() and b'gunicorn' in open('/proc/%s/cmdline' % p, 'rb').read():
                procs[int(p)] = int(open('/proc/%s/stat' % p).read().split()[3])
        except OSError:      # exited while we looked
            pass
    return procs

procs = gunicorns()
master = [p for p, parent in procs.items() if parent not in procs][0]
os.kill(master, signal.SIGHUP)
for _ in range(30):
    time.sleep(1)
    if set(gunicorns()) - set(procs):
        print('RELOADED')
        break
else:
    print('NOT RELOADED')
"""
_RELOAD_SCRIPT = ("import base64; exec(base64.b64decode('%s'))"
                  % base64.b64encode(_RELOAD_PY.encode()).decode())


def _site_serves(wait=45):
    """True once the live site reports the uploaded database's review count."""
    import json
    import urllib.request
    try:
        db.invalidate_caches()
        expected = db._get_total_count()
    except Exception:
        return False
    deadline = time.time() + wait
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("https://philreviews.org/health", timeout=10) as r:
                if json.load(r).get("reviews") == expected:
                    return True
        except Exception:
            pass
        time.sleep(3)
    return False


def sync_to_fly(retries=3):
    """Upload the database to Fly.io and swap it in live, retrying on failure.

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
             "rm -f /tmp/reviews_new.db.gz /data/reviews_new.db.gz /data/reviews_new.db"],
            cwd=ROOT, capture_output=True, text=True, timeout=30,
        )

        result = subprocess.run(
            ["fly", "ssh", "sftp", "shell", "-a", FLY_APP],
            # To the machine's own disk, not the 1 GB data volume: the volume
            # then only ever holds the live database and the new one. With the
            # compressed copy there as well it ran out of room on 2026-10-11
            # ("No space left on device") and two syncs were lost.
            input=f"put {gz_path} /tmp/reviews_new.db.gz\n",
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
        unpack = subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "sh -c 'gunzip -c /tmp/reviews_new.db.gz > /data/reviews_new.db"
             " && echo UNPACK_OK; rm -f /tmp/reviews_new.db.gz; df -h /data | tail -1'"],
            cwd=ROOT, capture_output=True, text=True, timeout=300,
        )
        # This step used to go unchecked, so a failed unpack surfaced one step
        # later as "remote_rows=-1" with nothing to say why.
        if "UNPACK_OK" not in (unpack.stdout or ""):
            detail = " ".join(((unpack.stdout or "") + " " + (unpack.stderr or "")).split())[-300:]
            log.error(f"Could not unpack the uploaded DB on Fly (is /data full?): {detail}")
            subprocess.run(["fly", "ssh", "console", "-a", FLY_APP, "-C", "rm -f /data/reviews_new.db"],
                           cwd=ROOT, capture_output=True, text=True, timeout=30)
            return False

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
             # The -wal/-shm files belong to the database being replaced. Left
             # beside the new file, SQLite replays them into it on the next open
             # and corrupts it: 2026-09-27 a deploy's schema migration wrote to
             # the WAL, the swap kept it, and production went "database disk
             # image is malformed". They go out with the old file.
             # One rename, so there is never a moment with no reviews.db for a
             # request to open (or to create empty).
             "sh -c 'rm -f /data/reviews.db-wal /data/reviews.db-shm;"
             " mv -f /data/reviews_new.db /data/reviews.db'"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.error(f"Remote swap failed: {result.stderr}")
            return False

        # No restart: the running app sees reviews.db is a new file on its next
        # request and reopens (db._check_db_replaced). `fly apps restart` left
        # the site refusing connections for ~13 s on every sync, which Search
        # Console reported as 5xx (2026-10-22). Reload and restart are only
        # fallbacks, if the live site doesn't come back with the new count.
        log.info("Database uploaded. Checking the live site picked it up...")
        if _site_serves():
            log.info("Fly.io deploy complete")
            return True
        log.warning("Live site still on the old database; reloading gunicorn")
        reload = subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             f"python3 -c \"{_RELOAD_SCRIPT}\""],
            cwd=ROOT, capture_output=True, text=True, timeout=90,
        )
        if "RELOADED" not in (reload.stdout or "") or not _site_serves():
            log.warning("Graceful reload not confirmed; restarting app instead")
            subprocess.run(
                ["fly", "apps", "restart", FLY_APP],
                cwd=ROOT, check=True, capture_output=True, text=True, timeout=60,
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
