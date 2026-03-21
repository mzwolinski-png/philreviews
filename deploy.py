#!/usr/bin/env python3
"""Shared deployment utilities for syncing the database to Fly.io."""

import gzip
import logging
import os
import shutil
import sqlite3
import subprocess

import db
from scraper_base import notify_failure

log = logging.getLogger(__name__)

FLY_APP = os.environ.get("FLY_APP", "philreviews")
ROOT = os.path.dirname(os.path.abspath(__file__))


def sync_to_fly():
    """Upload the database to Fly.io and restart the app.

    Creates a clean copy (no WAL), compresses with gzip (~65 MB vs ~200 MB),
    uploads via sftp, then decompresses on the remote machine.
    """
    log.info("Syncing database to Fly.io...")
    try:
        # Create a clean copy with DELETE journal mode (no WAL dependency)
        clean_path = db.DB_PATH + ".upload"
        gz_path = clean_path + ".gz"
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
            msg = f"DB upload failed: {result.stderr}"
            log.error(msg)
            notify_failure("sync_to_fly upload", msg)
            return

        # Decompress and verify, then atomically swap
        result = subprocess.run(
            ["fly", "ssh", "console", "-a", FLY_APP, "-C",
             "sh -c 'gunzip /data/reviews_new.db.gz"
             " && mv /data/reviews.db /data/reviews_old.db 2>/dev/null;"
             " mv /data/reviews_new.db /data/reviews.db"
             " && rm -f /data/reviews_old.db'"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            msg = f"Remote swap failed: {result.stderr}"
            log.error(msg)
            notify_failure("sync_to_fly swap", msg)
            return

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
    except Exception as e:
        log.exception("Fly.io sync failed")
        notify_failure("sync_to_fly exception", str(e))
