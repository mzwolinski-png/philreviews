#!/usr/bin/env python3
"""Base class and utilities shared across all PhilReviews scrapers."""

import logging
import os
import time
from datetime import datetime

import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

USER_AGENT = "PhilReviews/2.0 (academic research aggregator; mailto:mzwolinski@sandiego.edu)"


def setup_logging(name, log_file=None):
    """Configure structured logging for a scraper or script.

    Returns a standard library logger with consistent format across all
    scrapers: timestamp, level, logger name, and message.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    if log_file:
        path = log_file if os.path.isabs(log_file) else os.path.join(LOG_DIR, log_file)
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def notify_failure(subject, detail=""):
    """Write a failure alert that's easy to detect.

    Creates a timestamped file in logs/alerts/. A simple cron or
    LaunchAgent can check for new files in that directory and email/notify.
    Also logs the failure so it appears in normal log output.
    """
    alert_dir = os.path.join(LOG_DIR, "alerts")
    os.makedirs(alert_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{ts}_{subject.replace(' ', '_')}.txt"
    path = os.path.join(alert_dir, filename)
    with open(path, "w") as f:
        f.write(f"Alert: {subject}\n")
        f.write(f"Time: {datetime.now().isoformat()}\n")
        if detail:
            f.write(f"Detail: {detail}\n")
    logging.getLogger("alerts").error(f"ALERT: {subject} — {detail}")


class BaseScraper:
    """Base class for PhilReviews scrapers.

    Provides:
    - HTTP session with standard User-Agent and timeout
    - Structured logging via standard library
    - Rate-limited GET with retry and exponential backoff
    - Stats tracking
    """

    name = "base"
    default_timeout = 30
    default_delay = 1.0

    def __init__(self, log_file=None):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.stats = {}
        self.log = setup_logging(self.name, log_file)

    def get(self, url, timeout=None, **kwargs):
        """HTTP GET with timeout default. Raises on HTTP errors."""
        return self.session.get(url, timeout=timeout or self.default_timeout, **kwargs)

    def get_json(self, url, timeout=None, **kwargs):
        """HTTP GET, raise on error, return parsed JSON."""
        resp = self.get(url, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_with_retry(self, url, max_retries=3, backoff=2.0, timeout=None, **kwargs):
        """HTTP GET with exponential backoff on 429/5xx errors.

        Returns the response on success, or raises after max_retries.
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = self.get(url, timeout=timeout, **kwargs)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = min(300, backoff * (2 ** attempt))
                    self.log.warning(f"HTTP {resp.status_code} from {url}, "
                                     f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                last_exc = e
                if attempt < max_retries - 1:
                    wait = min(300, backoff * (2 ** attempt))
                    self.log.warning(f"Request failed: {e}, "
                                     f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
        raise last_exc

    def sleep(self, seconds=None):
        """Sleep for the given duration (or default_delay)."""
        time.sleep(seconds if seconds is not None else self.default_delay)

    def run(self, dry_run=False):
        """Main entry point. Override in subclasses."""
        raise NotImplementedError
