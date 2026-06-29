#!/usr/bin/env python3
"""Send a monthly reminder to download Philosopher's Index data."""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from notify import send_monthly_pi_reminder

if __name__ == "__main__":
    send_monthly_pi_reminder()
