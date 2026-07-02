#!/bin/bash
cd /Users/mzwolinski/PhilReview
while true; do
  python3 -u lrb_scraper.py >> lrb_backfill.log 2>&1 && break
  echo "$(date '+%F %T') NetworkDown — retrying in 10 min" >> lrb_backfill.log
  sleep 600
done
echo "$(date '+%F %T') LRB BACKFILL COMPLETE" >> lrb_backfill.log
