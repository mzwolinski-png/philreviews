#!/usr/bin/env python3
"""Preserve subscribers from the current DB into a new DB during sync.

Called remotely on Fly.io during sync_to_fly() to copy the subscribers
table from /data/reviews.db into /data/reviews_new.db before the swap.
"""

import sqlite3

OLD_DB = "/data/reviews.db"
NEW_DB = "/data/reviews_new.db"

try:
    old = sqlite3.connect(OLD_DB)
    rows = old.execute("SELECT * FROM subscribers").fetchall()
    cols = [d[0] for d in old.execute("SELECT * FROM subscribers LIMIT 0").description]
    old.close()
except Exception:
    print("No subscribers to preserve")
    exit(0)

if not rows:
    print("No subscribers to preserve")
    exit(0)

new = sqlite3.connect(NEW_DB)
new.execute(
    "CREATE TABLE IF NOT EXISTS subscribers ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "email TEXT UNIQUE NOT NULL, "
    "token TEXT UNIQUE NOT NULL, "
    "verified INTEGER DEFAULT 0, "
    "subfields TEXT DEFAULT 'all', "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    "last_sent_date TEXT)"
)
placeholders = ",".join(["?"] * len(cols))
col_names = ",".join(cols)
preserved = 0
for r in rows:
    try:
        new.execute(f"INSERT OR IGNORE INTO subscribers ({col_names}) VALUES ({placeholders})", r)
        preserved += 1
    except Exception:
        pass
new.commit()
new.close()
print(f"Preserved {preserved} subscribers")
