#!/bin/bash
# Weekly backup of PhilReviews database to Google Drive
# Keeps the 4 most recent backups (~260 MB total at most)

set -euo pipefail

DB_PATH="/Users/mzwolinski/PhilReview/reviews.db"
BACKUP_DIR="/Users/mzwolinski/My Drive/My Documents/PhilReviews-backups"
TIMESTAMP=$(date +%Y-%m-%d)
BACKUP_FILE="$BACKUP_DIR/reviews-$TIMESTAMP.db.gz"
KEEP=4

mkdir -p "$BACKUP_DIR"

# Compress a snapshot (safe even if DB is being written to, thanks to WAL mode)
sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/reviews-$TIMESTAMP.db'"
gzip -f "$BACKUP_DIR/reviews-$TIMESTAMP.db"

echo "Backup created: $BACKUP_FILE ($(du -h "$BACKUP_FILE" | cut -f1))"

# Prune old backups, keeping the most recent $KEEP
ls -t "$BACKUP_DIR"/reviews-*.db.gz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs rm -f

echo "Backups in $BACKUP_DIR:"
ls -lh "$BACKUP_DIR"/reviews-*.db.gz 2>/dev/null
