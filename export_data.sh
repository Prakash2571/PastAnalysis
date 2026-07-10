#!/usr/bin/env bash
#
# Dump the past_data database to a single portable archive.
#
# Usage:
#   ./export_data.sh                       # -> hourly_dump_YYYYMMDD.archive.gz
#   ./export_data.sh my_backup.archive.gz  # custom name
#
# Restore on your local machine:
#   mongorestore --archive=hourly_dump_YYYYMMDD.archive.gz --gzip
#
set -euo pipefail

DB="${MONGO_DB:-past_data}"
OUT="${1:-hourly_dump_$(date +%Y%m%d).archive.gz}"

echo "Dumping database '$DB' -> $OUT ..."
docker compose exec -T mongo mongodump --db="$DB" --archive --gzip > "$OUT"

SIZE=$(du -h "$OUT" | cut -f1)
echo "Done. Wrote $OUT ($SIZE)."
echo
echo "Copy to your machine:"
echo "  scp -i vivek.pem ubuntu@<ip>:~/PastAnalysis/$OUT ."
echo "Then restore locally:"
echo "  mongorestore --archive=$OUT --gzip"
