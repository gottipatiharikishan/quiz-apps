#!/bin/bash

source /opt/gcs-azure-sync-v2/.env

export PGPASSWORD="$PGPASSWORD"

echo "===== $(date) ====="

psql \
  -h "$PGHOST" \
  -p "$PGPORT" \
  -U "$PGUSER" \
  -d "$PGDATABASE" <<SQL

UPDATE sync_batches
SET status='PENDING',
    started_at=NULL
WHERE status='PROCESSING'
  AND started_at IS NOT NULL
  AND started_at < NOW() - INTERVAL '15 minutes';

UPDATE rollback_batches
SET status='PENDING',
    started_at=NULL
WHERE status='PROCESSING'
  AND started_at IS NOT NULL
  AND started_at < NOW() - INTERVAL '15 minutes';

SQL