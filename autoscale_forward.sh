#!/bin/bash

set -e

APP_HOME="/opt/gcs-azure-sync-v2"
LOG_FILE="$APP_HOME/autoscale_forward.log"

cd "$APP_HOME"

# Rotate log if larger than 50MB
if [ -f "$LOG_FILE" ]; then
    SIZE=$(stat -c%s "$LOG_FILE")
    if [ "$SIZE" -gt 52428800 ]; then
        : > "$LOG_FILE"
    fi
fi

# Load environment
set -a
source "$APP_HOME/.env"
set +a

# PostgreSQL Health Check
if ! PGPASSWORD="$PGPASSWORD" psql \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -U "$PGUSER" \
    -d "$PGDATABASE" \
    -c "SELECT 1;" >/dev/null 2>&1; then

    echo "$(date) HOST=$(hostname) ERROR: PostgreSQL unavailable" >> "$LOG_FILE"
    exit 1
fi

# Docker Health Check
if ! docker info >/dev/null 2>&1; then
    echo "$(date) HOST=$(hostname) ERROR: Docker unavailable" >> "$LOG_FILE"
    exit 1
fi

# Recover Stuck Batches (>30 Minutes)
PGPASSWORD="$PGPASSWORD" psql \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -U "$PGUSER" \
    -d "$PGDATABASE" \
    -c "
    UPDATE sync_batches
    SET status='PENDING',
        started_at=NULL
    WHERE status='PROCESSING'
      AND started_at < NOW() - INTERVAL '30 minutes';
" >/dev/null 2>&1

# Calculate Active Forward Files
PENDING_FILES=$(PGPASSWORD="$PGPASSWORD" psql \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -U "$PGUSER" \
    -d "$PGDATABASE" \
    -t -A \
    -c "
    SELECT COALESCE(SUM(file_count),0)
    FROM sync_batches
    WHERE status IN ('PENDING','PROCESSING');
")

# Validate Result
if ! [[ "$PENDING_FILES" =~ ^[0-9]+$ ]]; then
    echo "$(date) HOST=$(hostname) ERROR: Invalid pending file count: $PENDING_FILES" >> "$LOG_FILE"
    exit 1
fi

# Scaling Policy
if [ "$PENDING_FILES" -eq 0 ]; then

    SCALE=0

elif [ "$PENDING_FILES" -le 1000 ]; then

    SCALE=1

elif [ "$PENDING_FILES" -le 5000 ]; then

    SCALE=2

elif [ "$PENDING_FILES" -le 25000 ]; then

    SCALE=5

elif [ "$PENDING_FILES" -le 100000 ]; then

    SCALE=10

elif [ "$PENDING_FILES" -le 500000 ]; then

    SCALE=20

else

    SCALE=40

fi

# Never scale down while a worker may own a PROCESSING batch.
PROCESSING_BATCHES=$(PGPASSWORD="$PGPASSWORD" psql     -h "$PGHOST"     -p "$PGPORT"     -U "$PGUSER"     -d "$PGDATABASE"     -t -A     -c "
    SELECT COUNT(*)
    FROM sync_batches
    WHERE status='PROCESSING';
")

CURRENT_WORKERS=$(docker compose     -f "$APP_HOME/docker-compose.yml"     ps -q worker 2>/dev/null | wc -l)

PROCESSING_BATCHES=$(echo "$PROCESSING_BATCHES" | xargs)
CURRENT_WORKERS=$(echo "$CURRENT_WORKERS" | xargs)

if ! [[ "$PROCESSING_BATCHES" =~ ^[0-9]+$ ]]; then
    echo "$(date) HOST=$(hostname) ERROR: Invalid PROCESSING batch count: $PROCESSING_BATCHES" >> "$LOG_FILE"
    exit 1
fi

if ! [[ "$CURRENT_WORKERS" =~ ^[0-9]+$ ]]; then
    echo "$(date) HOST=$(hostname) ERROR: Invalid current worker count: $CURRENT_WORKERS" >> "$LOG_FILE"
    exit 1
fi

CALCULATED_SCALE="$SCALE"

if [ "$PROCESSING_BATCHES" -gt 0 ] && [ "$SCALE" -lt "$CURRENT_WORKERS" ]; then
    SCALE="$CURRENT_WORKERS"
fi

echo "$(date) HOST=$(hostname) FORWARD FILES=$PENDING_FILES PROCESSING_BATCHES=$PROCESSING_BATCHES CURRENT_WORKERS=$CURRENT_WORKERS CALCULATED_SCALE=$CALCULATED_SCALE FINAL_SCALE=$SCALE" >> "$LOG_FILE"

docker compose \
    -f "$APP_HOME/docker-compose.yml" \
    up -d \
    --scale worker="$SCALE" \
    worker >> "$LOG_FILE" 2>&1

echo "$(date) HOST=$(hostname) DONE SCALE=$SCALE" >> "$LOG_FILE"