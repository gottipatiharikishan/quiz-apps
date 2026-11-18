#!/bin/bash

set -e

APP_HOME="/opt/gcs-azure-sync-v2"
LOG_FILE="$APP_HOME/autoscale_rollback.log"

cd "$APP_HOME"

# Rotate log file if >50MB
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

# Recover Stuck Rollback Batches
PGPASSWORD="$PGPASSWORD" psql \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -U "$PGUSER" \
    -d "$PGDATABASE" \
    -c "
    UPDATE rollback_batches
    SET status='PENDING',
        started_at=NULL
    WHERE status='PROCESSING'
      AND started_at < NOW() - INTERVAL '30 minutes';
" >/dev/null 2>&1

# Calculate Active Rollback Files
PENDING_FILES=$(PGPASSWORD="$PGPASSWORD" psql \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -U "$PGUSER" \
    -d "$PGDATABASE" \
    -t -A \
    -c "
    SELECT COALESCE(SUM(file_count),0)
    FROM rollback_batches
    WHERE status IN ('PENDING','PROCESSING');
")

# Validate Result
if ! [[ "$PENDING_FILES" =~ ^[0-9]+$ ]]; then

    echo "$(date) HOST=$(hostname) ERROR: Invalid rollback file count: $PENDING_FILES" >> "$LOG_FILE"
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

echo "$(date) HOST=$(hostname) ROLLBACK FILES=$PENDING_FILES SCALE=$SCALE" >> "$LOG_FILE"

docker compose \
    -f "$APP_HOME/docker-compose.yml" \
    up -d \
    --scale rollback-worker="$SCALE" \
    rollback-worker >> "$LOG_FILE" 2>&1

echo "$(date) HOST=$(hostname) DONE SCALE=$SCALE" >> "$LOG_FILE"