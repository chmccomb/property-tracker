#!/usr/bin/env bash
# auto_refresh.sh — Poll Gmail for new MLS listings and rebuild/deploy if anything is new.
#
# Designed to run from cron. Logs to $LOG_FILE with timestamps.
# Only redeploys when gmail_ingest actually writes new rows.
#
# Cron entry (every hour at :07):
#   7 * * * * /Users/charles/Downloads/jc-heights-analysis/scripts/auto_refresh.sh

set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────
REPO=/Users/charles/Downloads/jc-heights-analysis
BASE_CSV="$REPO/data/raw/MLS_export.csv"
INCREMENTAL_CSV="$REPO/data/raw/mls_incremental.csv"
LOG_FILE=/tmp/jc_heights_auto_refresh.log
PYTHON=/usr/bin/python3
# How many days back to search Gmail (slightly longer than the cron interval
# so we don't miss anything if cron fires a bit late)
SINCE_DAYS=2

# ── Helpers ─────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# ── Step 1: Poll Gmail ───────────────────────────────────────────────────────
log "=== auto_refresh START ==="
log "Polling Gmail (last ${SINCE_DAYS} days) …"

ingest_output=$("$PYTHON" "$REPO/scripts/gmail_ingest.py" --since "$SINCE_DAYS" 2>&1) || {
    log "ERROR: gmail_ingest.py failed — see log for details"
    echo "$ingest_output" >> "$LOG_FILE"
    exit 1
}

echo "$ingest_output" >> "$LOG_FILE"

# ── Step 2: Check if any new rows were written ───────────────────────────────
if ! echo "$ingest_output" | grep -q "^Appended [1-9]"; then
    log "No new listings — nothing to rebuild."
    log "=== auto_refresh DONE (no-op) ==="
    exit 0
fi

new_count=$(echo "$ingest_output" | grep -oP "^Appended \K[0-9]+" || echo "?")
log "Found $new_count new listing(s) — rebuilding dashboard …"

# ── Step 3: Re-run pipeline and deploy ───────────────────────────────────────
"$PYTHON" "$REPO/refresh.py" \
    --csv "$BASE_CSV" \
    --incremental "$INCREMENTAL_CSV" \
    --deploy \
    2>&1 | tee -a "$LOG_FILE"

log "=== auto_refresh DONE ==="
