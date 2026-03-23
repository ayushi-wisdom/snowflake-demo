#!/usr/bin/env bash
# Install or update the cron job for the daily Snowflake update.
# Run from project root or schedule/: ./schedule/install_cron.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$PROJECT_DIR/run_daily_update.sh"
LOG_DIR="$PROJECT_DIR/logs"

# Default: run every day at 6:00 AM local time
CRON_TIME="${CRON_TIME:-0 6 * * *}"
CRON_LINE="$CRON_TIME $RUN_SCRIPT"

mkdir -p "$LOG_DIR"
chmod +x "$RUN_SCRIPT"

# Remove any existing line for this script, then add the new one
( crontab -l 2>/dev/null | grep -v "run_daily_update.sh" | grep -v "daily_financial_update.py" ; echo "$CRON_LINE" ) | crontab -

echo "Cron job installed. Daily update will run at 6:00 AM (use CRON_TIME='0 2 * * *' for 2 AM)."
echo "Current crontab:"
crontab -l
