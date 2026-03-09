#!/usr/bin/env bash
# Wrapper to run daily_financial_update.py with the project venv
# Use this script for cron or launchd so the correct Python and path are used.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Use venv Python (no need to activate; run script directly)
"$SCRIPT_DIR/venv/bin/python" daily_financial_update.py >> "$SCRIPT_DIR/logs/daily_update.log" 2>&1
