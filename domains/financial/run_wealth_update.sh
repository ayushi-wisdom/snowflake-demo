#!/usr/bin/env bash
# Independent wealth refresh runner (does not replace existing daily script).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

"/Users/ayushi/Documents/snowflake-daily-update/venv/bin/python" refresh_wealth_poc.py >> "$SCRIPT_DIR/logs/wealth_poc_refresh.log" 2>&1
