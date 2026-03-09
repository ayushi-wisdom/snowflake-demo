# Running the Daily Update Automatically

Use one of the options below so `daily_financial_update.py` runs every day.

---

## Option 1: cron (Mac / Linux)

1. **Create a log directory** (so the run script can write logs):
   ```bash
   mkdir -p /Users/ayushi/Documents/snowflake-daily-update/logs
   ```

2. **Make the run script executable**:
   ```bash
   chmod +x /Users/ayushi/Documents/snowflake-daily-update/run_daily_update.sh
   ```

3. **Open your crontab**:
   ```bash
   crontab -e
   ```

4. **Add a line** to run every day at 6:00 AM (adjust time as needed):
   ```cron
   0 6 * * * /Users/ayushi/Documents/snowflake-daily-update/run_daily_update.sh
   ```
   Or at 2:00 AM:
   ```cron
   0 2 * * * /Users/ayushi/Documents/snowflake-daily-update/run_daily_update.sh
   ```

5. **Save and exit** (in vim: `Esc` then `:wq`).

**Note:** cron runs with a minimal environment. If you get errors, use the full path to the script and ensure `.env` is in the project directory (it is). The script uses the venv inside the project, so no need to activate anything.

---

## Option 2: launchd (macOS – recommended)

launchd runs whether you’re logged in or not (if you use a LaunchAgent and stay logged in, or a LaunchDaemon with proper setup).

1. **Create the log directory**:
   ```bash
   mkdir -p /Users/ayushi/Documents/snowflake-daily-update/logs
   chmod +x /Users/ayushi/Documents/snowflake-daily-update/run_daily_update.sh
   ```

2. **Copy the plist** into your user LaunchAgents folder:
   ```bash
   cp /Users/ayushi/Documents/snowflake-daily-update/schedule/com.snowflake.dailyupdate.plist ~/Library/LaunchAgents/
   ```

3. **Load and start the job**:
   ```bash
   launchctl load ~/Library/LaunchAgents/com.snowflake.dailyupdate.plist
   ```

4. **Check status**:
   ```bash
   launchctl list | grep snowflake
   ```

5. **To stop or remove**:
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.snowflake.dailyupdate.plist
   ```

The plist is set to run every day at 6:00 AM. To change the time, edit the `StartCalendarInterval` in the plist (e.g. `Hour` and `Minute`).

---

## Option 3: GitHub Actions (run in the cloud)

If you want the job to run on GitHub’s servers every day:

1. In your repo go to **Settings → Secrets and variables → Actions**.
2. Add secrets (do **not** commit these):
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PAT_TOKEN`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`
   - `SNOWFLAKE_ROLE`

3. Add the workflow file from `schedule/daily-update-workflow.yml` into your repo at `.github/workflows/daily-update.yml`.

4. Push to the default branch; the workflow will run on the schedule (e.g. daily at 6:00 AM UTC).

---

## Logs

- **cron / launchd:** logs are written to `logs/daily_update.log` in the project directory.
- **GitHub Actions:** logs appear in the “Actions” tab for the workflow run.

To watch the log in real time:
```bash
tail -f /Users/ayushi/Documents/snowflake-daily-update/logs/daily_update.log
```
