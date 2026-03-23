# Current Logic of All Files & Run Order

This doc describes **what every file does** and the **exact order to run** on a fresh database (e.g. after you dropped previous tables). Everything uses **`.env`**: `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` (e.g. `SE_DEMOS_NEW` and `FINANCE_MAIN`).

---

## 1. Connection & config

| File | What it does |
|------|----------------|
| **`snowflake_connection.py`** | Shared module. Reads `.env` (account, user, PAT/password, warehouse, **database**, **schema**, role) and returns a Snowflake connection. **All other scripts use this** — so the active DB/schema is whatever is in `.env`. |
| **`.env`** | You must set `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` (e.g. `SE_DEMOS_NEW`, `FINANCE_MAIN`). All scripts that “create” or “update” data use this. |

---

## 2. Database & schema setup (run first on a fresh setup)

| File | What it does |
|------|----------------|
| **`create_new_database.py`** | **Creates** the database `SE_DEMOS_NEW` and schema `SE_DEMOS_NEW.FINANCE_MAIN`. Does **not** read DB/schema from `.env` — it hardcodes `SE_DEMOS_NEW` and `FINANCE_MAIN`. Use this when the DB doesn’t exist yet. |
| **`create_schema_finance_main.py`** | Creates **only** the schema `FINANCE_MAIN` inside the database from `.env` (e.g. `SE_DEMOS_NEW.FINANCE_MAIN`). Use when the DB already exists but the schema doesn’t. |
| **`create_schema.py`** | Creates **all 6 tables** in the current connection (so in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA` from `.env`): `customers`, `accounts`, `transactions`, `portfolio_holdings`, `loans`. No rows — structure only. |

**Run order (fresh DB):**  
1) `create_new_database.py` (if DB doesn’t exist), **or** ensure `.env` points to existing DB and run `create_schema_finance_main.py` if the schema is missing.  
2) Set `.env`: `SNOWFLAKE_DATABASE=SE_DEMOS_NEW`, `SNOWFLAKE_SCHEMA=FINANCE_MAIN`.  
3) `create_schema.py` → creates the 6 empty tables.

---

## 3. Core daily pipeline

| File | What it does |
|------|----------------|
| **`data_generator.py`** | **Library only** (not run directly). Generates synthetic data: customers, accounts, transactions, portfolio_holdings, loans. Includes logic for pay-day, tax-deadline, Iran/oil event, dormant accounts (~8%), and real market prices for holdings. |
| **`daily_financial_update.py`** | **Main daily script.** Uses `snowflake_connection` + `data_generator`. Logic: (1) If no master data → generate and insert customers, accounts, loans. (2) If no transactions for **today** → generate and insert today’s transactions. (3) If fewer than 365 distinct transaction dates → backfill historical transactions (up to 365 days). (4) Update account balances from **today’s** transactions. (5) Update `balance_last_updated_date` for active loans. (6) Fetch real market prices, apply Iran/oil shock if in window, update existing portfolio_holdings prices and add new holdings for investment accounts that have none. (7) **Prune** transactions older than 365 days (rolling window). |
| **`run_daily_update.sh`** | Runs `daily_financial_update.py` with the project venv and logs to `logs/daily_update.log`. Use for cron/scheduled runs. |

So: **one-time** master data creation when empty; **every day**: new transactions for today, optional historical backfill until 365 days, balance/loan/holdings updates, and pruning of old transactions.

---

## 4. Table update summary (what the daily script touches)

| Table | When rows are added | When rows/columns are updated |
|-------|----------------------|-------------------------------|
| **customers** | Only when master data is first created (no customers in DB). | Never. |
| **accounts** | Same as customers (first run). | Daily: `current_balance`, `available_balance`, `balance_last_updated_date` for accounts that have transactions on the run date. |
| **transactions** | Daily (today’s batch); also historical backfill until 365 days of distinct dates exist. | N/A (insert only). Old data: **deleted** if older than 365 days. |
| **portfolio_holdings** | When investment accounts have no holdings (daily script adds them). | Daily: `current_price`, `market_value`, `unrealized_pnl`, `updated_date` for all existing holdings. |
| **loans** | Only when master data is first created. | Daily: `balance_last_updated_date` (and `updated_at`) for all **active** loans. |

(There is no `market_prices` table in the current schema; prices are fetched live and used for holdings.)

---

## 5. Optional / one-time / utility scripts

| File | What it does |
|------|----------------|
| **`drop_old_database_tables.py`** | Drops the 5 tables (`transactions`, `portfolio_holdings`, `loans`, `accounts`, `customers`) in the DB/schema from **.env**. To drop tables in another DB (e.g. old SE_DEMO_DB), run with override: `SNOWFLAKE_DATABASE=SE_DEMO_DB SNOWFLAKE_SCHEMA=PUBLIC python drop_old_database_tables.py`. |
| **`backfill_with_anomalies.py`** | Deletes transactions in a date range, then re-generates them with pay-day / tax-deadline / Iran-oil anomalies and recalculates account balances. Default range: last 365 days. Optional `--start` / `--end`. |
| **`weekly_customer_churn.py`** | Weekly job: mark 1–3 random active customers as **Inactive**; add 3–8 new customers (and accounts, and some loans) with `created_date` in the last 7 days. Never deletes customers. |
| **`add_november_promotion_customers.py`** | One-time: add 120 customers (and accounts) with `created_date` in Nov 2025; Checking/Savings opened in Nov 2025 get +$100 `current_balance`. |
| **`backfill_november_account_transactions.py`** | One-time: backfill transactions from Nov 2025 to today for accounts linked to November promotion customers. Run after `add_november_promotion_customers.py`. |
| **`apply_dormant_to_existing_data.py`** | One-time: remove transactions in the last 6 months for the same ~8% “dormant” accounts used in the generator; then recalculate account balances. |
| **`fill_missing_transactions.py`** | Fills missing transaction **dates** in a fixed range (default 2026-02-19 to 2026-03-05). Generates transactions only for dates that have no data. |
| **`cleanup_duplicate_today_transactions.py`** | Removes a specified number of **most recent** transactions for a given date (default today) and recalculates balances. Use if the daily script ran twice and duplicated today’s batch. |
| **`daily_table_update.py`** | Separate from the main finance pipeline. Creates/updates a **`daily_metrics`** table with a sample row (date, metric_name, metric_value). Not part of the 6-table finance model. |
| **`test_connection.py`** | Tests Snowflake connection and prints current user, role, warehouse, database, schema and lists databases. |
| **`scripts/verify_snowflake_data.py`** | Reads from `.env` and prints: DB/schema, row counts for the 5 finance tables, transaction date range, November promotion counts, dormant-account count, customers by status. |

---

## 6. Recommended run order for your current (fresh) database

You said you have the database there but dropped previous ones. So:

1. **Ensure `.env`** has:
   - `SNOWFLAKE_DATABASE=SE_DEMOS_NEW` (or whatever DB you want)
   - `SNOWFLAKE_SCHEMA=FINANCE_MAIN`

2. **If the database or schema doesn’t exist yet:**
   - Run `create_new_database.py` (creates `SE_DEMOS_NEW` + `FINANCE_MAIN`),  
   **or** if the DB already exists, run `create_schema_finance_main.py` to ensure the schema exists.

3. **Create the 6 empty tables:**  
   `python create_schema.py`

4. **Run the main daily pipeline once (this will create master data + today + history up to 365 days):**  
   `python daily_financial_update.py`  
   Or use the wrapper:  
   `./run_daily_update.sh`

5. **Optional (only if you want them):**
   - November promotion: `add_november_promotion_customers.py` then `backfill_november_account_transactions.py`
   - Dormant cleanup: `apply_dormant_to_existing_data.py`
   - **Weekly churn (recommended for new launch):** schedule `weekly_customer_churn.py` (e.g. Sunday 3 AM). Adds 3–5 new customers and marks 1–3 existing as Inactive so the customer base isn’t static.
   - Verify: `python scripts/verify_snowflake_data.py`

**New launch / customers:** The daily update creates customers only once (initial master data). After that, customers stay static unless you run **weekly_customer_churn.py** (adds a few new, marks some Inactive) or the November promotion scripts. No other process adds or inactivates customers.

**Nothing in this repo creates or uses tables in a different database unless you override env (e.g. `drop_old_database_tables.py` with `SNOWFLAKE_DATABASE=SE_DEMO_DB`).** So for your “current” database: set `.env` once, then run steps 2–4 in order; after that you can approve and run the daily/optional scripts as above.
