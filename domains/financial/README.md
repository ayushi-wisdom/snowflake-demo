# Financial Services Synthetic Data Generator

A Python project that generates realistic synthetic financial services data and loads it into Snowflake daily.

## Overview

This project creates a comprehensive financial services dataset with 6 related tables:
- **customers** - Customer master data (~1,500 customers)
- **accounts** - Account master data (~3,000-5,000 accounts)
- **transactions** - Daily transaction history (~10,000-20,000 per day)
- **portfolio_holdings** - Investment positions (~2,000-4,000 holdings)
- **market_prices** - Stock market prices (~150 symbols daily)
- **loans** - Loan portfolio (~500-1,000 loans)

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure `.env` file:**
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PAT_TOKEN=your_token
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_ROLE=your_role
   ```
   **Optional (oil root-cause demo):** Set `DATA_AS_OF_DATE=2026-03-10` so all data is generated only through that date (no data after "current day"). See **`docs/DEMO_OUTLINE.md`**.

3. **Create tables:**
   ```bash
   python create_schema.py
   ```

4. **Run daily update:**
   ```bash
   python daily_financial_update.py
   ```

## Data catalog (for AI / data platforms)

Table and column descriptions, metric definitions, and relationships are in version-controlled docs:

- **`docs/schema.yml`** — Table and column metadata (names, types, descriptions, accepted values).
- **`docs/metrics.yml`** — Metric definitions and calculation logic.
- **`docs/data_model.md`** — Entity relationships and join keys.

To sync descriptions into Snowflake, run **`scripts/apply_table_comments.sql`** (adjust database/schema at the top if needed).

## Files

- `create_schema.py` - Creates all 6 tables in Snowflake
- `data_generator.py` - Generates realistic synthetic data
- `daily_financial_update.py` - Main script that runs daily updates
- `snowflake_connection.py` - Handles Snowflake connections
- `test_connection.py` - Tests Snowflake connection

## Data Relationships

- customers → accounts (1:many)
- accounts → transactions (1:many)
- accounts → portfolio_holdings (1:many, investment accounts only)
- market_prices → portfolio_holdings (price lookup)
- customers → loans (1:many)
- accounts → loans (linked account)

## Daily Update Process

1. Checks if master data (customers, accounts, loans) exists
2. If not, generates initial master data
3. Generates daily transactions
4. Updates account balances from transactions
5. Generates/updates market prices
6. Updates portfolio holdings with current prices

## Volume

- **Initial load:** ~7,000-8,000 master records
- **Daily updates:** ~12,000-25,000 new/updated rows per day
- All data is raw (no pre-calculated metrics)

## Real-world anomalies (automatic by date)

Transaction generation applies **date-based** patterns so when that date comes, the run automatically uses the right volume and category mix:

- **Pay-day (every month):** Last 2 days of the month and first 2 days — ~25% more volume; more **Deposits** and **Transfers**.
- **Tax deadline:** Apr 10–18 — ~35% more volume; more **Payments** and **Transfers**.
- **Holiday spending (2026):** Easter (Apr 3–6), Mother's Day (May 8–10), Memorial Day (May 23–26), Father's Day (Jun 19–21) — ~20% more volume; more **Purchase/Shopping/Restaurant/Entertainment**.
- **Quarter-end:** Last day of Mar/Jun/Sep/Dec — ~10% more volume; more **Transfers**.
- **FOMC day:** Fed meeting end dates (e.g. Mar 18, Apr 29, Jun 17 …) — ~10% more volume; more **Transfer/Withdrawal**.

- **Oil/energy (Feb 1 – Mar 10, 2026):** Spending mix shifts to Gas/Groceries/Utilities; more Failed/Pending and Credit Card usage; portfolio energy +15%, rest −5%; loans have **industry_sector** and Transport/Airline show higher delinquency. See **`docs/ANOMALY_LOGIC.md`** and **`docs/DEMO_OUTLINE.md`** for the full list and the root-cause demo flow.

To **backfill** a date range with these anomalies (replaces existing transactions in range, then recalculates balances):

```bash
python backfill_with_anomalies.py
python backfill_with_anomalies.py --start 2025-03-10 --end 2026-03-09
```

Defaults: `--start` = today − 364 days, `--end` = today (full rolling 365-day window). For a full year backfill, the script may run for a while.

## New customers and churn (weekly)

- **New customers:** A few (3–5) new customers and accounts are added each week so the company grows.
- **Churn:** A few (1–3) existing customers are marked **Inactive** each week; customers are never deleted so you keep full history.

Run weekly (e.g. Sunday 3 AM):

```bash
python weekly_customer_churn.py
```

Cron example: `0 3 * * 0 /path/to/venv/bin/python /path/to/weekly_customer_churn.py`

## November promotion (one-time)

To reflect a past promotion (“$100 when you open an account” in November), add an influx of customers with `created_date` and account `opened_date` in **November 2025**; Checking/Savings opened in that month get **+$100** in `current_balance`. Run once:

```bash
python add_november_promotion_customers.py
```

Then backfill transaction history for those accounts (Nov 2025 through today):

```bash
python backfill_november_account_transactions.py
```

## Dormant accounts (no use in 6 months)

About **8% of accounts** are treated as dormant: they get **no transactions** in the last 6 months (realistic inactive/dormant behavior). The same accounts are chosen deterministically. Going forward, the daily and backfill generators skip these accounts for dates within the last 180 days.

To make existing data match (remove recent transactions for those accounts and recalc balances), run once:

```bash
python apply_dormant_to_existing_data.py
```

## Manual anomalies (monthly)

To add **one-off events** without editing code (e.g. once a month): big customer churn, big customer join, or a new promotion. See **`docs/ANOMALY_LOGIC.md`** for how anomalies are chosen and how to add new ones.

```bash
# One high-value customer leaves (marked Inactive; their accounts closed)
python add_manual_anomaly.py --type big_churn --count 1

# One "whale" customer joins (high balances, several accounts)
python add_manual_anomaly.py --type big_join

# New promotion: e.g. April 2026, $50 bonus, 60 customers
python add_manual_anomaly.py --type promotion --start 2026-04-01 --end 2026-04-30 --bonus 50 --count 60
python backfill_promotion_account_transactions.py --start 2026-04-01 --end 2026-04-30
```
