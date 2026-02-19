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

3. **Create tables:**
   ```bash
   python create_schema.py
   ```

4. **Run daily update:**
   ```bash
   python daily_financial_update.py
   ```

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
