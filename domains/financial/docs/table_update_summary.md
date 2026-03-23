# Table update summary

## Tables (from create_schema.py)

| Table               | Daily update? | What happens |
|---------------------|---------------|--------------|
| **customers**       | No (insert once) | Only populated when master data is first created. Never updated after that. |
| **accounts**        | Yes           | Balances updated for accounts with today's transactions (`current_balance`, `available_balance`, `balance_last_updated_date`). New rows only on first run. |
| **transactions**    | Yes           | New rows inserted for today; old rows pruned (DELETE older than 365 days). |
| **portfolio_holdings** | Yes       | Existing rows updated with new prices and `updated_date`. New holdings inserted only for investment accounts that have none. |
| **loans**           | Yes           | `balance_last_updated_date` and `updated_at` set to today for all active loans. New rows only on first run. |

So: **customers** is the only table that is never updated after initial load. The rest are touched every day (accounts/transactions/holdings/loans as above).

## Database (SE_DEMOS_NEW only)

- All work uses **SE_DEMOS_NEW.FINANCE_MAIN** (set in .env). SE_DEMO_DB is no longer used; its tables can be dropped via `drop_old_database_tables.py` with env override to SE_DEMO_DB.
- To remove the old tables there, run: `python drop_old_database_tables.py` (see below).
