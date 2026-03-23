# Repo Summary & Oil Root Cause Demo Outline

## What This Repo Does Now

- **Purpose:** Generates and maintains synthetic financial data in Snowflake for a **root cause analysis demo**. The story is: *something changed in the business; we trace it to rising oil prices since February 1st.*

- **Data-as-of date:** No data is generated after the “current day.” Set **`DATA_AS_OF_DATE=2026-03-10`** in `.env` so all generation, balance updates, and pruning use March 10, 2026 as the cutoff. If unset, scripts use the real `date.today()`.

- **Tables (same names, same structure + one new column):**
  - **customers** — master; not updated after initial load.
  - **accounts** — balances updated from transactions for the target date.
  - **transactions** — `transaction_date`, `category`, `amount`, `account_id`, `status`, etc. Rolling 365-day window; never after `DATA_AS_OF_DATE`.
  - **portfolio_holdings** — prices (and oil shock) applied as of target date; energy +15%, rest −5% in oil window.
  - **loans** — now include **`industry_sector`** (Transport, Airline, Manufacturing, Other). When generated in the oil window (e.g. snapshot as of Mar 10), Transport has ~16% delinquency, Airline ~12%, rest ~5%. Variable-rate creep since Feb 1 still applied in daily update.

- **Oil window:** **Feb 1 – Mar 10, 2026.** In that window:
  - **Transactions:** Spending mix shifts to Gas/Groceries/Utilities (ramp Feb 1–14, full effect Feb 15+); ~7% Failed/Pending vs ~2%; more Credit Card usage; no volume drop.
  - **Portfolio:** Energy symbols +15%, rest −5%.
  - **Loans:** Sector-based delinquency (Transport/Airline higher) so you can show “why did delinquencies spike in trucking?”

- **Run flow for a fresh demo:**
  1. In `.env`: `DATA_AS_OF_DATE=2026-03-10`, plus your `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA`.
  2. **Drop all tables** (e.g. `python drop_old_database_tables.py`) so the new `loans.industry_sector` column is created. Then `python create_schema.py`, then `python daily_financial_update.py` once. That creates customers, accounts, loans (with sector and oil-period delinquency), “today” (Mar 10) transactions, and backfills 365 days of history ending Mar 10. Balances and pruning use Mar 10.
  3. Optional: `python backfill_with_anomalies.py` to regenerate a date range and recompute balances (default end = `DATA_AS_OF_DATE`).

---

## Demo Outline: How to Use This Data to Show Root Cause

Use this sequence so the **data backs up each step**. The narrative: *We see a change; we drill from transactions → categories → merchants → account types → failures → portfolio → loans by sector → root cause = oil.*

| Step | What you show | What the data backs up |
|------|----------------|------------------------|
| **1. Set the scene** | “We’re looking at the last 6 months of activity through March 10.” | Transaction dates run through `DATA_AS_OF_DATE` (e.g. Mar 10) only. |
| **2. Volume (no drop)** | “Transaction volume by week — notice it’s stable; we’re not seeing a collapse.” | Oil logic does **not** reduce volume; story is **spending mix**. |
| **3. Mix changed** | “Compare Feb vs Mar: transaction count by **type** (Purchase, Payment, Withdrawal, Transfer, Deposit).” | In the oil window, more Payment/Withdrawal and Purchase mix; category weights shift. |
| **4. Categories** | “Feb vs Mar: transaction count by **category** (Gas, Groceries, Utilities, Shopping, Entertainment).” | Mar (and oil-window dates) show higher share of Gas, Groceries, Utilities; Feb has more Shopping/Entertainment. |
| **5. Merchants** | “Top 15 merchants in March by transaction count.” | Gas (Shell, Exxon), grocery (Walmart, CVS), utilities (AT&T, Verizon) rise in the mix. |
| **6. Credit Card** | “Feb vs Mar: transaction count by **account type** (Checking, Savings, Credit Card).” | Credit Card share is higher in the oil window (more gas/essentials on cards). |
| **7. Failures** | “% of transactions Failed or Pending in Feb vs Mar.” | ~7% in oil window vs ~2% baseline. |
| **8. Portfolio** | “Portfolio: symbols with largest price change in March (or Feb–Mar).” | Energy symbols (XOM, CVX, etc.) up; many others down (energy +15%, rest −5%). |
| **9. Loans by sector** | “Delinquency rate by **industry_sector** (Transport, Airline, Manufacturing, Other).” | Transport and Airline visibly higher than Other/Manufacturing. |
| **10. Name the cause** | “What happened to oil/energy in early 2026?” (web search or narrative.) | Data is built so the only consistent explanation is oil up → spending mix, stress, sector pain. |
| **11. Summary** | One sentence: “Rising oil prices since Feb 1 drove a shift to essentials, more card usage and failures, energy up / rest down in portfolios, and Transport/Airline loans under pressure.” | All of the above queries support this. |

**Important:** Keep the story “**spending mix and sector impact**,” not “volume collapsed.” The data is designed so volume stays plausible and the demo lands on oil as the root cause through mix and sector delinquency.
