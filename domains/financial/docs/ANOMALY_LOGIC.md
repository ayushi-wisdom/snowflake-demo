# Anomaly logic and how data is chosen

## Current logic: not fully random

Data is **not** completely random. It’s set up to be a **good demo** with:

1. **Recurring date-based anomalies** (in `data_generator.py`) that change **transaction volume** and **category mix** by calendar:
   - **Pay-day:** last 2 + first 2 days of each month → higher volume, more Deposits/Transfers.
   - **Tax deadline:** Apr 10–18 → higher volume, more Payments/Transfers.
   - **Oil/energy:** **No volume dip**; story is **spending mix**. Date window **Feb 1 – Mar 10, 2026** (demo “since Feb 1”): $ on essentials **+35%** (full from Feb 15), **+15%** ramp Feb 1–14; non-essentials **−35%** / **−15%**. Heavier Gas/Withdrawal/Payment mix; ~7% Failed/Pending vs ~2%; more Credit Card usage. Portfolio: energy +15%, rest −5%. **Loans:** `industry_sector` (Transport, Airline, Manufacturing, Other); in oil window Transport delinquency ~16%, Airline ~12%, rest ~5%. When not using demo date, real XLE can drive the flag.
   - **Holiday spending:** Easter (Apr 3–6), Mother's Day (May 8–10), Memorial Day (May 23–26), Father's Day (Jun 19–21) 2026 → +20% volume, more Purchase/Shopping/Restaurant/Entertainment.
   - **Quarter-end:** last day of Mar/Jun/Sep/Dec → +10% volume, more Transfers (balance management).
   - **FOMC day:** Fed meeting end dates (e.g. Mar 18, Apr 29, Jun 17, … 2026) → +10% volume, more Transfer/Withdrawal (rate sensitivity).
2. **Dormant accounts:** ~8% of accounts get no transactions in the last 6 months (deterministic by `account_id`).
3. **One-off promotion:** November 2025 is implemented as a separate script (`add_november_promotion_customers.py` + backfill); the generator has no generic “promotion” event.

So **what gets generated** is decided by:
- **Which date** you’re generating for → picks volume multiplier and category weights (or normal mix).
- **Which accounts** are eligible → active, and not dormant when the date is in the last 180 days.
- **Random** within that: exact counts (within ranges), amounts, merchants, etc.

So there are **anomalies right now** (pay-day, tax, oil/energy, holiday, quarter-end, FOMC, dormant). **Oil/energy** uses the **date window Feb 1 – Mar 10** when `DATA_AS_OF_DATE` is set in `.env` (demo mode); otherwise real-data (XLE) can override. Portfolio prices can use **real stock prices** (yfinance) when the fetch succeeds. For “big customer left/joined” or “new promotion,” use the manual script. **Data never extends past the current day:** set `DATA_AS_OF_DATE=2026-03-10` so all generation and pruning use that as “today.”

---

## Real world → business impact

| What occurred (real world) | How it impacts this business and data |
|----------------------------|----------------------------------------|
| **Oil/energy prices up** | **Spending mix shift** (no volume dip): $ on **Gas**, **Groceries**, **Utilities** up **35%**; on everything else down **35%**. More **Withdrawals** and **Payments**; more **Failed/Pending** (~7% vs ~2%); more **Credit Card** usage. Portfolio: **energy** up, broad market down. Real XLE vs 30-day baseline. |
| **Tax deadline (Apr 10–18)** | More **Payments** and **Transfers** (tax payments, refunds). Higher volume. |
| **Pay-day (month-end/start)** | More **Deposits** and **Transfers** (payroll, rent). Higher volume. |
| **Fed rate decision (FOMC)** | More **Transfers** and **Withdrawals** (rate sensitivity, rebalancing). Slight volume bump. |
| **Quarter-end** | More **Transfers** (balance management, corporate flows). |
| **Holidays (Easter, Mother’s Day, etc.)** | More **Purchases**, **Shopping**, **Restaurant**, **Entertainment**. Higher volume. |
| **Big customer leaves / joins / promotion** | Use `add_manual_anomaly.py` so the data reflects churn, new whales, or a promo cohort. |
| **Variable-rate loan resets** | Since **Feb 1**, variable-rate loans (∼30% of loans) have **interest_rate** creeping up each day (e.g. +0.02% per day). Run `migrate_loans_variable_rate.py` once to add `rate_type` / `original_interest_rate`; daily update applies the creep. |

Oil/energy is the only one **driven by real market data** (XLE) each run; the rest use the calendar. Portfolio holdings use **real equity prices** (yfinance) when the fetch succeeds, so the data aligns with real-world markets.

---

## Predetermined dates (next 2–3 months and recurring)

| Type | 2026 dates / rule | Effect |
|------|--------------------|--------|
| **Easter** | Apr 3–6 | +20% volume, more Purchase/Shopping/Restaurant/Entertainment |
| **Tax deadline** | Apr 10–18 | +35% volume, more Payment/Transfer |
| **Mother's Day** | May 8–10 | +20% volume, holiday spending mix |
| **Memorial Day** | May 23–26 | +20% volume, holiday spending mix |
| **Father's Day** | Jun 19–21 | +20% volume, holiday spending mix |
| **Quarter-end** | Mar 31, Jun 30, Sep 30, Dec 31 (last day of quarter) | +10% volume, more Transfer |
| **FOMC** | Mar 18, Apr 29, Jun 17, Jul 29, Sep 16, Oct 28, Dec 9 | +10% volume, more Transfer/Withdrawal |

To add a new year’s holidays, extend the `*_2026_*` constants (or add a small calendar helper) in `data_generator.py`. FOMC dates are in `FOMC_DAYS_2026`; update each year from [Fed calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm).

---

## Adding new anomalies: two ways

### 1. Recurring / calendar events (code change)

To add a **new date-based anomaly** (e.g. “Black Friday” or “Fed meeting”):

1. In `data_generator.py` add a predicate, e.g. `_is_black_friday(target_date)`.
2. Use it in `_anomaly_volume_multiplier()` and `_anomaly_category_weights()`.
3. If it affects portfolio prices, add logic in `apply_iran_oil_price_shock()` or a similar helper and call it from `daily_financial_update.py`.

Everything stays in code; no config file.

### 2. Manual / one-off events (run monthly)

For **“once a month I add anomalies”** (big customer leaves, big customer joins, new promotion), use the **manual anomaly script** so you don’t edit code each time:

| Event type | What it does |
|------------|----------------|
| **Big customer churn** | Marks 1–2 high-value customers (by total balance) as **Inactive** and optionally closes their accounts. |
| **Big customer join** | Adds 1 (or 2) “whale” customers with high balances and several accounts. |
| **New promotion** | Adds a cohort of new customers in a date range with a signup bonus (like November, but any month). |

Run the script when you want the event, e.g. once a month:

```bash
# One big customer leaves (churn)
python add_manual_anomaly.py --type big_churn --count 1

# One big customer joins
python add_manual_anomaly.py --type big_join

# New promotion: April 2026, $50 bonus, 60 customers
python add_manual_anomaly.py --type promotion --start 2026-04-01 --end 2026-04-30 --bonus 50 --count 60
```

After a **promotion** run, backfill transactions for those accounts (same idea as November):

```bash
python backfill_promotion_account_transactions.py --start 2026-04-01 --end 2026-04-30
```

---

## Summary

- **Current:** Oil/energy is **real-data driven** (XLE vs baseline); portfolio can use **real stock prices** (yfinance). Other anomalies are date-based (pay-day, tax, holiday, quarter-end, FOMC); when that date comes, the run applies the right volume and category mix. Dormant accounts as before.
- **Recurring new anomalies:** Add new date predicates and use them in volume/category (and prices if needed) in `data_generator.py`.
- **Manual monthly anomalies:** Use `add_manual_anomaly.py` for big churn, big join, or a new promotion.
