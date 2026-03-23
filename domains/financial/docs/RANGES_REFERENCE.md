# Ranges Reference

All numeric ranges used when generating and updating data. Amounts are in **USD** unless noted.

---

## 1. Row counts (how many rows added per run)

### Master data (first run only)

| Entity | Range / count |
|--------|----------------|
| **Customers** | **1,500** (fixed in `daily_financial_update.py`) |
| **Accounts** | **1–3 per customer** (only Active customers); ~90% Active, ~10% Closed → roughly **~2,700–4,500** accounts |
| **Loans** | **30–50% of active customers** get at least one loan → roughly **450–750** loans |

### Transactions (daily and backfill)

| Context | Base count per day | After anomaly multiplier |
|---------|--------------------|---------------------------|
| **Today (daily update)** | **650–1,000** (random per run) | ×0.75 (Iran), ×1.0 (normal), ×1.25 (pay-day), ×1.35 (tax), ×1.4 (pay+tax) |
| **Historical backfill** (each missing day) | **400–800** per day | Same multipliers |
| **Fill missing** (`fill_missing_transactions.py`) | **650–1,000** per missing day | Same multipliers |
| **Backfill with anomalies** (`backfill_with_anomalies.py`) | **650–1,000** per day | Same multipliers |
| **November promo backfill** | **max(1, accounts_for_date × 1.5–3)** per day | Same multipliers |

So **per day**: roughly **400–1,400** transactions depending on day type and anomaly (e.g. Iran → ~488–750; pay-day → ~813–1,250).

### Portfolio holdings

| Item | Range |
|------|--------|
| **Holdings per investment account** | **1–5** (random) |
| **Total holdings** | Only for accounts with `account_type = 'Investment'`; created when an investment account has no holdings yet (daily script). |

### Optional / one-time scripts

| Script | Rows added (range) |
|--------|---------------------|
| **Weekly churn** | **3–5** new customers; **1–2** accounts per new customer; **1–3** customers marked Inactive; ~30% of new customers get loans |
| **November promotion** | **120** new customers; **1–2** accounts each → ~120–240 new accounts; **+$100** on Checking/Savings opened in Nov 2025 |
| **Backfill November** | **~1.5–3 × (Nov promo accounts)** transactions per day from Nov 1 to today |

---

## 2. Transaction amounts (per transaction)

All amounts are **rounded to 2 decimals**. Negative = debit/outflow.

### By account type and transaction type

| Account type | Transaction type | Amount range (USD) |
|--------------|-------------------|---------------------|
| **Credit Card** | Purchase | **-500 to -10** |
| **Credit Card** | Payment | **100 to 2,000** |
| **Other** | Deposit | **100 to 10,000** (normal); **200 to 12,000** (anomaly-weighted) |
| **Other** | Withdrawal | **-5,000 to -50** |
| **Other** | Transfer | **±100 to ±5,000** (normal); **±200 to ±6,000** (anomaly-weighted) |
| **Other** | Purchase | **-500 to -10** |
| **Other** | Payment | **-1,000 to -50** (normal); **-3,000 to -50** (anomaly-weighted) |

### Reference and status

- **Reference number:** `REF` + **100,000–999,999** (integer).
- **Status:** **~98%** `Completed`, **~2%** `Pending` or `Failed` (random).

---

## 3. Account balances (initial / by type)

| Account type | current_balance range (USD) | available_balance |
|--------------|----------------------------|-------------------|
| **Credit Card** | **-5,000 to 0** | balance × **0.95–1.0** (random) |
| **Investment** | **10,000 to 500,000** | same logic |
| **Savings** | **1,000 to 100,000** | same logic |
| **Checking** (default) | **500 to 50,000** | same logic |

- **November promo:** Checking/Savings opened in Nov 2025 get **+$100** added to the above.
- **Account number:** **1,000,000,000–9,999,999,999** (integer).
- **Accounts per customer:** **1–3**; **~90%** Active, **~10%** Closed.

---

## 4. Loans

| Loan type | Original principal (USD) | Term | Interest rate (APR %) |
|-----------|---------------------------|------|------------------------|
| **Mortgage** | **200,000 – 800,000** | **180, 240, or 360** months | **3.0 – 6.5** |
| **Auto** | **15,000 – 60,000** | **36, 48, 60, or 72** months | **2.5 – 8.0** |
| **Personal** | **5,000 – 50,000** | **12, 24, 36, 48, or 60** months | **5.0 – 15.0** |
| **Credit Line** | **10,000 – 100,000** | **60** months | **8.0 – 20.0** |

- **Current principal balance:** Derived from `original_principal × (1 - payment_ratio × random(0.3, 0.8))` (pay-down).
- **Days past due:** **0** (~95%) or **1–90** (~5%).
- **Status:** Active / Delinquent / Paid Off (balance &lt; 100).

---

## 5. Portfolio holdings (investment accounts)

| Field | Range |
|-------|--------|
| **Quantity** | **1 – 100** (per holding, random) |
| **Purchase price** | **current_price × (0.7 – 1.3)** (historical vs current) |
| **Current price (synthetic)** | **50 – 500** per symbol (when no real prices provided) |
| **Market value** | quantity × current_price |
| **Unrealized PnL** | (current_price - purchase_price) × quantity |

- **Holdings per investment account:** **1–5**.
- **Symbols:** Up to **150** from `STOCK_SYMBOLS`; prices default to **50–500** if not provided.
- **Iran/oil shock (Mar 2–9, 2026):** Energy symbols **×1.15**, others **×0.95**.

---

## 6. Customers (master data)

| Field | Range / values |
|-------|-----------------|
| **Created date** | **5 years ago → today** (initial); or custom range for promo/weekly |
| **Date of birth** | Year **1950–2000**; month/day **1–28** |
| **Status** | **~95%** Active, **~5%** Inactive (random) |
| **Segment** | **Premium, Standard, Basic** (random) |
| **Phone** | `XXX-XXX-XXXX` (random digits) |
| **ZIP** | **10000–99999** |

---

## 7. Volume multipliers (transaction count per day)

Applied to the **base count** for that day:

| Condition | Multiplier | Effective daily range (if base 650–1000) |
|-----------|------------|------------------------------------------|
| Iran/oil (Mar 2–9, 2026) | **0.75** | **488 – 750** |
| Normal | **1.0** | **650 – 1,000** |
| Pay-day (month end/start) | **1.25** | **813 – 1,250** |
| Tax (Apr 10–18) | **1.35** | **878 – 1,350** |
| Pay-day + tax | **1.4** | **910 – 1,400** |

---

## 8. Pruning (daily update)

- **Transactions:** **DELETE** where `transaction_date < today - 364 days` (keeps rolling **365 days**).
- Number deleted depends on how many days of data existed; no fixed row range.

---

## 9. Dormant accounts

- **Fraction:** **8%** of accounts (deterministic from `account_id`).
- **Effect:** Those accounts get **no transactions** when `target_date` is within the **last 180 days**; older dates can still have transactions for them.
