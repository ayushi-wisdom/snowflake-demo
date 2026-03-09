# Data model: relationships and join keys

Column definitions: `schema.yml`. Metric definitions: `metrics.yml`.

---

## Entity relationship summary

- **customers** — master; referenced by accounts and loans.
- **accounts** — master; referenced by transactions, portfolio_holdings, and loans.
- **transactions** — fact; rolling 365-day window; links to accounts only.
- **portfolio_holdings** — only for accounts where `account_type = 'Investment'`.
- **loans** — link to both customer (borrower) and optionally an account.

There is no persisted `market_prices` table; prices are applied to `portfolio_holdings.current_price` during the daily pipeline.

---

## Relationships

| Parent       | Child              | Join key(s)     | Cardinality |
|-------------|--------------------|-----------------|-------------|
| customers   | accounts           | customer_id     | 1 : N       |
| customers   | loans              | customer_id     | 1 : N       |
| accounts    | transactions       | account_id      | 1 : N       |
| accounts    | portfolio_holdings | account_id      | 1 : N       |
| accounts    | loans              | account_id      | 1 : 0..1    |

---

## Join patterns

- **Customer → accounts:** `customers.customer_id = accounts.customer_id`
- **Customer → loans:** `customers.customer_id = loans.customer_id`
- **Account → transactions:** `accounts.account_id = transactions.account_id`
- **Account → portfolio_holdings:** `accounts.account_id = portfolio_holdings.account_id` (restrict to Investment accounts for meaning)
- **Account → loans (linked):** `accounts.account_id = loans.account_id` (loan.account_id may be null)

---

## Update pattern

- **Master (slowly changing):** customers, accounts, loans — created/updated on initial load or backfill.
- **Daily:** New rows in transactions; account balances updated from transactions; portfolio_holdings refreshed with latest prices.
- **Rolling window:** Transactions older than 365 days are pruned.
