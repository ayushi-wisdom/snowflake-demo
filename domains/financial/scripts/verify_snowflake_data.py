"""Quick verify: what's in Snowflake (uses .env)."""
from datetime import date, timedelta
from snowflake_connection import get_snowflake_connection

conn = get_snowflake_connection()
cur = conn.cursor()

cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
db, schema = cur.fetchone()
print("Connected to:", db, schema)
print()

for t in ["customers", "accounts", "transactions", "portfolio_holdings", "loans"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"{t}: {cur.fetchone()[0]:,} rows")

cur.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions")
mn, mx = cur.fetchone()
print(f"\nTransactions date range: {mn} to {mx}")

cur.execute("""
    SELECT COUNT(*) FROM customers 
    WHERE created_date BETWEEN '2025-11-01' AND '2025-11-30'
""")
nov_cust = cur.fetchone()[0]
cur.execute("""
    SELECT COUNT(*) FROM accounts a
    JOIN customers c ON c.customer_id = a.customer_id
    WHERE c.created_date BETWEEN '2025-11-01' AND '2025-11-30'
""")
nov_acc = cur.fetchone()[0]
cur.execute("""
    SELECT COUNT(*) FROM transactions t
    JOIN accounts a ON a.account_id = t.account_id
    JOIN customers c ON c.customer_id = a.customer_id
    WHERE c.created_date BETWEEN '2025-11-01' AND '2025-11-30'
""")
nov_txn = cur.fetchone()[0]
print(f"\nNovember promotion: {nov_cust} customers, {nov_acc} accounts, {nov_txn:,} transactions")

cutoff = date.today() - timedelta(days=180)
cur.execute("SELECT COUNT(*) FROM accounts WHERE status = 'Active'")
total_active = cur.fetchone()[0]
cur.execute(
    """
    SELECT COUNT(*) FROM accounts a
    WHERE a.status = 'Active'
    AND NOT EXISTS (
        SELECT 1 FROM transactions t 
        WHERE t.account_id = a.account_id AND t.transaction_date >= %s
    )
    """,
    (cutoff,),
)
dormant = cur.fetchone()[0]
print(f"\nDormant (no txns since {cutoff}): {dormant} of {total_active} active accounts")

cur.execute("SELECT status, COUNT(*) FROM customers GROUP BY status")
print("\nCustomers by status:", dict(cur.fetchall()))

cur.close()
conn.close()
print("\nDone.")
