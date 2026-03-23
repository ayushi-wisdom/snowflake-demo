# EXISTING DOMAIN AUDIT:
# Existing financial domain is preserved; this adds WEALTH_POC cash flow events only.
# Iran/oil and existing daily update behavior remain unchanged.
# Deterministic seed behavior comes from wealth_common.

from __future__ import annotations

import random
from datetime import date, timedelta

from .wealth_common import connect, fq


def main():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('FACT_CASH_FLOWS')}")
        cur.execute(f"SELECT DISTINCT CUSTOMER_KEY FROM {fq('FACT_PORTFOLIO_PERFORMANCE')} ORDER BY CUSTOMER_KEY")
        clients = [r[0] for r in cur.fetchall()]
        rows = []
        key = 1
        for c in clients:
            for _ in range(random.randint(18, 30)):
                d = date(2022, 1, 1) + timedelta(days=random.randint(0, (date.today() - date(2022, 1, 1)).days))
                acct = c * 10 + random.choice([1, 2])
                flow = random.choice(["Deposit", "Withdrawal", "Dividend", "Interest", "Fee", "Transfer In", "Transfer Out"])
                amt = random.uniform(500, 35000)
                if flow in ("Withdrawal", "Fee", "Transfer Out"):
                    amt *= -1
                rows.append((key, c, acct, d, flow, round(amt, 2), None, random.random() < 0.25, random.choice(["Monthly", "Quarterly", "Annual"]) if random.random() < 0.25 else None, None))
                key += 1
        cur.executemany(
            f"""INSERT INTO {fq('FACT_CASH_FLOWS')}
            (CASH_FLOW_KEY,CUSTOMER_KEY,ACCOUNT_KEY,FLOW_DATE,FLOW_TYPE,AMOUNT,ASSET_KEY,IS_RECURRING,RECURRING_FREQUENCY,NOTES)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
        conn.commit()
        print(f"FACT_CASH_FLOWS: {len(rows)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
