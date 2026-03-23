# EXISTING DOMAIN AUDIT:
# Core financial domain schema and anomaly behavior audited from existing scripts.
# Iran/oil anomaly and daily job behavior are preserved; this script only generates WEALTH_POC trade facts.
# Existing project has no explicit random seed; deterministic generation here uses wealth_common seed setup.

from __future__ import annotations

import random
from datetime import timedelta

from .wealth_common import connect, fq


def main():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('FACT_TRADES')}")
        cur.execute(
            f"""SELECT CUSTOMER_KEY, ACCOUNT_KEY, ADVISOR_KEY, PERFORMANCE_DATE
                FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
                WHERE DAY(PERFORMANCE_DATE) IN (5, 12, 19, 26)"""
        )
        perf_rows = cur.fetchall()
        cur.execute(f"SELECT ASSET_KEY FROM {fq('DIM_ASSET')}")
        assets = [r[0] for r in cur.fetchall()]
        rows = []
        key = 1
        sample = random.sample(perf_rows, min(12000, len(perf_rows)))
        for customer_key, account_key, advisor_key, trade_date in sample:
            qty = round(random.uniform(5, 500), 4)
            px = round(random.uniform(12, 420), 4)
            gross = round(qty * px, 2)
            commission = round(random.uniform(0, 9.95), 2)
            direction = random.choice(["Buy", "Sell"])
            rows.append(
                (
                    key, f"TRD-{trade_date.year}-{key:06d}", customer_key, account_key, random.choice(assets), advisor_key,
                    trade_date, trade_date + timedelta(days=2), direction, qty, px, gross, commission,
                    round(gross + commission, 2), random.choice(["Rebalance", "Tax Loss Harvest", "New Money", "Distribution", "Client-Directed"]),
                    random.random() < 0.7, random.random() < 0.65, random.choice(["Fidelity", "Schwab", "Pershing"]),
                    random.choice(["Market", "Limit", "Stop"]), round(random.uniform(-3000, 4500), 2) if direction == "Sell" else None,
                    random.random() < 0.015, None
                )
            )
            key += 1
        cur.executemany(
            f"""INSERT INTO {fq('FACT_TRADES')}
            (TRADE_KEY,TRADE_ID,CUSTOMER_KEY,ACCOUNT_KEY,ASSET_KEY,ADVISOR_KEY,TRADE_DATE,SETTLEMENT_DATE,DIRECTION,QUANTITY,EXECUTION_PRICE,
             GROSS_AMOUNT,COMMISSION,NET_AMOUNT,TRADE_TYPE,IS_ADVISOR_INITIATED,IS_DISCRETIONARY,BROKER,ORDER_TYPE,REALIZED_GAIN_LOSS,IS_WASH_SALE,NOTES)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
        conn.commit()
        print(f"FACT_TRADES: {len(rows)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
