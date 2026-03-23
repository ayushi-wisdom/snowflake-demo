# EXISTING DOMAIN AUDIT:
# Existing domain tables and anomalies audited in create_schema.py/data_generator.py/daily_financial_update.py.
# Iran/oil anomaly window is 2026-02-01..2026-03-10 and was preserved in wealth benchmark/performance modeling.
# Daily update append/update behavior retained; this script only populates WEALTH_POC fact holdings.
# Existing domain had no explicit seed; this script uses shared deterministic seed via wealth_common.

from __future__ import annotations

import random
from datetime import date, timedelta

import numpy as np

from .wealth_common import connect, fq, month_end_trading_dates
from .wealth_common import get_trading_dates

DEMO_HOLDING_ASSETS_MIN = 6
DEMO_HOLDING_ASSETS_MAX = 10


def main():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT ASSET_KEY, ASSET_CLASS, ESG_RATING, ESG_CONTROVERSY_FLAG FROM {fq('DIM_ASSET')} WHERE IS_ACTIVE = TRUE")
        assets = cur.fetchall()
        asset_keys = [a[0] for a in assets]
        esg_bad = [a[0] for a in assets if a[3]]

        cur.execute(f"""SELECT DISTINCT CUSTOMER_KEY FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}""")
        active_customers = {r[0] for r in cur.fetchall()}
        cur.execute(f"""SELECT POLICY_KEY, CUSTOMER_KEY, HAS_ESG_MANDATE, TAX_SENSITIVE
                        FROM {fq('DIM_INVESTMENT_POLICY')} WHERE IS_ACTIVE = TRUE ORDER BY CUSTOMER_KEY""")
        policies = [p for p in cur.fetchall() if p[1] in active_customers]

        cur.execute(f"""SELECT CUSTOMER_KEY, ACCOUNT_KEY, PERFORMANCE_DATE, ENDING_MARKET_VALUE
                        FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}""")
        perf = cur.fetchall()
        perf_map = {(r[0], r[1], r[2]): float(r[3]) for r in perf}
        all_dates = sorted(list({r[2] for r in perf}))
        month_ends = set(month_end_trading_dates(all_dates))

        cur.execute(f"TRUNCATE TABLE {fq('FACT_HOLDINGS')}")
        rows = []
        key = 1
        latest_snapshot = max(month_ends)
        tlh_accounts = set(random.sample([p[1] * 10 + 1 for p in policies], 18))
        esg_mandate_clients = [p[1] for p in policies if p[2]]
        esg_violation_accounts = set((c * 10 + 1) for c in random.sample(esg_mandate_clients, 12))

        for _, customer_key, has_esg, tax_sensitive in policies:
            account_keys = [customer_key * 10 + 1]
            chosen_assets = random.sample(asset_keys, random.randint(DEMO_HOLDING_ASSETS_MIN, DEMO_HOLDING_ASSETS_MAX))
            for d in sorted(month_ends):
                for account_key in account_keys:
                    account_value = perf_map.get((customer_key, account_key, d))
                    if not account_value:
                        continue
                    weights = np.random.dirichlet(np.ones(len(chosen_assets)))
                    for idx, asset_key in enumerate(chosen_assets):
                        w = float(weights[idx])
                        mkt_value = account_value * w
                        price = random.uniform(20, 450)
                        qty = mkt_value / price if price else 0
                        holding_days = random.randint(31, 1200)
                        cost_basis_total = mkt_value * random.uniform(0.88, 1.12)
                        if d == latest_snapshot and account_key in tlh_accounts and idx < 2:
                            cost_basis_total = mkt_value * random.uniform(1.08, 1.15)
                            holding_days = random.randint(31, 45)
                        ugl = mkt_value - cost_basis_total
                        ugl_pct = ugl / cost_basis_total if cost_basis_total else 0
                        is_tlh = d == latest_snapshot and tax_sensitive and ugl < 0 and holding_days > 30 and account_key in tlh_accounts and idx < 2
                        tlh_value = abs(ugl) if is_tlh else None
                        violates = d == latest_snapshot and has_esg and account_key in esg_violation_accounts and asset_key in esg_bad and idx == 0
                        esg_rating = next((a[2] for a in assets if a[0] == asset_key), None)
                        first_purchase = d - timedelta(days=holding_days)
                        rows.append(
                            (
                                key, customer_key, account_key, asset_key, d, round(qty, 4), round(price, 4), round(mkt_value, 2),
                                round(cost_basis_total, 2), round(cost_basis_total / qty, 4) if qty else None, round(ugl, 2), round(ugl_pct, 4),
                                round(w, 4), round(w, 4), first_purchase, holding_days, holding_days < 365, ugl < 0,
                                is_tlh, round(tlh_value, 2) if tlh_value else None, esg_rating, violates
                            )
                        )
                        key += 1

        insert_sql = f"""INSERT INTO {fq('FACT_HOLDINGS')}
            (HOLDING_KEY,CUSTOMER_KEY,ACCOUNT_KEY,ASSET_KEY,SNAPSHOT_DATE,QUANTITY,MARKET_PRICE,MARKET_VALUE,COST_BASIS_TOTAL,
             COST_BASIS_PER_SHARE,UNREALIZED_GAIN_LOSS,UNREALIZED_GAIN_LOSS_PCT,PORTFOLIO_WEIGHT,ASSET_CLASS_WEIGHT,
             FIRST_PURCHASE_DATE,HOLDING_DAYS,IS_SHORT_TERM,IS_LOSS_POSITION,IS_TLH_ELIGIBLE,TLH_HARVEST_VALUE,
             POSITION_ESG_RATING,VIOLATES_CLIENT_ESG)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        chunk = 5000
        for i in range(0, len(rows), chunk):
            cur.executemany(insert_sql, rows[i:i + chunk])
        conn.commit()
        print(f"FACT_HOLDINGS: {len(rows)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
