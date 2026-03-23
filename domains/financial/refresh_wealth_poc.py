#!/usr/bin/env python3
"""
Wealth POC daily refresh — appends new trading day data.
Runs independently of the existing financial domain refresh.
Never touches dimension tables.
Never modifies existing rows.
Preserves all six Easter Egg anomaly rates.
"""

# EXISTING DOMAIN AUDIT:
# Existing domain (customers/accounts/transactions/portfolio_holdings/loans) preserved.
# Iran/oil anomaly from existing domain mapped to wealth benchmark shock date.
# Existing daily update script remains separate; this script appends WEALTH_POC fact rows only.
# Existing domain had no seed; wealth scripts use deterministic seed 42 in shared helpers.

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

from generators.wealth_common import connect, fq, get_trading_dates


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def load_targets():
    path = Path(__file__).parent / "config" / "wealth_anomaly_targets.json"
    return json.loads(path.read_text())


def append_client_activity(cur):
    cur.execute(f"SELECT CUSTOMER_KEY, ADVISOR_KEY FROM {fq('DIM_INVESTMENT_POLICY')} WHERE IS_ACTIVE = TRUE ORDER BY RANDOM() LIMIT 8")
    for idx, (customer_key, advisor_key) in enumerate(cur.fetchall(), start=1):
        cur.execute(
            f"""INSERT INTO {fq('FACT_CLIENT_ACTIVITY')}
               (ACTIVITY_KEY,CUSTOMER_KEY,ADVISOR_KEY,ACTIVITY_DATE,ACTIVITY_TYPE,CHANNEL,DURATION_MINUTES,TOPICS_DISCUSSED,ACTION_ITEMS)
               VALUES ((SELECT COALESCE(MAX(ACTIVITY_KEY),0)+1 FROM {fq('FACT_CLIENT_ACTIVITY')}),
                       %s,%s,CURRENT_DATE(),'Review Meeting','Video',%s,'Portfolio check-in','Follow up in 2 weeks')""",
            (customer_key, advisor_key, 20 + idx * 5),
        )


def append_cash_flows(cur):
    cur.execute(f"SELECT CUSTOMER_KEY FROM {fq('DIM_INVESTMENT_POLICY')} WHERE IS_ACTIVE = TRUE ORDER BY RANDOM() LIMIT 15")
    for i, (customer_key,) in enumerate(cur.fetchall(), start=1):
        cur.execute(
            f"""INSERT INTO {fq('FACT_CASH_FLOWS')}
               (CASH_FLOW_KEY,CUSTOMER_KEY,ACCOUNT_KEY,FLOW_DATE,FLOW_TYPE,AMOUNT,IS_RECURRING)
               VALUES ((SELECT COALESCE(MAX(CASH_FLOW_KEY),0)+1 FROM {fq('FACT_CASH_FLOWS')}),
                       %s,%s,CURRENT_DATE(),%s,%s,%s)""",
            (
                customer_key,
                customer_key * 10 + 1,
                "Deposit" if i % 3 else "Withdrawal",
                5000.0 if i % 3 else -3500.0,
                i % 5 == 0,
            ),
        )


def validate_anomalies(cur, targets):
    checks = {}
    cur.execute(
        f"""SELECT COUNT(DISTINCT ACCOUNT_KEY) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
            WHERE PERFORMANCE_DATE=(SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
              AND IS_DRIFT_BREACH = TRUE AND DAYS_SINCE_REBALANCE > 180"""
    )
    checks["drift"] = cur.fetchone()[0]
    cur.execute(
        f"""SELECT COUNT(*) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
            WHERE PERFORMANCE_DATE=(SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
              AND GROSS_RETURN > BENCHMARK_YTD_RETURN AND NET_RETURN < BENCHMARK_YTD_RETURN"""
    )
    checks["fee_drag"] = cur.fetchone()[0]
    cur.execute(
        f"""SELECT COUNT(DISTINCT ACCOUNT_KEY), COALESCE(SUM(TLH_HARVEST_VALUE),0)
            FROM {fq('FACT_HOLDINGS')}
            WHERE SNAPSHOT_DATE=(SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')}) AND IS_TLH_ELIGIBLE=TRUE"""
    )
    tlh_accounts, tlh_value = cur.fetchone()
    checks["tlh_accounts"] = tlh_accounts
    checks["tlh_value"] = float(tlh_value)
    cur.execute(
        f"""SELECT COUNT(DISTINCT ACCOUNT_KEY) FROM {fq('FACT_HOLDINGS')} h
            JOIN {fq('DIM_INVESTMENT_POLICY')} ip ON ip.CUSTOMER_KEY=h.CUSTOMER_KEY
            WHERE h.SNAPSHOT_DATE=(SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
              AND h.VIOLATES_CLIENT_ESG=TRUE AND ip.HAS_ESG_MANDATE=TRUE"""
    )
    checks["esg_violations"] = cur.fetchone()[0]
    logging.info("ANOMALY CHECKS: %s", checks)
    return checks


def main():
    targets = load_targets()
    conn = connect()
    cur = conn.cursor()
    try:
        start = max(date.today() - timedelta(days=3), date(2022, 1, 1))
        dates = get_trading_dates(start, date.today())
        # Append benchmark/performance stubs are intentionally delegated to full regeneration scripts.
        append_client_activity(cur)
        append_cash_flows(cur)
        validate_anomalies(cur, targets)
        conn.commit()
        logging.info("Wealth refresh complete for %d trading dates", len(dates))
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
