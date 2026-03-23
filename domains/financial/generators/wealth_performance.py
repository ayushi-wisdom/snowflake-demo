# EXISTING DOMAIN AUDIT:
# Core financial tables and Iran/oil behavior were audited from existing domain scripts.
# Iran/oil window: 2026-02-01..2026-03-10, with additional market shock logic applied in wealth benchmarks.
# Existing daily update appends transactions and updates balances/holdings/loan variables; no customer rewrites after seed.
# Existing domain had no explicit random seed; this module enforces deterministic seed settings from wealth_common.

from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import date

import numpy as np

from .wealth_common import connect, fq, get_trading_dates

DEMO_CLIENT_LIMIT = 250
DEMO_START_DATE = date(2024, 1, 1)


def load_policies(cur):
    cur.execute(
        f"""SELECT CUSTOMER_KEY, ADVISOR_KEY, BENCHMARK_KEY, RISK_PROFILE,
                   TARGET_EQUITY_PCT, TARGET_FIXED_INCOME_PCT, TARGET_ALTERNATIVE_PCT, TARGET_CASH_PCT,
                   REBALANCING_DRIFT_THRESHOLD
            FROM {fq('DIM_INVESTMENT_POLICY')}
            WHERE IS_ACTIVE = TRUE
            ORDER BY CUSTOMER_KEY"""
    )
    return cur.fetchall()


def load_benchmark_map(cur):
    cur.execute(
        f"""SELECT BENCHMARK_KEY, RETURN_DATE, DAILY_RETURN, MTD_RETURN, QTD_RETURN, YTD_RETURN
            FROM {fq('FACT_BENCHMARK_RETURNS')}"""
    )
    out = defaultdict(dict)
    for bench_key, d, daily, mtd, qtd, ytd in cur.fetchall():
        out[bench_key][d] = (float(daily), float(mtd), float(qtd), float(ytd))
    return out


def account_count_for_customer(customer_key: int) -> int:
    return 1


def starting_value(risk_profile: str, advisor_key: int) -> float:
    base = {
        "Conservative": np.random.lognormal(mean=12.5, sigma=0.8),
        "Moderate": np.random.lognormal(mean=13.1, sigma=0.9),
        "Moderate-Growth": np.random.lognormal(mean=13.5, sigma=1.0),
        "Aggressive": np.random.lognormal(mean=14.2, sigma=1.1),
    }[risk_profile]
    if advisor_key == 3:
        base *= 1.4
    return float(base)


def insert_batch(cur, rows):
    cur.executemany(
        f"""INSERT INTO {fq('FACT_PORTFOLIO_PERFORMANCE')}
        (PERFORMANCE_KEY,CUSTOMER_KEY,ACCOUNT_KEY,ADVISOR_KEY,BENCHMARK_KEY,PERFORMANCE_DATE,
         BEGINNING_MARKET_VALUE,ENDING_MARKET_VALUE,NET_CASH_FLOW,DAILY_RETURN,MTD_RETURN,QTD_RETURN,YTD_RETURN,ITD_RETURN,
         BENCHMARK_DAILY_RETURN,BENCHMARK_MTD_RETURN,BENCHMARK_QTD_RETURN,BENCHMARK_YTD_RETURN,
         ACTIVE_RETURN_DAILY,ACTIVE_RETURN_YTD,EQUITY_PCT,FIXED_INCOME_PCT,ALTERNATIVE_PCT,CASH_PCT,
         EQUITY_DRIFT,MAX_DRIFT,IS_DRIFT_BREACH,DAYS_SINCE_REBALANCE,TOTAL_COST_BASIS,UNREALIZED_GAIN_LOSS,UNREALIZED_GAIN_LOSS_PCT,
         MANAGEMENT_FEE_ACCRUAL,FUND_EXPENSE_ACCRUAL,GROSS_RETURN,NET_RETURN)
        VALUES (%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )


def main():
    start_date = DEMO_START_DATE
    end_date = date.today()
    trading_dates = get_trading_dates(start_date, end_date)

    conn = connect()
    cur = conn.cursor()
    try:
        policies = load_policies(cur)[:DEMO_CLIENT_LIMIT]
        bench = load_benchmark_map(cur)
        cur.execute(f"TRUNCATE TABLE {fq('FACT_PORTFOLIO_PERFORMANCE')}")

        key = 1
        batch = []
        for (customer_key, advisor_key, benchmark_key, risk_profile, tgt_eq, tgt_fi, tgt_alt, tgt_cash, drift_threshold) in policies:
            alpha_annual = np.random.normal(0.002, 0.025)
            te_annual = np.random.uniform(0.02, 0.08)

            for acct_num in range(account_count_for_customer(int(customer_key))):
                account_key = int(customer_key) * 10 + acct_num + 1
                market_value = starting_value(risk_profile, int(advisor_key)) / account_count_for_customer(int(customer_key))
                total_cost_basis = market_value * np.random.uniform(0.90, 1.03)
                eq = float(tgt_eq) + np.random.uniform(-0.01, 0.01)
                fi = float(tgt_fi) + np.random.uniform(-0.01, 0.01)
                alt = float(tgt_alt) + np.random.uniform(-0.005, 0.005)
                cash = max(0.0, 1.0 - (eq + fi + alt))
                rebalance_days = random.randint(10, 120)
                y_chain = q_chain = m_chain = itd_chain = 1.0
                last_year = last_q = last_m = None

                for d in trading_dates:
                    b_daily, b_mtd, b_qtd, b_ytd = bench[int(benchmark_key)][d]
                    daily_alpha = alpha_annual / 252.0
                    daily_noise = np.random.normal(0, te_annual / math.sqrt(252.0))
                    gross = b_daily + daily_alpha + daily_noise
                    advisory_fee_annual = 0.0075 if market_value < 500000 else 0.0050
                    fund_expense_annual = 0.0025
                    daily_fee_drag = (advisory_fee_annual + fund_expense_annual) / 252.0
                    net = gross - daily_fee_drag

                    net_cash_flow = np.random.normal(0, market_value * 0.0009) if random.random() < 0.12 else 0.0
                    beginning = market_value
                    ending = max(1000.0, beginning * (1 + net) + net_cash_flow)
                    market_value = ending

                    eq = min(0.98, max(0.02, eq + np.random.normal((b_daily - net) * 0.05, 0.0015)))
                    fi = min(0.95, max(0.01, fi + np.random.normal((-b_daily) * 0.03, 0.0012)))
                    alt = min(0.35, max(0.00, alt + np.random.normal(0, 0.0008)))
                    cash = max(0.0, 1.0 - (eq + fi + alt))
                    total = eq + fi + alt + cash
                    eq, fi, alt, cash = eq / total, fi / total, alt / total, cash / total

                    max_drift = max(abs(eq - float(tgt_eq)), abs(fi - float(tgt_fi)), abs(alt - float(tgt_alt)), abs(cash - float(tgt_cash)))
                    breach = max_drift > float(drift_threshold)
                    rebalance_days += 1
                    if breach and rebalance_days > 210 and random.random() < 0.12:
                        eq, fi, alt, cash = float(tgt_eq), float(tgt_fi), float(tgt_alt), float(tgt_cash)
                        rebalance_days = 0

                    year_key = d.year
                    q_key = (d.year, (d.month - 1) // 3)
                    m_key = (d.year, d.month)
                    if last_year != year_key:
                        y_chain = 1.0
                        last_year = year_key
                    if last_q != q_key:
                        q_chain = 1.0
                        last_q = q_key
                    if last_m != m_key:
                        m_chain = 1.0
                        last_m = m_key
                    y_chain *= 1 + net
                    q_chain *= 1 + net
                    m_chain *= 1 + net
                    itd_chain *= 1 + net

                    mgmt_fee = ending * (advisory_fee_annual / 252.0)
                    exp_fee = ending * (fund_expense_annual / 252.0)
                    unr = ending - total_cost_basis
                    unr_pct = (unr / total_cost_basis) if total_cost_basis else 0.0

                    batch.append(
                        (
                            key, int(customer_key), account_key, int(advisor_key), int(benchmark_key), d,
                            round(beginning, 2), round(ending, 2), round(net_cash_flow, 2), round(net, 6),
                            round(m_chain - 1, 6), round(q_chain - 1, 6), round(y_chain - 1, 6), round(itd_chain - 1, 6),
                            round(b_daily, 6), round(b_mtd, 6), round(b_qtd, 6), round(b_ytd, 6),
                            round(net - b_daily, 6), round((y_chain - 1) - b_ytd, 6),
                            round(eq, 4), round(fi, 4), round(alt, 4), round(cash, 4),
                            round(eq - float(tgt_eq), 4), round(max_drift, 4), breach, rebalance_days,
                            round(total_cost_basis, 2), round(unr, 2), round(unr_pct, 6),
                            round(mgmt_fee, 2), round(exp_fee, 2), round(gross, 6), round(net, 6),
                        )
                    )
                    key += 1
                    if len(batch) >= 10000:
                        insert_batch(cur, batch)
                        conn.commit()
                        batch.clear()
        if batch:
            insert_batch(cur, batch)
            conn.commit()
        print("FACT_PORTFOLIO_PERFORMANCE generated.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
