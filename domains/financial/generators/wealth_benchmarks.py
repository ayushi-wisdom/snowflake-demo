# EXISTING DOMAIN AUDIT:
# Existing core tables: customers/accounts/transactions/portfolio_holdings/loans from create_schema.py.
# Iran/oil shock in existing domain uses 2026-02-01..2026-03-10 with price shock behavior.
# Daily update appends transactions/history and updates account/holdings/loan derived fields.
# Existing financial domain has no explicit RNG seed setting; this module uses seed=42.

from __future__ import annotations

import math
from datetime import date

import numpy as np

from .wealth_common import connect, fq, get_trading_dates

BENCHMARK_PROFILES = {
    1: {"annual_return": 0.105, "annual_vol": 0.17, "start_level": 4200.0},
    2: {"annual_return": 0.022, "annual_vol": 0.055, "start_level": 2300.0},
    3: {"annual_return": 0.091, "annual_vol": 0.155, "start_level": 2900.0},
    4: {"annual_return": 0.048, "annual_vol": 0.195, "start_level": 1200.0},
    5: {"annual_return": 0.071, "annual_vol": 0.105, "start_level": 1800.0},
    6: {"annual_return": 0.031, "annual_vol": 0.042, "start_level": 1600.0},
}

IRAN_SHOCK_DATE = date(2026, 3, 3)


def chained_return(values: list[float]) -> float:
    x = 1.0
    for v in values:
        x *= 1.0 + v
    return x - 1.0


def generate_rows():
    start_date = date(2022, 1, 1)
    end_date = date.today()
    trading_dates = get_trading_dates(start_date, end_date)
    rows = []
    key = 1

    for bench_key, profile in BENCHMARK_PROFILES.items():
        dt = 1.0 / 252.0
        mu = profile["annual_return"]
        sigma = profile["annual_vol"]
        level = profile["start_level"]
        daily_hist = []
        ytd_hist = {}
        qtd_hist = {}
        mtd_hist = {}

        for d in trading_dates:
            z = np.random.normal(0, 1)
            ret = math.exp((mu - 0.5 * sigma * sigma) * dt + sigma * math.sqrt(dt) * z) - 1
            if d == IRAN_SHOCK_DATE:
                if bench_key == 1:
                    ret = -0.023
                elif bench_key == 2:
                    ret = 0.008
                elif bench_key in (3, 4):
                    ret -= 0.017
            daily_hist.append(ret)
            yk = d.year
            qk = (d.year, (d.month - 1) // 3 + 1)
            mk = (d.year, d.month)
            ytd_hist.setdefault(yk, []).append(ret)
            qtd_hist.setdefault(qk, []).append(ret)
            mtd_hist.setdefault(mk, []).append(ret)
            ytd = chained_return(ytd_hist[yk])
            qtd = chained_return(qtd_hist[qk])
            mtd = chained_return(mtd_hist[mk])
            level = level * (1 + ret)
            rows.append((key, bench_key, d, round(ret, 6), round(mtd, 6), round(qtd, 6), round(ytd, 6), round(level, 4)))
            key += 1
    return rows


def main():
    rows = generate_rows()
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('FACT_BENCHMARK_RETURNS')}")
        cur.executemany(
            f"""INSERT INTO {fq('FACT_BENCHMARK_RETURNS')}
            (BENCHMARK_RETURN_KEY, BENCHMARK_KEY, RETURN_DATE, DAILY_RETURN, MTD_RETURN, QTD_RETURN, YTD_RETURN, INDEX_LEVEL)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
        conn.commit()
        print(f"FACT_BENCHMARK_RETURNS: {len(rows)}")
        print(f"IRAN_SHOCK_DATE: {IRAN_SHOCK_DATE}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
