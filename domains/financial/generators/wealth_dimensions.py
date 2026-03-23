# EXISTING DOMAIN AUDIT:
# Existing financial columns and logic audited from create_schema.py, data_generator.py, daily_financial_update.py.
# Iran/oil event dates in existing domain: 2026-02-01..2026-03-10.
# Daily job appends transactions and updates balances/holdings/loan rates; preserves master rows after first load.
# Existing domain has no explicit random.seed; this file uses deterministic seed=42 in wealth_common.

from __future__ import annotations

import random
from datetime import date, timedelta

from .wealth_common import connect, fake, fq

TOTAL_CLIENTS = 847


def seed_benchmarks():
    return [
        (1, "BENCH-001", "S&P 500 Total Return Index", "S&P 500", "Equity", "US large cap equity market benchmark", False, None, None, True),
        (2, "BENCH-002", "Bloomberg US Aggregate Bond Index", "BBG Agg", "Fixed Income", "Core bond market benchmark", False, None, None, True),
        (3, "BENCH-003", "MSCI World Index", "MSCI World", "Equity", "Developed global equity benchmark", False, None, None, True),
        (4, "BENCH-004", "MSCI Emerging Markets Index", "MSCI EM", "Equity", "Emerging market equity benchmark", False, None, None, True),
        (5, "BENCH-005", "60/40 Blended (S&P/BBG Agg)", "60/40 Blend", "Blended", "60% equity and 40% bonds blended index", True, 0.6000, 0.4000, True),
        (6, "BENCH-006", "ICE BofA US Treasury Index", "US Treasury", "Fixed Income", "US Treasury exposure benchmark", False, None, None, True),
    ]


def build_assets():
    assets = []
    k = 1
    sectors = ["Technology", "Healthcare", "Financials", "Consumer Discretionary", "Communication Services", "Industrials", "Energy"]

    for i in range(20):
        sector = sectors[min(i // 3, len(sectors) - 1)]
        controversy = sector == "Energy" and i >= 18
        assets.append((k, f"ASSET-{k:04d}", f"EQ{k:02d}", f"{fake.company()} Holdings", "Equity", "Large Cap", sector, "US", "USD", False, None, None, "S&P 500", random.choice(["AAA", "AA", "A", "BBB"]), controversy, "Fossil fuel extraction and production" if controversy else None, True if i == 4 else False, random.randint(1_000_000, 30_000_000), date(2000, 1, 1) + timedelta(days=i * 90), True))
        k += 1

    for i in range(15):
        esg_etf = i in (10, 11)
        assets.append((k, f"ASSET-{k:04d}", f"ETF{i:02d}", f"{'ESG ' if esg_etf else ''}{fake.word().title()} ETF", "Equity", random.choice(["Broad Market", "International", "Sector", "Factor"]), random.choice(sectors[:-1]), random.choice(["US", "Global", "International Developed", "Emerging Markets"]), "USD", True, "ETF", round(random.uniform(0.0003, 0.0075), 4), random.choice(["S&P 500 Index", "MSCI World Index", "Russell 1000 Index"]), "AAA" if esg_etf else random.choice(["AAA", "AA", "A", "BBB"]), False, None, False, random.randint(300_000, 15_000_000), date(2005, 1, 1) + timedelta(days=i * 120), True))
        k += 1

    for i in range(20):
        is_fund = i < 8
        assets.append((k, f"ASSET-{k:04d}", f"FI{i:02d}" if is_fund else None, f"{'Core Bond ETF' if is_fund else 'Corporate Bond'} {i+1}", "Fixed Income", random.choice(["Aggregate", "Treasury", "Investment Grade Corp", "Municipal", "TIPS"]), "Fixed Income", "US", "USD", is_fund, "ETF" if is_fund else None, round(random.uniform(0.0005, 0.0045), 4) if is_fund else None, "Bloomberg US Aggregate Bond Index", random.choice(["AAA", "AA", "A", "BBB"]), False, None, False, random.randint(30_000, 4_000_000), date(2002, 1, 1) + timedelta(days=i * 180), True))
        k += 1

    for i in range(10):
        controversy = i == 6
        assets.append((k, f"ASSET-{k:04d}", f"ALT{i:02d}", f"{random.choice(['Real Asset','Commodity','Alternative'])} Strategy {i+1}", "Alternative", random.choice(["REIT", "Commodity", "Multi-Strategy", "Gold"]), random.choice(["Real Estate", "Commodities", "Multi-Asset", "Energy"]), "Global", "USD", True, "ETF", round(random.uniform(0.0015, 0.0095), 4), "MSCI World Index", random.choice(["AA", "A", "BBB", "BB"]), controversy, "Fossil fuel extraction and production" if controversy else None, False, random.randint(20_000, 2_000_000), date(2008, 1, 1) + timedelta(days=i * 250), True))
        k += 1

    for i in range(5):
        assets.append((k, f"ASSET-{k:04d}", f"CASH{i:02d}", f"Cash Equivalent {i+1}", "Cash", "Money Market", "Cash", "US", "USD", True, "Mutual Fund", round(random.uniform(0.0001, 0.0015), 4), "US Treasury", "AAA", False, None, False, random.randint(10_000, 1_000_000), date(2015, 1, 1) + timedelta(days=i * 60), True))
        k += 1

    # Additional 15 assets to satisfy exact 85 target
    for i in range(15):
        assets.append(
            (
                k, f"ASSET-{k:04d}", f"MX{i:02d}", f"{fake.company()} Strategic Fund {i+1}",
                random.choice(["Equity", "Fixed Income", "Alternative"]),
                random.choice(["International Developed", "Emerging Markets", "Thematic", "Credit"]),
                random.choice(["Technology", "Healthcare", "Financials", "Energy", "Utilities"]),
                random.choice(["US", "Global", "International Developed", "Emerging Markets"]),
                "USD", True, random.choice(["ETF", "Mutual Fund"]),
                round(random.uniform(0.0004, 0.0099), 4),
                random.choice(["S&P 500 Index", "MSCI World Index", "Bloomberg US Aggregate Bond Index"]),
                random.choice(["AAA", "AA", "A", "BBB", "BB"]),
                False, None, False, random.randint(50_000, 5_000_000),
                date(2010, 1, 1) + timedelta(days=i * 120), True
            )
        )
        k += 1

    return assets  # 85 rows


def build_advisors():
    rows = []
    total_aum = 2_400_000_000.0
    adv3 = total_aum * 0.34
    remainder = total_aum - adv3
    per = remainder / 11
    for k in range(1, 13):
        aum = adv3 if k == 3 else per * random.uniform(0.88, 1.14)
        rows.append((k, f"ADV-{k:03d}", fake.name(), random.choice(["Senior Wealth Advisor", "Wealth Advisor", "Associate Advisor"]), random.choice(["Blue Team", "Gold Team", "Private Client Pod"]), round(aum, 2), 70 if k == 3 else random.randint(55, 78), random.randint(2, 24), random.choice(["CFP", "CFA", "CFP,CFA", "CIMA", "CFP,CIMA"]), True, None if k == 3 else random.choice([x for x in range(1, 13) if x != k])))
    return rows


def build_policies():
    risk_profiles = (["Conservative"] * 186) + (["Moderate"] * 263) + (["Moderate-Growth"] * 246) + (["Aggressive"] * 152)
    random.shuffle(risk_profiles)
    esg_clients = set(random.sample(range(1, TOTAL_CLIENTS + 1), 67))
    strict = set(list(esg_clients)[:12])
    interm = set(list(esg_clients)[12:37])
    basic = esg_clients - strict - interm
    drift5 = set(random.sample(range(1, TOTAL_CLIENTS + 1), 340))
    advisors = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rows = []
    for i in range(1, TOTAL_CLIENTS + 1):
        rp = risk_profiles[i - 1]
        if rp == "Conservative":
            t = (0.35, 0.55, 0.05, 0.05); b = 6
        elif rp == "Moderate":
            t = (0.55, 0.35, 0.06, 0.04); b = 5
        elif rp == "Moderate-Growth":
            t = (0.68, 0.22, 0.06, 0.04); b = 1
        else:
            t = (0.80, 0.10, 0.06, 0.04); b = random.choice([1, 3, 4])
        has_esg = i in esg_clients
        screen = "Strict" if i in strict else ("Intermediate" if i in interm else ("Basic" if i in basic else None))
        rows.append(
            (
                i, f"IPS-{i:04d}", i, rp, t[0], t[1], t[2], t[3], 0.10,
                0.05 if i in drift5 else 0.08,
                random.choice(["Quarterly", "Semi-Annual", "Annual", "Drift-Based"]),
                b, has_esg, screen,
                "Weapons,Fossil Fuels,Tobacco" if has_esg else None,
                random.random() < 0.12, "RST1,RST2" if random.random() < 0.12 else None,
                random.random() < 0.62, 5, date(2022, 1, 3), date.today() - timedelta(days=random.randint(5, 180)),
                date.today() + timedelta(days=random.randint(20, 180)), random.choices(advisors, weights=[6, 6, 12, 7, 7, 7, 7, 7, 7, 7, 7, 7], k=1)[0], True
            )
        )
    return rows


def main():
    assets = build_assets()
    benchmarks = seed_benchmarks()
    advisors = build_advisors()
    policies = build_policies()

    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('DIM_ASSET')}")
        cur.execute(f"TRUNCATE TABLE {fq('DIM_BENCHMARK')}")
        cur.execute(f"TRUNCATE TABLE {fq('DIM_ADVISOR')}")
        cur.execute(f"TRUNCATE TABLE {fq('DIM_INVESTMENT_POLICY')}")
        cur.executemany(f"""INSERT INTO {fq('DIM_ASSET')}
            (ASSET_KEY,ASSET_ID,TICKER,ASSET_NAME,ASSET_CLASS,SUB_ASSET_CLASS,SECTOR,GEOGRAPHY,CURRENCY,IS_FUND,FUND_TYPE,EXPENSE_RATIO,BENCHMARK_INDEX,ESG_RATING,ESG_CONTROVERSY_FLAG,ESG_CONTROVERSY_REASON,IS_RESTRICTED,AVERAGE_DAILY_VOLUME,INCEPTION_DATE,IS_ACTIVE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", assets)
        cur.executemany(f"""INSERT INTO {fq('DIM_BENCHMARK')}
            (BENCHMARK_KEY,BENCHMARK_ID,BENCHMARK_NAME,BENCHMARK_SHORT_NAME,ASSET_CLASS,DESCRIPTION,IS_BLENDED,BLEND_EQUITY_PCT,BLEND_FIXED_PCT,IS_ACTIVE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", benchmarks)
        cur.executemany(f"""INSERT INTO {fq('DIM_ADVISOR')}
            (ADVISOR_KEY,ADVISOR_ID,ADVISOR_NAME,TITLE,TEAM,AUM_MANAGED,CLIENT_COUNT,YEARS_AT_FIRM,CERTIFICATION,IS_ACTIVE,BACKUP_ADVISOR_KEY)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", advisors)
        cur.executemany(f"""INSERT INTO {fq('DIM_INVESTMENT_POLICY')}
            (POLICY_KEY,POLICY_ID,CUSTOMER_KEY,RISK_PROFILE,TARGET_EQUITY_PCT,TARGET_FIXED_INCOME_PCT,TARGET_ALTERNATIVE_PCT,TARGET_CASH_PCT,MAX_SINGLE_POSITION_PCT,REBALANCING_DRIFT_THRESHOLD,REBALANCING_FREQUENCY,BENCHMARK_KEY,HAS_ESG_MANDATE,ESG_SCREEN_LEVEL,ESG_EXCLUSION_CATEGORIES,HAS_RESTRICTED_SECURITIES,RESTRICTED_TICKERS,TAX_SENSITIVE,MIN_LIQUIDITY_DAYS,INCEPTION_DATE,LAST_REVIEWED_DATE,NEXT_REVIEW_DATE,ADVISOR_KEY,IS_ACTIVE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", policies)
        conn.commit()
        print(f"DIM_ASSET: {len(assets)}")
        print(f"DIM_BENCHMARK: {len(benchmarks)}")
        print(f"DIM_ADVISOR: {len(advisors)}")
        print(f"DIM_INVESTMENT_POLICY: {len(policies)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
