# EXISTING DOMAIN AUDIT:
# Existing domain audited from create_schema.py/data_generator.py/daily_financial_update.py.
# Iran/oil event window in existing domain: 2026-02-01..2026-03-10.
# Daily update in existing domain appends/updates without replacing core master rows after initialization.
# Existing domain has no explicit random seed; this script performs deterministic SQL overrides.

from __future__ import annotations

from .wealth_common import connect, fq

IRAN_SHOCK_DATE = "2026-03-03"


def main():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('ANOMALY_ALERTS')}")
        # EGG-1: exactly 23 shock-affected client accounts
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET DAILY_RETURN = BENCHMARK_DAILY_RETURN + UNIFORM(-0.01, 0.01, RANDOM())
            WHERE PERFORMANCE_DATE = '{IRAN_SHOCK_DATE}'
            """
        )
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET DAILY_RETURN = UNIFORM(-0.092, -0.068, RANDOM()),
                EQUITY_PCT = GREATEST(EQUITY_PCT, 0.16)
            WHERE PERFORMANCE_DATE = '{IRAN_SHOCK_DATE}'
              AND ACCOUNT_KEY IN (
                SELECT ACCOUNT_KEY
                FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
                WHERE PERFORMANCE_DATE = '{IRAN_SHOCK_DATE}'
                ORDER BY ACCOUNT_KEY
                LIMIT 23
              )
            """
        )

        # EGG-2: exactly 31 drift violations on latest date
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET IS_DRIFT_BREACH = FALSE,
                MAX_DRIFT = LEAST(MAX_DRIFT, 0.045),
                DAYS_SINCE_REBALANCE = LEAST(DAYS_SINCE_REBALANCE, 120)
            WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
            """
        )
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET MAX_DRIFT = UNIFORM(0.081, 0.125, RANDOM()),
                DAYS_SINCE_REBALANCE = 181 + UNIFORM(0, 160, RANDOM()),
                IS_DRIFT_BREACH = TRUE
            WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
              AND ACCOUNT_KEY IN (
                SELECT ACCOUNT_KEY
                FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
                WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
                ORDER BY ACCOUNT_KEY
                LIMIT 31
              )
            """
        )

        # EGG-3: exactly 47 fee-drag accounts on latest date
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET GROSS_RETURN = BENCHMARK_YTD_RETURN + UNIFORM(-0.002,0.002,RANDOM()),
                NET_RETURN = BENCHMARK_YTD_RETURN + UNIFORM(-0.001,0.002,RANDOM())
            WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
            """
        )
        cur.execute(
            f"""
            UPDATE {fq('FACT_PORTFOLIO_PERFORMANCE')}
            SET GROSS_RETURN = BENCHMARK_YTD_RETURN + UNIFORM(0.003,0.008,RANDOM()),
                NET_RETURN = BENCHMARK_YTD_RETURN - UNIFORM(0.003,0.010,RANDOM())
            WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
              AND ACCOUNT_KEY IN (
                SELECT ACCOUNT_KEY
                FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
                WHERE PERFORMANCE_DATE = (SELECT MAX(PERFORMANCE_DATE) FROM {fq('FACT_PORTFOLIO_PERFORMANCE')})
                ORDER BY ACCOUNT_KEY
                LIMIT 47
              )
            """
        )

        # EGG-4: advisor concentration risk + stale contacts
        cur.execute(f"UPDATE {fq('DIM_ADVISOR')} SET BACKUP_ADVISOR_KEY = NULL WHERE ADVISOR_ID = 'ADV-003'")
        cur.execute(
            f"""
            UPDATE {fq('FACT_CLIENT_ACTIVITY')}
            SET ACTIVITY_DATE = DATEADD(day,-130,CURRENT_DATE())
            WHERE ADVISOR_KEY = 3
              AND CUSTOMER_KEY IN (
                SELECT CUSTOMER_KEY
                FROM {fq('FACT_PORTFOLIO_PERFORMANCE')}
                WHERE ADVISOR_KEY = 3
                GROUP BY CUSTOMER_KEY
                ORDER BY MAX(ENDING_MARKET_VALUE) DESC
                LIMIT 3
              )
            """
        )

        # EGG-5: exactly 18 TLH accounts with total 1.1M-1.3M
        cur.execute(
            f"""
            UPDATE {fq('FACT_HOLDINGS')}
            SET IS_TLH_ELIGIBLE = FALSE,
                TLH_HARVEST_VALUE = NULL
            WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
            """
        )
        cur.execute(
            f"""
            UPDATE {fq('FACT_HOLDINGS')}
            SET IS_TLH_ELIGIBLE = TRUE,
                TLH_HARVEST_VALUE = UNIFORM(45000, 75000, RANDOM())
            WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
              AND ACCOUNT_KEY IN (
                SELECT ACCOUNT_KEY
                FROM (
                  SELECT DISTINCT ACCOUNT_KEY
                  FROM {fq('FACT_HOLDINGS')}
                  WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
                  ORDER BY ACCOUNT_KEY
                  LIMIT 18
                )
              )
              AND IS_LOSS_POSITION = TRUE
            """
        )
        cur.execute(
            f"""SELECT COALESCE(SUM(TLH_HARVEST_VALUE),0)
                FROM {fq('FACT_HOLDINGS')}
                WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
                  AND IS_TLH_ELIGIBLE = TRUE"""
        )
        tlh_impact = float(cur.fetchone()[0])
        cur.execute(
            f"""INSERT INTO {fq('ANOMALY_ALERTS')}
                (ALERT_KEY, ALERT_DATE, ALERT_TYPE, SEVERITY, ALERT_DESCRIPTION, RECOMMENDED_ACTION, IS_RESOLVED, DOLLAR_IMPACT)
                VALUES (%s, CURRENT_DATE(), 'Tax_Loss_Harvest_Opportunity', 'High',
                        '18 accounts have harvestable losses and window closes in under 30 days.',
                        'Prioritize client outreach and trade prep this month.', FALSE, %s)""",
            (1, tlh_impact),
        )

        # EGG-6: exactly 12 ESG violating accounts
        cur.execute(
            f"""
            UPDATE {fq('FACT_HOLDINGS')}
            SET VIOLATES_CLIENT_ESG = FALSE
            WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
            """
        )
        cur.execute(
            f"""
            UPDATE {fq('FACT_HOLDINGS')} h
            SET VIOLATES_CLIENT_ESG = TRUE
            FROM {fq('DIM_INVESTMENT_POLICY')} ip, {fq('DIM_ASSET')} a
            WHERE h.CUSTOMER_KEY = ip.CUSTOMER_KEY
              AND h.ASSET_KEY = a.ASSET_KEY
              AND ip.HAS_ESG_MANDATE = TRUE
              AND a.ESG_CONTROVERSY_FLAG = TRUE
              AND h.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
              AND h.ACCOUNT_KEY IN (
                SELECT ACCOUNT_KEY
                FROM (
                  SELECT DISTINCT h2.ACCOUNT_KEY
                  FROM {fq('FACT_HOLDINGS')} h2
                  JOIN {fq('DIM_INVESTMENT_POLICY')} ip2 ON ip2.CUSTOMER_KEY = h2.CUSTOMER_KEY
                  JOIN {fq('DIM_ASSET')} a2 ON a2.ASSET_KEY = h2.ASSET_KEY
                  WHERE h2.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM {fq('FACT_HOLDINGS')})
                    AND ip2.HAS_ESG_MANDATE = TRUE
                    AND a2.ESG_CONTROVERSY_FLAG = TRUE
                  ORDER BY h2.ACCOUNT_KEY
                  LIMIT 12
                )
              )
            """
        )
        cur.execute(
            f"""INSERT INTO {fq('ANOMALY_ALERTS')}
                (ALERT_KEY, ALERT_DATE, ALERT_TYPE, SEVERITY, ALERT_DESCRIPTION, RECOMMENDED_ACTION, IS_RESOLVED, DOLLAR_IMPACT)
                VALUES (2, CURRENT_DATE(), 'ESG_Policy_Violation', 'Critical',
                        'ESG mandate accounts currently hold controversy-flagged assets.',
                        'Review restrictions and propose compliant substitutes immediately.',
                        FALSE, 0)"""
        )

        conn.commit()
        print("Applied wealth Easter Egg overrides (EGG-1..EGG-6).")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
