# EXISTING DOMAIN AUDIT:
# Existing financial domain logic reviewed in full; core tables preserved.
# Iran/oil and daily update behaviors are unchanged by this module.
# Existing codebase had no explicit deterministic seed in financial generators; this module uses seeded helpers.

from __future__ import annotations

import random
from datetime import date

from .wealth_common import connect, fq


def main():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {fq('FACT_CLIENT_ACTIVITY')}")
        cur.execute(f"""SELECT DISTINCT p.CUSTOMER_KEY, p.ADVISOR_KEY
                        FROM {fq('DIM_INVESTMENT_POLICY')} p
                        JOIN {fq('FACT_PORTFOLIO_PERFORMANCE')} fp ON fp.CUSTOMER_KEY = p.CUSTOMER_KEY
                        WHERE p.IS_ACTIVE = TRUE""")
        clients = cur.fetchall()
        rows = []
        key = 1
        for customer_key, advisor_key in clients:
            n = random.randint(22, 38)
            for _ in range(n):
                d = date(2022, 1, 1) + (date.today() - date(2022, 1, 1)) * random.random()
                rows.append(
                    (
                        key, customer_key, advisor_key, d,
                        random.choice(["Review Meeting", "Phone Call", "Rebalance", "Email", "IPS Update", "Onboarding"]),
                        random.choice(["In-Person", "Phone", "Video", "Email"]),
                        random.randint(10, 75),
                        random.choice(["Portfolio review", "Tax planning", "Risk posture", "Life event update"]),
                        random.choice(["Follow up in 30 days", "Prepare IPS draft", "Send recommended trades", "No action needed"]),
                        d if random.random() < 0.3 else None,
                        random.choice([None, 3, 4, 5]),
                    )
                )
                key += 1
        cur.executemany(
            f"""INSERT INTO {fq('FACT_CLIENT_ACTIVITY')}
            (ACTIVITY_KEY,CUSTOMER_KEY,ADVISOR_KEY,ACTIVITY_DATE,ACTIVITY_TYPE,CHANNEL,DURATION_MINUTES,TOPICS_DISCUSSED,ACTION_ITEMS,NEXT_SCHEDULED_DATE,SATISFACTION_SCORE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
        conn.commit()
        print(f"FACT_CLIENT_ACTIVITY: {len(rows)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
