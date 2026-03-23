# EXISTING DOMAIN AUDIT:
# customers/accounts/transactions/portfolio_holdings/loans are implemented in create_schema.py.
# Iran/oil shock: 2026-02-01..2026-03-10; affects transaction behavior, price shock and loan delinquency logic.
# Daily update appends transactions and updates balances/holdings/loan rates; does not rewrite master customers.
# No explicit random seeds were found in existing financial generators.

from __future__ import annotations

import os
import random
from datetime import date, timedelta
from typing import Iterable

import holidays
import numpy as np
from faker import Faker

from snowflake_connection import get_snowflake_connection

random.seed(42)
np.random.seed(42)
fake = Faker()
Faker.seed(42)

DB = os.getenv("SNOWFLAKE_DATABASE", "SE_DEMOS_NEW")
SCHEMA = "WEALTH_POC"


def fq(name: str) -> str:
    return f"{DB}.{SCHEMA}.{name}"


def connect():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DB}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")
        cur.execute(f"USE SCHEMA {DB}.{SCHEMA}")
    finally:
        cur.close()
    return conn


def get_trading_dates(start_date: date, end_date: date) -> list[date]:
    us_holidays = holidays.US(years=range(start_date.year, end_date.year + 1))
    dates = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5 and current not in us_holidays:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def month_end_trading_dates(trading_dates: Iterable[date]) -> list[date]:
    out = []
    prev = None
    for d in trading_dates:
        if prev and d.month != prev.month:
            out.append(prev)
        prev = d
    if prev:
        out.append(prev)
    return out
