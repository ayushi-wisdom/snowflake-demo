#!/usr/bin/env python3
"""Month-end WEALTH_POC refresh for holdings snapshot and alert recalculation."""

# EXISTING DOMAIN AUDIT:
# Existing financial domain behavior remains untouched; this script adds WEALTH_POC month-end tasks only.
# Iran/oil anomaly context and seed behavior documented in wealth modules.

from __future__ import annotations

import logging
from datetime import date

from generators.wealth_holdings import main as regenerate_holdings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    logging.info("Starting month-end WEALTH_POC refresh at %s", date.today())
    regenerate_holdings()
    logging.info("Month-end WEALTH_POC refresh complete.")


if __name__ == "__main__":
    main()
