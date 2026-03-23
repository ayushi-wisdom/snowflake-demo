# Project-wide instructions

- **.env is correct.** Use it as-is (SE_DEMOS_NEW, FINANCE_MAIN). Do not override or second-guess.
- **Snowflake:** SE_DEMOS_NEW exists with schema FINANCE_MAIN and 5 tables. Do not recreate schema or tables.
- **Data change workflow:** When asked to change data:
  1. Propose a plan (code + data impact).
  2. After the user approves, make the code changes and run the update to Snowflake (e.g. `daily_financial_update.py` or backfill scripts as needed).
- **Running:** You may run any commands needed to apply updates to Snowflake.
