# Healthcare POC Build

This folder builds the synthetic healthcare dataset in Snowflake schema `HEALTHCARE_POC`.

## Run order

1. Install deps:
   - `pip install -r requirements.txt`
2. Create schema and tables:
   - `python healthcare_poc/create_healthcare_schema.py`
3. Generate and load data:
   - `python healthcare_poc/build_healthcare_data.py`

Both scripts read Snowflake credentials from `.env`.
