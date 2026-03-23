import os

import snowflake.connector
from dotenv import load_dotenv

# Load domain .env (SE_DEMOS_NEW / FINANCE_MAIN, etc.)
_base = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_base, ".env"), override=True)


def get_snowflake_connection(schema_override: str | None = None):
    """Establish connection to Snowflake using PAT or password."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    pat_token = os.getenv("SNOWFLAKE_PAT_TOKEN")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = schema_override or os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not account or not user:
        raise ValueError(
            "Missing required Snowflake connection parameters: "
            "SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set"
        )
    if not pat_token and not password:
        raise ValueError(
            "Missing authentication: Either SNOWFLAKE_PAT_TOKEN or SNOWFLAKE_PASSWORD must be set"
        )

    conn_params = {"account": account, "user": user}
    if pat_token:
        conn_params["password"] = pat_token
    else:
        conn_params["password"] = password

    if warehouse:
        conn_params["warehouse"] = warehouse
    if database:
        conn_params["database"] = database
    if schema:
        conn_params["schema"] = schema
    if role:
        conn_params["role"] = role

    try:
        return snowflake.connector.connect(**conn_params)
    except snowflake.connector.errors.HttpError as e:
        if "404" in str(e):
            raise ConnectionError(
                f"Failed to connect to Snowflake account '{account}'. "
                "Please verify:\n"
                "1. The account identifier is correct (case-sensitive)\n"
                f"2. The account might need region information (e.g., '{account}.us-east-1')\n"
                "3. The account exists and is accessible\n"
                f"Original error: {e}"
            ) from e
        raise
