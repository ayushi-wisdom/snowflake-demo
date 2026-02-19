import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

def get_snowflake_connection():
    """Establish connection to Snowflake using PAT (Personal Access Token) or password"""
    # Get environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    pat_token = os.getenv('SNOWFLAKE_PAT_TOKEN')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    # Validate required parameters
    if not account or not user:
        raise ValueError("Missing required Snowflake connection parameters: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set")
    
    if not pat_token and not password:
        raise ValueError("Missing authentication: Either SNOWFLAKE_PAT_TOKEN or SNOWFLAKE_PASSWORD must be set")
    
    # Build connection parameters
    conn_params = {
        'account': account,
        'user': user,
    }
    
    # Use PAT token if provided, otherwise use password
    # PAT tokens can be used directly as password without OAuth authenticator
    if pat_token:
        conn_params['password'] = pat_token
    else:
        conn_params['password'] = password
    
    # Add optional parameters if provided
    if warehouse:
        conn_params['warehouse'] = warehouse
    if database:
        conn_params['database'] = database
    if schema:
        conn_params['schema'] = schema
    if role:
        conn_params['role'] = role
    
    try:
        conn = snowflake.connector.connect(**conn_params)
        return conn
    except snowflake.connector.errors.HttpError as e:
        if "404" in str(e):
            raise ConnectionError(
                f"Failed to connect to Snowflake account '{account}'. "
                f"Please verify:\n"
                f"1. The account identifier is correct (case-sensitive)\n"
                f"2. The account might need region information (e.g., '{account}.us-east-1')\n"
                f"3. The account exists and is accessible\n"
                f"Original error: {e}"
            ) from e
        raise