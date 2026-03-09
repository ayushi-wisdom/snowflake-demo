-- Apply table and column comments to Snowflake (sync from docs/schema.yml).
-- Run in Snowflake; adjust database/schema if different (e.g. USE DATABASE your_db; USE SCHEMA your_schema;).

USE DATABASE SE_DEMOS_NEW;
USE SCHEMA FINANCE_MAIN;

-- customers
COMMENT ON TABLE customers IS 'Customer master data; one row per customer.';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier (e.g. CUST_xxxxxxxx).';
COMMENT ON COLUMN customers.first_name IS 'Customer first name.';
COMMENT ON COLUMN customers.last_name IS 'Customer last name.';
COMMENT ON COLUMN customers.email IS 'Customer email address.';
COMMENT ON COLUMN customers.phone IS 'Phone number.';
COMMENT ON COLUMN customers.date_of_birth IS 'Customer date of birth.';
COMMENT ON COLUMN customers.address_city IS 'City in mailing address.';
COMMENT ON COLUMN customers.address_state IS 'US state code (e.g. NY, CA).';
COMMENT ON COLUMN customers.address_zip IS 'ZIP or postal code.';
COMMENT ON COLUMN customers.customer_segment IS 'Tier; accepted values Premium, Standard, Basic.';
COMMENT ON COLUMN customers.created_date IS 'Date the customer record was created.';
COMMENT ON COLUMN customers.status IS 'Accepted values Active, Inactive.';
COMMENT ON COLUMN customers.updated_at IS 'Last row update timestamp.';

-- accounts
COMMENT ON TABLE accounts IS 'Account master data; one row per account, linked to one customer.';
COMMENT ON COLUMN accounts.account_id IS 'Unique account identifier (e.g. ACC_xxxxxxxx).';
COMMENT ON COLUMN accounts.customer_id IS 'Owner customer; references customers.';
COMMENT ON COLUMN accounts.account_number IS 'Display account number (e.g. 10-digit).';
COMMENT ON COLUMN accounts.account_type IS 'Accepted values Checking, Savings, Investment, Credit Card.';
COMMENT ON COLUMN accounts.opened_date IS 'Account opening date.';
COMMENT ON COLUMN accounts.status IS 'Accepted values Active, Closed.';
COMMENT ON COLUMN accounts.currency IS 'Account currency (e.g. USD).';
COMMENT ON COLUMN accounts.current_balance IS 'Book balance; updated from transactions (see metrics.yml).';
COMMENT ON COLUMN accounts.available_balance IS 'Available-to-use balance; derived from current_balance.';
COMMENT ON COLUMN accounts.balance_last_updated_date IS 'Date balances were last recalculated.';
COMMENT ON COLUMN accounts.updated_at IS 'Last row update timestamp.';

-- transactions
COMMENT ON TABLE transactions IS 'One row per transaction; daily and historical, rolling 365-day window.';
COMMENT ON COLUMN transactions.transaction_id IS 'Unique transaction identifier (e.g. TXN_xxxxxxxx).';
COMMENT ON COLUMN transactions.account_id IS 'Account on which the transaction posted; references accounts.';
COMMENT ON COLUMN transactions.transaction_date IS 'Posting date of the transaction.';
COMMENT ON COLUMN transactions.transaction_time IS 'Timestamp of the transaction.';
COMMENT ON COLUMN transactions.amount IS 'Signed amount; positive for credits (e.g. Deposit), negative for debits.';
COMMENT ON COLUMN transactions.transaction_type IS 'Accepted values Deposit, Withdrawal, Transfer, Payment, Purchase.';
COMMENT ON COLUMN transactions.merchant_name IS 'Merchant or counterparty name when applicable.';
COMMENT ON COLUMN transactions.category IS 'Spend category (e.g. Groceries, Gas, Utilities, Other).';
COMMENT ON COLUMN transactions.description IS 'Free-text transaction description.';
COMMENT ON COLUMN transactions.status IS 'Accepted values Completed, Pending, Failed (balance logic uses Completed).';
COMMENT ON COLUMN transactions.reference_number IS 'External or internal reference ID.';
COMMENT ON COLUMN transactions.created_at IS 'Row insert timestamp.';

-- portfolio_holdings
COMMENT ON TABLE portfolio_holdings IS 'Investment positions per account; only for account_type = Investment.';
COMMENT ON COLUMN portfolio_holdings.holding_id IS 'Unique holding identifier (e.g. HOLD_xxxxxxxx).';
COMMENT ON COLUMN portfolio_holdings.account_id IS 'Investment account; references accounts.';
COMMENT ON COLUMN portfolio_holdings.security_symbol IS 'Ticker symbol (e.g. AAPL, MSFT).';
COMMENT ON COLUMN portfolio_holdings.security_name IS 'Full security name.';
COMMENT ON COLUMN portfolio_holdings.quantity IS 'Number of shares or units held.';
COMMENT ON COLUMN portfolio_holdings.purchase_date IS 'Original purchase date.';
COMMENT ON COLUMN portfolio_holdings.purchase_price IS 'Price per unit at purchase.';
COMMENT ON COLUMN portfolio_holdings.current_price IS 'Price per unit as of updated_date (from market feed).';
COMMENT ON COLUMN portfolio_holdings.market_value IS 'quantity * current_price (see metrics.yml).';
COMMENT ON COLUMN portfolio_holdings.unrealized_pnl IS 'Unrealized gain/loss vs purchase (see metrics.yml).';
COMMENT ON COLUMN portfolio_holdings.updated_date IS 'Date current_price and derived fields were updated.';
COMMENT ON COLUMN portfolio_holdings.updated_at IS 'Last row update timestamp.';

-- loans
COMMENT ON TABLE loans IS 'Loan contracts; one row per loan, linked to customer and optionally an account.';
COMMENT ON COLUMN loans.loan_id IS 'Unique loan identifier (e.g. LOAN_xxxxxxxx).';
COMMENT ON COLUMN loans.customer_id IS 'Borrower; references customers.';
COMMENT ON COLUMN loans.account_id IS 'Linked account when applicable; references accounts.';
COMMENT ON COLUMN loans.loan_type IS 'Accepted values Mortgage, Auto, Personal, Credit Line.';
COMMENT ON COLUMN loans.original_principal IS 'Original loan amount at origination.';
COMMENT ON COLUMN loans.current_principal_balance IS 'Outstanding principal as of balance_last_updated_date.';
COMMENT ON COLUMN loans.interest_rate IS 'Stated interest rate (e.g. annual).';
COMMENT ON COLUMN loans.term_months IS 'Loan term in months.';
COMMENT ON COLUMN loans.origination_date IS 'Loan origination date.';
COMMENT ON COLUMN loans.maturity_date IS 'Loan maturity date.';
COMMENT ON COLUMN loans.days_past_due IS 'Days past due as of last update.';
COMMENT ON COLUMN loans.next_payment_date IS 'Next scheduled payment date.';
COMMENT ON COLUMN loans.next_payment_amount IS 'Next scheduled payment amount.';
COMMENT ON COLUMN loans.status IS 'Accepted values Active, Delinquent, Paid Off, Default.';
COMMENT ON COLUMN loans.balance_last_updated_date IS 'Date current_principal_balance was last updated.';
COMMENT ON COLUMN loans.updated_at IS 'Last row update timestamp.';
