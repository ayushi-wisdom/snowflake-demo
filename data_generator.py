"""
Generate realistic synthetic financial services data
"""
import calendar
import random
import uuid
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger(__name__)

# Realistic data pools
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor",
    "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris", "Sanchez",
    "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams"
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
    "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"
]

STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "WA", "CO", "NC", "IN", "OH", "GA", "VA"]

ACCOUNT_TYPES = ["Checking", "Savings", "Investment", "Credit Card"]
CUSTOMER_SEGMENTS = ["Premium", "Standard", "Basic"]
STATUS_ACTIVE = "Active"
STATUS_INACTIVE = "Inactive"
STATUS_CLOSED = "Closed"

TRANSACTION_TYPES = ["Deposit", "Withdrawal", "Transfer", "Payment", "Purchase"]
TRANSACTION_CATEGORIES = [
    "Groceries", "Gas", "Restaurant", "Utilities", "Shopping", "Entertainment",
    "Healthcare", "Transportation", "Education", "Other"
]

MERCHANTS = [
    "Walmart", "Target", "Amazon", "Starbucks", "Shell", "Exxon", "CVS", "Walgreens",
    "Home Depot", "Best Buy", "McDonald's", "Subway", "AT&T", "Verizon", "Netflix"
]

LOAN_TYPES = ["Mortgage", "Auto", "Personal", "Credit Line"]
LOAN_STATUSES = ["Active", "Delinquent", "Paid Off", "Default"]

# Real-world anomalies (recur every year/month)
# Pay-day: last 2 days of month + first 2 days of month (payroll, rent, transfers)
# Tax deadline: US federal Apr 15 — window Apr 10–18 (payments, transfers)
TAX_MONTH = 4
TAX_WINDOW_START_DAY = 10
TAX_WINDOW_END_DAY = 18

# Iran conflict / oil spike (Mar 2–9, 2026): Strait of Hormuz, oil >$100, market sell-off
IRAN_OIL_EVENT_START = date(2026, 3, 2)
IRAN_OIL_EVENT_END = date(2026, 3, 9)
# Energy symbols that rally on oil spike; rest of market down
ENERGY_SYMBOLS = {"XOM", "CVX", "COP", "SLB", "EOG", "PXD", "MPC", "VLO", "PSX", "OXY"}

# Popular stock symbols for demo
STOCK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ",
    "WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "ADBE", "NFLX", "CRM",
    "PYPL", "INTC", "CMCSA", "PEP", "T", "COST", "XOM", "AVGO", "CSCO", "ABT",
    "NKE", "MRK", "TMO", "ACN", "DHR", "VZ", "CVX", "WFC", "LIN", "ABBV",
    "ORCL", "PM", "NEE", "RTX", "TXN", "BMY", "UPS", "QCOM", "SPGI", "HON"
]

def generate_customer_id() -> str:
    return f"CUST_{uuid.uuid4().hex[:8].upper()}"

def generate_account_id() -> str:
    return f"ACC_{uuid.uuid4().hex[:8].upper()}"

def generate_transaction_id() -> str:
    return f"TXN_{uuid.uuid4().hex[:8].upper()}"

def generate_loan_id() -> str:
    return f"LOAN_{uuid.uuid4().hex[:8].upper()}"

def generate_holding_id() -> str:
    return f"HOLD_{uuid.uuid4().hex[:8].upper()}"

def generate_customers(count: int = 1500) -> List[Dict]:
    """Generate customer master data"""
    customers = []
    base_date = date.today() - timedelta(days=365*5)  # Customers created over last 5 years
    
    for i in range(count):
        created_date = base_date + timedelta(days=random.randint(0, 365*5))
        birth_year = random.randint(1950, 2000)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))
        
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}@email.com"
        
        customer = {
            "customer_id": generate_customer_id(),
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "date_of_birth": birth_date,
            "address_city": random.choice(CITIES),
            "address_state": random.choice(STATES),
            "address_zip": f"{random.randint(10000, 99999)}",
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),
            "created_date": created_date,
            "status": STATUS_ACTIVE if random.random() > 0.05 else STATUS_INACTIVE
        }
        customers.append(customer)
    
    return customers

def generate_accounts(customers: List[Dict], accounts_per_customer: Tuple[int, int] = (1, 3)) -> List[Dict]:
    """Generate accounts linked to customers"""
    accounts = []
    base_date = date.today() - timedelta(days=365*3)
    
    for customer in customers:
        if customer["status"] != STATUS_ACTIVE:
            continue
            
        num_accounts = random.randint(accounts_per_customer[0], accounts_per_customer[1])
        
        for i in range(num_accounts):
            opened_date = customer["created_date"] + timedelta(days=random.randint(0, 365))
            if opened_date > date.today():
                opened_date = customer["created_date"]
            
            account_type = random.choice(ACCOUNT_TYPES)
            
            # Initial balance based on account type
            if account_type == "Credit Card":
                balance = random.uniform(-5000, 0)  # Credit card debt
            elif account_type == "Investment":
                balance = random.uniform(10000, 500000)
            elif account_type == "Savings":
                balance = random.uniform(1000, 100000)
            else:  # Checking
                balance = random.uniform(500, 50000)
            
            account = {
                "account_id": generate_account_id(),
                "customer_id": customer["customer_id"],
                "account_number": f"{random.randint(1000000000, 9999999999)}",
                "account_type": account_type,
                "opened_date": opened_date,
                "status": STATUS_ACTIVE if random.random() > 0.1 else STATUS_CLOSED,
                "currency": "USD",
                "current_balance": round(balance, 2),
                "available_balance": round(balance * random.uniform(0.95, 1.0), 2),
                "balance_last_updated_date": date.today() - timedelta(days=random.randint(0, 7))
            }
            accounts.append(account)
    
    return accounts


def _is_payday(target_date: date) -> bool:
    """True if date is in pay-day window: last 2 days of month or first 2 days of month."""
    day = target_date.day
    if day <= 2:
        return True
    _, last_day = calendar.monthrange(target_date.year, target_date.month)
    return day >= last_day - 1


def _is_tax_deadline_window(target_date: date) -> bool:
    """True if date is in US federal tax deadline window (Apr 10–18)."""
    return (
        target_date.month == TAX_MONTH
        and TAX_WINDOW_START_DAY <= target_date.day <= TAX_WINDOW_END_DAY
    )


def _is_iran_oil_event_window(target_date: date) -> bool:
    """True if date is in Iran conflict / oil spike event window (Mar 2–9, 2026)."""
    return IRAN_OIL_EVENT_START <= target_date <= IRAN_OIL_EVENT_END


def _anomaly_volume_multiplier(target_date: date) -> float:
    """Volume multiplier for pay-day, tax-deadline, and Iran/oil anomalies."""
    pay = _is_payday(target_date)
    tax = _is_tax_deadline_window(target_date)
    iran = _is_iran_oil_event_window(target_date)
    if iran:
        return 0.85  # ~15% lower volume: uncertainty, belt-tightening
    if pay and tax:
        return 1.4  # e.g. Apr 1–2 or Apr 10–18 overlapping pay-day
    if pay:
        return 1.25  # ~25% more around month-end/start
    if tax:
        return 1.35  # ~35% more around tax deadline (Apr 15)
    return 1.0


def _anomaly_category_weights(target_date: date) -> Optional[List[str]]:
    """Heavier weight toward Deposit/Transfer (pay-day), Payment/Transfer (tax), or Gas/Withdrawal (Iran/oil). Returns None for normal mix."""
    if _is_iran_oil_event_window(target_date):
        # Higher fuel costs, more withdrawals and payments; stress spending mix
        return ["Gas", "Gas", "Withdrawal", "Payment", "Payment"] + TRANSACTION_CATEGORIES
    if _is_tax_deadline_window(target_date):
        return ["Payment", "Payment", "Transfer", "Transfer"] + TRANSACTION_CATEGORIES
    if _is_payday(target_date):
        return ["Deposit", "Deposit", "Transfer", "Transfer", "Payment"] + TRANSACTION_CATEGORIES
    return None


def generate_transactions(accounts: List[Dict], transactions_per_day: int = 15000,
                         target_date: date = None) -> List[Dict]:
    """Generate daily transactions. Applies pay-day and tax-deadline anomalies when applicable."""
    if target_date is None:
        target_date = date.today()

    volume_mult = _anomaly_volume_multiplier(target_date)
    effective_count = max(1, int(transactions_per_day * volume_mult))
    category_weights = _anomaly_category_weights(target_date)

    transactions = []
    active_accounts = [a for a in accounts if a["status"] == STATUS_ACTIVE]

    if not active_accounts:
        return transactions

    for _ in range(effective_count):
        account = random.choice(active_accounts)
        account_type = account["account_type"]

        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        transaction_time = datetime.combine(target_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute)

        if account_type == "Credit Card":
            transaction_type = random.choice(["Purchase", "Payment"])
            if transaction_type == "Purchase":
                amount = -random.uniform(10, 500)
                merchant = random.choice(MERCHANTS)
                category = random.choice(TRANSACTION_CATEGORIES)
            else:
                amount = random.uniform(100, 2000)
                merchant = None
                category = "Payment"
        else:
            if category_weights:
                category = random.choice(category_weights)
                if category == "Deposit":
                    transaction_type = "Deposit"
                    amount = random.uniform(200, 12000)
                    merchant = None
                elif category == "Payment":
                    transaction_type = "Payment"
                    amount = -random.uniform(50, 3000)
                    merchant = None
                elif category == "Transfer":
                    transaction_type = "Transfer"
                    amount = random.choice([random.uniform(200, 6000), -random.uniform(200, 6000)])
                    merchant = None
                else:
                    transaction_type = "Purchase"
                    amount = -random.uniform(10, 500)
                    merchant = random.choice(MERCHANTS)
            else:
                transaction_type = random.choice(TRANSACTION_TYPES)
                if transaction_type == "Deposit":
                    amount = random.uniform(100, 10000)
                    merchant = None
                    category = "Deposit"
                elif transaction_type == "Withdrawal":
                    amount = -random.uniform(50, 5000)
                    merchant = None
                    category = "Withdrawal"
                elif transaction_type == "Transfer":
                    amount = random.choice([random.uniform(100, 5000), -random.uniform(100, 5000)])
                    merchant = None
                    category = "Transfer"
                elif transaction_type == "Purchase":
                    amount = -random.uniform(10, 500)
                    merchant = random.choice(MERCHANTS)
                    category = random.choice(TRANSACTION_CATEGORIES)
                else:
                    amount = -random.uniform(50, 1000)
                    merchant = None
                    category = "Payment"

        transaction = {
            "transaction_id": generate_transaction_id(),
            "account_id": account["account_id"],
            "transaction_date": target_date,
            "transaction_time": transaction_time,
            "amount": round(amount, 2),
            "transaction_type": transaction_type,
            "merchant_name": merchant,
            "category": category,
            "description": f"{transaction_type} - {category}" + (f" at {merchant}" if merchant else ""),
            "status": "Completed" if random.random() > 0.02 else random.choice(["Pending", "Failed"]),
            "reference_number": f"REF{random.randint(100000, 999999)}"
        }
        transactions.append(transaction)

    return transactions

def fetch_real_market_prices(symbols: List[str] = None, web_search_func=None, real_prices_dict: Dict[str, float] = None) -> Dict[str, float]:
    """
    Get market prices - accepts real prices dictionary or uses synthetic.
    
    Args:
        symbols: List of stock symbols (defaults to STOCK_SYMBOLS[:150])
        web_search_func: Optional web search function (not used if real_prices_dict provided)
        real_prices_dict: Optional dictionary of {symbol: price} with real market prices
    
    Returns:
        Dictionary mapping symbol to current price
    """
    if symbols is None:
        symbols = STOCK_SYMBOLS[:150]  # Use 150 symbols
    
    prices = {}
    
    # If real prices provided, use them
    if real_prices_dict:
        logger.info(f"Using provided real market prices for {len(real_prices_dict)} symbols")
        for symbol in symbols:
            if symbol in real_prices_dict:
                prices[symbol] = real_prices_dict[symbol]
            else:
                # Fallback to synthetic if symbol not in provided prices
                prices[symbol] = round(random.uniform(50, 500), 2)
                logger.warning(f"Symbol {symbol} not in provided prices, using synthetic")
        return prices
    
    # Otherwise use synthetic prices
    logger.info(f"Using synthetic market prices for {len(symbols)} symbols")
    for symbol in symbols:
        prices[symbol] = round(random.uniform(50, 500), 2)
    
    return prices


def apply_iran_oil_price_shock(prices: Dict[str, float], as_of_date: date) -> Dict[str, float]:
    """
    Apply Iran conflict / oil spike shock to prices when as_of_date is in event window.
    Energy symbols rally (~+15%), rest of market down (~-5%). Returns new dict; does not mutate input.
    """
    if not _is_iran_oil_event_window(as_of_date):
        return dict(prices)
    out = {}
    for symbol, p in prices.items():
        if symbol in ENERGY_SYMBOLS:
            out[symbol] = round(p * 1.15, 2)
        else:
            out[symbol] = round(p * 0.95, 2)
    logger.info(f"Applied Iran/oil price shock for {as_of_date}: energy +15%, broad market -5%")
    return out


def generate_portfolio_holdings(accounts: List[Dict], price_lookup: Dict[str, float]) -> List[Dict]:
    """Generate portfolio holdings for investment accounts using real market prices"""
    holdings = []
    investment_accounts = [a for a in accounts if a["account_type"] == "Investment" and a["status"] == STATUS_ACTIVE]
    
    for account in investment_accounts:
        # Each investment account has 1-5 holdings
        num_holdings = random.randint(1, 5)
        symbols_used = random.sample(list(price_lookup.keys()), min(num_holdings, len(price_lookup)))
        
        for symbol in symbols_used:
            purchase_date = account["opened_date"] + timedelta(days=random.randint(0, (date.today() - account["opened_date"]).days))
            current_price = price_lookup[symbol]
            purchase_price = current_price * random.uniform(0.7, 1.3)  # Historical purchase price
            quantity = random.uniform(1, 100)
            market_value = quantity * current_price
            unrealized_pnl = (current_price - purchase_price) * quantity
            
            holding = {
                "holding_id": generate_holding_id(),
                "account_id": account["account_id"],
                "security_symbol": symbol,
                "security_name": f"{symbol} Corp",
                "quantity": round(quantity, 2),
                "purchase_date": purchase_date,
                "purchase_price": round(purchase_price, 2),
                "current_price": round(current_price, 2),
                "market_value": round(market_value, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "updated_date": date.today()
            }
            holdings.append(holding)
    
    return holdings

def generate_loans(customers: List[Dict], accounts: List[Dict]) -> List[Dict]:
    """Generate loans for customers"""
    loans = []
    active_customers = [c for c in customers if c["status"] == STATUS_ACTIVE]
    
    # 30-50% of customers have loans
    customers_with_loans = random.sample(active_customers, int(len(active_customers) * random.uniform(0.3, 0.5)))
    
    for customer in customers_with_loans:
        loan_type = random.choice(LOAN_TYPES)
        
        # Loan amounts vary by type
        if loan_type == "Mortgage":
            original_principal = random.uniform(200000, 800000)
            term_months = random.choice([180, 240, 360])  # 15, 20, or 30 years
            interest_rate = random.uniform(3.0, 6.5)
        elif loan_type == "Auto":
            original_principal = random.uniform(15000, 60000)
            term_months = random.choice([36, 48, 60, 72])
            interest_rate = random.uniform(2.5, 8.0)
        elif loan_type == "Personal":
            original_principal = random.uniform(5000, 50000)
            term_months = random.choice([12, 24, 36, 48, 60])
            interest_rate = random.uniform(5.0, 15.0)
        else:  # Credit Line
            original_principal = random.uniform(10000, 100000)
            term_months = 60
            interest_rate = random.uniform(8.0, 20.0)
        
        origination_date = customer["created_date"] + timedelta(days=random.randint(30, 365*2))
        if origination_date > date.today():
            origination_date = customer["created_date"] + timedelta(days=30)
        
        maturity_date = origination_date + timedelta(days=term_months * 30)
        
        # Calculate current balance (some loans paid down)
        months_elapsed = min((date.today() - origination_date).days // 30, term_months)
        if months_elapsed < 0:
            months_elapsed = 0
        
        # Simple amortization: pay down 1/term_months per month
        payment_ratio = months_elapsed / term_months if term_months > 0 else 0
        current_principal_balance = original_principal * (1 - payment_ratio * random.uniform(0.3, 0.8))
        
        # Link to an account if available
        customer_accounts = [a for a in accounts if a["customer_id"] == customer["customer_id"] and a["status"] == STATUS_ACTIVE]
        linked_account = random.choice(customer_accounts)["account_id"] if customer_accounts else None
        
        # Payment status
        days_past_due = 0 if random.random() > 0.05 else random.randint(1, 90)
        status = "Delinquent" if days_past_due > 0 else ("Paid Off" if current_principal_balance < 100 else "Active")
        
        # Next payment
        next_payment_date = origination_date + timedelta(days=(months_elapsed + 1) * 30)
        monthly_payment = (original_principal * (interest_rate / 100 / 12)) / (1 - (1 + interest_rate / 100 / 12) ** -term_months) if term_months > 0 else 0
        next_payment_amount = round(monthly_payment, 2) if status == "Active" else 0
        
        loan = {
            "loan_id": generate_loan_id(),
            "customer_id": customer["customer_id"],
            "account_id": linked_account,
            "loan_type": loan_type,
            "original_principal": round(original_principal, 2),
            "current_principal_balance": round(max(0, current_principal_balance), 2),
            "interest_rate": round(interest_rate, 2),
            "term_months": term_months,
            "origination_date": origination_date,
            "maturity_date": maturity_date,
            "days_past_due": days_past_due,
            "next_payment_date": next_payment_date if status == "Active" else None,
            "next_payment_amount": next_payment_amount,
            "status": status,
            "balance_last_updated_date": date.today() - timedelta(days=random.randint(0, 7))
        }
        loans.append(loan)
    
    return loans
