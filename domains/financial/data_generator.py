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

# --- Real-world data checks (oil/energy, stock prices) ---
def get_oil_elevated_from_real_data(
    baseline_days: int = 30,
    elevation_pct: float = 5.0,
    ticker: str = "XLE",
) -> Optional[bool]:
    """
    Fetch real oil/energy (XLE) and compare to baseline. Returns True if elevated, False if not, None if fetch failed.
    Used to drive oil-shock logic from real data instead of a fixed date window.
    """
    try:
        import yfinance as yf
        end = date.today()
        start = end - timedelta(days=baseline_days + 5)
        hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True, threads=False)
        if hist is None or hist.empty or "Close" not in hist.columns:
            logger.warning("Oil/energy fetch: no data for %s", ticker)
            return None
        closes = hist["Close"]
        n = len(closes)
        if n < baseline_days + 1:
            return None
        current = float(closes.iloc[-1])
        baseline = float(closes.iloc[-baseline_days - 1 : -1].mean())
        if baseline <= 0:
            return None
        pct_above = (current - baseline) / baseline * 100
        elevated = pct_above >= elevation_pct
        logger.info("Oil/energy (%s): current=%.2f, baseline(%~dd)=%.2f, %.1f%% above → elevated=%s",
                    ticker, current, baseline_days, baseline, pct_above, elevated)
        return elevated
    except Exception as e:
        logger.warning("Oil/energy fetch failed: %s", e)
        return None


def fetch_real_stock_prices_yfinance(symbols: List[str] = None) -> Optional[Dict[str, float]]:
    """
    Fetch latest close prices for symbols via yfinance. Returns dict symbol -> price, or None on failure.
    """
    if symbols is None:
        symbols = STOCK_SYMBOLS[:80]
    try:
        import yfinance as yf
        out = {}
        for sym in symbols:
            try:
                t = yf.Ticker(sym)
                hist = t.history(period="5d", auto_adjust=True)
                if hist is not None and not hist.empty and "Close" in hist.columns:
                    out[sym] = round(float(hist["Close"].iloc[-1]), 2)
            except Exception:
                continue
        if not out:
            return None
        logger.info("Fetched real prices for %s symbols", len(out))
        return out
    except Exception as e:
        logger.warning("Real stock fetch failed: %s", e)
        return None

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
# Purchase-only category weights during oil shock (Gas/Groceries/Utilities up; no Shopping/Entertainment)
# Used for Credit Card Purchases so "transaction volume by category" shows Gas with smallest dip
PURCHASE_CATEGORIES_OIL = [
    "Gas", "Gas", "Gas", "Groceries", "Groceries", "Utilities", "Utilities",
    "Restaurant", "Healthcare", "Transportation", "Education", "Other"
]
# During oil shock: weight toward gas, grocery, utilities (essentials)
MERCHANTS_ESSENTIALS_OIL = [
    "Shell", "Exxon", "Walmart", "CVS", "Walgreens", "AT&T", "Verizon",
    "Shell", "Exxon", "Walmart", "CVS",
]

LOAN_TYPES = ["Mortgage", "Auto", "Personal", "Credit Line"]
LOAN_STATUSES = ["Active", "Delinquent", "Paid Off", "Default"]
# Industry sectors for root-cause demo: transport/airline hit first by oil
LOAN_INDUSTRY_SECTORS = ["Transport", "Airline", "Manufacturing", "Other"]
# Delinquency: baseline ~5%; in oil window Transport ~16%, Airline ~12%, rest ~5%
DELINQUENCY_RATE_BASELINE = 0.05
DELINQUENCY_RATE_TRANSPORT_OIL = 0.16
DELINQUENCY_RATE_AIRLINE_OIL = 0.12
# Variable-rate loans: rate resets creep up since this date (e.g. Fed/benchmark)
VARIABLE_RATE_RESET_START = date(2026, 2, 1)
VARIABLE_RATE_CREEP_PER_DAY = 0.02  # 0.02% per day added to variable rate since reset start
VARIABLE_RATE_FRACTION = 0.30  # ~30% of loans are variable-rate

# Real-world anomalies (recur every year/month)
# Pay-day: last 2 days of month + first 2 days of month (payroll, rent, transfers)
# Tax deadline: US federal Apr 15 — window Apr 10–18 (payments, transfers)
TAX_MONTH = 4
TAX_WINDOW_START_DAY = 10
TAX_WINDOW_END_DAY = 18

# Oil/energy shock: "since Feb 1" through current day (demo data ends Mar 10, 2026). Date window used when real-data fetch fails.
IRAN_OIL_EVENT_START = date(2026, 2, 1)
IRAN_OIL_EVENT_END = date(2026, 3, 10)
# Spending mix during oil: full effect from Feb 15+; ramp Feb 1-14 (milder)
ESSENTIALS_SPEND_MULTIPLIER = 1.35
NON_ESSENTIALS_SPEND_MULTIPLIER = 0.65
ESSENTIALS_SPEND_MULTIPLIER_RAMP = 1.15   # Feb 1-14
NON_ESSENTIALS_SPEND_MULTIPLIER_RAMP = 0.85
OIL_RAMP_END = date(2026, 2, 14)  # After this date, full oil multipliers
ESSENTIALS_CATEGORIES = {"Gas", "Groceries", "Utilities"}
# Energy symbols that rally on oil spike; rest of market down
ENERGY_SYMBOLS = {"XOM", "CVX", "COP", "SLB", "EOG", "PXD", "MPC", "VLO", "PSX", "OXY"}

# Predetermined anomalies: next 2–3 months + recurring banking patterns
# Holiday spending (2026 dates; add new year in list when needed)
EASTER_2026_START = date(2026, 4, 3)   # Fri before Easter Sun Apr 5
EASTER_2026_END = date(2026, 4, 6)     # Mon after
MOTHERS_DAY_2026_START = date(2026, 5, 8)
MOTHERS_DAY_2026_END = date(2026, 5, 10)
MEMORIAL_DAY_2026_START = date(2026, 5, 23)   # weekend before Mon May 25
MEMORIAL_DAY_2026_END = date(2026, 5, 26)
FATHERS_DAY_2026_START = date(2026, 6, 19)
FATHERS_DAY_2026_END = date(2026, 6, 21)
# FOMC meeting end dates 2026 (rate decision day / day after = more transfers, rate sensitivity)
FOMC_DAYS_2026 = {
    date(2026, 3, 18), date(2026, 4, 29), date(2026, 6, 17), date(2026, 7, 29),
    date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9),
}

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

def generate_customers(
    count: int = 1500,
    created_date_min: date = None,
    created_date_max: date = None,
    all_active: bool = False,
) -> List[Dict]:
    """Generate customer master data. Optional created_date range; if all_active=True, all get status Active."""
    customers = []
    if created_date_min is not None and created_date_max is not None:
        start = created_date_min
        end = created_date_max
        span_days = (end - start).days
    else:
        base_date = date.today() - timedelta(days=365*5)
        start = base_date
        span_days = 365 * 5
        end = date.today()

    for i in range(count):
        created_date = start + timedelta(days=random.randint(0, max(0, span_days)))
        if created_date > end:
            created_date = end
        birth_year = random.randint(1950, 2000)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))

        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99999)}@email.com" if created_date_min is not None else f"{first_name.lower()}.{last_name.lower()}@email.com"

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
            "status": STATUS_ACTIVE if (all_active or random.random() > 0.05) else STATUS_INACTIVE
        }
        customers.append(customer)

    return customers

def generate_accounts(
    customers: List[Dict],
    accounts_per_customer: Tuple[int, int] = (1, 3),
    signup_bonus_for_dates: Tuple[date, date] = None,
    signup_bonus_amount: float = 0,
) -> List[Dict]:
    """Generate accounts linked to customers. If signup_bonus_for_dates and signup_bonus_amount set,
    add that amount to current_balance for Checking/Savings opened in that date range."""
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

            if account_type == "Credit Card":
                balance = random.uniform(-5000, 0)
            elif account_type == "Investment":
                balance = random.uniform(10000, 500000)
            elif account_type == "Savings":
                balance = random.uniform(1000, 100000)
            else:
                balance = random.uniform(500, 50000)

            if signup_bonus_for_dates and signup_bonus_amount and account_type in ("Checking", "Savings"):
                sstart, send = signup_bonus_for_dates
                if sstart <= opened_date <= send:
                    balance += signup_bonus_amount

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
    """True if date is in oil-up / energy shock window (sustained period, no one-day spike)."""
    return IRAN_OIL_EVENT_START <= target_date <= IRAN_OIL_EVENT_END


def _is_holiday_spending_window(target_date: date) -> str:
    """Returns which holiday spending window the date is in, or None. Used for volume + category mix."""
    if EASTER_2026_START <= target_date <= EASTER_2026_END:
        return "easter"
    if MOTHERS_DAY_2026_START <= target_date <= MOTHERS_DAY_2026_END:
        return "mothers_day"
    if MEMORIAL_DAY_2026_START <= target_date <= MEMORIAL_DAY_2026_END:
        return "memorial_day"
    if FATHERS_DAY_2026_START <= target_date <= FATHERS_DAY_2026_END:
        return "fathers_day"
    return None


def _is_quarter_end(target_date: date) -> bool:
    """Last day of quarter: common banking transfers, balance management."""
    if target_date.month not in (3, 6, 9, 12):
        return False
    _, last_day = calendar.monthrange(target_date.year, target_date.month)
    return target_date.day == last_day


def _is_fomc_day(target_date: date) -> bool:
    """FOMC meeting end date (rate decision): more transfers/withdrawals, rate sensitivity."""
    return target_date in FOMC_DAYS_2026


def _anomaly_volume_multiplier(target_date: date, oil_elevated_override: Optional[bool] = None) -> float:
    """Volume multiplier for pay-day, tax-deadline, Iran/oil, holidays, quarter-end, FOMC.
    When oil_elevated_override is not None, use it for oil; else use date window (fallback)."""
    pay = _is_payday(target_date)
    tax = _is_tax_deadline_window(target_date)
    iran = oil_elevated_override if oil_elevated_override is not None else _is_iran_oil_event_window(target_date)
    holiday = _is_holiday_spending_window(target_date)
    quarter_end = _is_quarter_end(target_date)
    fomc = _is_fomc_day(target_date)
    if iran:
        return 1.0  # No volume dip; story is spending mix (essentials up, discretionary down)
    if pay and tax:
        return 1.4  # e.g. Apr 1–2 or Apr 10–18 overlapping pay-day
    if pay:
        return 1.25  # ~25% more around month-end/start
    if tax:
        return 1.35  # ~35% more around tax deadline (Apr 15)
    if holiday:
        return 1.2  # holiday spending: Easter, Mother's Day, Memorial Day, Father's Day
    if fomc:
        return 1.1  # Fed rate decision day: more transfers, rate sensitivity
    if quarter_end:
        return 1.1  # quarter-end: more transfers, balance management
    return 1.0


def _anomaly_category_weights(target_date: date, oil_elevated_override: Optional[bool] = None) -> Optional[List[str]]:
    """Heavier weight by anomaly type. When oil_elevated_override is not None, use it for oil; else date window.
    Oil shock: essentials (Gas, Groceries, Utilities) up; discretionary (Shopping, Entertainment) de-emphasized."""
    oil_elevated = oil_elevated_override if oil_elevated_override is not None else _is_iran_oil_event_window(target_date)
    if oil_elevated:
        # Extra Gas weight so "transaction volume by category" shows Gas with smallest dip / flat
        essentials = ["Gas", "Gas", "Gas", "Groceries", "Groceries", "Utilities", "Withdrawal", "Payment", "Payment"]
        rest = [c for c in TRANSACTION_CATEGORIES if c not in ("Shopping", "Entertainment")]
        return essentials + rest
    if _is_tax_deadline_window(target_date):
        return ["Payment", "Payment", "Transfer", "Transfer"] + TRANSACTION_CATEGORIES
    if _is_payday(target_date):
        return ["Deposit", "Deposit", "Transfer", "Transfer", "Payment"] + TRANSACTION_CATEGORIES
    if _is_holiday_spending_window(target_date):
        # Easter, Mother's Day, Memorial Day, Father's Day: gifts, dining, travel, shopping
        return ["Purchase", "Purchase", "Shopping", "Restaurant", "Entertainment"] + TRANSACTION_CATEGORIES
    if _is_fomc_day(target_date):
        return ["Transfer", "Transfer", "Withdrawal", "Payment"] + TRANSACTION_CATEGORIES
    if _is_quarter_end(target_date):
        return ["Transfer", "Transfer"] + TRANSACTION_CATEGORIES
    return None


# ~8% of accounts are "dormant": no transactions in the last 6 months (realistic inactive accounts)
DORMANT_DAYS = 180
DORMANT_FRACTION = 0.08  # hash(account_id) % 100 < 8 -> dormant


def _is_dormant_account(account_id: str) -> bool:
    """Deterministic: same account is always dormant or not (no transactions in last 6 months)."""
    return (sum(ord(c) for c in account_id) % 100) < int(DORMANT_FRACTION * 100)


def generate_transactions(accounts: List[Dict], transactions_per_day: int = 15000,
                         target_date: date = None, oil_elevated_override: Optional[bool] = None,
                         reference_date: Optional[date] = None) -> List[Dict]:
    """Generate daily transactions. Applies pay-day, tax, oil/energy anomalies when applicable.
    oil_elevated_override: when not None (e.g. from real-data check for today), use it instead of date window.
    reference_date: if set, used for dormant cutoff (last 6 months relative to this date); else date.today()."""
    if target_date is None:
        target_date = date.today()
    ref = reference_date if reference_date is not None else date.today()

    volume_mult = _anomaly_volume_multiplier(target_date, oil_elevated_override=oil_elevated_override)
    effective_count = max(1, int(transactions_per_day * volume_mult))
    category_weights = _anomaly_category_weights(target_date, oil_elevated_override=oil_elevated_override)
    oil_elevated = oil_elevated_override if oil_elevated_override is not None else _is_iran_oil_event_window(target_date)
    # Ramp: Feb 1-14 milder spend shift, Feb 15+ full
    oil_ramp = oil_elevated and (target_date <= OIL_RAMP_END)

    transactions = []
    active_accounts = [a for a in accounts if a["status"] == STATUS_ACTIVE]
    # Exclude dormant accounts for dates within last 6 months of reference_date
    cutoff = ref - timedelta(days=DORMANT_DAYS)
    if target_date >= cutoff:
        active_accounts = [a for a in active_accounts if not _is_dormant_account(a["account_id"])]

    if not active_accounts:
        return transactions

    # Oil shock: more transactions on credit cards (people put gas/essentials on cards)
    account_pool = active_accounts
    if oil_elevated:
        cc_accounts = [a for a in active_accounts if a["account_type"] == "Credit Card"]
        if cc_accounts:
            account_pool = active_accounts + cc_accounts

    for _ in range(effective_count):
        account = random.choice(account_pool)
        account_type = account["account_type"]

        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        transaction_time = datetime.combine(target_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute)

        if account_type == "Credit Card":
            transaction_type = random.choice(["Purchase", "Payment"])
            if transaction_type == "Purchase":
                amount = -random.uniform(10, 500)
                merchant = random.choice(MERCHANTS_ESSENTIALS_OIL if oil_elevated else MERCHANTS)
                category = random.choice(PURCHASE_CATEGORIES_OIL if oil_elevated else TRANSACTION_CATEGORIES)
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
                    merchant = random.choice(MERCHANTS_ESSENTIALS_OIL if oil_elevated else MERCHANTS)
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
                    merchant = random.choice(MERCHANTS_ESSENTIALS_OIL if oil_elevated else MERCHANTS)
                    category = random.choice(TRANSACTION_CATEGORIES)
                else:
                    amount = -random.uniform(50, 1000)
                    merchant = None
                    category = "Payment"

        # Oil: spending mix — ramp (Feb 1-14) milder, Feb 15+ full
        if oil_elevated and transaction_type == "Purchase" and category:
            if oil_ramp:
                if category in ESSENTIALS_CATEGORIES:
                    amount *= ESSENTIALS_SPEND_MULTIPLIER_RAMP
                else:
                    amount *= NON_ESSENTIALS_SPEND_MULTIPLIER_RAMP
            else:
                if category in ESSENTIALS_CATEGORIES:
                    amount *= ESSENTIALS_SPEND_MULTIPLIER
                else:
                    amount *= NON_ESSENTIALS_SPEND_MULTIPLIER

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
            "status": "Completed" if random.random() > (0.07 if oil_elevated else 0.02) else random.choice(["Pending", "Failed"]),
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


def apply_iran_oil_price_shock(
    prices: Dict[str, float], as_of_date: date, oil_elevated_override: Optional[bool] = None
) -> Dict[str, float]:
    """
    Apply oil/energy shock to prices when elevated (from real-data check or, if None, date window).
    Energy symbols +15%, rest -5%. Returns new dict; does not mutate input.
    """
    oil_elevated = oil_elevated_override if oil_elevated_override is not None else _is_iran_oil_event_window(as_of_date)
    if not oil_elevated:
        return dict(prices)
    out = {}
    for symbol, p in prices.items():
        if symbol in ENERGY_SYMBOLS:
            out[symbol] = round(p * 1.15, 2)
        else:
            out[symbol] = round(p * 0.95, 2)
    logger.info("Applied oil/energy price shock for %s: energy +15%%, broad market -5%%", as_of_date)
    return out


def generate_portfolio_holdings(accounts: List[Dict], price_lookup: Dict[str, float], as_of_date: Optional[date] = None) -> List[Dict]:
    """Generate portfolio holdings for investment accounts. as_of_date sets updated_date (for demo)."""
    as_of = as_of_date if as_of_date is not None else date.today()
    holdings = []
    investment_accounts = [a for a in accounts if a["account_type"] == "Investment" and a["status"] == STATUS_ACTIVE]
    
    for account in investment_accounts:
        # Each investment account has 1-5 holdings
        num_holdings = random.randint(1, 5)
        symbols_used = random.sample(list(price_lookup.keys()), min(num_holdings, len(price_lookup)))
        open_days = (as_of - account["opened_date"]).days
        for symbol in symbols_used:
            purchase_date = account["opened_date"] + timedelta(days=random.randint(0, max(0, open_days)))
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
                "updated_date": as_of
            }
            holdings.append(holding)
    
    return holdings

def generate_loans(customers: List[Dict], accounts: List[Dict], as_of_date: Optional[date] = None) -> List[Dict]:
    """Generate loans for customers. as_of_date: if in oil window, Transport/Airline sectors get higher delinquency."""
    loans = []
    active_customers = [c for c in customers if c["status"] == STATUS_ACTIVE]
    as_of = as_of_date if as_of_date is not None else date.today()
    in_oil_window = _is_iran_oil_event_window(as_of)
    
    # 30-50% of customers have loans
    customers_with_loans = random.sample(active_customers, int(len(active_customers) * random.uniform(0.3, 0.5)))
    
    for customer in customers_with_loans:
        loan_type = random.choice(LOAN_TYPES)
        # Assign industry sector (deterministic by customer_id): ~22% Transport, ~12% Airline, ~18% Manufacturing, ~48% Other
        r = (sum(ord(c) for c in customer["customer_id"]) % 100) / 100.0
        if r < 0.22:
            industry_sector = "Transport"
        elif r < 0.34:
            industry_sector = "Airline"
        elif r < 0.52:
            industry_sector = "Manufacturing"
        else:
            industry_sector = "Other"
        
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
        if origination_date > as_of:
            origination_date = customer["created_date"] + timedelta(days=30)
        
        maturity_date = origination_date + timedelta(days=term_months * 30)
        
        # Calculate current balance (some loans paid down) as of as_of_date
        months_elapsed = min((as_of - origination_date).days // 30, term_months)
        if months_elapsed < 0:
            months_elapsed = 0
        
        # Simple amortization: pay down 1/term_months per month
        payment_ratio = months_elapsed / term_months if term_months > 0 else 0
        current_principal_balance = original_principal * (1 - payment_ratio * random.uniform(0.3, 0.8))
        
        # Link to an account if available
        customer_accounts = [a for a in accounts if a["customer_id"] == customer["customer_id"] and a["status"] == STATUS_ACTIVE]
        linked_account = random.choice(customer_accounts)["account_id"] if customer_accounts else None
        
        # Payment status: in oil window, Transport/Airline have higher delinquency (root-cause demo)
        if in_oil_window and industry_sector == "Transport":
            delinquent_prob = DELINQUENCY_RATE_TRANSPORT_OIL
        elif in_oil_window and industry_sector == "Airline":
            delinquent_prob = DELINQUENCY_RATE_AIRLINE_OIL
        else:
            delinquent_prob = DELINQUENCY_RATE_BASELINE
        days_past_due = 0 if random.random() > delinquent_prob else random.randint(1, 90)
        status = "Delinquent" if days_past_due > 0 else ("Paid Off" if current_principal_balance < 100 else "Active")
        
        # Next payment
        next_payment_date = origination_date + timedelta(days=(months_elapsed + 1) * 30)
        monthly_payment = (original_principal * (interest_rate / 100 / 12)) / (1 - (1 + interest_rate / 100 / 12) ** -term_months) if term_months > 0 else 0
        next_payment_amount = round(monthly_payment, 2) if status == "Active" else 0

        # ~30% of loans are variable-rate (rate resets creep up since Feb 1 in daily update)
        is_variable = (sum(ord(c) for c in customer["customer_id"]) % 100) < int(VARIABLE_RATE_FRACTION * 100)

        loan = {
            "loan_id": generate_loan_id(),
            "customer_id": customer["customer_id"],
            "account_id": linked_account,
            "loan_type": loan_type,
            "industry_sector": industry_sector,
            "original_principal": round(original_principal, 2),
            "current_principal_balance": round(max(0, current_principal_balance), 2),
            "interest_rate": round(interest_rate, 2),
            "original_interest_rate": round(interest_rate, 2),
            "rate_type": "variable" if is_variable else "fixed",
            "term_months": term_months,
            "origination_date": origination_date,
            "maturity_date": maturity_date,
            "days_past_due": days_past_due,
            "next_payment_date": next_payment_date if status == "Active" else None,
            "next_payment_amount": next_payment_amount,
            "status": status,
            "balance_last_updated_date": as_of - timedelta(days=random.randint(0, 7))
        }
        loans.append(loan)
    
    return loans
