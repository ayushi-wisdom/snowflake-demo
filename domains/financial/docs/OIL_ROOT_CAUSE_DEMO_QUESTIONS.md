# Root cause analysis demo: oil/energy anomaly (sequential)

Data is built so that **root cause = rising oil prices since Feb 1**. Each question is a SQL-friendly step; ask them **in order**. Data runs **through Mar 10, 2026** only (set `DATA_AS_OF_DATE=2026-03-10` in `.env`).

---

## Question 1

**Show me transaction volume by week for the past 6 months (through March 10).**

*What you get:* Weekly transaction counts. Volume is **stable** (no big drop); the story is **spending mix**, not volume collapse.

---

## Question 3  
*Based on: “Something changed in recent weeks.”*

**Compare March (through Mar 10) to February: show me transaction count by transaction type (Purchase, Payment, Withdrawal, Transfer, Deposit) for each month.**

*What you get:* In Feb–Mar (oil window), more Payment/Withdrawal and heavier Purchase mix toward essentials; category mix shifts.

---

## Question 4  
*Based on: “March has a different mix of transaction types.”*

**For March vs February, show me transaction count by category (e.g. Gas, Groceries, Utilities, Shopping, Entertainment).**

*What you get:* March has a higher share of Gas, Groceries, and Utilities; February has more Shopping/Entertainment.

---

## Question 5  
*Based on: “Spending shifted toward Gas, Groceries, Utilities in March.”*

**In March, which merchants had the most transactions? Show me top 15 merchants by transaction count in March.**

*What you get:* Gas stations, grocery stores, and utility-type merchants rise to the top in March.

---

## Question 6  
*Based on: “Merchant mix in March is heavy on gas, grocery, utilities.”*

**For March vs February, break down transaction count by account type (Checking, Savings, Credit Card). Did Credit Card share change?**

*What you get:* Credit Card’s share of transactions is higher in March than in February.

---

## Question 7  
*Based on: “Credit Card usage went up in March.”*

**What percentage of transactions were Failed or Pending in March vs February?**

*What you get:* March has a higher % Failed/Pending (~7% vs ~2% in the model).

---

## Question 8  
*Based on: “We’ve seen volume drop, mix shift to essentials, more Credit Card and more failures in March.”*

**For our investment accounts, how did portfolio holding prices change in March? Show me symbols with the largest price change (up or down) in March.**

*What you get:* Energy-related symbols up; many other names down (energy +15%, rest −5% in the model).

---

## Question 9  
*Based on: “Energy holdings went up while the rest of the portfolio was down in March.”*

**[Web search]** What happened to oil prices or the energy sector (XLE) in March 2026?

*What you get:* Real-world context (oil/energy up) to name the root cause.

---

## Question 10  
*Based on: “We have data + external context pointing to oil/energy.”*

**Summarize: what caused the drop in transaction volume and the shift to Gas and Groceries in March?**

*What you get:* One sentence: elevated oil/energy prices (since Feb 1) led to a **spending mix shift** to essentials (gas, groceries, utilities), more Credit Card use and failed/pending transactions, and portfolios with energy up and the rest down. Transport/Airline loans show higher delinquency.

---

## Question: Delinquency by sector (root cause)

**Show me delinquency rate by `industry_sector` (Transport, Airline, Manufacturing, Other).**

*What you get:* Transport and Airline have **higher delinquency** than Other/Manufacturing (Transport ~16%, Airline ~12%, rest ~5% in the model). Supports the story: “Oil hit transport and airline first.”

---

## Quick reference: what the data encodes

| Signal in the data | Interpretation |
|--------------------|----------------|
| **Stable volume**, different mix Feb vs Jan | Oil story is **spending mix**, not volume drop. |
| More Gas, Groceries, Utilities (Feb 1 – Mar 10) | Shift to essentials when oil/energy is elevated; ramp Feb 1–14, full from Feb 15. |
| More Withdrawals, Payments | Cash flow stress; paying bills. |
| More Failed/Pending (~7% vs ~2% in oil window) | Strain on cards/accounts during stress period. |
| More Credit Card transactions | People putting gas/essentials on cards. |
| Energy holdings +15%, rest −5% | Oil shock applied to portfolio prices in oil window. |
| Loans: Transport/Airline higher delinquency | Sector impact; root cause “why trucking/airline first.” |

**Oil window:** **Feb 1 – Mar 10, 2026**. Data does not extend past the current day (use `DATA_AS_OF_DATE=2026-03-10`).
