# WisdomAI Healthcare POC — Data Build + Cron Refresh

## Snowflake context
- Database: [YOUR_DATABASE]
- Schema: HEALTHCARE_POC
- Warehouse: [YOUR_WAREHOUSE]
- Use fully qualified names: [YOUR_DATABASE].HEALTHCARE_POC.[table]

---

## What you are building

A synthetic healthcare analytics dataset for a sales demo. Hospital name: **Meridian Health System** (fictional). All patient and provider names must be synthetic — no real PII. Geography: mix of Florida and Texas zip codes.

Build in this order. Do not skip ahead. Validate row counts after each step.

---

## PHASE 1: SCHEMA DDL

### Dimension tables

```sql
-- DIM_PATIENT
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_PATIENT (
  PATIENT_KEY       INT PRIMARY KEY,
  PATIENT_ID        VARCHAR,        -- e.g. PT-00001
  AGE_BAND          VARCHAR,        -- 18-34, 35-49, 50-64, 65-74, 75+
  GENDER            VARCHAR,
  ZIP_CODE          VARCHAR,
  COUNTY            VARCHAR,
  PRIMARY_PAYER     VARCHAR,        -- Medicare, Medicaid, Commercial, Self-Pay
  SECONDARY_PAYER   VARCHAR,
  CHRONIC_CONDITIONS VARCHAR,       -- comma-separated: CHF, COPD, Diabetes, etc.
  RISK_SCORE        DECIMAL(5,2),   -- 0.00–5.00 HCC-style
  IS_ACTIVE         BOOLEAN
);

-- DIM_PROVIDER
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_PROVIDER (
  PROVIDER_KEY            INT PRIMARY KEY,
  PROVIDER_ID             VARCHAR,   -- e.g. MD-0042
  PROVIDER_NAME           VARCHAR,   -- Synthetic: Dr. [Name]
  SPECIALTY               VARCHAR,   -- Hospitalist, Cardiology, Orthopedics, Pulmonology, General Surgery, Emergency Medicine
  DEPARTMENT              VARCHAR,
  FTE_STATUS              VARCHAR,   -- Full-Time, Part-Time, Locum
  YEARS_ON_STAFF          INT,
  CREDENTIALING_STATUS    VARCHAR,   -- Active, Pending, Suspended
  AVERAGE_LOS_DAYS        DECIMAL(5,2),
  AVERAGE_CMI             DECIMAL(5,3)
);

-- DIM_FACILITY
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_FACILITY (
  FACILITY_KEY    INT PRIMARY KEY,
  FACILITY_ID     VARCHAR,   -- e.g. UNIT-4W
  FACILITY_NAME   VARCHAR,   -- 4 West Medical, ICU, ED, Cardiac Step-Down, Ortho, NICU
  FACILITY_TYPE   VARCHAR,   -- Med-Surg, ICU, ED, Step-Down, Specialty, NICU
  LICENSED_BEDS   INT,
  STAFFED_BEDS    INT,
  FLOOR           INT,
  BUILDING        VARCHAR    -- Main, North Tower, Outpatient Pavilion
);

-- DIM_DRG (use real MS-DRG codes)
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_DRG (
  DRG_KEY                INT PRIMARY KEY,
  DRG_CODE               VARCHAR,
  DRG_DESCRIPTION        VARCHAR,
  MDC                    VARCHAR,
  DRG_WEIGHT             DECIMAL(6,4),
  GEOMETRIC_MEAN_LOS     DECIMAL(5,2),
  ARITHMETIC_MEAN_LOS    DECIMAL(5,2),
  DRG_TYPE               VARCHAR,   -- Medical, Surgical, Procedure
  IS_HRRP_CONDITION      BOOLEAN
);

-- DIM_PAYER
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_PAYER (
  PAYER_KEY               INT PRIMARY KEY,
  PAYER_ID                VARCHAR,
  PAYER_NAME              VARCHAR,
  PAYER_TYPE              VARCHAR,   -- Medicare, Medicare Advantage, Medicaid, Commercial, Self-Pay
  CONTRACT_ON_FILE        BOOLEAN,
  CONTRACT_EFFECTIVE_DATE DATE,
  CONTRACT_EXPIRY_DATE    DATE
);

-- DIM_DIAGNOSIS (ICD-10, plain English descriptions — no concept IDs)
CREATE OR REPLACE TABLE HEALTHCARE_POC.DIM_DIAGNOSIS (
  DIAGNOSIS_KEY             INT PRIMARY KEY,
  ICD10_CODE                VARCHAR,
  DIAGNOSIS_DESCRIPTION     VARCHAR,
  CONDITION_GROUP           VARCHAR,   -- Cardiovascular, Respiratory, Infectious, Musculoskeletal, Neurological, Metabolic
  IS_HAC                    BOOLEAN,
  IS_CHRONIC                BOOLEAN,
  HRRP_CONDITION            VARCHAR    -- CHF, AMI, COPD, Pneumonia, NULL
);
```

### Fact tables

```sql
-- FACT_ENCOUNTER (spine — everything joins here)
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_ENCOUNTER (
  ENCOUNTER_KEY             INT PRIMARY KEY,
  ENCOUNTER_ID              VARCHAR,
  PATIENT_KEY               INT,
  PROVIDER_KEY              INT,
  FACILITY_KEY              INT,
  DRG_KEY                   INT,
  PAYER_KEY                 INT,
  ADMIT_DATE                DATE,
  DISCHARGE_DATE            DATE,
  LOS_DAYS                  INT,
  ADMIT_SOURCE              VARCHAR,   -- Emergency, Transfer, Elective, Direct Admit
  DISCHARGE_DISPOSITION     VARCHAR,   -- Home, SNF, Rehab, AMA, Expired, Home Health, LTACH
  PRINCIPAL_DIAGNOSIS_KEY   INT,
  SECONDARY_DIAGNOSIS_1_KEY INT,
  SECONDARY_DIAGNOSIS_2_KEY INT,
  PROCEDURE_CODE            VARCHAR,
  IS_READMISSION_30         BOOLEAN,
  IS_READMISSION_90         BOOLEAN,
  INDEX_ENCOUNTER_KEY       INT,
  HAI_FLAG                  VARCHAR,   -- CLABSI, CAUTI, SSI, CDiff, NULL
  CASE_MIX_INDEX            DECIMAL(5,3),
  SEVERITY_OF_ILLNESS       VARCHAR,   -- Minor, Moderate, Major, Extreme
  RISK_OF_MORTALITY         VARCHAR,
  CREATED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_FINANCIALS
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_FINANCIALS (
  FINANCIAL_KEY           INT PRIMARY KEY,
  ENCOUNTER_KEY           INT,
  PAYER_KEY               INT,
  DRG_KEY                 INT,
  TOTAL_CHARGES           DECIMAL(12,2),
  TOTAL_COST              DECIMAL(12,2),
  CONTRACTUAL_ADJUSTMENT  DECIMAL(12,2),
  NET_REVENUE             DECIMAL(12,2),
  AMOUNT_COLLECTED        DECIMAL(12,2),
  UNDERPAYMENT_AMOUNT     DECIMAL(12,2),   -- NET_REVENUE - AMOUNT_COLLECTED
  DENIAL_FLAG             BOOLEAN,
  DENIAL_REASON           VARCHAR,         -- Medical Necessity, Timely Filing, Authorization, Coding, NULL
  DENIAL_DATE             DATE,
  APPEAL_STATUS           VARCHAR,         -- None, Pending, Won, Lost
  GROSS_MARGIN            DECIMAL(12,2),
  MARGIN_PCT              DECIMAL(6,4),
  OUTLIER_PAYMENT         DECIMAL(10,2),
  FISCAL_YEAR             INT,
  FISCAL_QUARTER          INT,
  CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_READMISSION
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_READMISSION (
  READMISSION_KEY               INT PRIMARY KEY,
  READMISSION_ENCOUNTER_KEY     INT,
  INDEX_ENCOUNTER_KEY           INT,
  PATIENT_KEY                   INT,
  DAYS_TO_READMISSION           INT,
  READMISSION_WINDOW            VARCHAR,   -- 7-day, 30-day, 90-day
  INDEX_DRG_KEY                 INT,
  READMISSION_DRG_KEY           INT,
  SAME_CONDITION                BOOLEAN,
  HRRP_APPLICABLE               BOOLEAN,
  DISCHARGE_DISPOSITION_INDEX   VARCHAR,
  CONTRIBUTING_FACTOR           VARCHAR,
  PREVENTED_FLAG                BOOLEAN,
  CREATED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_QUALITY_METRICS
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_QUALITY_METRICS (
  QUALITY_KEY           INT PRIMARY KEY,
  ENCOUNTER_KEY         INT,
  FACILITY_KEY          INT,
  METRIC_DATE           DATE,
  METRIC_TYPE           VARCHAR,     -- CLABSI, CAUTI, SSI, CDiff, Fall, Pressure Injury, HCAHPS_Overall
  METRIC_VALUE          DECIMAL(8,4),
  BENCHMARK_VALUE       DECIMAL(8,4),
  PERFORMANCE_BAND      VARCHAR,     -- Above Average, Average, Below Average, Alert
  CMS_STAR_CONTRIBUTION BOOLEAN,
  REPORTABLE_EVENT      BOOLEAN,
  CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_STAFFING
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_STAFFING (
  STAFFING_KEY              INT PRIMARY KEY,
  FACILITY_KEY              INT,
  STAFFING_DATE             DATE,
  SHIFT                     VARCHAR,     -- Day, Evening, Night
  ROLE                      VARCHAR,     -- RN, LPN, CNA, MD, PA, NP
  SCHEDULED_HOURS           DECIMAL(6,2),
  ACTUAL_HOURS              DECIMAL(6,2),
  OVERTIME_HOURS            DECIMAL(6,2),
  AGENCY_HOURS              DECIMAL(6,2),
  CENSUS                    INT,
  NURSE_TO_PATIENT_RATIO    DECIMAL(5,2),
  TARGET_RATIO              DECIMAL(5,2),
  RATIO_COMPLIANCE          BOOLEAN,
  VACANCY_RATE              DECIMAL(5,4),
  CREATED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_CAPACITY
CREATE OR REPLACE TABLE HEALTHCARE_POC.FACT_CAPACITY (
  CAPACITY_KEY              INT PRIMARY KEY,
  FACILITY_KEY              INT,
  SNAPSHOT_DATE             DATE,
  CENSUS                    INT,
  LICENSED_BED_OCCUPANCY    DECIMAL(5,4),
  STAFFED_BED_OCCUPANCY     DECIMAL(5,4),
  ED_BOARDING_HOURS         DECIMAL(8,2),
  AVG_DISCHARGE_HOUR        DECIMAL(5,2),
  DISCHARGES                INT,
  ADMISSIONS                INT,
  TRANSFERS_IN              INT,
  TRANSFERS_OUT             INT,
  DIVERSION_HOURS           DECIMAL(6,2),
  ALOS_ROLLING_30           DECIMAL(5,2),
  CREATED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ANOMALY_ALERTS
CREATE OR REPLACE TABLE HEALTHCARE_POC.ANOMALY_ALERTS (
  ALERT_KEY           INT PRIMARY KEY,
  ALERT_DATE          DATE,
  ALERT_TYPE          VARCHAR,     -- Staffing_Ratio_Breach, HRRP_Threshold_Risk, Payer_Underpayment, HAI_Cluster, LOS_Outlier, Denial_Spike
  SEVERITY            VARCHAR,     -- Low, Medium, High, Critical
  FACILITY_KEY        INT,
  PROVIDER_KEY        INT,
  PAYER_KEY           INT,
  ALERT_DESCRIPTION   VARCHAR,
  RECOMMENDED_ACTION  VARCHAR,
  IS_RESOLVED         BOOLEAN,
  RESOLUTION_DATE     DATE,
  DOLLAR_IMPACT       DECIMAL(12,2),
  CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Views

```sql
-- V_ENCOUNTER_FULL: master view, all keys resolved to human-readable names
-- Join FACT_ENCOUNTER to all dimensions, return no raw keys in output

-- V_HRRP_SCORECARD: rolling 12-month readmission rates by HRRP condition
-- Compare to CMS penalty thresholds, flag conditions at risk, estimate penalty amount

-- V_PAYER_PERFORMANCE: FACT_FINANCIALS aggregated by payer
-- Return: collected vs. contracted, denial rate, underpayment total, appeal win rate

-- V_PROVIDER_SCORECARD: provider-level summary
-- Return: avg LOS vs. DRG geometric mean, readmission rate, CMI, volume, quality outcomes

-- V_UNIT_DAILY_OPERATIONS: current operational status by unit
-- Return: occupancy, staffing ratio compliance, quality flags, ED boarding hours
```

---

## PHASE 2: DATA GENERATION

Write Python generators. Use `faker` for names, `numpy` for distributions, `random.seed(42)` for reproducibility. Output CSVs then load via Snowflake `COPY INTO`. Print row counts after each file.

### Volume targets

| Table | Rows | Date range |
|-------|------|------------|
| DIM_PATIENT | 3,500 | — |
| DIM_PROVIDER | 85 | — |
| DIM_FACILITY | 12 | — |
| DIM_DRG | 45 | — |
| DIM_PAYER | 8 | — |
| DIM_DIAGNOSIS | 120 | — |
| FACT_ENCOUNTER | 11,000 | Jan 2022 – present |
| FACT_FINANCIALS | 11,000 | matches encounters |
| FACT_READMISSION | ~1,800 | derived from encounters |
| FACT_QUALITY_METRICS | ~85,000 | Jan 2022 – present |
| FACT_STAFFING | ~52,000 | Jan 2022 – present |
| FACT_CAPACITY | ~17,500 | Jan 2022 – present |
| ANOMALY_ALERTS | ~400 | rolling |

### Payer mix
38% Medicare FFS, 12% Medicare Advantage, 22% Medicaid, 24% Commercial, 4% Self-Pay

### Seasonal patterns
- Higher admissions November–March (respiratory season)
- Higher orthopedic volume Q3
- Weekend admissions 30% lower than weekday

### Discharge disposition distribution
55% Home, 18% SNF, 10% Home Health, 8% Rehab, 5% AMA, 4% Expired

### DRG seed data
Include at minimum these real MS-DRG codes with correct CMS FY2025 weights and geometric mean LOS:
291, 292, 293 (CHF), 194, 195, 196 (COPD), 690, 641, 177 (pneumonia), 470 (major joint replacement), 247 (cardiac cath), 65, 66 (intracranial hemorrhage), 871, 872 (septicemia)

---

## PHASE 3: HARDCODED ANOMALIES

These six patterns must be deliberately engineered into the data. They are the most important part of the build — do not let them get washed out by random generation. Implement them as targeted overrides after the base generation runs.

### Anomaly 1 — Aetna CHF underpayment
In FACT_FINANCIALS, for all encounters where PAYER = 'Aetna PPO' and DRG_CODE IN ('291','292','293'):
- Set AMOUNT_COLLECTED = NET_REVENUE * random.uniform(0.80, 0.84)
- This creates a systematic ~82% collection rate vs. the contracted 94%
- Result must total $1.7M–$1.9M in UNDERPAYMENT_AMOUNT across these claims
- Target: 45–50 claims

### Anomaly 2 — ICU night shift staffing violations
In FACT_STAFFING, for FACILITY where FACILITY_TYPE = 'ICU' and SHIFT = 'Night':
- Set NURSE_TO_PATIENT_RATIO = random.uniform(2.8, 3.3) on 28% of night shifts
- TARGET_RATIO = 2.0 (policy maximum)
- Set RATIO_COMPLIANCE = FALSE on these rows
- On 14 of those exact dates, create a corresponding CLABSI or CAUTI event in FACT_QUALITY_METRICS within 72 hours

### Anomaly 3 — HRRP readmission threshold breach
In FACT_ENCOUNTER and FACT_READMISSION:
- CHF 30-day readmission rate must land at 17.5%–19.0% of CHF encounters
- COPD 30-day readmission rate must land at 16.5%–18.0% of COPD encounters
- CMS penalty threshold is ~14% for both — ensure both are clearly above it
- HRRP_APPLICABLE = TRUE on all qualifying readmissions

### Anomaly 4 — Physician LOS outliers
In FACT_ENCOUNTER, for exactly 2 specific provider IDs (e.g. MD-0031 and MD-0047):
- Set LOS_DAYS = DRG geometric mean + random.uniform(2.0, 2.8) for 80% of their encounters
- Their peers on same DRGs should cluster within 0.5 days of geometric mean
- Combined excess bed-days should total $1.1M–$1.3M annually when costed

### Anomaly 5 — Late discharge / ED boarding correlation
In FACT_CAPACITY:
- Set AVG_DISCHARGE_HOUR = random.uniform(14.5, 15.8) (2:30–3:48 PM average)
- Set ED_BOARDING_HOURS to correlate positively: boarding = 2.1 + (discharge_hour - 13.0) * 0.8 + noise
- Result: average ED boarding of 4.2–5.0 hours

### Anomaly 6 — Cigna denial coding cluster
In FACT_FINANCIALS, for 32–38 Cigna encounters with orthopedic DRGs:
- Set DENIAL_FLAG = TRUE
- Set DENIAL_REASON = 'Authorization'
- Set DENIAL_DATE to within last 90 days
- Set APPEAL_STATUS = 'Pending'
- Total UNDERPAYMENT_AMOUNT for this cluster: $380K–$450K
- Set ALERT in ANOMALY_ALERTS with DOLLAR_IMPACT and note that 18 claims have timely filing deadline within 100 days

---

## PHASE 4: CRON JOB

Build a Python script `refresh_healthcare_poc.py` that runs on a schedule and keeps the data feeling live. The script should:

### What it refreshes
1. **FACT_CAPACITY** — append yesterday's row for each facility (12 new rows per run)
2. **FACT_STAFFING** — append yesterday's staffing rows for each unit/shift/role combination
3. **FACT_ENCOUNTER** — append 8–15 new encounters dated yesterday with realistic distribution
4. **FACT_FINANCIALS** — append matching financial rows for new encounters
5. **FACT_QUALITY_METRICS** — append quality metric rows for new encounters
6. **ANOMALY_ALERTS** — re-evaluate alert conditions and insert new alerts where thresholds are breached; resolve alerts where conditions have cleared

### What it must NOT do
- Never touch dimension tables (patients, providers, facilities, DRGs, payers)
- Never modify existing rows — append only
- Never break the hardcoded anomalies — new data should continue the same statistical patterns

### Anomaly preservation rules
When generating new encounters, maintain these rates in the rolling data:
- Aetna CHF collection rate stays at 80–84% (not the ~91% commercial average)
- ICU night shift non-compliance stays at 25–30%
- CHF and COPD 30-day readmission rates stay at 17–19% and 16–18% respectively
- MD-0031 and MD-0047 LOS premium stays at +2.0–2.8 days above DRG benchmark
- Cigna orthopedic denials: add 1–2 new denial claims per week

### Snowflake connection
Use `snowflake-connector-python`. Read credentials from environment variables:
```python
import os
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema='HEALTHCARE_POC'
)
```

### Cron schedule
Set up as a cron job running every 3 days at 2:00 AM:
```
0 2 */3 * * /path/to/venv/bin/python /path/to/refresh_healthcare_poc.py >> /var/log/healthcare_poc_refresh.log 2>&1
```

Also provide a `Dockerfile` and `docker-compose.yml` so the cron can run in a container if preferred. Use `crond` inside the container.

### Logging
Log to both stdout and a rotating file. Each run should log:
- Run start time
- Rows appended per table
- Any anomaly threshold checks (e.g. "CHF readmission rate: 18.1% — within target range")
- Run duration
- Any errors with full traceback

### Script structure
```
healthcare_poc/
├── refresh_healthcare_poc.py     # Main refresh script
├── generators/
│   ├── daily_encounters.py       # Generates new encounter batch
│   ├── daily_capacity.py         # Generates capacity rows
│   ├── daily_staffing.py         # Generates staffing rows
│   └── alert_evaluator.py        # Evaluates and inserts/resolves alerts
├── config/
│   └── anomaly_targets.json      # Anomaly rate targets — read by generators
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

`anomaly_targets.json` should contain the target rates so they can be adjusted without touching code:
```json
{
  "aetna_chf_collection_rate": { "min": 0.80, "max": 0.84 },
  "icu_night_noncompliance_rate": { "min": 0.25, "max": 0.30 },
  "chf_readmission_rate_30day": { "min": 0.175, "max": 0.190 },
  "copd_readmission_rate_30day": { "min": 0.165, "max": 0.180 },
  "physician_outlier_ids": ["MD-0031", "MD-0047"],
  "physician_los_premium_days": { "min": 2.0, "max": 2.8 }
}
```

---

## VALIDATION

After full build, run these checks before considering done:

```sql
-- Row counts
SELECT 'DIM_PATIENT' as t, COUNT(*) FROM HEALTHCARE_POC.DIM_PATIENT
UNION ALL SELECT 'FACT_ENCOUNTER', COUNT(*) FROM HEALTHCARE_POC.FACT_ENCOUNTER
UNION ALL SELECT 'FACT_FINANCIALS', COUNT(*) FROM HEALTHCARE_POC.FACT_FINANCIALS
UNION ALL SELECT 'FACT_READMISSION', COUNT(*) FROM HEALTHCARE_POC.FACT_READMISSION
UNION ALL SELECT 'FACT_STAFFING', COUNT(*) FROM HEALTHCARE_POC.FACT_STAFFING
UNION ALL SELECT 'FACT_CAPACITY', COUNT(*) FROM HEALTHCARE_POC.FACT_CAPACITY
UNION ALL SELECT 'ANOMALY_ALERTS', COUNT(*) FROM HEALTHCARE_POC.ANOMALY_ALERTS;

-- Anomaly 1: Aetna CHF underpayment total
SELECT COUNT(*), ROUND(SUM(UNDERPAYMENT_AMOUNT),0) as total_underpayment
FROM HEALTHCARE_POC.FACT_FINANCIALS f
JOIN HEALTHCARE_POC.DIM_PAYER p ON f.PAYER_KEY = p.PAYER_KEY
JOIN HEALTHCARE_POC.DIM_DRG d ON f.DRG_KEY = d.DRG_KEY
WHERE p.PAYER_NAME = 'Aetna PPO'
AND d.DRG_CODE IN ('291','292','293');
-- Expected: 45-50 rows, $1.7M–$1.9M

-- Anomaly 2: ICU night shift non-compliance rate
SELECT ROUND(SUM(CASE WHEN RATIO_COMPLIANCE = FALSE THEN 1 ELSE 0 END) / COUNT(*), 3) as noncompliance_rate
FROM HEALTHCARE_POC.FACT_STAFFING s
JOIN HEALTHCARE_POC.DIM_FACILITY f ON s.FACILITY_KEY = f.FACILITY_KEY
WHERE f.FACILITY_TYPE = 'ICU' AND s.SHIFT = 'Night';
-- Expected: 0.25–0.30

-- Anomaly 3: CHF 30-day readmission rate
SELECT ROUND(SUM(CASE WHEN IS_READMISSION_30 = TRUE THEN 1 ELSE 0 END) / COUNT(*), 3) as readmit_rate
FROM HEALTHCARE_POC.FACT_ENCOUNTER e
JOIN HEALTHCARE_POC.DIM_DRG d ON e.DRG_KEY = d.DRG_KEY
WHERE d.DRG_CODE IN ('291','292','293');
-- Expected: 0.175–0.190

-- Anomaly 6: Cigna denial cluster
SELECT COUNT(*), ROUND(SUM(UNDERPAYMENT_AMOUNT),0)
FROM HEALTHCARE_POC.FACT_FINANCIALS f
JOIN HEALTHCARE_POC.DIM_PAYER p ON f.PAYER_KEY = p.PAYER_KEY
WHERE p.PAYER_NAME LIKE '%Cigna%'
AND f.DENIAL_FLAG = TRUE
AND f.DENIAL_REASON = 'Authorization';
-- Expected: 32-38 rows, $380K–$450K

-- Referential integrity check
SELECT COUNT(*) as orphaned_encounters
FROM HEALTHCARE_POC.FACT_ENCOUNTER e
LEFT JOIN HEALTHCARE_POC.DIM_PATIENT p ON e.PATIENT_KEY = p.PATIENT_KEY
WHERE p.PATIENT_KEY IS NULL;
-- Expected: 0
```
