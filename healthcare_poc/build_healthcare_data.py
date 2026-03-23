"""Generate and load synthetic healthcare POC data into Snowflake."""
from __future__ import annotations

import os
import random
import sys
from datetime import date, datetime, timedelta

import numpy as np
from faker import Faker

_hp = os.path.dirname(os.path.abspath(__file__))
if _hp not in sys.path:
    sys.path.insert(0, _hp)

from snowflake_connection import get_snowflake_connection

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def random_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_dimensions():
    counties = ["Miami-Dade", "Broward", "Hillsborough", "Dallas", "Travis", "Harris"]
    zips = ["33101", "33024", "33602", "75201", "73301", "77001"]
    patient_rows = []
    payer_options = ["Medicare FFS", "Medicare Advantage", "Medicaid", "Commercial", "Self-Pay"]
    payer_weights = [0.38, 0.12, 0.22, 0.24, 0.04]
    conditions = ["CHF", "COPD", "Diabetes", "CKD", "Hypertension", "Asthma"]

    for i in range(1, 3501):
        chronic = random.sample(conditions, random.randint(0, 3))
        patient_rows.append(
            (
                i,
                f"PT-{i:05d}",
                random.choice(["18-34", "35-49", "50-64", "65-74", "75+"]),
                random.choice(["Female", "Male"]),
                random.choice(zips),
                random.choice(counties),
                random.choices(payer_options, weights=payer_weights, k=1)[0],
                random.choice(payer_options + [None]),
                ",".join(chronic) if chronic else None,
                round(random.uniform(0.2, 4.8), 2),
                random.random() > 0.03,
            )
        )

    specialties = [
        "Hospitalist",
        "Cardiology",
        "Orthopedics",
        "Pulmonology",
        "General Surgery",
        "Emergency Medicine",
    ]
    provider_rows = []
    for i in range(1, 86):
        provider_rows.append(
            (
                i,
                f"MD-{i:04d}",
                f"Dr. {fake.last_name()}",
                random.choice(specialties),
                random.choice(["Medicine", "Surgery", "ED", "Critical Care"]),
                random.choice(["Full-Time", "Part-Time", "Locum"]),
                random.randint(1, 25),
                random.choice(["Active", "Pending", "Suspended"]),
                round(random.uniform(3.0, 6.0), 2),
                round(random.uniform(0.9, 2.1), 3),
            )
        )

    facilities = [
        ("UNIT-ICU", "ICU", "ICU", 50),
        ("UNIT-ED", "Emergency Department", "ED", 70),
        ("UNIT-4W", "4 West Medical", "Med-Surg", 40),
        ("UNIT-5W", "5 West Medical", "Med-Surg", 40),
        ("UNIT-CARD", "Cardiac Step-Down", "Step-Down", 32),
        ("UNIT-ORTHO", "Orthopedic Unit", "Specialty", 28),
        ("UNIT-NICU", "NICU", "NICU", 24),
        ("UNIT-6N", "6 North Medical", "Med-Surg", 36),
        ("UNIT-7N", "7 North Medical", "Med-Surg", 36),
        ("UNIT-CCU", "Cardiac ICU", "ICU", 22),
        ("UNIT-OBS", "Observation", "Step-Down", 20),
        ("UNIT-OUT", "Outpatient Pavilion", "Specialty", 30),
    ]
    facility_rows = []
    for i, (facility_id, name, ftype, beds) in enumerate(facilities, start=1):
        facility_rows.append(
            (i, facility_id, name, ftype, beds, int(beds * 0.92), random.randint(1, 9), random.choice(["Main", "North Tower", "Outpatient Pavilion"]))
        )

    drg_seed = [
        ("291", "Heart Failure & Shock w MCC", "Diseases of Circulatory System", 1.16, 5.2, 6.1, "Medical", True),
        ("292", "Heart Failure & Shock w CC", "Diseases of Circulatory System", 0.91, 4.4, 5.2, "Medical", True),
        ("293", "Heart Failure & Shock w/o CC/MCC", "Diseases of Circulatory System", 0.68, 3.7, 4.5, "Medical", True),
        ("194", "Simple Pneumonia & Pleurisy w CC", "Respiratory", 0.95, 4.9, 5.8, "Medical", True),
        ("195", "Simple Pneumonia & Pleurisy w/o CC/MCC", "Respiratory", 0.76, 3.9, 4.7, "Medical", True),
        ("196", "Interstitial Lung Disease", "Respiratory", 1.02, 5.1, 6.3, "Medical", False),
        ("470", "Major Joint Replacement", "Musculoskeletal", 1.89, 2.3, 2.9, "Surgical", False),
        ("247", "Perc Cardiac Procedure", "Circulatory", 1.45, 2.6, 3.2, "Procedure", False),
        ("690", "Kidney & Urinary Tract Infection", "Kidney", 1.03, 4.5, 5.5, "Medical", False),
        ("641", "Nutritional & Misc Metabolic", "Endocrine", 0.84, 3.8, 4.6, "Medical", False),
        ("177", "Respiratory Infections & Inflammations", "Respiratory", 1.32, 5.7, 6.9, "Medical", True),
        ("65", "Intracranial Hemorrhage w CC", "Neurology", 1.55, 5.3, 6.7, "Medical", False),
        ("66", "Intracranial Hemorrhage w/o CC", "Neurology", 1.21, 4.8, 5.8, "Medical", False),
        ("871", "Septicemia w MV >96 Hours", "Infectious", 2.75, 9.2, 11.0, "Medical", False),
        ("872", "Septicemia w/o MV >96 Hours", "Infectious", 1.65, 6.4, 7.8, "Medical", False),
    ]
    while len(drg_seed) < 45:
        code = str(500 + len(drg_seed))
        drg_seed.append((code, f"Synthetic DRG {code}", "Mixed", round(random.uniform(0.65, 2.4), 4), round(random.uniform(2.0, 8.5), 2), round(random.uniform(2.2, 9.1), 2), random.choice(["Medical", "Surgical", "Procedure"]), False))
    drg_rows = [(i + 1, *drg_seed[i]) for i in range(45)]

    payer_rows = [
        (1, "PY-001", "Medicare FFS", "Medicare", True, date(2022, 1, 1), date(2030, 12, 31)),
        (2, "PY-002", "Medicare Advantage", "Medicare Advantage", True, date(2022, 1, 1), date(2030, 12, 31)),
        (3, "PY-003", "Medicaid State Plan", "Medicaid", True, date(2022, 1, 1), date(2030, 12, 31)),
        (4, "PY-004", "Aetna PPO", "Commercial", True, date(2022, 1, 1), date(2030, 12, 31)),
        (5, "PY-005", "BCBS PPO", "Commercial", True, date(2022, 1, 1), date(2030, 12, 31)),
        (6, "PY-006", "United PPO", "Commercial", True, date(2022, 1, 1), date(2030, 12, 31)),
        (7, "PY-007", "Cigna HMO", "Commercial", True, date(2022, 1, 1), date(2030, 12, 31)),
        (8, "PY-008", "Self-Pay", "Self-Pay", False, None, None),
    ]

    diagnosis_rows = []
    groups = ["Cardiovascular", "Respiratory", "Infectious", "Musculoskeletal", "Neurological", "Metabolic"]
    hrrp_map = {"Cardiovascular": "CHF", "Respiratory": random.choice(["COPD", "Pneumonia", None]), "Infectious": None, "Musculoskeletal": None, "Neurological": None, "Metabolic": None}
    for i in range(1, 121):
        grp = random.choice(groups)
        diagnosis_rows.append((i, f"I{10+i:02d}.{random.randint(0,9)}", f"Synthetic {grp} diagnosis {i}", grp, random.random() < 0.08, random.random() < 0.45, hrrp_map[grp]))

    return patient_rows, provider_rows, facility_rows, drg_rows, payer_rows, diagnosis_rows


def generate_facts(drg_rows, payer_rows, facility_rows):
    start = date(2022, 1, 1)
    end = date.today() - timedelta(days=1)

    drg_by_code = {r[1]: r for r in drg_rows}
    drg_map = {r[0]: r for r in drg_rows}
    payer_name_by_key = {r[0]: r[2] for r in payer_rows}
    payer_keys = [r[0] for r in payer_rows]
    facility_keys = [r[0] for r in facility_rows]

    encounter_rows = []
    financial_rows = []
    readmit_rows = []

    aetna_chf_candidates = []
    cigna_ortho_candidates = []
    chf_encounter_keys = []
    copd_encounter_keys = []

    for i in range(1, 11001):
        admit = random_date(start, end)
        seasonal_boost = 1.2 if admit.month in [11, 12, 1, 2, 3] else 1.0
        base_los = max(1, int(np.random.normal(4.2 * seasonal_boost, 1.8)))
        provider_key = random.randint(1, 85)
        if provider_key in (31, 47) and random.random() < 0.80:
            base_los += random.randint(2, 3)
        discharge = admit + timedelta(days=base_los)

        drg_key = random.randint(1, 45)
        drg_code = drg_map[drg_key][1]
        if random.random() < 0.17:
            drg_key = next(k for k, v in drg_map.items() if v[1] in ("291", "292", "293"))
            drg_code = drg_map[drg_key][1]
        elif random.random() < 0.14:
            drg_key = next(k for k, v in drg_map.items() if v[1] in ("194", "195", "196"))
            drg_code = drg_map[drg_key][1]

        payer_key = random.choices(payer_keys, weights=[38, 12, 22, 9, 8, 7, 4, 4], k=1)[0]
        encounter_rows.append(
            (
                i,
                f"ENC-{i:07d}",
                random.randint(1, 3500),
                provider_key,
                random.choice(facility_keys),
                drg_key,
                payer_key,
                admit,
                discharge,
                base_los,
                random.choice(["Emergency", "Transfer", "Elective", "Direct Admit"]),
                random.choices(["Home", "SNF", "Home Health", "Rehab", "AMA", "Expired"], weights=[55, 18, 10, 8, 5, 4], k=1)[0],
                random.randint(1, 120),
                random.randint(1, 120),
                random.randint(1, 120),
                f"{random.randint(10000,99999)}",
                False,
                False,
                None,
                random.choice(["CLABSI", "CAUTI", "SSI", "CDiff", None, None, None]),
                round(random.uniform(0.8, 2.4), 3),
                random.choice(["Minor", "Moderate", "Major", "Extreme"]),
                random.choice(["Minor", "Moderate", "Major", "Extreme"]),
            )
        )

        weight = float(drg_map[drg_key][4])
        total_charges = round(np.random.normal(18000 * weight, 5000), 2)
        total_cost = round(total_charges * random.uniform(0.52, 0.74), 2)
        contractual_adj = round(total_charges * random.uniform(0.18, 0.35), 2)
        net_revenue = round(total_charges - contractual_adj, 2)
        amount_collected = round(net_revenue * random.uniform(0.89, 0.96), 2)

        denial_flag = random.random() < 0.05
        denial_reason = random.choice(["Medical Necessity", "Timely Filing", "Authorization", "Coding"]) if denial_flag else None
        denial_date = random_date(end - timedelta(days=89), end) if denial_flag else None
        appeal_status = random.choice(["Pending", "Won", "Lost"]) if denial_flag else "None"

        financial_rows.append(
            (
                i,
                i,
                payer_key,
                drg_key,
                total_charges,
                total_cost,
                contractual_adj,
                net_revenue,
                amount_collected,
                round(net_revenue - amount_collected, 2),
                denial_flag,
                denial_reason,
                denial_date,
                appeal_status,
                round(net_revenue - total_cost, 2),
                round((net_revenue - total_cost) / net_revenue, 4) if net_revenue else 0,
                round(random.uniform(0, 2500), 2),
                admit.year,
                ((admit.month - 1) // 3) + 1,
            )
        )

        if drg_code in ("291", "292", "293"):
            chf_encounter_keys.append(i)
        if drg_code in ("194", "195", "196"):
            copd_encounter_keys.append(i)
        if payer_name_by_key[payer_key] == "Aetna PPO" and drg_code in ("291", "292", "293"):
            aetna_chf_candidates.append(i)
        if payer_name_by_key[payer_key].startswith("Cigna") and drg_code == "470":
            cigna_ortho_candidates.append(i)

    # Anomaly 1: Aetna CHF underpayment
    target_aetna = random.randint(45, 50)
    selected = random.sample(aetna_chf_candidates, k=min(target_aetna, len(aetna_chf_candidates)))
    for idx in selected:
        fr = list(financial_rows[idx - 1])
        fr[8] = round(fr[7] * random.uniform(0.80, 0.84), 2)
        fr[9] = round(fr[7] - fr[8], 2)
        fr[10] = True
        fr[11] = "Coding"
        fr[13] = "Pending"
        financial_rows[idx - 1] = tuple(fr)

    # Anomaly 3: raise readmission rates for CHF/COPD
    chf_target = int(len(chf_encounter_keys) * random.uniform(0.175, 0.19))
    copd_target = int(len(copd_encounter_keys) * random.uniform(0.165, 0.18))
    readmit_candidates = random.sample(chf_encounter_keys, min(chf_target, len(chf_encounter_keys))) + random.sample(copd_encounter_keys, min(copd_target, len(copd_encounter_keys)))
    rkey = 1
    for idx in readmit_candidates:
        er = list(encounter_rows[idx - 1])
        er[16] = True
        er[17] = random.random() < 0.35
        er[18] = max(1, idx - random.randint(1, 90))
        encounter_rows[idx - 1] = tuple(er)
        readmit_rows.append((rkey, idx, er[18], er[2], random.randint(4, 30), "30-day", encounter_rows[er[18] - 1][5], er[5], random.random() < 0.72, True, encounter_rows[er[18] - 1][11], random.choice(["Medication adherence", "Post-acute gap", "Care transition"]), random.random() < 0.25))
        rkey += 1

    # Anomaly 6: Cigna denial coding cluster
    cigna_target = random.randint(32, 38)
    chosen_cigna = random.sample(cigna_ortho_candidates, k=min(cigna_target, len(cigna_ortho_candidates)))
    for idx in chosen_cigna:
        fr = list(financial_rows[idx - 1])
        fr[10] = True
        fr[11] = "Authorization"
        fr[12] = random_date(end - timedelta(days=90), end)
        fr[13] = "Pending"
        fr[8] = round(fr[7] * random.uniform(0.70, 0.82), 2)
        fr[9] = round(fr[7] - fr[8], 2)
        financial_rows[idx - 1] = tuple(fr)

    # Quality metrics and staffing/capacity
    quality_rows = []
    staffing_rows = []
    capacity_rows = []
    qk = sk = ck = 1
    metric_types = ["CLABSI", "CAUTI", "SSI", "CDiff", "Fall", "Pressure Injury", "HCAHPS_Overall"]
    roles = ["RN", "LPN", "CNA", "MD", "PA", "NP"]
    shifts = ["Day", "Evening", "Night"]

    for d in daterange(start, end):
        for facility_key in facility_keys:
            discharge_hour = round(random.uniform(11.0, 15.6), 2)
            boarding = round(2.1 + (discharge_hour - 13.0) * 0.8 + np.random.normal(0, 0.6), 2)
            capacity_rows.append(
                (
                    ck,
                    facility_key,
                    d,
                    random.randint(18, 72),
                    round(random.uniform(0.72, 1.04), 4),
                    round(random.uniform(0.70, 1.02), 4),
                    max(0.1, boarding),
                    discharge_hour,
                    random.randint(4, 26),
                    random.randint(4, 26),
                    random.randint(0, 5),
                    random.randint(0, 5),
                    round(random.uniform(0.0, 4.0), 2),
                    round(random.uniform(3.2, 5.8), 2),
                )
            )
            ck += 1

            for shift in shifts:
                for role in roles:
                    ratio = round(random.uniform(1.6, 2.3), 2)
                    target_ratio = 2.0
                    # Anomaly 2: ICU night shift non-compliance
                    if facility_key in [1, 10] and shift == "Night" and role == "RN" and random.random() < 0.28:
                        ratio = round(random.uniform(2.8, 3.3), 2)
                    staffing_rows.append(
                        (
                            sk,
                            facility_key,
                            d,
                            shift,
                            role,
                            round(random.uniform(8, 14), 2),
                            round(random.uniform(7, 16), 2),
                            round(random.uniform(0, 4.5), 2),
                            round(random.uniform(0, 3.5), 2),
                            random.randint(10, 70),
                            ratio,
                            target_ratio,
                            ratio <= target_ratio,
                            round(random.uniform(0.03, 0.22), 4),
                        )
                    )
                    sk += 1

            metric_count = random.randint(1, 2)
            for _ in range(metric_count):
                m = random.choice(metric_types)
                val = round(abs(np.random.normal(1.0, 0.55)), 4)
                bench = round(abs(np.random.normal(0.8, 0.4)), 4)
                quality_rows.append(
                    (
                        qk,
                        random.randint(1, 11000),
                        facility_key,
                        d,
                        m,
                        val,
                        bench,
                        "Above Average" if val <= bench * 0.95 else ("Average" if val <= bench * 1.1 else ("Below Average" if val <= bench * 1.3 else "Alert")),
                        random.random() < 0.5,
                        random.random() < 0.2,
                    )
                )
                qk += 1

    alerts = []
    ak = 1
    alerts.append((ak, end, "Payer_Underpayment", "High", None, None, 4, "Aetna CHF collection lag below target band.", "Initiate contract variance review and bundle-level appeal.", False, None, 1800000.00))
    ak += 1
    alerts.append((ak, end, "Denial_Spike", "High", None, None, 7, "Cigna orthopedic authorization denials elevated.", "Prioritize 18 claims near timely filing deadline.", False, None, 410000.00))
    ak += 1

    return encounter_rows, financial_rows, readmit_rows, quality_rows, staffing_rows, capacity_rows, alerts


def insert_many(cur, table, cols, rows):
    if not rows:
        print(f"{table}: 0")
        return
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO HEALTHCARE_POC.{table} ({','.join(cols)}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    print(f"{table}: {len(rows)}")


def main():
    conn = get_snowflake_connection(schema_override="HEALTHCARE_POC")
    cur = conn.cursor()
    try:
        dims = generate_dimensions()
        insert_many(cur, "DIM_PATIENT", ["PATIENT_KEY", "PATIENT_ID", "AGE_BAND", "GENDER", "ZIP_CODE", "COUNTY", "PRIMARY_PAYER", "SECONDARY_PAYER", "CHRONIC_CONDITIONS", "RISK_SCORE", "IS_ACTIVE"], dims[0])
        insert_many(cur, "DIM_PROVIDER", ["PROVIDER_KEY", "PROVIDER_ID", "PROVIDER_NAME", "SPECIALTY", "DEPARTMENT", "FTE_STATUS", "YEARS_ON_STAFF", "CREDENTIALING_STATUS", "AVERAGE_LOS_DAYS", "AVERAGE_CMI"], dims[1])
        insert_many(cur, "DIM_FACILITY", ["FACILITY_KEY", "FACILITY_ID", "FACILITY_NAME", "FACILITY_TYPE", "LICENSED_BEDS", "STAFFED_BEDS", "FLOOR", "BUILDING"], dims[2])
        insert_many(cur, "DIM_DRG", ["DRG_KEY", "DRG_CODE", "DRG_DESCRIPTION", "MDC", "DRG_WEIGHT", "GEOMETRIC_MEAN_LOS", "ARITHMETIC_MEAN_LOS", "DRG_TYPE", "IS_HRRP_CONDITION"], dims[3])
        insert_many(cur, "DIM_PAYER", ["PAYER_KEY", "PAYER_ID", "PAYER_NAME", "PAYER_TYPE", "CONTRACT_ON_FILE", "CONTRACT_EFFECTIVE_DATE", "CONTRACT_EXPIRY_DATE"], dims[4])
        insert_many(cur, "DIM_DIAGNOSIS", ["DIAGNOSIS_KEY", "ICD10_CODE", "DIAGNOSIS_DESCRIPTION", "CONDITION_GROUP", "IS_HAC", "IS_CHRONIC", "HRRP_CONDITION"], dims[5])

        facts = generate_facts(dims[3], dims[4], dims[2])
        insert_many(cur, "FACT_ENCOUNTER", ["ENCOUNTER_KEY", "ENCOUNTER_ID", "PATIENT_KEY", "PROVIDER_KEY", "FACILITY_KEY", "DRG_KEY", "PAYER_KEY", "ADMIT_DATE", "DISCHARGE_DATE", "LOS_DAYS", "ADMIT_SOURCE", "DISCHARGE_DISPOSITION", "PRINCIPAL_DIAGNOSIS_KEY", "SECONDARY_DIAGNOSIS_1_KEY", "SECONDARY_DIAGNOSIS_2_KEY", "PROCEDURE_CODE", "IS_READMISSION_30", "IS_READMISSION_90", "INDEX_ENCOUNTER_KEY", "HAI_FLAG", "CASE_MIX_INDEX", "SEVERITY_OF_ILLNESS", "RISK_OF_MORTALITY"], facts[0])
        insert_many(cur, "FACT_FINANCIALS", ["FINANCIAL_KEY", "ENCOUNTER_KEY", "PAYER_KEY", "DRG_KEY", "TOTAL_CHARGES", "TOTAL_COST", "CONTRACTUAL_ADJUSTMENT", "NET_REVENUE", "AMOUNT_COLLECTED", "UNDERPAYMENT_AMOUNT", "DENIAL_FLAG", "DENIAL_REASON", "DENIAL_DATE", "APPEAL_STATUS", "GROSS_MARGIN", "MARGIN_PCT", "OUTLIER_PAYMENT", "FISCAL_YEAR", "FISCAL_QUARTER"], facts[1])
        insert_many(cur, "FACT_READMISSION", ["READMISSION_KEY", "READMISSION_ENCOUNTER_KEY", "INDEX_ENCOUNTER_KEY", "PATIENT_KEY", "DAYS_TO_READMISSION", "READMISSION_WINDOW", "INDEX_DRG_KEY", "READMISSION_DRG_KEY", "SAME_CONDITION", "HRRP_APPLICABLE", "DISCHARGE_DISPOSITION_INDEX", "CONTRIBUTING_FACTOR", "PREVENTED_FLAG"], facts[2])
        insert_many(cur, "FACT_QUALITY_METRICS", ["QUALITY_KEY", "ENCOUNTER_KEY", "FACILITY_KEY", "METRIC_DATE", "METRIC_TYPE", "METRIC_VALUE", "BENCHMARK_VALUE", "PERFORMANCE_BAND", "CMS_STAR_CONTRIBUTION", "REPORTABLE_EVENT"], facts[3])
        insert_many(cur, "FACT_STAFFING", ["STAFFING_KEY", "FACILITY_KEY", "STAFFING_DATE", "SHIFT", "ROLE", "SCHEDULED_HOURS", "ACTUAL_HOURS", "OVERTIME_HOURS", "AGENCY_HOURS", "CENSUS", "NURSE_TO_PATIENT_RATIO", "TARGET_RATIO", "RATIO_COMPLIANCE", "VACANCY_RATE"], facts[4])
        insert_many(cur, "FACT_CAPACITY", ["CAPACITY_KEY", "FACILITY_KEY", "SNAPSHOT_DATE", "CENSUS", "LICENSED_BED_OCCUPANCY", "STAFFED_BED_OCCUPANCY", "ED_BOARDING_HOURS", "AVG_DISCHARGE_HOUR", "DISCHARGES", "ADMISSIONS", "TRANSFERS_IN", "TRANSFERS_OUT", "DIVERSION_HOURS", "ALOS_ROLLING_30"], facts[5])
        insert_many(cur, "ANOMALY_ALERTS", ["ALERT_KEY", "ALERT_DATE", "ALERT_TYPE", "SEVERITY", "FACILITY_KEY", "PROVIDER_KEY", "PAYER_KEY", "ALERT_DESCRIPTION", "RECOMMENDED_ACTION", "IS_RESOLVED", "RESOLUTION_DATE", "DOLLAR_IMPACT"], facts[6])
        conn.commit()
        print("Healthcare synthetic dataset loaded to HEALTHCARE_POC.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
