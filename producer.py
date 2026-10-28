#!/usr/bin/env python3
"""
Live data source for the telecom churn project.
1. Loads the 3,333 Kaggle customers into MongoDB (customers collection).
2. Streams their daily activity into the events collection, one simulated day per batch:
     call      : duration_min, roaming (0/1)
     data      : data_gb
     recharge  : amount, recharge_kind (plan / topup)
     complaint : complaint_type
Each customer's events follow his real usage in the dataset
(DayCalls, DayMins, RoamMins, DataUsage, MonthlyCharge, OverageFee, CustServCalls).
The Churn column is NEVER used to generate events, so churn patterns found later are real.
Usage: python3 producer.py telecom_dataset/telecom_churn.csv 30 10
       (30 simulated days, one day every 10 seconds). Ctrl+C to stop, rerun to resume.
"""
import csv
import math
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from pymongo import MongoClient

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "telecom_dataset/telecom_churn.csv"
DAYS = int(sys.argv[2]) if len(sys.argv) > 2 else 30
INTERVAL = float(sys.argv[3]) if len(sys.argv) > 3 else 10
COMPLAINT_TYPES = ["Network coverage", "Call drop", "Billing", "Data speed", "Recharge issue"]

db = MongoClient("mongodb://localhost:27017")["telecom"]

with open(CSV_PATH, newline="") as f:
    rows = list(csv.DictReader(f))

if db.customers.count_documents({}) == 0:
    db.customers.insert_many([{
        "customerId": f"C{i:04d}",
        "account_weeks": int(r["AccountWeeks"]),
        "contract_renewal": int(r["ContractRenewal"]),
        "data_plan": int(r["DataPlan"]),
        "churn": int(r["Churn"]),
        "status": "Churned" if r["Churn"] == "1" else "Active",
    } for i, r in enumerate(rows, 1)])
    db.customers.create_index("customerId", unique=True)
    print(f"Loaded {len(rows)} customers into MongoDB")
db.events.create_index("ts")


def poisson(rng, lam):
    """Random count with average lam (Knuth's method, no numpy needed)."""
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def day_events(rng, cid, r, day, now):
    def ev(event_type, **fields):
        return {"customerId": cid, "event_type": event_type, "sim_day": day, "ts": now, **fields}

    out = []
    calls, mins = float(r["DayCalls"]), float(r["DayMins"])
    if calls > 0 and mins > 0:                                  # normal calls
        avg = mins / calls
        for _ in range(poisson(rng, calls / 30)):
            out.append(ev("call", duration_min=round(avg * rng.uniform(0.3, 1.7), 2), roaming=0))
    roam = float(r["RoamMins"])
    if roam > 0 and rng.random() < 0.5:                         # roaming calls
        out.append(ev("call", duration_min=round(roam / 15 * rng.uniform(0.5, 1.5), 2), roaming=1))
    data = float(r["DataUsage"])
    if data > 0:                                                # data sessions
        for _ in range(poisson(rng, 2)):
            out.append(ev("data", data_gb=round(data / 60 * rng.uniform(0.5, 1.5), 3)))
    if rng.random() < 0.1:                                      # plan recharge (~3 per month)
        out.append(ev("recharge", amount=round(float(r["MonthlyCharge"]) / 3 * rng.uniform(0.8, 1.2), 2),
                      recharge_kind="plan"))
    over = float(r["OverageFee"])
    if over > 0 and rng.random() < 0.05:                        # top-up recharge for overage
        out.append(ev("recharge", amount=round(over / 1.5 * rng.uniform(0.8, 1.2), 2),
                      recharge_kind="topup"))
    if rng.random() < int(r["CustServCalls"]) / 30:             # complaint
        out.append(ev("complaint", complaint_type=rng.choice(COMPLAINT_TYPES)))
    return out


day = (db.meta.find_one({"_id": "producer"}) or {"day": 0})["day"]
print(f"{len(rows)} customers. Simulating {DAYS} days, one day every {INTERVAL}s. Starting after day {day} ...")

try:
    while day < DAYS:
        day += 1
        rng = random.Random(day)
        now = datetime.now(timezone.utc)
        events = []
        for i, r in enumerate(rows, 1):
            events += day_events(rng, f"C{i:04d}", r, day, now)
        db.events.insert_many(events)
        db.meta.update_one({"_id": "producer"}, {"$set": {"day": day}}, upsert=True)
        counts = dict(Counter(e["event_type"] for e in events))
        print(f"[{datetime.now():%H:%M:%S}] Day {day}/{DAYS}: +{len(events)} events {counts}"
              f" | total: {db.events.estimated_document_count()}")
        time.sleep(INTERVAL)
    print("Simulation finished.")
except KeyboardInterrupt:
    print(f"\nStopped after day {day}. Run again to continue.")
