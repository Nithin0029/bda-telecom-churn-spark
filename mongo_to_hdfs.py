#!/usr/bin/env python3
"""
Incremental ingestion: MongoDB -> HDFS (telecom project).
Customers are copied once; each run copies only the events that arrived since the last run.
HDFS: /bda/telecom/customers/customers.csv
      /bda/telecom/events/events_<time>.csv
"""
import csv
import os
import subprocess
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

IST = timezone(timedelta(hours=5, minutes=30))
HDFS_CUSTOMERS = "/bda/telecom/customers"
HDFS_EVENTS = "/bda/telecom/events"
STATE = "last_export.txt"
OUT = "exports"
CUSTOMER_COLS = ["customerId", "account_weeks", "contract_renewal", "data_plan", "churn", "status"]
EVENT_COLS = ["customerId", "event_type", "sim_day", "ts", "duration_min", "roaming",
              "data_gb", "amount", "recharge_kind", "complaint_type"]


def hdfs(*args):
    subprocess.run(["hdfs", "dfs", *args], check=True)


db = MongoClient("mongodb://localhost:27017")["telecom"]
os.makedirs(OUT, exist_ok=True)

# Customers - once
cust_file = f"{OUT}/customers.csv"
if not os.path.exists(cust_file):
    with open(cust_file, "w", newline="") as f:
        w = csv.writer(f)
        for c in db.customers.find({}, {"_id": 0}).sort("customerId"):
            w.writerow([c[k] for k in CUSTOMER_COLS])
    hdfs("-mkdir", "-p", HDFS_CUSTOMERS)
    hdfs("-put", "-f", cust_file, HDFS_CUSTOMERS + "/")
    print(f"Customers copied to HDFS {HDFS_CUSTOMERS}")

# New events since the last run
if os.path.exists(STATE):
    with open(STATE) as f:
        last = datetime.fromisoformat(f.read().strip())
else:
    last = datetime(1970, 1, 1, tzinfo=timezone.utc)
cutoff = datetime.now(timezone.utc) - timedelta(seconds=5)   # skip a batch still being written

stamp = cutoff.astimezone(IST).strftime("%Y%m%d_%H%M%S")
path = f"{OUT}/events_{stamp}.csv"
n = 0
with open(path, "w", newline="") as f:
    w = csv.writer(f)
    for e in db.events.find({"ts": {"$gt": last, "$lte": cutoff}}, {"_id": 0}):
        e["ts"] = e["ts"].replace(tzinfo=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
        w.writerow([e.get(k, "") for k in EVENT_COLS])
        n += 1

if n == 0:
    os.remove(path)
    print("No new events since last export.")
else:
    hdfs("-mkdir", "-p", HDFS_EVENTS)
    hdfs("-put", "-f", path, HDFS_EVENTS + "/")
    print(f"Exported {n} new events -> HDFS {HDFS_EVENTS}/{os.path.basename(path)}")

with open(STATE, "w") as f:
    f.write(cutoff.isoformat())
